/// Kernel Fusion: Combine Multiple Operations in Single Pass
/// 
/// This module provides fused kernels that combine multiple operations
/// (e.g., filter + projection) in a single loop, reducing materialization
/// overhead and improving cache locality.
use crate::execution::vectorized::VectorBatch;
use crate::execution::vectorized::kernels::{arithmetic, comparison, string_ops};
use crate::error::EngineResult;
use crate::query::plan::{FilterPredicate, ProjectionExpr, ProjectionExprType};
use crate::storage::fragment::Value;
use arrow::array::*;
use arrow::datatypes::*;
use bitvec::prelude::*;
use tracing::debug;
use regex;
use std::sync::Arc;

/// Fused Filter + Project Operator
/// 
/// Combines filtering and projection in a single pass, avoiding
/// intermediate materialization of filtered batches.
pub struct FusedFilterProject {
    /// Filter predicates
    predicates: Vec<FilterPredicate>,
    
    /// Projection expressions
    expressions: Vec<ProjectionExpr>,
}

impl FusedFilterProject {
    /// Create a new fused filter+project operator
    pub fn new(
        predicates: Vec<FilterPredicate>,
        expressions: Vec<ProjectionExpr>,
    ) -> Self {
        Self {
            predicates,
            expressions,
        }
    }
    
    /// Apply fused filter + project to a batch
    /// 
    /// This performs both filtering and projection in a single pass,
    /// only materializing rows that pass the filter.
    pub fn apply_fused(&self, batch: &VectorBatch) -> EngineResult<VectorBatch> {
        // Step 1: Apply filters to get selection bitmap
        let selection = self.apply_filters(batch)?;
        
        // Step 2: Count selected rows
        let selected_count = selection.count_ones();
        
        if selected_count == 0 {
            // No rows selected - return empty batch
            return Ok(self.create_empty_batch());
        }
        
        // Step 3: Project only selected rows
        let output_arrays = self.project_selected_rows(batch, &selection)?;
        
        // Step 4: Build output schema
        let output_schema = self.build_output_schema(batch)?;
        
        // Step 5: Create output batch
        let output_batch = VectorBatch::new(output_arrays, output_schema);
        
        debug!(
            input_rows = batch.row_count,
            filtered_rows = selected_count,
            output_columns = output_batch.schema.fields().len(),
            "Fused filter+project applied"
        );
        
        Ok(output_batch)
    }
    
    /// Apply all filter predicates
    fn apply_filters(&self, batch: &VectorBatch) -> EngineResult<BitVec> {
        // Start with all rows selected
        let mut selection = BitVec::repeat(true, batch.row_count);
        
        // Apply each predicate
        for predicate in &self.predicates {
            let predicate_selection = self.apply_single_filter(batch, predicate)?;
            
            // Combine with AND (intersection)
            // Collect values first to avoid borrow checker issues
            let combined: Vec<bool> = (0..selection.len())
                .map(|i| {
                    let current = *selection.get(i)
                        .ok_or_else(|| crate::error::EngineError::execution(
                            format!("Index {} out of bounds in selection bitmap", i)
                        ))?;
                    let predicate_val = *predicate_selection.get(i)
                        .ok_or_else(|| crate::error::EngineError::execution(
                            format!("Index {} out of bounds in predicate selection bitmap", i)
                        ))?;
                    Ok(current && predicate_val)
                })
                .collect::<EngineResult<Vec<bool>>>()?;
            
            // Apply combined values
            for (i, val) in combined.iter().enumerate() {
                selection.set(i, *val);
            }
        }
        
        Ok(selection)
    }
    
    /// Apply a single filter predicate
    fn apply_single_filter(
        &self,
        batch: &VectorBatch,
        predicate: &FilterPredicate,
    ) -> EngineResult<BitVec> {
        // Find the column array
        let col_idx = batch.schema.fields()
            .iter()
            .position(|f| f.name() == predicate.column.as_str())
            .ok_or_else(|| crate::error::EngineError::execution(format!(
                "Column '{}' not found in batch schema",
                predicate.column
            )))?;
        
        let array = batch.column(col_idx)?;
        
        // Apply predicate based on operator
        match &predicate.operator {
            crate::query::plan::PredicateOperator::Equals => {
                self.apply_equals(array.as_ref(), &predicate.value)
            }
            crate::query::plan::PredicateOperator::NotEquals => {
                let equals = self.apply_equals(array.as_ref(), &predicate.value)?;
                let mut result = BitVec::with_capacity(equals.len());
                for i in 0..equals.len() {
                    result.push(!equals[i]);
                }
                Ok(result)
            }
            crate::query::plan::PredicateOperator::GreaterThan => {
                self.apply_greater_than(array.as_ref(), &predicate.value)
            }
            crate::query::plan::PredicateOperator::LessThan => {
                self.apply_less_than(array.as_ref(), &predicate.value)
            }
            crate::query::plan::PredicateOperator::GreaterThanOrEqual => {
                let less_than = self.apply_less_than(array.as_ref(), &predicate.value)?;
                let mut result = BitVec::with_capacity(less_than.len());
                for i in 0..less_than.len() {
                    result.push(!less_than[i]);
                }
                Ok(result)
            }
            crate::query::plan::PredicateOperator::LessThanOrEqual => {
                let greater_than = self.apply_greater_than(array.as_ref(), &predicate.value)?;
                let mut result = BitVec::with_capacity(greater_than.len());
                for i in 0..greater_than.len() {
                    result.push(!greater_than[i]);
                }
                Ok(result)
            }
            crate::query::plan::PredicateOperator::Like => {
                if let Some(ref pattern) = predicate.pattern {
                    self.apply_like(array.as_ref(), pattern)
                } else {
                    Err(crate::error::EngineError::execution("LIKE predicate missing pattern"))
                }
            }
            _ => Err(crate::error::EngineError::execution(format!(
                "Unsupported predicate operator: {:?}",
                predicate.operator
            ))),
        }
    }
    
    /// Apply equals predicate
    fn apply_equals(&self, array: &dyn Array, value: &Value) -> EngineResult<BitVec> {
        match (array.data_type(), value) {
            (DataType::Int64, Value::Int64(v)) => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                Ok(comparison::equals_int64_scalar(arr, *v))
            }
            (DataType::Utf8, Value::String(v)) => {
                let arr = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                Ok(comparison::equals_string_scalar(arr, v))
            }
            _ => Err(crate::error::EngineError::execution(format!(
                "Unsupported type combination for equals: {:?} vs {:?}",
                array.data_type(),
                value
            ))),
        }
    }
    
    /// Apply greater than predicate
    fn apply_greater_than(&self, array: &dyn Array, value: &Value) -> EngineResult<BitVec> {
        match (array.data_type(), value) {
            (DataType::Int64, Value::Int64(v)) => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                let scalar_array = Int64Array::from(vec![*v; arr.len()]);
                comparison::greater_than_int64(arr, &scalar_array)
            }
            _ => Err(crate::error::EngineError::execution(format!(
                "Unsupported type combination for greater than: {:?} vs {:?}",
                array.data_type(),
                value
            ))),
        }
    }
    
    /// Apply less than predicate
    fn apply_less_than(&self, array: &dyn Array, value: &Value) -> EngineResult<BitVec> {
        match (array.data_type(), value) {
            (DataType::Int64, Value::Int64(v)) => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                let scalar_array = Int64Array::from(vec![*v; arr.len()]);
                comparison::less_than_int64(arr, &scalar_array)
            }
            _ => Err(crate::error::EngineError::execution(format!(
                "Unsupported type combination for less than: {:?} vs {:?}",
                array.data_type(),
                value
            ))),
        }
    }
    
    /// Apply LIKE predicate
    fn apply_like(&self, array: &dyn Array, pattern: &str) -> EngineResult<BitVec> {
        match array.data_type() {
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                
                // Convert SQL LIKE pattern to regex
                let regex_pattern = self.like_pattern_to_regex(pattern);
                let re = regex::Regex::new(&regex_pattern)
                    .map_err(|e| crate::error::EngineError::execution(format!(
                        "Invalid LIKE pattern: {} - {}",
                        pattern, e
                    )))?;
                
                let mut result = BitVec::with_capacity(arr.len());
                for i in 0..arr.len() {
                    let val = arr.value(i);
                    result.push(re.is_match(val));
                }
                Ok(result)
            }
            _ => Err(crate::error::EngineError::execution(format!(
                "LIKE operator only supported for string types, got: {:?}",
                array.data_type()
            ))),
        }
    }
    
    /// Convert SQL LIKE pattern to regex
    fn like_pattern_to_regex(&self, pattern: &str) -> String {
        let mut regex = String::from("^");
        for ch in pattern.chars() {
            match ch {
                '%' => regex.push_str(".*"),
                '_' => regex.push_str("."),
                c if ".*+?^${}[]|\\".contains(c) => {
                    regex.push('\\');
                    regex.push(c);
                }
                c => regex.push(c),
            }
        }
        regex.push('$');
        regex
    }
    
    /// Project only selected rows (fused with filter)
    fn project_selected_rows(
        &self,
        batch: &VectorBatch,
        selection: &BitVec,
    ) -> EngineResult<Vec<Arc<dyn Array>>> {
        let mut output_arrays = Vec::new();
        
        for expr in &self.expressions {
            let array = self.evaluate_expression_for_selected(batch, expr, selection)?;
            output_arrays.push(array);
        }
        
        Ok(output_arrays)
    }
    
    /// Evaluate expression only for selected rows
    fn evaluate_expression_for_selected(
        &self,
        batch: &VectorBatch,
        expr: &ProjectionExpr,
        selection: &BitVec,
    ) -> EngineResult<Arc<dyn Array>> {
        match &expr.expr_type {
            ProjectionExprType::Column(col_name) => {
                // Simple column reference - extract only selected rows
                let col_idx = batch.schema.fields()
                    .iter()
                    .position(|f| f.name() == col_name.as_str())
                    .ok_or_else(|| crate::error::EngineError::execution(format!(
                        "Column '{}' not found",
                        col_name
                    )))?;
                let source_array = batch.column(col_idx)?;
                self.extract_selected_rows(source_array.as_ref(), selection)
            }
            ProjectionExprType::Cast { column, target_type } => {
                // CAST expression - extract, then cast
                let col_idx = batch.schema.fields()
                    .iter()
                    .position(|f| f.name() == column.as_str())
                    .ok_or_else(|| crate::error::EngineError::execution(format!(
                        "Column '{}' not found",
                        column
                    )))?;
                let source_array = batch.column(col_idx)?;
                let selected_array = self.extract_selected_rows(source_array.as_ref(), selection)?;
                cast_array(&selected_array, target_type)
            }
            _ => Err(crate::error::EngineError::execution(
                "Complex expressions not yet supported in fused operator"
            )),
        }
    }
    
    /// Extract only selected rows from an array
    fn extract_selected_rows(
        &self,
        array: &dyn Array,
        selection: &BitVec,
    ) -> EngineResult<Arc<dyn Array>> {
        // Count selected rows
        let selected_count = selection.count_ones();
        
        match array.data_type() {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                let mut builder = Int64Builder::with_capacity(selected_count);
                for i in 0..arr.len() {
                    if selection[i] {
                        builder.append_value(arr.value(i));
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                let mut builder = StringBuilder::with_capacity(selected_count, selected_count * 20);
                for i in 0..arr.len() {
                    if selection[i] {
                        builder.append_value(arr.value(i));
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            _ => Err(crate::error::EngineError::execution(format!(
                "Unsupported data type for extraction: {:?}",
                array.data_type()
            ))),
        }
    }
    
    /// Build output schema from projection expressions
    fn build_output_schema(&self, _batch: &VectorBatch) -> EngineResult<SchemaRef> {
        let mut fields = Vec::new();
        for expr in &self.expressions {
            let field_name = &expr.alias;
            // Infer type from expression (simplified)
            let data_type = match &expr.expr_type {
                ProjectionExprType::Column(_) => DataType::Int64, // Placeholder
                ProjectionExprType::Cast { target_type, .. } => target_type.clone(),
                _ => DataType::Int64,
            };
            fields.push(Field::new(field_name, data_type, true));
        }
        Ok(Arc::new(Schema::new(fields)))
    }
    
    /// Create an empty batch with the correct schema
    fn create_empty_batch(&self) -> VectorBatch {
        let fields: Vec<Field> = self.expressions.iter()
            .map(|expr| {
                let data_type = match &expr.expr_type {
                    ProjectionExprType::Column(_) => DataType::Int64,
                    ProjectionExprType::Cast { target_type, .. } => target_type.clone(),
                    _ => DataType::Int64,
                };
                Field::new(&expr.alias, data_type, true)
            })
            .collect();
        let schema = Arc::new(Schema::new(fields));
        VectorBatch::new(Vec::new(), schema)
    }
}

/// Cast array to target type
fn cast_array(
    array: &Arc<dyn Array>,
    target_type: &DataType,
) -> EngineResult<Arc<dyn Array>> {
    match (array.data_type(), target_type) {
        (DataType::Int64, DataType::Float64) => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
            Ok(Arc::new(crate::execution::vectorized::kernels::cast::int64_to_float64(arr)))
        }
        (DataType::Float64, DataType::Int64) => {
            let arr = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Float64Array"))?;
            Ok(Arc::new(crate::execution::vectorized::kernels::cast::float64_to_int64(arr)))
        }
        (DataType::Int64, DataType::Utf8) => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
            Ok(Arc::new(crate::execution::vectorized::kernels::cast::int64_to_string(arr)))
        }
        _ => Err(crate::error::EngineError::execution(format!(
            "Unsupported cast: {:?} -> {:?}",
            array.data_type(),
            target_type
        ))),
    }
}

