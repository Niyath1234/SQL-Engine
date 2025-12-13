/// Fused Scan Operator: Scan + Filter + Projection in one pass
/// Phase 1: Kernel Fusion for hot-path optimization
/// 
/// This operator combines three operations into a single vectorized pass:
/// 1. Scan: Read data from storage
/// 2. Filter: Apply predicates (uses SIMD where possible)
/// 3. Project: Select/compute output columns
/// 
/// Benefits:
/// - Single pass over data (better cache locality)
/// - Uses selection vectors instead of materializing intermediate results
/// - SIMD-accelerated filters
/// - Reduces memory allocations

use crate::execution::batch::{ExecutionBatch, BatchIterator};
use crate::storage::columnar::ColumnarBatch;
use crate::execution::exec_node::ExecNode;
use crate::execution::simd_avx2;
use crate::query::expression::Expression;
// Removed: use crate::storage::Predicate; // Using tuple instead
use crate::query::plan::PredicateOperator;
use crate::storage::fragment::{ColumnFragment, Value};
use crate::hypergraph::node::NodeId;
use crate::hypergraph::graph::HyperGraph;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use bitvec::prelude::*;
use anyhow::Result;

/// Fused scan operator that combines scan + filter + projection
pub struct FusedScanOperator {
    /// Table name
    table: String,
    
    /// Node ID in hypergraph
    node_id: NodeId,
    
    /// Hypergraph reference
    graph: Arc<HyperGraph>,
    
    /// Columns to scan (input columns)
    scan_columns: Vec<String>,
    
    /// Filter predicates (applied during scan)
    /// Format: (column_name, operator, value)
    predicates: Vec<(String, PredicateOperator, Value)>,
    
    /// Output projection expressions
    projections: Vec<ProjectionExpr>,
    
    /// Current fragment index
    current_fragment: usize,
    
    /// Total fragments
    total_fragments: usize,
    
    /// Batch size for processing
    batch_size: usize,
    
    /// Whether this operator has been prepared
    prepared: bool,
    
    /// Cached schema
    cached_schema: Option<SchemaRef>,
}

/// Projection expression (column or computed expression)
#[derive(Clone, Debug)]
pub struct ProjectionExpr {
    /// Output column name
    pub name: String,
    
    /// Expression to compute
    pub expr: ProjectionExprType,
}

#[derive(Clone, Debug)]
pub enum ProjectionExprType {
    /// Direct column reference
    Column(String),
    
    /// Arithmetic expression (for simple cases)
    Add(String, String),  // col1 + col2
    Mul(String, String),  // col1 * col2
    
    /// Constant value
    Constant(Value),
}

impl FusedScanOperator {
    /// Create a new fused scan operator
    pub fn new(
        table: String,
        node_id: NodeId,
        graph: Arc<HyperGraph>,
        scan_columns: Vec<String>,
        predicates: Vec<(String, PredicateOperator, Value)>,
        projections: Vec<ProjectionExpr>,
    ) -> Self {
        Self {
            table,
            node_id,
            graph,
            scan_columns,
            predicates,
            projections,
            current_fragment: 0,
            total_fragments: 0,
            batch_size: 8192,
            prepared: false,
            cached_schema: None,
        }
    }
    
    /// Process one batch with fused scan+filter+project
    fn process_batch(&self, fragments: &[Arc<ColumnFragment>]) -> Result<ExecutionBatch> {
        let num_rows = fragments[0].get_array()
            .map(|a| a.len())
            .unwrap_or(0);
        
        // Step 1: Apply filters and build selection vector (SIMD accelerated)
        let mut selection = self.apply_filters_simd(fragments, num_rows)?;
        let selected_count = selection.count_ones();
        
        // Early exit if no rows pass filters
        if selected_count == 0 {
            return Ok(self.create_empty_batch());
        }
        
        // Step 2: Project output columns (only for selected rows)
        let output_columns = self.project_columns_fused(fragments, &selection)?;
        
        // Step 3: Build output batch
        let schema = self.build_output_schema()?;
        let batch = ColumnarBatch {
            schema: schema.clone(),
            columns: output_columns,
            row_count: selected_count,
        };
        
        Ok(ExecutionBatch {
            batch,
            selection: bitvec![1; selected_count],
            row_count: selected_count,
            column_fragments: std::collections::HashMap::new(),
            column_schema: None,
        })
    }
    
    /// Apply filters using SIMD kernels and build selection vector
    fn apply_filters_simd(&self, fragments: &[Arc<ColumnFragment>], num_rows: usize) -> Result<BitVec> {
        if self.predicates.is_empty() {
            // No filters: all rows selected
            return Ok(bitvec![1; num_rows]);
        }
        
        // Start with all rows selected
        let mut selection = bitvec![1; num_rows];
        let mut temp_selection = vec![0u8; num_rows];
        
        // Apply each predicate and AND with selection
        for (column, operator, value) in &self.predicates {
            // Find column fragment for this predicate
            let col_idx = self.scan_columns.iter()
                .position(|c| c == column)
                .ok_or_else(|| anyhow::anyhow!("Column {} not found", column))?;
            
            let fragment = &fragments[col_idx];
            
            // Get array and convert to values for SIMD filter
            let values = if let Some(array) = fragment.get_array() {
                (0..array.len())
                    .filter_map(|i| crate::execution::operators::extract_value(&array, i).ok())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            
            // Apply SIMD filter based on type
            self.apply_simd_filter(&values, value, operator, &mut temp_selection)?;
            
            // AND with existing selection
            for i in 0..num_rows {
                if temp_selection[i] == 0 {
                    selection.set(i, false);
                }
            }
        }
        
        Ok(selection)
    }
    
    /// Apply SIMD filter for a single predicate
    fn apply_simd_filter(
        &self,
        values: &[Value],
        target: &Value,
        operator: &PredicateOperator,
        selection: &mut [u8],
    ) -> Result<()> {
        
        // Try to extract typed slices for SIMD
        match (values.first(), target) {
            (Some(Value::Int64(_)), Value::Int64(target_val)) => {
                // Extract Int64 slice
                let int_slice: Vec<i64> = values.iter()
                    .map(|v| match v {
                        Value::Int64(x) => *x,
                        _ => 0,
                    })
                    .collect();
                
                // Use SIMD filters
                match operator {
                    PredicateOperator::Equals => {
                        simd_avx2::filter_eq_i64(&int_slice, *target_val, selection);
                    }
                    PredicateOperator::GreaterThan => {
                        simd_avx2::filter_gt_i64(&int_slice, *target_val, selection);
                    }
                    PredicateOperator::LessThan => {
                        simd_avx2::filter_lt_i64(&int_slice, *target_val, selection);
                    }
                    PredicateOperator::GreaterThanOrEqual => {
                        simd_avx2::filter_gt_i64(&int_slice, *target_val - 1, selection);
                    }
                    PredicateOperator::LessThanOrEqual => {
                        simd_avx2::filter_lt_i64(&int_slice, *target_val + 1, selection);
                    }
                    _ => {
                        // Fallback to scalar
                        self.apply_scalar_filter(values, target, operator, selection)?;
                    }
                }
                Ok(())
            }
            (Some(Value::Float64(_)), Value::Float64(target_val)) => {
                let float_slice: Vec<f64> = values.iter()
                    .map(|v| match v {
                        Value::Float64(x) => *x,
                        _ => 0.0,
                    })
                    .collect();
                
                match operator {
                    PredicateOperator::GreaterThan => {
                        simd_avx2::filter_gt_f64(&float_slice, *target_val, selection);
                    }
                    PredicateOperator::LessThan => {
                        simd_avx2::filter_lt_f64(&float_slice, *target_val, selection);
                    }
                    _ => {
                        self.apply_scalar_filter(values, target, operator, selection)?;
                    }
                }
                Ok(())
            }
            _ => {
                // Fallback to scalar for other types
                self.apply_scalar_filter(values, target, operator, selection)
            }
        }
    }
    
    /// Scalar fallback for filters (for types without SIMD support)
    fn apply_scalar_filter(
        &self,
        values: &[Value],
        target: &Value,
        operator: &PredicateOperator,
        selection: &mut [u8],
    ) -> Result<()> {
        
        for (i, value) in values.iter().enumerate() {
            let matches = match operator {
                PredicateOperator::Equals => value == target,
                PredicateOperator::NotEquals => value != target,
                PredicateOperator::GreaterThan => value > target,
                PredicateOperator::LessThan => value < target,
                PredicateOperator::GreaterThanOrEqual => value >= target,
                PredicateOperator::LessThanOrEqual => value <= target,
                _ => false,
            };
            selection[i] = if matches { 1 } else { 0 };
        }
        Ok(())
    }
    
    /// Project output columns (only materializing selected rows)
    fn project_columns_fused(
        &self,
        fragments: &[Arc<ColumnFragment>],
        selection: &BitVec,
    ) -> Result<Vec<Arc<dyn Array>>> {
        let mut output_columns = Vec::new();
        
        for proj in &self.projections {
            let array = match &proj.expr {
                ProjectionExprType::Column(col_name) => {
                    // Direct column reference
                    let col_idx = self.scan_columns.iter()
                        .position(|c| c == col_name)
                        .ok_or_else(|| anyhow::anyhow!("Column {} not found", col_name))?;
                    
                    {
                        let fragment = &fragments[col_idx];
                        if let Some(array) = fragment.get_array() {
                            self.materialize_selected_from_array(array, selection)?
                        } else {
                            return Err(anyhow::anyhow!("Fragment array not available"));
                        }
                    }
                }
                ProjectionExprType::Add(col1, col2) => {
                    // Simple addition
                    self.compute_add(fragments, col1, col2, selection)?
                }
                ProjectionExprType::Mul(col1, col2) => {
                    // Simple multiplication
                    self.compute_mul(fragments, col1, col2, selection)?
                }
                ProjectionExprType::Constant(val) => {
                    // Constant column
                    self.materialize_constant(val, selection.count_ones())?
                }
            };
            
            output_columns.push(array);
        }
        
        Ok(output_columns)
    }
    
    /// Materialize only selected rows from an Arrow array
    fn materialize_selected_from_array(&self, array: Arc<dyn Array>, selection: &BitVec) -> Result<Arc<dyn Array>> {
        use crate::execution::operators::extract_value;
        
        let selected_values: Vec<Value> = (0..array.len())
            .filter(|i| selection.get(*i).map(|b| *b).unwrap_or(false))
            .filter_map(|i| extract_value(&array, i).ok())
            .collect();
        
        // Convert to array based on data type
        let data_type = array.data_type();
        self.values_to_array_with_type(&selected_values, data_type)
    }
    
    /// Materialize only selected rows from a column (from Value slice)
    fn materialize_selected(&self, values: &[Value], selection: &BitVec) -> Result<Arc<dyn Array>> {
        let selected_values: Vec<Value> = values.iter()
            .enumerate()
            .filter(|(i, _)| selection.get(*i).map(|b| *b).unwrap_or(false))
            .map(|(_, v)| v.clone())
            .collect();
        
        self.values_to_array(&selected_values)
    }
    
    /// Convert values to Arrow array with specific data type
    fn values_to_array_with_type(&self, values: &[Value], data_type: &DataType) -> Result<Arc<dyn Array>> {
        if values.is_empty() {
            return Ok(match data_type {
                DataType::Int64 => Arc::new(Int64Array::from(vec![] as Vec<i64>)),
                DataType::Float64 => Arc::new(Float64Array::from(vec![] as Vec<f64>)),
                DataType::Utf8 => Arc::new(StringArray::from(vec![] as Vec<String>)),
                _ => Arc::new(Int64Array::from(vec![] as Vec<i64>)),
            });
        }
        
        match data_type {
            DataType::Int64 => {
                let ints: Vec<i64> = values.iter()
                    .map(|v| match v {
                        Value::Int64(x) => *x,
                        Value::Int32(x) => *x as i64,
                        _ => 0,
                    })
                    .collect();
                Ok(Arc::new(Int64Array::from(ints)))
            }
            DataType::Float64 => {
                let floats: Vec<f64> = values.iter()
                    .map(|v| match v {
                        Value::Float64(x) => *x,
                        Value::Float32(x) => *x as f64,
                        Value::Int64(x) => *x as f64,
                        _ => 0.0,
                    })
                    .collect();
                Ok(Arc::new(Float64Array::from(floats)))
            }
            DataType::Utf8 | DataType::LargeUtf8 => {
                let strings: Vec<String> = values.iter()
                    .map(|v| match v {
                        Value::String(s) => s.clone(),
                        _ => String::new(),
                    })
                    .collect();
                Ok(Arc::new(StringArray::from(strings)))
            }
            _ => self.values_to_array(values),
        }
    }
    
    /// Convert values to Arrow array
    fn values_to_array(&self, values: &[Value]) -> Result<Arc<dyn Array>> {
        if values.is_empty() {
            return Ok(Arc::new(Int64Array::from(vec![] as Vec<i64>)));
        }
        
        match &values[0] {
            Value::Int64(_) => {
                let ints: Vec<i64> = values.iter()
                    .map(|v| match v {
                        Value::Int64(x) => *x,
                        _ => 0,
                    })
                    .collect();
                Ok(Arc::new(Int64Array::from(ints)))
            }
            Value::Float64(_) => {
                let floats: Vec<f64> = values.iter()
                    .map(|v| match v {
                        Value::Float64(x) => *x,
                        _ => 0.0,
                    })
                    .collect();
                Ok(Arc::new(Float64Array::from(floats)))
            }
            Value::String(_) => {
                let strings: Vec<&str> = values.iter()
                    .map(|v| match v {
                        Value::String(s) => s.as_str(),
                        _ => "",
                    })
                    .collect();
                Ok(Arc::new(StringArray::from(strings)))
            }
            _ => {
                anyhow::bail!("Unsupported value type")
            }
        }
    }
    
    /// Compute addition of two columns
    fn compute_add(
        &self,
        fragments: &[Arc<ColumnFragment>],
        col1: &str,
        col2: &str,
        selection: &BitVec,
    ) -> Result<Arc<dyn Array>> {
        use crate::execution::operators::extract_value;
        
        let idx1 = self.scan_columns.iter().position(|c| c == col1)
            .ok_or_else(|| anyhow::anyhow!("Column {} not found", col1))?;
        let idx2 = self.scan_columns.iter().position(|c| c == col2)
            .ok_or_else(|| anyhow::anyhow!("Column {} not found", col2))?;
        
        let array1 = fragments[idx1].get_array()
            .ok_or_else(|| anyhow::anyhow!("Column {} has no array", col1))?;
        let array2 = fragments[idx2].get_array()
            .ok_or_else(|| anyhow::anyhow!("Column {} has no array", col2))?;
        
        let len = array1.len().min(array2.len());
        let result: Vec<Value> = (0..len)
            .filter(|i| selection.get(*i).map(|b| *b).unwrap_or(false))
            .filter_map(|i| {
                let v1 = extract_value(&array1, i).ok()?;
                let v2 = extract_value(&array2, i).ok()?;
                match (v1, v2) {
                    (Value::Int64(a), Value::Int64(b)) => Some(Value::Int64(a + b)),
                    (Value::Float64(a), Value::Float64(b)) => Some(Value::Float64(a + b)),
                    _ => Some(Value::Int64(0)),
                }
            })
            .collect();
        
        self.values_to_array(&result)
    }
    
    /// Compute multiplication of two columns
    fn compute_mul(
        &self,
        fragments: &[Arc<ColumnFragment>],
        col1: &str,
        col2: &str,
        selection: &BitVec,
    ) -> Result<Arc<dyn Array>> {
        use crate::execution::operators::extract_value;
        
        let idx1 = self.scan_columns.iter().position(|c| c == col1)
            .ok_or_else(|| anyhow::anyhow!("Column {} not found", col1))?;
        let idx2 = self.scan_columns.iter().position(|c| c == col2)
            .ok_or_else(|| anyhow::anyhow!("Column {} not found", col2))?;
        
        let array1 = fragments[idx1].get_array()
            .ok_or_else(|| anyhow::anyhow!("Column {} has no array", col1))?;
        let array2 = fragments[idx2].get_array()
            .ok_or_else(|| anyhow::anyhow!("Column {} has no array", col2))?;
        
        let len = array1.len().min(array2.len());
        let result: Vec<Value> = (0..len)
            .filter(|i| selection.get(*i).map(|b| *b).unwrap_or(false))
            .filter_map(|i| {
                let v1 = extract_value(&array1, i).ok()?;
                let v2 = extract_value(&array2, i).ok()?;
                match (v1, v2) {
                    (Value::Int64(a), Value::Int64(b)) => Some(Value::Int64(a * b)),
                    (Value::Float64(a), Value::Float64(b)) => Some(Value::Float64(a * b)),
                    _ => Some(Value::Int64(0)),
                }
            })
            .collect();
        
        self.values_to_array(&result)
    }
    
    /// Materialize a constant value for selected rows
    fn materialize_constant(&self, value: &Value, count: usize) -> Result<Arc<dyn Array>> {
        match value {
            Value::Int64(x) => Ok(Arc::new(Int64Array::from(vec![*x; count]))),
            Value::Float64(x) => Ok(Arc::new(Float64Array::from(vec![*x; count]))),
            Value::String(s) => Ok(Arc::new(StringArray::from(vec![s.as_str(); count]))),
            _ => anyhow::bail!("Unsupported constant type"),
        }
    }
    
    /// Build output schema
    fn build_output_schema(&self) -> Result<SchemaRef> {
        if let Some(ref schema) = self.cached_schema {
            return Ok(schema.clone());
        }
        
        let fields: Vec<Field> = self.projections.iter()
            .map(|proj| Field::new(&proj.name, DataType::Int64, true))
            .collect();
        
        Ok(Arc::new(Schema::new(fields)))
    }
    
    /// Create empty batch
    fn create_empty_batch(&self) -> ExecutionBatch {
        let schema = self.build_output_schema().unwrap();
        let columns: Vec<Arc<dyn Array>> = self.projections.iter()
            .map(|_| Arc::new(Int64Array::from(vec![] as Vec<i64>)) as Arc<dyn Array>)
            .collect();
        
        ExecutionBatch {
            batch: ColumnarBatch {
                schema,
                columns,
                row_count: 0,
            },
            selection: bitvec![],
            row_count: 0,
            column_fragments: std::collections::HashMap::new(),
            column_schema: None,
        }
    }
}

impl ExecNode for FusedScanOperator {
    fn prepare(&mut self) -> Result<()> {
        if self.prepared {
            return Ok(());
        }
        
        // Get node from hypergraph
        let node = self.graph.get_node(self.node_id)
            .ok_or_else(|| anyhow::anyhow!("Node {} not found", self.node_id))?;
        
        self.total_fragments = node.fragments.len();
        self.cached_schema = Some(self.build_output_schema()?);
        self.prepared = true;
        
        Ok(())
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        if !self.prepared {
            anyhow::bail!("FusedScanOperator not prepared");
        }
        
        if self.current_fragment >= self.total_fragments {
            return Ok(None);
        }
        
        // Get node and fragments
        let node = self.graph.get_node(self.node_id)
            .ok_or_else(|| anyhow::anyhow!("Node {} not found", self.node_id))?;
        
        // Get table name from node
        let table_name = node.table_name.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Node {} is not a table node", self.node_id))?;
        
        // Collect fragments for requested columns
        let mut fragments = Vec::new();
        for col_name in &self.scan_columns {
            let col_node = self.graph.get_node_by_table_column(table_name, col_name)
                .ok_or_else(|| anyhow::anyhow!("Column {}.{} not found", table_name, col_name))?;
            
            if self.current_fragment < col_node.fragments.len() {
                fragments.push(Arc::new(col_node.fragments[self.current_fragment].clone()));
            } else {
                anyhow::bail!("Fragment {} not found for column {}", self.current_fragment, col_name);
            }
        }
        
        self.current_fragment += 1;
        
        // Process batch with fusion
        self.process_batch(fragments.as_slice()).map(Some)
    }
}

impl BatchIterator for FusedScanOperator {
    fn prepare(&mut self) -> Result<()> {
        <Self as ExecNode>::prepare(self)
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        <Self as ExecNode>::next(self)
    }
    
    fn schema(&self) -> SchemaRef {
        self.cached_schema.clone().unwrap_or_else(|| {
            Arc::new(Schema::new(vec![Field::new("dummy", DataType::Int64, true)]))
        })
    }
}

