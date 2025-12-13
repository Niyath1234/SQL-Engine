/// VectorAggregateOperator: Async, vectorized aggregation operator
/// 
/// This is the Phase 1 replacement for AggregateOperator, designed for:
/// - Vectorized group-by operations
/// - Hash-based aggregation
/// - Support for COUNT, SUM, AVG, MIN, MAX
/// - Streaming aggregation (process batches incrementally)
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::error::EngineResult;
use crate::query::plan::AggregateExpr;
use crate::storage::fragment::Value;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use tracing::{debug, info};
use fxhash::FxHashMap;

/// Aggregation state for a group
#[derive(Clone, Debug)]
pub(crate) struct AggregationState {
    /// Group key values
    pub(crate) group_key: Vec<Value>,
    
    /// Aggregation results (one per aggregate expression)
    pub(crate) aggregates: Vec<AggregateValue>,
}

/// Aggregate value (intermediate state)
#[derive(Clone, Debug)]
pub(crate) enum AggregateValue {
    Count(usize),
    Sum(Value),
    Min(Value),
    Max(Value),
    Avg { sum: Value, count: usize },
}

impl AggregateValue {
    pub(crate) fn new(agg_type: crate::query::plan::AggregateFunction) -> Self {
        match agg_type {
            crate::query::plan::AggregateFunction::Count => AggregateValue::Count(0),
            crate::query::plan::AggregateFunction::CountDistinct => AggregateValue::Count(0), // TODO: Implement distinct
            crate::query::plan::AggregateFunction::Sum => AggregateValue::Sum(Value::Int64(0)),
            crate::query::plan::AggregateFunction::Min => AggregateValue::Min(Value::Int64(i64::MAX)),
            crate::query::plan::AggregateFunction::Max => AggregateValue::Max(Value::Int64(i64::MIN)),
            crate::query::plan::AggregateFunction::Avg => AggregateValue::Avg { sum: Value::Int64(0), count: 0 },
        }
    }
    
    pub(crate) fn update(&mut self, value: &Value) {
        match (self, value) {
            (AggregateValue::Count(ref mut count), _) => {
                *count += 1;
            }
            (AggregateValue::Sum(ref mut sum), v) => {
                *sum = Self::add_values_static(sum, v);
            }
            (AggregateValue::Min(ref mut min), v) => {
                let cmp_result = Self::compare_static(v, min);
                if cmp_result < 0 {
                    *min = v.clone();
                }
            }
            (AggregateValue::Max(ref mut max), v) => {
                let cmp_result = Self::compare_static(v, max);
                if cmp_result > 0 {
                    *max = v.clone();
                }
            }
            (AggregateValue::Avg { ref mut sum, ref mut count }, v) => {
                *sum = Self::add_values_static(sum, v);
                *count += 1;
            }
        }
    }
    
    fn finalize(&self) -> Value {
        match self {
            AggregateValue::Count(count) => Value::Int64(*count as i64),
            AggregateValue::Sum(sum) => sum.clone(),
            AggregateValue::Min(min) => min.clone(),
            AggregateValue::Max(max) => max.clone(),
            AggregateValue::Avg { sum, count } => {
                if *count > 0 {
                    match sum {
                        Value::Int64(s) => Value::Float64(*s as f64 / *count as f64),
                        Value::Float64(s) => Value::Float64(s / *count as f64),
                        _ => Value::Float64(0.0),
                    }
                } else {
                    Value::Float64(0.0)
                }
            }
        }
    }
    
    fn add_values_static(a: &Value, b: &Value) -> Value {
        match (a, b) {
            (Value::Int64(a), Value::Int64(b)) => Value::Int64(a + b),
            (Value::Float64(a), Value::Float64(b)) => Value::Float64(a + b),
            (Value::Int64(a), Value::Float64(b)) => Value::Float64(*a as f64 + b),
            (Value::Float64(a), Value::Int64(b)) => Value::Float64(a + *b as f64),
            _ => a.clone(),
        }
    }
    
    fn compare_static(a: &Value, b: &Value) -> i32 {
        match (a, b) {
            (Value::Int64(a), Value::Int64(b)) => {
                match a.cmp(b) {
                    std::cmp::Ordering::Less => -1,
                    std::cmp::Ordering::Equal => 0,
                    std::cmp::Ordering::Greater => 1,
                }
            }
            (Value::Float64(a), Value::Float64(b)) => {
                a.partial_cmp(b).map(|o| match o {
                    std::cmp::Ordering::Less => -1,
                    std::cmp::Ordering::Equal => 0,
                    std::cmp::Ordering::Greater => 1,
                }).unwrap_or(0)
            }
            _ => 0,
        }
    }
}

/// Vector aggregate operator - async, vectorized aggregation
pub struct VectorAggregateOperator {
    /// Input operator
    pub(crate) input: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
    
    /// Group by columns
    pub(crate) group_by: Vec<String>,
    
    /// Aggregate expressions
    pub(crate) aggregates: Vec<AggregateExpr>,
    
    /// Aggregation state: group_key -> AggregationState
    pub(crate) aggregation_state: FxHashMap<Vec<Value>, AggregationState>,
    
    /// Output batches (materialized after all input processed)
    pub(crate) output_batches: Vec<VectorBatch>,
    
    /// Current output batch index
    current_output_idx: usize,
    
    /// Input exhausted flag
    input_exhausted: bool,
    
    /// Prepared flag
    prepared: bool,
}

impl VectorAggregateOperator {
    /// Create a new vector aggregate operator
    pub fn new(
        input: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
    ) -> Self {
        Self {
            input,
            group_by,
            aggregates,
            aggregation_state: FxHashMap::default(),
            output_batches: Vec::new(),
            current_output_idx: 0,
            input_exhausted: false,
            prepared: false,
        }
    }
    
    /// Process a batch and update aggregation state
    async fn process_batch(&mut self, batch: &VectorBatch) -> EngineResult<()> {
        // Extract group key columns
        let group_key_indices: Vec<usize> = self.group_by.iter()
            .map(|col| {
                batch.schema.fields()
                    .iter()
                    .position(|f| f.name() == col.as_str())
                    .ok_or_else(|| crate::error::EngineError::execution(format!(
                        "Group by column '{}' not found in batch schema",
                        col
                    )))
            })
            .collect::<EngineResult<Vec<_>>>()?;
        
        // Extract group key values for each row
        let group_keys: Vec<Vec<Value>> = (0..batch.row_count)
            .map(|row_idx| {
                group_key_indices.iter()
                    .map(|&col_idx| {
                        let array = batch.column(col_idx)?;
                        Self::extract_value_static(array.as_ref(), row_idx)
                    })
                    .collect::<EngineResult<Vec<_>>>()
            })
            .collect::<EngineResult<Vec<_>>>()?;
        
        // Extract aggregate column values
        let aggregate_indices: Vec<usize> = self.aggregates.iter()
            .map(|agg| {
                if agg.column == "*" {
                    // COUNT(*) - use first column
                    Ok(0)
                } else {
                    batch.schema.fields()
                        .iter()
                        .position(|f| f.name() == agg.column.as_str())
                        .ok_or_else(|| crate::error::EngineError::execution(format!(
                            "Aggregate column '{}' not found in batch schema",
                            agg.column
                        )))
                }
            })
            .collect::<EngineResult<Vec<_>>>()?;
        
        // Update aggregation state for each row
        for (row_idx, group_key) in group_keys.iter().enumerate() {
            // Get or create aggregation state for this group
            let state = self.aggregation_state
                .entry(group_key.clone())
                .or_insert_with(|| {
                    let aggregates = self.aggregates.iter()
                        .map(|agg| AggregateValue::new(agg.function.clone()))
                        .collect();
                    AggregationState {
                        group_key: group_key.clone(),
                        aggregates,
                    }
                });
            
            // Update aggregates
            // Extract all values first (before mutable borrow)
            let mut agg_values = Vec::new();
            for (agg_idx, agg_expr) in self.aggregates.iter().enumerate() {
                if agg_expr.column == "*" {
                    agg_values.push(Value::Int64(1));
                } else {
                    let col_idx = aggregate_indices[agg_idx];
                    let array = batch.column(col_idx)?;
                    let value = Self::extract_value_static(array.as_ref(), row_idx)?;
                    agg_values.push(value);
                }
            }
            
            // Now update state (mutable borrow)
            for (agg_idx, value) in agg_values.iter().enumerate() {
                state.aggregates[agg_idx].update(value);
            }
        }
        
        Ok(())
    }
    
    /// Extract value from array at row index (static method)
    pub(crate) fn extract_value_static(array: &dyn Array, row_idx: usize) -> EngineResult<Value> {
        match array.data_type() {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                Ok(Value::Int64(arr.value(row_idx)))
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Float64Array"))?;
                Ok(Value::Float64(arr.value(row_idx)))
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                Ok(Value::String(arr.value(row_idx).to_string()))
            }
            _ => {
                Err(crate::error::EngineError::execution(format!(
                    "Unsupported type for aggregation: {:?}",
                    array.data_type()
                )))
            }
        }
    }
    
    /// Materialize output batches from aggregation state
    fn materialize_output(&mut self) -> EngineResult<()> {
        if !self.output_batches.is_empty() {
            return Ok(()); // Already materialized
        }
        
        info!(
            group_count = self.aggregation_state.len(),
            "Materializing aggregate output"
        );
        
        // Build output schema
        let mut output_fields = Vec::new();
        
        // Group by columns
        for col in &self.group_by {
            output_fields.push(Field::new(col, DataType::Utf8, true)); // Placeholder type
        }
        
        // Aggregate columns
        for agg in &self.aggregates {
            let field_name = agg.alias.as_ref().map(|s| s.as_str()).unwrap_or(&agg.column);
            output_fields.push(Field::new(field_name, DataType::Float64, true)); // Placeholder type
        }
        
        let output_schema = Arc::new(Schema::new(output_fields));
        
        // Materialize arrays
        let mut group_key_arrays: Vec<Vec<Arc<dyn Array>>> = vec![Vec::new(); self.group_by.len()];
        let mut aggregate_arrays: Vec<Vec<Arc<dyn Array>>> = vec![Vec::new(); self.aggregates.len()];
        
        for (group_key, state) in self.aggregation_state.iter() {
            // Materialize group keys
            for (key_idx, key_value) in group_key.iter().enumerate() {
                let array = self.value_to_array(key_value)?;
                group_key_arrays[key_idx].push(array);
            }
            
            // Materialize aggregates
            for (agg_idx, agg_value) in state.aggregates.iter().enumerate() {
                let final_value = agg_value.finalize();
                let array = self.value_to_array(&final_value)?;
                aggregate_arrays[agg_idx].push(array);
            }
        }
        
        // Concatenate arrays (simplified - assumes single batch)
        // TODO: Handle multiple batches properly
        let mut output_arrays = Vec::new();
        
        // Concatenate group key arrays
        for key_arrays in group_key_arrays {
            if !key_arrays.is_empty() {
                output_arrays.push(key_arrays[0].clone());
            }
        }
        
        // Concatenate aggregate arrays
        for agg_arrays in aggregate_arrays {
            if !agg_arrays.is_empty() {
                output_arrays.push(agg_arrays[0].clone());
            }
        }
        
        let output_batch = VectorBatch::new(output_arrays, output_schema);
        self.output_batches.push(output_batch);
        
        Ok(())
    }
    
    /// Convert Value to Arrow array
    fn value_to_array(&self, value: &Value) -> EngineResult<Arc<dyn Array>> {
        match value {
            Value::Int64(v) => {
                let mut builder = Int64Builder::with_capacity(1);
                builder.append_value(*v);
                Ok(Arc::new(builder.finish()))
            }
            Value::Float64(v) => {
                let mut builder = Float64Builder::with_capacity(1);
                builder.append_value(*v);
                Ok(Arc::new(builder.finish()))
            }
            Value::String(v) => {
                let mut builder = StringBuilder::with_capacity(1, v.len());
                builder.append_value(v);
                Ok(Arc::new(builder.finish()))
            }
            _ => {
                Err(crate::error::EngineError::execution(format!(
                    "Unsupported value type for array conversion: {:?}",
                    value
                )))
            }
        }
    }
}

#[async_trait::async_trait]
impl VectorOperator for VectorAggregateOperator {
    fn schema(&self) -> SchemaRef {
        // Build output schema: group_by columns + aggregate columns
        // For now, return placeholder - will be set during materialization
        Arc::new(Schema::new(Vec::<Field>::new()))
    }
    
    async fn prepare(&mut self) -> EngineResult<()> {
        if self.prepared {
            return Ok(());
        }
        
        info!(
            group_by_count = self.group_by.len(),
            aggregate_count = self.aggregates.len(),
            "Preparing vector aggregate operator"
        );
        
        // Recursively prepare input
        {
            let mut input = self.input.lock().await;
            input.prepare().await?;
        }
        
        self.prepared = true;
        debug!("Vector aggregate operator prepared");
        Ok(())
    }
    
    async fn next_batch(&mut self) -> EngineResult<Option<VectorBatch>> {
        if !self.prepared {
            return Err(crate::error::EngineError::execution(
                "VectorAggregateOperator::next_batch() called before prepare()"
            ));
        }
        
        // Process all input batches first
        if !self.input_exhausted {
            loop {
                let input_batch = {
                    let mut input = self.input.lock().await;
                    input.next_batch().await?
                };
                
                let Some(batch) = input_batch else {
                    self.input_exhausted = true;
                    break;
                };
                
                // Process batch and update aggregation state
                self.process_batch(&batch).await?;
            }
            
            // Materialize output after all input processed
            self.materialize_output()?;
        }
        
        // Return output batches
        if self.current_output_idx < self.output_batches.len() {
            let batch = self.output_batches[self.current_output_idx].clone();
            self.current_output_idx += 1;
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }
    
    fn children_mut(&mut self) -> Vec<&mut dyn VectorOperator> {
        // Cannot return mutable reference through Arc<Mutex>
        vec![]
    }
}

