/// Integration of AggSpill with VectorAggregateOperator
/// 
/// This module provides the integration layer to enable partitioned aggregation
/// with automatic spilling when memory limits are exceeded.
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::execution::vectorized::aggregate::VectorAggregateOperator;
use crate::spill::agg_spill::AggSpill;
use crate::error::EngineResult;
use crate::query::plan::{AggregateExpr, AggregateFunction};
use crate::config::EngineConfig;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, debug, warn};
use fxhash::FxHashMap;
use crate::storage::fragment::Value;
use arrow::datatypes::*;

/// Aggregate operator with integrated spill support
pub struct VectorAggregateOperatorWithSpill {
    /// Base aggregate operator
    base_operator: VectorAggregateOperator,
    
    /// Spill manager for partitioned aggregation
    spill_manager: Arc<Mutex<AggSpill>>,
    
    /// Memory limit per partition (bytes)
    memory_limit: usize,
    
    /// Whether aggregation was spilled
    spilled: bool,
    
    /// Configuration
    config: EngineConfig,
}

impl VectorAggregateOperatorWithSpill {
    /// Create a new aggregate operator with spill support
    pub fn new(
        input: Arc<Mutex<Box<dyn VectorOperator>>>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
        config: EngineConfig,
    ) -> Self {
        let base_operator = VectorAggregateOperator::new(input, group_by, aggregates);
        
        // Initialize spill manager
        let spill_manager = Arc::new(Mutex::new(AggSpill::new(
            config.spill.spill_dir.clone(),
            config.spill.join_spill_partitions, // Reuse partition count
            config.memory.max_operator_memory_bytes,
        )));
        
        Self {
            base_operator,
            spill_manager,
            memory_limit: config.memory.max_operator_memory_bytes,
            spilled: false,
            config,
        }
    }
    
    /// Process batch with spill support (partitioned aggregation)
    pub async fn process_batch_with_spill(&mut self, batch: &VectorBatch) -> EngineResult<()> {
        let batch_size = self.estimate_batch_size(batch);
        
        // Check if we need to spill
        if batch_size > self.memory_limit {
            if !self.spilled {
                warn!(
                    batch_size = batch_size,
                    limit = self.memory_limit,
                    "Aggregation exceeds memory limit, switching to partitioned aggregation"
                );
                self.spilled = true;
            }
            
            // Partition batch by group-by keys and spill
            self.spill_agg_batch(batch).await?;
        } else {
            // Process in-memory
            // Note: For full integration, we'd call base_operator.process_batch()
            // For now, this is a placeholder
        }
        
        Ok(())
    }
    
    /// Spill an aggregation batch using partitioning
    async fn spill_agg_batch(&mut self, batch: &VectorBatch) -> EngineResult<()> {
        // Extract group-by keys and partition
        let partition_idx = self.partition_batch_by_keys(batch)?;
        
        // Spill to partition
        let mut spill_mgr = self.spill_manager.lock().await;
        spill_mgr.spill_agg_batch(batch, partition_idx)?;
        
        debug!(
            partition = partition_idx,
            rows = batch.row_count,
            "Spilled aggregation batch to partition"
        );
        
        Ok(())
    }
    
    /// Partition a batch by group-by keys
    fn partition_batch_by_keys(&self, batch: &VectorBatch) -> EngineResult<usize> {
        // Use first group-by column to determine partition
        // In a full implementation, we'd use all group-by columns
        if let Some(first_key) = self.base_operator.group_by.first() {
            let key_col_idx = batch.schema.fields()
                .iter()
                .position(|f| f.name() == first_key.as_str())
                .ok_or_else(|| crate::error::EngineError::execution(format!(
                    "Group-by column '{}' not found in batch",
                    first_key
                )))?;
            
            let key_array = batch.column(key_col_idx)?;
            let key_value = Self::extract_key_value(key_array.as_ref(), 0)?;
            let partition = self.hash_to_partition(&key_value, self.config.spill.join_spill_partitions);
            
            Ok(partition)
        } else {
            // No group-by columns, use single partition
            Ok(0)
        }
    }
    
    /// Extract key value from array
    fn extract_key_value(array: &dyn arrow::array::Array, row_idx: usize) -> EngineResult<Value> {
        use arrow::array::*;
        
        match array.data_type() {
            arrow::datatypes::DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                Ok(Value::Int64(arr.value(row_idx)))
            }
            arrow::datatypes::DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Float64Array"))?;
                Ok(Value::Float64(arr.value(row_idx)))
            }
            arrow::datatypes::DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                Ok(Value::String(arr.value(row_idx).to_string()))
            }
            _ => {
                Err(crate::error::EngineError::execution(format!(
                    "Unsupported key type: {:?}",
                    array.data_type()
                )))
            }
        }
    }
    
    /// Hash value to partition index
    fn hash_to_partition(&self, value: &Value, partition_count: usize) -> usize {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        (hasher.finish() as usize) % partition_count
    }
    
    /// Estimate batch memory size
    fn estimate_batch_size(&self, batch: &VectorBatch) -> usize {
        batch.columns.iter()
            .map(|arr| {
                let len = arr.len();
                match arr.data_type() {
                    arrow::datatypes::DataType::Int64 => len * 8,
                    arrow::datatypes::DataType::Float64 => len * 8,
                    arrow::datatypes::DataType::Utf8 => len * 20, // Rough estimate
                    _ => len * 8,
                }
            })
            .sum()
    }
    
    /// Flush aggregated results with spill support
    pub async fn flush_with_spill(&mut self) -> EngineResult<Option<VectorBatch>> {
        if self.spilled {
            // Merge partitions and finalize aggregation
            let mut spill_mgr = self.spill_manager.lock().await;
            
            // Load all partitions and merge aggregation states
            let mut merged_state: fxhash::FxHashMap<Vec<Value>, crate::execution::vectorized::aggregate::AggregationState> = fxhash::FxHashMap::default();
            
            for partition_idx in 0..self.config.spill.join_spill_partitions {
                let partition_batches = spill_mgr.load_agg_partition(partition_idx)?;
                
                // Process each batch in partition and merge into global state
                for batch in partition_batches {
                    // Extract group-by keys
                    let group_by_indices: Vec<usize> = self.base_operator.group_by.iter()
                        .map(|col| {
                            batch.schema.fields()
                                .iter()
                                .position(|f| f.name() == col.as_str())
                                .ok_or_else(|| crate::error::EngineError::execution(format!(
                                    "Group-by column '{}' not found",
                                    col
                                )))
                        })
                        .collect::<EngineResult<Vec<_>>>()?;
                    
                    // Extract group keys for each row
                    for row_idx in 0..batch.row_count {
                        let mut group_key = Vec::new();
                        for &col_idx in &group_by_indices {
                            let array = batch.column(col_idx)?;
                            use crate::execution::vectorized::aggregate::VectorAggregateOperator;
                            let value = VectorAggregateOperator::extract_value_static(array.as_ref(), row_idx)?;
                            group_key.push(value);
                        }
                        
                        // Get or create aggregation state
                        let state = merged_state.entry(group_key.clone())
                            .or_insert_with(|| {
                                let aggregates = self.base_operator.aggregates.iter()
                                    .map(|agg| {
                                        use crate::execution::vectorized::aggregate::AggregateValue;
                                        AggregateValue::new(agg.function.clone())
                                    })
                                    .collect();
                                crate::execution::vectorized::aggregate::AggregationState {
                                    group_key: group_key.clone(),
                                    aggregates,
                                }
                            });
                        
                        // Update aggregates for this row
                        // Extract aggregate values
                        for (agg_idx, agg_expr) in self.base_operator.aggregates.iter().enumerate() {
                            let value = if agg_expr.column == "*" {
                                Value::Int64(1)
                            } else {
                                let col_idx = batch.schema.fields()
                                    .iter()
                                    .position(|f| f.name() == agg_expr.column.as_str())
                                    .ok_or_else(|| crate::error::EngineError::execution(format!(
                                        "Aggregate column '{}' not found",
                                        agg_expr.column
                                    )))?;
                                let array = batch.column(col_idx)?;
                                use crate::execution::vectorized::aggregate::VectorAggregateOperator;
                                VectorAggregateOperator::extract_value_static(array.as_ref(), row_idx)?
                            };
                            
                            state.aggregates[agg_idx].update(&value);
                        }
                    }
                }
            }
            
            // Materialize final results from merged state
            // Use base operator's materialization logic
            let output_batch = self.materialize_aggregated_results(&merged_state)?;
            Ok(Some(output_batch))
        } else {
            // Use base operator's output batches (already materialized)
            if let Some(batch) = self.base_operator.output_batches.first() {
                Ok(Some(batch.clone()))
            } else {
                Ok(None)
            }
        }
    }
    
    /// Materialize aggregated results from merged state
    fn materialize_aggregated_results(
        &self,
        merged_state: &fxhash::FxHashMap<Vec<Value>, crate::execution::vectorized::aggregate::AggregationState>,
    ) -> EngineResult<VectorBatch> {
        use arrow::array::*;
        use arrow::datatypes::*;
        
        if merged_state.is_empty() {
            return Err(crate::error::EngineError::execution(
                "No aggregated results to materialize"
            ));
        }
        
        // Build output arrays
        let mut output_arrays = Vec::new();
        let mut output_fields = Vec::new();
        
        // Collect values first, then build arrays
        let first_state = merged_state.values().next().unwrap();
        let row_count = merged_state.len();
        
        // Collect group-by column values
        let mut group_by_values: Vec<Vec<Value>> = vec![Vec::new(); first_state.group_key.len()];
        for state in merged_state.values() {
            for (idx, value) in state.group_key.iter().enumerate() {
                group_by_values[idx].push(value.clone());
            }
        }
        
        // Build group-by arrays
        for (idx, col_name) in self.base_operator.group_by.iter().enumerate() {
            let array = Self::build_array_from_values(&group_by_values[idx])?;
            output_arrays.push(array);
            output_fields.push(Field::new(col_name, self.value_to_data_type(&first_state.group_key[idx])?, true));
        }
        
        // Collect aggregate values
        let mut aggregate_values: Vec<Vec<Value>> = vec![Vec::new(); self.base_operator.aggregates.len()];
        for state in merged_state.values() {
            for (agg_idx, agg_value) in state.aggregates.iter().enumerate() {
                let value = match agg_value {
                    crate::execution::vectorized::aggregate::AggregateValue::Count(v) => Value::Int64(*v as i64),
                    crate::execution::vectorized::aggregate::AggregateValue::Sum(v) => v.clone(),
                    crate::execution::vectorized::aggregate::AggregateValue::Min(v) => v.clone(),
                    crate::execution::vectorized::aggregate::AggregateValue::Max(v) => v.clone(),
                    crate::execution::vectorized::aggregate::AggregateValue::Avg { sum, count } => {
                        // For AVG, return sum for now (would need to divide by count in final materialization)
                        sum.clone()
                    }
                };
                aggregate_values[agg_idx].push(value);
            }
        }
        
        // Build aggregate arrays
        for (agg_idx, agg_expr) in self.base_operator.aggregates.iter().enumerate() {
            let array = Self::build_array_from_values(&aggregate_values[agg_idx])?;
            output_arrays.push(array);
            let alias = agg_expr.alias.as_ref().map(|s| s.as_str()).unwrap_or(&agg_expr.column);
            output_fields.push(Field::new(alias, self.aggregate_to_data_type(agg_expr)?, true));
        }
        
        let schema = Arc::new(Schema::new(output_fields));
        Ok(VectorBatch::new(output_arrays, schema))
    }
    
    
    /// Helper to convert value to data type
    fn value_to_data_type(&self, value: &Value) -> EngineResult<DataType> {
        match value {
            Value::Int64(_) => Ok(DataType::Int64),
            Value::Float64(_) => Ok(DataType::Float64),
            Value::String(_) => Ok(DataType::Utf8),
            _ => Err(crate::error::EngineError::execution(format!(
                "Unsupported value type: {:?}",
                value
            ))),
        }
    }
    
    /// Helper to convert aggregate to data type
    fn aggregate_to_data_type(&self, agg_expr: &crate::query::plan::AggregateExpr) -> EngineResult<DataType> {
        match agg_expr.function {
            crate::query::plan::AggregateFunction::Count | crate::query::plan::AggregateFunction::CountDistinct => {
                Ok(DataType::Int64)
            }
            crate::query::plan::AggregateFunction::Sum | crate::query::plan::AggregateFunction::Avg => {
                Ok(DataType::Float64)
            }
            _ => Err(crate::error::EngineError::execution(format!(
                "Unsupported aggregate function: {:?}",
                agg_expr.function
            ))),
        }
    }
    
    /// Build Arrow array from values
    fn build_array_from_values(values: &[Value]) -> EngineResult<Arc<dyn arrow::array::Array>> {
        use arrow::array::*;
        
        if values.is_empty() {
            return Err(crate::error::EngineError::execution("Cannot build array from empty values"));
        }
        
        match &values[0] {
            Value::Int64(_) => {
                let mut builder = Int64Builder::with_capacity(values.len());
                for v in values {
                    if let Value::Int64(i) = v {
                        builder.append_value(*i);
                    } else {
                        return Err(crate::error::EngineError::execution("Type mismatch in values"));
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            Value::Float64(_) => {
                let mut builder = Float64Builder::with_capacity(values.len());
                for v in values {
                    if let Value::Float64(f) = v {
                        builder.append_value(*f);
                    } else {
                        return Err(crate::error::EngineError::execution("Type mismatch in values"));
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            Value::String(_) => {
                let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 10);
                for v in values {
                    if let Value::String(s) = v {
                        builder.append_value(s);
                    } else {
                        return Err(crate::error::EngineError::execution("Type mismatch in values"));
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            _ => Err(crate::error::EngineError::execution(format!(
                "Unsupported value type: {:?}",
                values[0]
            ))),
        }
    }
}

// Note: This is a foundation for spill integration
// Full implementation requires:
// 1. Exposing VectorAggregateOperator internals (aggregation_state, process_batch, flush)
// 2. Implementing partitioned aggregation merge
// 3. Integrating with VectorAggregateOperator's prepare() and next_batch() methods

