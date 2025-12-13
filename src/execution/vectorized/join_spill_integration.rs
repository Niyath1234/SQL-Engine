/// Integration of PartitionedJoinSpill with VectorJoinOperator
/// 
/// This module provides the integration layer to enable Grace hash join
/// with automatic spilling when memory limits are exceeded.
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::execution::vectorized::join::VectorJoinOperator;
use crate::spill::partitioned_join_spill::PartitionedJoinSpill;
use crate::error::EngineResult;
use crate::query::plan::{JoinType, JoinPredicate};
use crate::config::EngineConfig;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, debug, warn};
use fxhash::FxHashMap;
use crate::storage::fragment::Value;
use arrow::datatypes::*;

/// Join operator with integrated spill support
pub struct VectorJoinOperatorWithSpill {
    /// Base join operator
    base_operator: VectorJoinOperator,
    
    /// Spill manager for partitioned joins
    spill_manager: Arc<Mutex<PartitionedJoinSpill>>,
    
    /// Memory limit for build side (bytes)
    build_memory_limit: usize,
    
    /// Current build side memory usage (bytes)
    build_memory_used: usize,
    
    /// Configuration
    config: EngineConfig,
    
    /// Whether build side was spilled
    build_spilled: bool,
    
    /// Whether probe side was spilled
    probe_spilled: bool,
}

impl VectorJoinOperatorWithSpill {
    /// Create a new join operator with spill support
    pub fn new(
        left: Arc<Mutex<Box<dyn VectorOperator>>>,
        right: Arc<Mutex<Box<dyn VectorOperator>>>,
        join_type: JoinType,
        predicate: JoinPredicate,
        config: EngineConfig,
    ) -> Self {
        let base_operator = VectorJoinOperator::new(left, right, join_type, predicate);
        
        // Initialize spill manager
        let spill_manager = Arc::new(Mutex::new(PartitionedJoinSpill::new(
            config.spill.spill_dir.clone(),
            config.spill.join_spill_partitions,
        )));
        
        Self {
            base_operator,
            spill_manager,
            build_memory_limit: config.memory.max_operator_memory_bytes,
            build_memory_used: 0,
            config,
            build_spilled: false,
            probe_spilled: false,
        }
    }
    
    /// Build hash table with spill support (Grace hash join)
    pub async fn build_hash_table_with_spill(&mut self) -> EngineResult<()> {
        info!(
            memory_limit = self.build_memory_limit,
            "Building hash table with spill support"
        );
        
        // Collect right-side batches and check memory
        let mut right_batches: Vec<VectorBatch> = Vec::new();
        let mut total_memory = 0;
        
        {
            let mut right_op = self.base_operator.right.lock().await;
            loop {
                let batch = right_op.next_batch().await?;
                let Some(batch) = batch else {
                    break;
                };
                
                let batch_size = self.estimate_batch_size(&batch);
                total_memory += batch_size;
                
                // Check if we need to spill
                if total_memory > self.build_memory_limit {
                    if !self.build_spilled {
                        warn!(
                            total_memory = total_memory,
                            limit = self.build_memory_limit,
                            "Build side exceeds memory limit, switching to Grace hash join"
                        );
                        self.build_spilled = true;
                    }
                    
                    // Partition and spill batch
                    let partition_idx = self.partition_batch_by_key(&batch)?;
                    {
                        let mut spill_mgr = self.spill_manager.lock().await;
                        spill_mgr.spill_build_batch(&batch, partition_idx)?;
                    }
                } else {
                    // Keep in memory
                    right_batches.push(batch);
                }
            }
        }
        
        // Store in-memory batches
        // Note: VectorJoinOperator stores batches internally during build_hash_table()
        // For spill integration, we need to modify VectorJoinOperator to support
        // both in-memory and spilled builds
        
        self.build_memory_used = total_memory;
        
        info!(
            memory_used = self.build_memory_used,
            build_spilled = self.build_spilled,
            "Hash table build complete"
        );
        
        Ok(())
    }
    
    
    /// Partition a batch by join key
    fn partition_batch_by_key(&self, batch: &VectorBatch) -> EngineResult<usize> {
        // Extract join key column
        let key_col_idx = batch.schema.fields()
            .iter()
            .position(|f| f.name() == self.base_operator.predicate.right.1.as_str())
            .ok_or_else(|| crate::error::EngineError::execution(format!(
                "Join key column '{}' not found in batch",
                self.base_operator.predicate.right.1
            )))?;
        
        let key_array = batch.column(key_col_idx)?;
        
        // Use first row's key to determine partition
        // In a full implementation, we'd partition all rows
        let key_value = Self::extract_key_value(key_array.as_ref(), 0)?;
        let partition = self.hash_to_partition(&key_value, self.config.spill.join_spill_partitions);
        
        Ok(partition)
    }
    
    /// Extract key values from an array (helper method)
    fn extract_key_values(array: &dyn arrow::array::Array) -> EngineResult<Vec<Value>> {
        use arrow::array::*;
        let mut values = Vec::new();
        
        match array.data_type() {
            arrow::datatypes::DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                for val in arr.iter() {
                    values.push(Value::Int64(val.unwrap_or(0)));
                }
            }
            arrow::datatypes::DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                for val in arr.iter() {
                    values.push(Value::String(val.unwrap_or("").to_string()));
                }
            }
            _ => {
                return Err(crate::error::EngineError::execution(format!(
                    "Unsupported join key type: {:?}",
                    array.data_type()
                )));
            }
        }
        
        Ok(values)
    }
    
    /// Extract key value from array (single row)
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
    
    /// Execute join with spill support
    pub async fn execute_with_spill(&mut self) -> EngineResult<Option<VectorBatch>> {
        if self.build_spilled {
            // Grace hash join: process partitions
            self.execute_grace_hash_join().await
        } else {
            // Standard in-memory join
            // Use base operator's next_batch method
            // TODO: Integrate with base operator's next_batch()
            Err(crate::error::EngineError::execution(
                "Standard join execution not yet integrated. Use base operator directly."
            ))
        }
    }
    
    /// Execute Grace hash join (partition-based)
    async fn execute_grace_hash_join(&mut self) -> EngineResult<Option<VectorBatch>> {
        // Grace hash join algorithm:
        // 1. For each partition:
        //    a. Load build partition from disk
        //    b. Build hash table from partition
        //    c. Partition probe side by same key
        //    d. Load matching probe partition
        //    e. Join in-memory
        //    f. Return results
        
        let spill_mgr = self.spill_manager.lock().await;
        
        // Get next partition to process
        // For now, process partition 0 (in full implementation, track current partition)
        let partition_idx = 0; // TODO: Track current partition index
        
        // Load build partition
        let build_batches = spill_mgr.load_build_partition(partition_idx)?;
        
        // Build hash table from partition
        let mut build_hash: fxhash::FxHashMap<crate::storage::fragment::Value, Vec<usize>> = fxhash::FxHashMap::default();
        for (batch_idx, batch) in build_batches.iter().enumerate() {
            let key_col_idx = batch.schema.fields()
                .iter()
                .position(|f| f.name() == self.base_operator.predicate.right.1.as_str())
                .ok_or_else(|| crate::error::EngineError::execution(format!(
                    "Join key column '{}' not found in build batch",
                    self.base_operator.predicate.right.1
                )))?;
            
            let key_array = batch.column(key_col_idx)?;
            let key_values = Self::extract_key_values(key_array.as_ref())?;
            
            for (row_idx, key_value) in key_values.iter().enumerate() {
                let encoded_idx = (batch_idx << 16) | row_idx;
                build_hash.entry(key_value.clone()).or_insert_with(Vec::new).push(encoded_idx);
            }
        }
        
        // Partition and probe left side
        // For now, collect all left batches and partition them
        // In a full implementation, we'd stream left batches and partition on-the-fly
        let mut left_batches = Vec::new();
        {
            let mut left_op = self.base_operator.left.lock().await;
            loop {
                let batch = left_op.next_batch().await?;
                let Some(batch) = batch else {
                    break;
                };
                left_batches.push(batch);
            }
        }
        
        // Partition left batches and join matching partitions
        let mut matched_pairs: Vec<(usize, usize)> = Vec::new();
        
        for (left_batch_idx, left_batch) in left_batches.iter().enumerate() {
            let left_key_col_idx = left_batch.schema.fields()
                .iter()
                .position(|f| f.name() == self.base_operator.predicate.left.1.as_str())
                .ok_or_else(|| crate::error::EngineError::execution(format!(
                    "Join key column '{}' not found in probe batch",
                    self.base_operator.predicate.left.1
                )))?;
            
            let left_key_array = left_batch.column(left_key_col_idx)?;
            let left_key_values = Self::extract_key_values(left_key_array.as_ref())?;
            
            for (left_row_idx, key_value) in left_key_values.iter().enumerate() {
                // Check if this row belongs to current partition
                let row_partition = self.hash_to_partition(key_value, self.config.spill.join_spill_partitions);
                if row_partition == partition_idx {
                    // Probe hash table
                    if let Some(build_indices) = build_hash.get(key_value) {
                        for &build_encoded_idx in build_indices {
                            matched_pairs.push((left_row_idx, build_encoded_idx));
                        }
                    }
                }
            }
        }
        
        if matched_pairs.is_empty() {
            // No matches in this partition, return None (caller will try next partition)
            return Ok(None);
        }
        
        // Materialize join output from first left batch and build batches
        // For simplicity, use first left batch (in full implementation, we'd handle all left batches)
        if let Some(left_batch) = left_batches.first() {
            let output_batch = self.materialize_grace_join_output(left_batch, &build_batches, &matched_pairs).await?;
            Ok(Some(output_batch))
        } else {
            Ok(None)
        }
    }
    
    /// Materialize join output for Grace hash join
    async fn materialize_grace_join_output(
        &self,
        left_batch: &VectorBatch,
        build_batches: &[VectorBatch],
        matched_pairs: &[(usize, usize)],
    ) -> EngineResult<VectorBatch> {
        use arrow::array::*;
        use arrow::datatypes::*;
        
        let mut output_arrays = Vec::new();
        let mut output_fields = Vec::new();
        
        // Add left columns
        for (idx, field) in left_batch.schema.fields().iter().enumerate() {
            let left_array = left_batch.column(idx)?;
            let materialized = self.materialize_column_from_pairs(left_array.as_ref(), matched_pairs, |pair| pair.0)?;
            output_arrays.push(materialized);
            output_fields.push(field.as_ref().clone());
        }
        
        // Add right columns from build batches
        if let Some(first_build_batch) = build_batches.first() {
            for (idx, field) in first_build_batch.schema.fields().iter().enumerate() {
                let materialized = self.materialize_build_column_from_pairs(build_batches, idx, matched_pairs)?;
                output_arrays.push(materialized);
                output_fields.push(field.as_ref().clone());
            }
        }
        
        let output_schema = Arc::new(Schema::new(output_fields));
        Ok(VectorBatch::new(output_arrays, output_schema))
    }
    
    /// Materialize a column from matched pairs (left side)
    fn materialize_column_from_pairs<F>(
        &self,
        source_array: &dyn arrow::array::Array,
        matched_pairs: &[(usize, usize)],
        get_row_idx: F,
    ) -> EngineResult<Arc<dyn arrow::array::Array>>
    where
        F: Fn(&(usize, usize)) -> usize,
    {
        use arrow::array::*;
        
        match source_array.data_type() {
            DataType::Int64 => {
                let arr = source_array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                let mut builder = Int64Builder::with_capacity(matched_pairs.len());
                for pair in matched_pairs {
                    let row_idx = get_row_idx(pair);
                    if row_idx < arr.len() {
                        builder.append_value(arr.value(row_idx));
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Utf8 => {
                let arr = source_array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                let mut builder = StringBuilder::with_capacity(matched_pairs.len(), matched_pairs.len() * 10);
                for pair in matched_pairs {
                    let row_idx = get_row_idx(pair);
                    if row_idx < arr.len() {
                        builder.append_value(arr.value(row_idx));
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            _ => {
                Err(crate::error::EngineError::execution(format!(
                    "Unsupported column type: {:?}",
                    source_array.data_type()
                )))
            }
        }
    }
    
    /// Materialize build column from matched pairs
    fn materialize_build_column_from_pairs(
        &self,
        build_batches: &[VectorBatch],
        col_idx: usize,
        matched_pairs: &[(usize, usize)],
    ) -> EngineResult<Arc<dyn arrow::array::Array>> {
        use arrow::array::*;
        
        // Decode build_encoded_idx: (batch_idx, row_idx)
        if build_batches.is_empty() {
            return Err(crate::error::EngineError::execution("No build batches available"));
        }
        
        let first_batch = &build_batches[0];
        let array = first_batch.column(col_idx)?;
        
        match array.data_type() {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                let mut builder = Int64Builder::with_capacity(matched_pairs.len());
                for pair in matched_pairs {
                    let build_encoded_idx = pair.1;
                    let build_batch_idx = (build_encoded_idx >> 16) & 0xFFFF;
                    let build_row_idx = build_encoded_idx & 0xFFFF;
                    
                    if build_batch_idx < build_batches.len() && build_row_idx < arr.len() {
                        let build_batch = &build_batches[build_batch_idx];
                        let build_array = build_batch.column(col_idx)?;
                        let build_arr = build_array.as_any().downcast_ref::<Int64Array>()
                            .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast build array"))?;
                        builder.append_value(build_arr.value(build_row_idx));
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                let mut builder = StringBuilder::with_capacity(matched_pairs.len(), matched_pairs.len() * 10);
                for pair in matched_pairs {
                    let build_encoded_idx = pair.1;
                    let build_batch_idx = (build_encoded_idx >> 16) & 0xFFFF;
                    let build_row_idx = build_encoded_idx & 0xFFFF;
                    
                    if build_batch_idx < build_batches.len() && build_row_idx < arr.len() {
                        let build_batch = &build_batches[build_batch_idx];
                        let build_array = build_batch.column(col_idx)?;
                        let build_arr = build_array.as_any().downcast_ref::<StringArray>()
                            .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast build array"))?;
                        builder.append_value(build_arr.value(build_row_idx));
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            _ => {
                Err(crate::error::EngineError::execution(format!(
                    "Unsupported build column type: {:?}",
                    array.data_type()
                )))
            }
        }
    }
}

// Note: This is a foundation for spill integration
// Full implementation requires:
// 1. Exposing VectorJoinOperator internals (right_batches, build_hash_table)
// 2. Implementing full Grace hash join algorithm
// 3. Integrating with VectorJoinOperator's prepare() and next_batch() methods

// Note: This is a foundation for spill integration
// Full implementation requires:
// 1. Exposing VectorJoinOperator internals (right_batches, build_hash_table)
// 2. Implementing full Grace hash join algorithm
// 3. Integrating with VectorJoinOperator's prepare() and next_batch() methods

