/// VectorJoinOperator with spill support
/// 
/// This extends VectorJoinOperator to support Grace hash join with spilling
/// when the build side exceeds memory limits.
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

/// Join operator with spill support
pub struct VectorJoinOperatorWithSpill {
    /// Base join operator
    base_operator: VectorJoinOperator,
    
    /// Spill manager for partitioned joins
    spill_manager: Option<Arc<Mutex<PartitionedJoinSpill>>>,
    
    /// Memory limit for build side (bytes)
    build_memory_limit: usize,
    
    /// Current build side memory usage (bytes)
    build_memory_used: usize,
    
    /// Configuration
    config: EngineConfig,
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
        
        // Initialize spill manager if spill directory is configured
        let spill_manager = Some(Arc::new(Mutex::new(PartitionedJoinSpill::new(
            config.spill.spill_dir.clone(),
            8, // Default partition count
        ))));
        
        Self {
            base_operator,
            spill_manager,
            build_memory_limit: config.memory.max_operator_memory_bytes,
            build_memory_used: 0,
            config,
        }
    }
    
    /// Build hash table with spill support
    async fn build_hash_table_with_spill(&mut self) -> EngineResult<()> {
        info!(
            memory_limit = self.build_memory_limit,
            "Building hash table with spill support"
        );
        
        // Collect right-side batches
        let mut right_batches = Vec::new();
        let mut total_memory = 0;
        
        loop {
            let batch = {
                let mut right = self.base_operator.right.lock().await;
                right.next_batch().await?
            };
            
            let Some(batch) = batch else {
                break;
            };
            
            // Estimate batch memory size
            let batch_size = self.estimate_batch_size(&batch);
            total_memory += batch_size;
            
            // Check if we need to spill
            if total_memory > self.build_memory_limit {
                if let Some(spill_mgr) = &self.spill_manager {
                    warn!(
                        total_memory = total_memory,
                        limit = self.build_memory_limit,
                        "Build side exceeds memory limit, spilling to disk"
                    );
                    
                    // Partition and spill batches
                    self.spill_build_batches(&right_batches, spill_mgr).await?;
                    right_batches.clear();
                    total_memory = 0;
                } else {
                    warn!(
                        "Build side exceeds memory limit but spill is not configured"
                    );
                }
            }
            
            right_batches.push(batch);
        }
        
        // Build hash table from remaining in-memory batches
        if !right_batches.is_empty() {
            self.base_operator.right_batches = Some(right_batches);
            // Build hash table using base operator's method
            // TODO: Call base_operator.build_hash_table() when exposed
        }
        
        self.build_memory_used = total_memory;
        
        info!(
            memory_used = self.build_memory_used,
            batch_count = self.base_operator.right_batches.as_ref().map(|b| b.len()).unwrap_or(0),
            "Hash table build complete"
        );
        
        Ok(())
    }
    
    /// Spill build batches to disk using partitioning
    async fn spill_build_batches(
        &self,
        batches: &[VectorBatch],
        spill_mgr: &Arc<Mutex<PartitionedJoinSpill>>,
    ) -> EngineResult<()> {
        info!(
            batch_count = batches.len(),
            "Spilling build batches to disk"
        );
        
        // For each batch, partition by join key and spill
        for batch in batches {
            // Extract join key values and partition
            let partition_indices = self.partition_batch_by_key(batch)?;
            
            // Group rows by partition
            let mut partition_rows: Vec<Vec<usize>> = vec![Vec::new(); 8]; // 8 partitions
            for (row_idx, &partition_idx) in partition_indices.iter().enumerate() {
                partition_rows[partition_idx].push(row_idx);
            }
            
            // Spill each partition
            for (partition_idx, row_indices) in partition_rows.iter().enumerate() {
                if !row_indices.is_empty() {
                    let partition_batch = self.create_partition_batch(batch, row_indices)?;
                    let mut spill_mgr = spill_mgr.lock().await;
                    spill_mgr.spill_build_batch(&partition_batch, partition_idx).await?;
                }
            }
        }
        
        Ok(())
    }
    
    /// Partition a batch by join key (returns partition index for each row)
    fn partition_batch_by_key(&self, batch: &VectorBatch) -> EngineResult<Vec<usize>> {
        // Extract join key column
        let key_col_idx = batch.schema.fields()
            .iter()
            .position(|f| f.name() == self.base_operator.predicate.right_column.as_str())
            .ok_or_else(|| crate::error::EngineError::execution(format!(
                "Join key column '{}' not found in batch",
                self.base_operator.predicate.right_column
            )))?;
        
        let key_array = batch.column(key_col_idx)?;
        
        // Compute partition for each row
        let mut partitions = Vec::new();
        for row_idx in 0..batch.row_count {
            let key_value = Self::extract_key_value(key_array.as_ref(), row_idx)?;
            let partition = self.hash_to_partition(&key_value, 8); // 8 partitions
            partitions.push(partition);
        }
        
        Ok(partitions)
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
    
    /// Create a batch from selected row indices
    fn create_partition_batch(
        &self,
        batch: &VectorBatch,
        row_indices: &[usize],
    ) -> EngineResult<VectorBatch> {
        // Materialize selected rows into new batch
        // TODO: Implement efficient materialization
        // For now, return original batch (this is a placeholder)
        Ok(batch.clone())
    }
    
    /// Estimate batch memory size
    fn estimate_batch_size(&self, batch: &VectorBatch) -> usize {
        // Rough estimate: sum of array sizes
        batch.columns.iter()
            .map(|arr| {
                // Estimate based on data type and length
                let len = arr.len();
                match arr.data_type() {
                    arrow::datatypes::DataType::Int64 => len * 8,
                    arrow::datatypes::DataType::Float64 => len * 8,
                    arrow::datatypes::DataType::Utf8 => len * 20, // Rough estimate
                    _ => len * 8, // Default estimate
                }
            })
            .sum()
    }
}

// Note: This is a placeholder implementation
// Full integration requires exposing build_hash_table() from VectorJoinOperator
// and implementing proper Grace hash join algorithm

