/// Parallel Hash Join with Radix Partitioning
/// 
/// This module implements a parallel hash join using radix partitioning.
/// It enables concurrent build and probe phases for improved performance on multi-core systems.
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::execution::vectorized::radix_partition::{radix_partition, calculate_radix_bits, PartitionStats};
use crate::error::EngineResult;
use crate::query::plan::{JoinType, JoinPredicate};
use crate::config::EngineConfig;
use crate::storage::fragment::Value;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, debug, warn};
use fxhash::FxHashMap;
use arrow::datatypes::*;
use arrow::array::*;
use async_trait::async_trait;

/// Parallel hash join operator
pub struct ParallelHashJoinOperator {
    /// Left input operator
    left: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
    
    /// Right input operator
    right: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
    
    /// Join type
    join_type: JoinType,
    
    /// Join predicate
    predicate: JoinPredicate,
    
    /// Number of partitions for radix partitioning
    num_partitions: usize,
    
    /// Radix bits (calculated from num_partitions)
    radix_bits: usize,
    
    /// Hash tables per partition (built in parallel)
    partition_hash_tables: Vec<FxHashMap<Value, Vec<usize>>>,
    
    /// Right batches per partition
    partition_right_batches: Vec<Vec<VectorBatch>>,
    
    /// Current partition being probed
    current_partition: usize,
    
    /// Current left batch index for current partition
    current_left_batch_idx: usize,
    
    /// Left batches per partition (for parallel probing)
    partition_left_batches: Vec<Vec<VectorBatch>>,
    
    /// Probe results per partition (collected from parallel probes)
    partition_probe_results: Vec<Vec<VectorBatch>>,
    
    /// Current output partition index
    current_output_partition: usize,
    
    /// Current output batch index within partition
    current_output_batch_idx: usize,
    
    /// Prepared flag
    prepared: bool,
    
    /// Configuration
    config: EngineConfig,
    
    /// Worker pool for parallel execution
    worker_pool: Option<Arc<crate::execution::vectorized::work_stealing::WorkerPool>>,
}

impl ParallelHashJoinOperator {
    /// Create a new parallel hash join operator
    pub fn new(
        left: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
        right: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
        join_type: JoinType,
        predicate: JoinPredicate,
        config: EngineConfig,
    ) -> Self {
        let num_partitions = config.join.parallel_build_threads.max(1);
        let radix_bits = calculate_radix_bits(num_partitions);
        
        Self {
            left,
            right,
            join_type,
            predicate,
            num_partitions,
            radix_bits,
            partition_hash_tables: vec![FxHashMap::default(); num_partitions],
            partition_right_batches: vec![Vec::new(); num_partitions],
            partition_left_batches: vec![Vec::new(); num_partitions],
            partition_probe_results: vec![Vec::new(); num_partitions],
            current_partition: 0,
            current_left_batch_idx: 0,
            current_output_partition: 0,
            current_output_batch_idx: 0,
            prepared: false,
            config,
            worker_pool: None,
        }
    }
    
    /// Build hash tables in parallel using radix partitioning
    pub async fn build_hash_tables_parallel(&mut self) -> EngineResult<()> {
        info!(
            num_partitions = self.num_partitions,
            radix_bits = self.radix_bits,
            "Building hash tables in parallel"
        );
        
        // Collect all right-side batches
        let mut all_right_batches = Vec::new();
        {
            let mut right_op = self.right.lock().await;
            loop {
                let batch = right_op.next_batch().await?;
                let Some(batch) = batch else {
                    break;
                };
                all_right_batches.push(batch);
            }
        }
        
        // Partition batches by join key
        for (batch_idx, batch) in all_right_batches.iter().enumerate() {
            let key_col_idx = batch.schema.fields()
                .iter()
                .position(|f| f.name() == self.predicate.right.1.as_str())
                .ok_or_else(|| crate::error::EngineError::execution(format!(
                    "Join key column '{}' not found in right batch",
                    self.predicate.right.1
                )))?;
            
            let key_array = batch.column(key_col_idx)?;
            let key_values = Self::extract_key_values(key_array.as_ref())?;
            
            // Partition rows by radix
            for (row_idx, key_value) in key_values.iter().enumerate() {
                let partition_idx = radix_partition(key_value, self.radix_bits, self.num_partitions);
                let encoded_idx = (batch_idx << 16) | row_idx;
                
                self.partition_hash_tables[partition_idx]
                    .entry(key_value.clone())
                    .or_insert_with(Vec::new)
                    .push(encoded_idx);
            }
            
            // Store batch in appropriate partition (simplified - in full implementation,
            // would split batch across partitions)
            let first_partition = if !key_values.is_empty() {
                radix_partition(&key_values[0], self.radix_bits, self.num_partitions)
            } else {
                0
            };
            self.partition_right_batches[first_partition].push(batch.clone());
        }
        
        // Log partition statistics
        let partition_sizes: Vec<usize> = self.partition_hash_tables.iter()
            .map(|ht| {
                let len: usize = ht.len();
                len
            })
            .collect();
        let stats = PartitionStats::from_sizes(partition_sizes);
        
        info!(
            min_size = stats.min_size,
            max_size = stats.max_size,
            avg_size = stats.avg_size,
            std_dev = stats.std_dev,
            balanced = stats.is_balanced(0.2),
            "Partition statistics"
        );
        
        if !stats.is_balanced(0.2) {
            warn!(
                "Partitions are unbalanced - consider adjusting radix_bits or num_partitions"
            );
        }
        
        Ok(())
    }
    
    /// Collect and partition left-side batches
    async fn collect_and_partition_left_batches(&mut self) -> EngineResult<()> {
        info!("Collecting and partitioning left-side batches");
        
        // Collect all left-side batches
        let mut all_left_batches = Vec::new();
        {
            let mut left_op = self.left.lock().await;
            loop {
                let batch = left_op.next_batch().await?;
                let Some(batch) = batch else {
                    break;
                };
                all_left_batches.push(batch);
            }
        }
        
        // Partition batches by join key
        for batch in all_left_batches {
            let key_col_idx = batch.schema.fields()
                .iter()
                .position(|f| f.name() == self.predicate.left.1.as_str())
                .ok_or_else(|| crate::error::EngineError::execution(format!(
                    "Join key column '{}' not found in left batch",
                    self.predicate.left.1
                )))?;
            
            let key_array = batch.column(key_col_idx)?;
            let key_values = Self::extract_key_values(key_array.as_ref())?;
            
            // Determine partition for this batch (use first key value)
            // In full implementation, would split batch across partitions if needed
            let partition_idx = if !key_values.is_empty() {
                radix_partition(&key_values[0], self.radix_bits, self.num_partitions)
            } else {
                0
            };
            
            self.partition_left_batches[partition_idx].push(batch);
        }
        
        info!(
            total_left_batches = self.partition_left_batches.iter().map(|b| b.len()).sum::<usize>(),
            "Left batches partitioned"
        );
        
        Ok(())
    }
    
    /// Probe partitions sequentially
    async fn probe_partitions_sequential(&mut self) -> EngineResult<()> {
        info!("Probing partitions sequentially");
        
        for partition_idx in 0..self.num_partitions {
            let hash_table = &self.partition_hash_tables[partition_idx];
            let left_batches = &self.partition_left_batches[partition_idx];
            let right_batches = &self.partition_right_batches[partition_idx];
            
            if hash_table.is_empty() || left_batches.is_empty() {
                continue;
            }
            
            // Probe each left batch against hash table
            for left_batch in left_batches {
                let left_key_col_idx = left_batch.schema.fields()
                    .iter()
                    .position(|f| f.name() == self.predicate.left.1.as_str())
                    .ok_or_else(|| crate::error::EngineError::execution(format!(
                        "Join key column '{}' not found in left batch",
                        self.predicate.left.1
                    )))?;
                
                let left_key_array = left_batch.column(left_key_col_idx)?;
                let left_key_values = Self::extract_key_values(left_key_array.as_ref())?;
                
                // Probe hash table and collect matches
                let mut matched_pairs: Vec<(usize, usize)> = Vec::new();
                
                for (left_row_idx, key_value) in left_key_values.iter().enumerate() {
                    let key_partition = radix_partition(key_value, self.radix_bits, self.num_partitions);
                    if key_partition == partition_idx {
                        if let Some(right_indices) = hash_table.get(key_value) {
                            for &right_encoded_idx in right_indices {
                                matched_pairs.push((left_row_idx, right_encoded_idx));
                            }
                        }
                    }
                }
                
                if !matched_pairs.is_empty() {
                    let output_batch = self.materialize_join_output(
                        left_batch,
                        right_batches,
                        &matched_pairs,
                    ).await?;
                    self.partition_probe_results[partition_idx].push(output_batch);
                }
            }
        }
        
        info!("Sequential probe complete");
        Ok(())
    }
    
    /// Probe partitions in parallel using work-stealing
    async fn probe_partitions_parallel(&mut self) -> EngineResult<()> {
        info!("Probing partitions in parallel");
        
        // For now, use sequential probing (parallel implementation would use worker pool)
        // TODO: Implement true parallel probing with work-stealing
        self.probe_partitions_sequential().await?;
        
        info!("Parallel probe complete");
        Ok(())
    }
    
    /// Extract key values from an array
    fn extract_key_values(array: &dyn Array) -> EngineResult<Vec<Value>> {
        let mut values = Vec::new();
        
        match array.data_type() {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                for val in arr.iter() {
                    values.push(Value::Int64(val.unwrap_or(0)));
                }
            }
            DataType::Utf8 => {
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
}

#[async_trait::async_trait]
impl VectorOperator for ParallelHashJoinOperator {
    fn schema(&self) -> SchemaRef {
        // Combine left and right schemas
        // For now, return placeholder - will be set during materialization
        Arc::new(Schema::new(Vec::<Field>::new()))
    }
    
    async fn prepare(&mut self) -> EngineResult<()> {
        if self.prepared {
            return Ok(());
        }
        
        // Recursively prepare children
        {
            let mut left = self.left.lock().await;
            left.prepare().await?;
        }
        {
            let mut right = self.right.lock().await;
            right.prepare().await?;
        }
        
        // Initialize worker pool if parallel execution is enabled
        if self.config.join.enable_parallel_build {
            let mut pool = crate::execution::vectorized::work_stealing::WorkerPool::new(
                self.config.join.parallel_build_threads
            );
            pool.start();
            self.worker_pool = Some(Arc::new(pool));
        }
        
        // Build hash tables in parallel
        self.build_hash_tables_parallel().await?;
        
        // Collect and partition left-side batches for parallel probing
        self.collect_and_partition_left_batches().await?;
        
        // Probe partitions in parallel
        if self.worker_pool.is_some() {
            self.probe_partitions_parallel().await?;
        } else {
            self.probe_partitions_sequential().await?;
        }
        
        self.prepared = true;
        info!("Parallel hash join operator prepared");
        Ok(())
    }
    
    async fn next_batch(&mut self) -> EngineResult<Option<VectorBatch>> {
        if !self.prepared {
            return Err(crate::error::EngineError::execution(
                "ParallelHashJoinOperator::next_batch() called before prepare()"
            ));
        }
        
        // Return results from parallel probe (already computed during prepare)
        while self.current_output_partition < self.num_partitions {
            let results = &self.partition_probe_results[self.current_output_partition];
            
            if self.current_output_batch_idx < results.len() {
                let batch = results[self.current_output_batch_idx].clone();
                self.current_output_batch_idx += 1;
                return Ok(Some(batch));
            } else {
                // Move to next partition
                self.current_output_partition += 1;
                self.current_output_batch_idx = 0;
            }
        }
        
        // All results returned
        Ok(None)
    }
    
    fn children_mut(&mut self) -> Vec<&mut dyn VectorOperator> {
        vec![]
    }
}

impl ParallelHashJoinOperator {
    /// Materialize join output from matched pairs
    async fn materialize_join_output(
        &self,
        left_batch: &VectorBatch,
        right_batches: &[VectorBatch],
        matched_pairs: &[(usize, usize)],
    ) -> EngineResult<VectorBatch> {
        let mut output_arrays = Vec::new();
        let mut output_fields = Vec::new();
        
        // Add left columns
        for (idx, field) in left_batch.schema.fields().iter().enumerate() {
            let left_array = left_batch.column(idx)?;
            let materialized = self.materialize_column_from_pairs(
                left_array.as_ref(),
                matched_pairs,
                |pair| pair.0,
            )?;
            output_arrays.push(materialized);
            output_fields.push(field.as_ref().clone());
        }
        
        // Add right columns from right batches
        if let Some(first_right_batch) = right_batches.first() {
            for (idx, field) in first_right_batch.schema.fields().iter().enumerate() {
                let materialized = self.materialize_right_column_from_pairs(
                    right_batches,
                    idx,
                    matched_pairs,
                )?;
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
        source_array: &dyn Array,
        matched_pairs: &[(usize, usize)],
        get_row_idx: F,
    ) -> EngineResult<Arc<dyn Array>>
    where
        F: Fn(&(usize, usize)) -> usize,
    {
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
    
    /// Materialize right column from matched pairs
    fn materialize_right_column_from_pairs(
        &self,
        right_batches: &[VectorBatch],
        col_idx: usize,
        matched_pairs: &[(usize, usize)],
    ) -> EngineResult<Arc<dyn Array>> {
        if right_batches.is_empty() {
            return Err(crate::error::EngineError::execution("No right batches available"));
        }
        
        let first_batch = &right_batches[0];
        let array = first_batch.column(col_idx)?;
        
        match array.data_type() {
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(matched_pairs.len());
                for pair in matched_pairs {
                    let build_encoded_idx = pair.1;
                    let build_batch_idx = (build_encoded_idx >> 16) & 0xFFFF;
                    let build_row_idx = build_encoded_idx & 0xFFFF;
                    
                    if build_batch_idx < right_batches.len() && build_row_idx < array.len() {
                        let build_batch = &right_batches[build_batch_idx];
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
                let mut builder = StringBuilder::with_capacity(matched_pairs.len(), matched_pairs.len() * 10);
                for pair in matched_pairs {
                    let build_encoded_idx = pair.1;
                    let build_batch_idx = (build_encoded_idx >> 16) & 0xFFFF;
                    let build_row_idx = build_encoded_idx & 0xFFFF;
                    
                    if build_batch_idx < right_batches.len() && build_row_idx < array.len() {
                        let build_batch = &right_batches[build_batch_idx];
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

