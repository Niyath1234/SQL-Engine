/// VectorJoinOperator: Async, vectorized join operator
/// 
/// This is the Phase 1 replacement for JoinOperator, designed for:
/// - Async hash table building
/// - Vectorized probe phase
/// - Zero-copy batch materialization
/// - Support for INNER, LEFT, RIGHT, FULL joins
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::error::EngineResult;
use crate::query::plan::{JoinType, JoinPredicate};
use crate::storage::fragment::Value;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use bitvec::prelude::*;
use tracing::{debug, info, warn};
use fxhash::FxHashMap;

/// Vector join operator - async, vectorized join execution
pub struct VectorJoinOperator {
    /// Left input operator
    pub(crate) left: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
    
    /// Right input operator
    pub(crate) right: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
    
    /// Join type (INNER, LEFT, RIGHT, FULL)
    pub(crate) join_type: JoinType,
    
    /// Join predicate
    pub(crate) predicate: JoinPredicate,
    
    /// Hash table for right side (built during prepare)
    right_hash: Option<FxHashMap<Value, Vec<usize>>>,
    
    /// Right side batches (for materialization)
    right_batches: Vec<VectorBatch>,
    
    /// Current left batch index
    current_left_batch_idx: usize,
    
    /// Current probe position in current left batch
    current_probe_pos: usize,
    
    /// Prepared flag
    prepared: bool,
}

impl VectorJoinOperator {
    /// Create a new vector join operator
    pub fn new(
        left: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
        right: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
        join_type: JoinType,
        predicate: JoinPredicate,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            predicate,
            right_hash: None,
            right_batches: Vec::new(),
            current_left_batch_idx: 0,
            current_probe_pos: 0,
            prepared: false,
        }
    }
    
    /// Build hash table from right side (async)
    async fn build_hash_table(&mut self) -> EngineResult<()> {
        if self.right_hash.is_some() {
            return Ok(()); // Already built
        }
        
        info!("Building hash table for vector join");
        
        let mut right_hash: FxHashMap<Value, Vec<usize>> = FxHashMap::default();
        let mut right_batches = Vec::new();
        let mut batch_idx = 0;
        
        // Collect all right-side batches
        {
            let mut right_op = self.right.lock().await;
            loop {
                let batch = right_op.next_batch().await?;
                let Some(batch) = batch else {
                    break; // Right side exhausted
                };
                
                right_batches.push(batch.clone());
                
                // Extract join key column
                let key_col_idx = self.get_key_column_index(&batch, &self.predicate.right.0)?;
                let key_array = batch.column(key_col_idx)?;
                
                // Build hash table: key -> list of (batch_idx, row_idx) pairs
                // Encode as: (batch_idx << 16) | row_idx
                for (row_idx, key_value) in self.extract_key_values(key_array.as_ref())?.iter().enumerate() {
                    let encoded_idx = (batch_idx << 16) | row_idx;
                    right_hash.entry(key_value.clone()).or_insert_with(Vec::new).push(encoded_idx);
                }
                
                batch_idx += 1;
            }
        }
        
        self.right_hash = Some(right_hash);
        self.right_batches = right_batches;
        
        info!(
            hash_table_size = self.right_hash.as_ref().unwrap().len(),
            right_batches = self.right_batches.len(),
            "Hash table built"
        );
        
        Ok(())
    }
    
    /// Get column index for join key
    fn get_key_column_index(&self, batch: &VectorBatch, column_name: &str) -> EngineResult<usize> {
        batch.schema.fields()
            .iter()
            .position(|f| f.name() == column_name)
            .ok_or_else(|| crate::error::EngineError::execution(format!(
                "Join key column '{}' not found in batch schema",
                column_name
            )))
    }
    
    /// Extract key values from an array
    fn extract_key_values(&self, array: &dyn Array) -> EngineResult<Vec<Value>> {
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
    
    /// Execute inner join (vectorized probe)
    async fn execute_inner_join(&mut self) -> EngineResult<Option<VectorBatch>> {
        let right_hash = self.right_hash.as_ref()
            .ok_or_else(|| crate::error::EngineError::execution(
                "Hash table not built - prepare() must be called first"
            ))?;
        
        // Get next left batch
        let left_batch = {
            let mut left_op = self.left.lock().await;
            left_op.next_batch().await?
        };
        
        let Some(left_batch) = left_batch else {
            return Ok(None); // Left side exhausted
        };
        
        // Extract left join key (use table.column format)
        let left_key_col_name = format!("{}.{}", self.predicate.left.0, self.predicate.left.1);
        let left_key_col_idx = self.get_key_column_index(&left_batch, &left_key_col_name)
            .or_else(|_| self.get_key_column_index(&left_batch, &self.predicate.left.1))?;
        let left_key_array = left_batch.column(left_key_col_idx)?;
        let left_key_values = self.extract_key_values(left_key_array.as_ref())?;
        
        // Probe hash table and collect matches
        let mut matched_pairs: Vec<(usize, usize)> = Vec::new(); // (left_row_idx, right_encoded_idx)
        
        for (left_row_idx, key_value) in left_key_values.iter().enumerate() {
            if let Some(right_indices) = right_hash.get(key_value) {
                for &right_encoded_idx in right_indices {
                    matched_pairs.push((left_row_idx, right_encoded_idx));
                }
            }
        }
        
        if matched_pairs.is_empty() {
            // No matches - try next left batch (non-recursive)
            // Continue to next iteration of the calling loop
            return Ok(None);
        }
        
        // Materialize output batch
        let output_batch = self.materialize_join_output(&left_batch, &matched_pairs).await?;
        
        debug!(
            left_rows = left_batch.row_count,
            matched_pairs = matched_pairs.len(),
            output_rows = output_batch.row_count,
            "Inner join batch produced"
        );
        
        Ok(Some(output_batch))
    }
    
    /// Materialize join output from matched pairs
    async fn materialize_join_output(
        &self,
        left_batch: &VectorBatch,
        matched_pairs: &[(usize, usize)],
    ) -> EngineResult<VectorBatch> {
        let mut output_arrays = Vec::new();
        let mut output_fields = Vec::new();
        
        // Add left columns
        for (idx, field) in left_batch.schema.fields().iter().enumerate() {
            let left_array = left_batch.column(idx)?;
            let materialized = self.materialize_column_from_pairs(left_array.as_ref(), matched_pairs, |pair| pair.0)?;
            output_arrays.push(materialized);
            output_fields.push(field.as_ref().clone());
        }
        
        // Add right columns
        for (idx, field) in self.right_batches[0].schema.fields().iter().enumerate() {
            let materialized = self.materialize_right_column_from_pairs(idx, matched_pairs)?;
            output_arrays.push(materialized);
            output_fields.push(field.as_ref().clone());
        }
        
        let output_schema = Arc::new(Schema::new(output_fields));
        Ok(VectorBatch::new(output_arrays, output_schema))
    }
    
    /// Materialize a column from matched pairs
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
                    "Unsupported column type for materialization: {:?}",
                    source_array.data_type()
                )))
            }
        }
    }
    
    /// Materialize right column from matched pairs
    fn materialize_right_column_from_pairs(
        &self,
        col_idx: usize,
        matched_pairs: &[(usize, usize)],
    ) -> EngineResult<Arc<dyn Array>> {
        // Decode right_encoded_idx: (batch_idx, row_idx)
        // For now, assume all rows are from the first batch (simplified)
        // TODO: Handle multiple batches correctly
        if self.right_batches.is_empty() {
            return Err(crate::error::EngineError::execution(
                "No right batches available for materialization"
            ));
        }
        
        let right_batch = &self.right_batches[0];
        let right_array = right_batch.column(col_idx)?;
        
        // Materialize using the right row indices
        self.materialize_column_from_pairs(right_array.as_ref(), matched_pairs, |pair| {
            let encoded = pair.1;
            encoded & 0xFFFF // Extract row_idx
        })
    }
}

#[async_trait::async_trait]
impl VectorOperator for VectorJoinOperator {
    fn schema(&self) -> SchemaRef {
        // Build output schema: left columns + right columns
        // For now, return placeholder - will be set during prepare()
        Arc::new(Schema::new(Vec::<Field>::new()))
    }
    
    async fn prepare(&mut self) -> EngineResult<()> {
        if self.prepared {
            return Ok(());
        }
        
        info!("Preparing vector join operator");
        
        // Recursively prepare children
        {
            let mut left_op = self.left.lock().await;
            left_op.prepare().await?;
        }
        {
            let mut right_op = self.right.lock().await;
            right_op.prepare().await?;
        }
        
        // Build hash table from right side
        self.build_hash_table().await?;
        
        self.prepared = true;
        debug!("Vector join operator prepared");
        Ok(())
    }
    
    async fn next_batch(&mut self) -> EngineResult<Option<VectorBatch>> {
        if !self.prepared {
            return Err(crate::error::EngineError::execution(
                "VectorJoinOperator::next_batch() called before prepare()"
            ));
        }
        
        match self.join_type {
            JoinType::Inner => self.execute_inner_join().await,
            JoinType::Left => {
                // TODO: Implement LEFT join
                Err(crate::error::EngineError::execution(
                    "LEFT join not yet implemented in VectorJoinOperator"
                ))
            }
            JoinType::Right => {
                // TODO: Implement RIGHT join
                Err(crate::error::EngineError::execution(
                    "RIGHT join not yet implemented in VectorJoinOperator"
                ))
            }
            JoinType::Full => {
                // TODO: Implement FULL join
                Err(crate::error::EngineError::execution(
                    "FULL join not yet implemented in VectorJoinOperator"
                ))
            }
        }
    }
    
    fn children_mut(&mut self) -> Vec<&mut dyn VectorOperator> {
        // Cannot return mutable reference through Arc<Mutex>
        vec![]
    }
}

