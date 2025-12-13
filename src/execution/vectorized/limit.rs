/// VectorLimitOperator: Async, vectorized limit operator
/// 
/// This is the Phase 1 replacement for LimitOperator, designed for:
/// - Zero-copy row limiting
/// - OFFSET support
/// - Early termination
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::error::EngineResult;
use arrow::datatypes::*;
use std::sync::Arc;
use tracing::{debug, info};

/// Vector limit operator - async, vectorized row limiting
pub struct VectorLimitOperator {
    /// Input operator
    input: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
    
    /// Maximum number of rows to return
    limit: usize,
    
    /// Number of rows to skip
    offset: usize,
    
    /// Rows returned so far
    rows_returned: usize,
    
    /// Prepared flag
    prepared: bool,
}

impl VectorLimitOperator {
    /// Create a new vector limit operator
    pub fn new(
        input: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
        limit: usize,
        offset: usize,
    ) -> Self {
        Self {
            input,
            limit,
            offset,
            rows_returned: 0,
            prepared: false,
        }
    }
    
    /// Apply limit and offset to a batch
    fn apply_limit(&mut self, batch: VectorBatch) -> EngineResult<Option<VectorBatch>> {
        // Skip offset rows
        if self.rows_returned < self.offset {
            let skip = self.offset - self.rows_returned;
            if skip >= batch.row_count {
                // Skip entire batch
                self.rows_returned += batch.row_count;
                return Ok(None);
            }
            
            // Skip partial batch
            let remaining = batch.slice(skip, batch.row_count - skip)?;
            self.rows_returned += skip;
            return self.apply_limit(remaining);
        }
        
        // Apply limit
        let remaining_limit = self.limit - (self.rows_returned - self.offset);
        if remaining_limit == 0 {
            return Ok(None); // Limit reached
        }
        
        if batch.row_count <= remaining_limit {
            // Return entire batch
            self.rows_returned += batch.row_count;
            Ok(Some(batch))
        } else {
            // Return partial batch
            let limited = batch.slice(0, remaining_limit)?;
            self.rows_returned += remaining_limit;
            Ok(Some(limited))
        }
    }
}

#[async_trait::async_trait]
impl VectorOperator for VectorLimitOperator {
    fn schema(&self) -> SchemaRef {
        // Limit doesn't change schema
        // We can't get schema synchronously from async Mutex
        // For now, return placeholder - will be set during prepare()
        Arc::new(Schema::new(Vec::<Field>::new()))
    }
    
    async fn prepare(&mut self) -> EngineResult<()> {
        if self.prepared {
            return Ok(());
        }
        
        info!(
            limit = self.limit,
            offset = self.offset,
            "Preparing vector limit operator"
        );
        
        // Recursively prepare input
        {
            let mut input = self.input.lock().await;
            input.prepare().await?;
        }
        
        self.prepared = true;
        debug!("Vector limit operator prepared");
        Ok(())
    }
    
    async fn next_batch(&mut self) -> EngineResult<Option<VectorBatch>> {
        if !self.prepared {
            return Err(crate::error::EngineError::execution(
                "VectorLimitOperator::next_batch() called before prepare()"
            ));
        }
        
        // Check if limit reached
        if self.rows_returned >= self.offset + self.limit {
            return Ok(None);
        }
        
        // Get next batch from input
        let input_batch = {
            let mut input = self.input.lock().await;
            input.next_batch().await?
        };
        
        let Some(batch) = input_batch else {
            return Ok(None); // Input exhausted
        };
        
        // Apply limit and offset
        self.apply_limit(batch)
    }
    
    fn children_mut(&mut self) -> Vec<&mut dyn VectorOperator> {
        vec![]
    }
}

