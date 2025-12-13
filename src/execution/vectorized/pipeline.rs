/// PipelinedOperator: Operators that maintain internal state for pipelined execution
/// 
/// This trait is for operators that need to maintain state between batches
/// (e.g., hash joins building hash tables, aggregations accumulating results).
/// 
/// Unlike VectorOperator which is stateless, PipelinedOperator can maintain
/// internal buffers, hash tables, and other stateful structures.
use crate::execution::vectorized::batch::VectorBatch;
use crate::execution::vectorized::operator::VectorOperator;
use crate::error::EngineResult;
use std::sync::Arc;

/// Pipelined operator - maintains state between batches
/// 
/// This is for operators that need to:
/// - Build hash tables incrementally
/// - Accumulate aggregations
/// - Maintain window function state
/// - Buffer data for sorting
/// 
/// The key difference from VectorOperator is that PipelinedOperator
/// maintains internal state that persists across next_batch() calls.
#[async_trait::async_trait]
pub trait PipelinedOperator: Send + Sync {
    /// Get the output schema
    fn schema(&self) -> arrow::datatypes::SchemaRef;
    
    /// Process a batch and produce output batches
    /// 
    /// This method:
    /// - Receives input batches from upstream
    /// - Maintains internal state (hash tables, accumulators, etc.)
    /// - Produces output batches as they become available
    /// 
    /// Returns:
    /// - `Ok(Some(batch))` - An output batch is ready
    /// - `Ok(None)` - No output yet, but operator is still active
    /// - `Err(e)` - An error occurred
    async fn process_batch(&mut self, input: VectorBatch) -> EngineResult<Option<VectorBatch>>;
    
    /// Flush remaining state and produce final batches
    /// 
    /// Called when input is exhausted to produce any remaining output.
    /// For example:
    /// - Hash join: Produce remaining unmatched rows (for LEFT/RIGHT/FULL joins)
    /// - Aggregation: Produce final aggregated results
    /// - Sort: Produce final sorted batches
    /// 
    /// Returns:
    /// - `Ok(Some(batch))` - A final batch
    /// - `Ok(None)` - No more batches
    /// - `Err(e)` - An error occurred
    async fn flush(&mut self) -> EngineResult<Option<VectorBatch>> {
        // Default: no flushing needed
        Ok(None)
    }
    
    /// Prepare this operator (called once before processing)
    async fn prepare(&mut self) -> EngineResult<()> {
        // Default: no preparation needed
        Ok(())
    }
    
    /// Check if operator is finished (all state flushed)
    fn is_finished(&self) -> bool {
        false
    }
}

/// Pipeline stage - wraps a PipelinedOperator with input/output handling
/// 
/// This struct manages the lifecycle of a pipelined operator:
/// - Receives batches from upstream
/// - Passes them to the operator
/// - Collects output batches
/// - Handles flushing when input is exhausted
pub struct PipelineStage {
    operator: Box<dyn PipelinedOperator>,
    input_exhausted: bool,
    finished: bool,
}

impl PipelineStage {
    pub fn new(operator: Box<dyn PipelinedOperator>) -> Self {
        Self {
            operator,
            input_exhausted: false,
            finished: false,
        }
    }
    
    /// Process an input batch
    pub async fn process(&mut self, input: VectorBatch) -> EngineResult<Option<VectorBatch>> {
        self.operator.process_batch(input).await
    }
    
    /// Mark input as exhausted and flush remaining state
    pub async fn finish(&mut self) -> EngineResult<Option<VectorBatch>> {
        if self.finished {
            return Ok(None);
        }
        
        self.input_exhausted = true;
        let result = self.operator.flush().await?;
        
        if result.is_none() {
            self.finished = true;
        }
        
        Ok(result)
    }
    
    /// Check if this stage is finished
    pub fn is_finished(&self) -> bool {
        self.finished || self.operator.is_finished()
    }
}

