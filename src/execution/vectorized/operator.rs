/// VectorOperator: Async, vectorized operator trait
/// 
/// This is the new operator trait for Phase 1, replacing BatchIterator.
/// Key differences:
/// - Async: Uses async/await for non-blocking I/O
/// - Vectorized: Operates on entire batches, not row-by-row
/// - Arc-based: Uses Arc for shared ownership (enables parallelism)
/// - Send + Sync: Required for multi-threaded execution
use crate::execution::vectorized::batch::VectorBatch;
use crate::error::EngineResult;
use std::sync::Arc;

/// Vector operator trait - async, vectorized execution
/// 
/// This trait defines the interface for all operators in the new execution model.
/// Operators are:
/// - Async: Can yield control during I/O operations
/// - Vectorized: Process entire batches at once
/// - Stateless (or minimal state): State is managed externally
/// - Send + Sync: Safe for multi-threaded execution
#[async_trait::async_trait]
pub trait VectorOperator: Send + Sync {
    /// Get the output schema for this operator
    fn schema(&self) -> arrow::datatypes::SchemaRef;
    
    /// Get the next batch asynchronously
    /// 
    /// Returns:
    /// - `Ok(Some(batch))` - A batch of data
    /// - `Ok(None)` - No more data (operator exhausted)
    /// - `Err(e)` - An error occurred
    /// 
    /// # Contract
    /// - Must eventually return `None` when exhausted
    /// - Must not block on CPU-bound work (use spawn_blocking for that)
    /// - Must be safe to call concurrently (if operator supports it)
    async fn next_batch(&mut self) -> EngineResult<Option<VectorBatch>>;
    
    /// Prepare this operator (called once before next_batch())
    /// 
    /// This is where operators can:
    /// - Build hash tables
    /// - Initialize data structures
    /// - Recursively prepare child operators
    /// 
    /// # Contract
    /// - Must be called exactly once before any next_batch() calls
    /// - Must be idempotent (safe to call multiple times)
    async fn prepare(&mut self) -> EngineResult<()> {
        // Default: no preparation needed
        Ok(())
    }
    
    /// Get child operators (for recursive prepare())
    /// 
    /// Returns a vector of mutable references to child operators.
    /// Used for recursive prepare() calls.
    fn children_mut(&mut self) -> Vec<&mut dyn VectorOperator> {
        // Default: no children
        vec![]
    }
}

/// Helper trait for operators that can be fused
/// 
/// Operator fusion allows combining multiple operators into a single
/// execution unit for better performance (e.g., Filter + Project).
pub trait FusableOperator: VectorOperator {
    /// Check if this operator can be fused with another
    fn can_fuse_with(&self, other: &dyn VectorOperator) -> bool;
    
    /// Fuse this operator with another (if possible)
    fn fuse_with(&mut self, other: Box<dyn VectorOperator>) -> EngineResult<Box<dyn VectorOperator>>;
}

