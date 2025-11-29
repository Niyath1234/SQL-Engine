/// ExecNode: Two-Phase Execution Model
/// 
/// This trait defines the core execution lifecycle for all operators:
/// 1. prepare() - Called ONCE before execution, allows precomputation (e.g., hash table builds)
/// 2. next() - Called repeatedly to produce batches until None is returned
/// 
/// # Lifecycle Contract
/// - prepare() MUST be called exactly once before any next() calls
/// - next() MUST NOT be called before prepare()
/// - prepare() may recursively call child.prepare() for nested operators
/// - next() streams data and eventually returns None when exhausted
/// 
/// # Benefits
/// - Eliminates recursion bugs: prepare() builds all nested structures before execution
/// - No retry loops: next() is simple and deterministic
/// - Type-safe: No unsafe downcasts needed
/// - Scalable: Works for unlimited join depth
use crate::execution::batch::ExecutionBatch;
use anyhow::Result;

/// Core execution node trait
pub trait ExecNode {
    /// Prepare phase: Called ONCE before execution starts
    /// 
    /// This is where operators can:
    /// - Build hash tables (for joins)
    /// - Pre-compute aggregations
    /// - Initialize data structures
    /// - Recursively prepare child operators
    /// 
    /// # Contract
    /// - Must be called exactly once before any next() calls
    /// - Must recursively call child.prepare() if operator has children
    /// - Must be idempotent (safe to call multiple times, but should only be called once)
    fn prepare(&mut self) -> Result<()> {
        // Default implementation: no preparation needed
        Ok(())
    }
    
    /// Execution phase: Called repeatedly to produce batches
    /// 
    /// This is where operators:
    /// - Stream data from children
    /// - Apply transformations
    /// - Produce output batches
    /// 
    /// # Contract
    /// - Must NOT be called before prepare()
    /// - Must eventually return None when exhausted
    /// - Must NOT consume children that were already consumed in prepare()
    /// - Must be deterministic (same inputs produce same outputs)
    fn next(&mut self) -> Result<Option<ExecutionBatch>>;
}

/// Helper trait for operators with children
/// 
/// This allows recursive prepare() calls in a type-safe way
pub trait HasChildren {
    /// Get mutable references to all child operators
    /// Used for recursive prepare() calls
    fn children_mut(&mut self) -> Vec<&mut dyn ExecNode>;
}

/// DEPRECATED: Helper function to call prepare() on a BatchIterator
/// 
/// This function is DEPRECATED. Use FilterOperator::prepare_child() instead,
/// which uses type-erased downcasting via Any::downcast_mut().
/// 
/// This function is kept for backward compatibility but does nothing.
#[deprecated(note = "Use FilterOperator::prepare_child() instead")]
pub unsafe fn prepare_batch_iterator(_child: &mut Box<dyn crate::execution::batch::BatchIterator>) -> Result<()> {
    // This function is deprecated - use FilterOperator::prepare_child() instead
    // which properly handles type-erased downcasting
    Ok(())
}


