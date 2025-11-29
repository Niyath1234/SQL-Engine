/// DEPRECATED: Pre-build phase for join execution
/// 
/// This module is DEPRECATED and replaced by the ExecNode::prepare() mechanism.
/// All operators now implement ExecNode with a prepare() method that recursively
/// builds hash tables in a type-safe way.
/// 
/// This file is kept for reference but should not be used in new code.
/// Use ExecNode::prepare() instead.
use crate::execution::batch::BatchIterator;
use anyhow::Result;

/// DEPRECATED: Pre-build all right-side hash tables for joins
/// 
/// This function is DEPRECATED. Use ExecNode::prepare() instead.
/// 
/// # Migration
/// Replace calls to `prebuild_joins(&mut root_op)?` with:
/// ```rust
/// unsafe {
///     crate::execution::exec_node::prepare_batch_iterator(&mut root_op)?;
/// }
/// ```
#[deprecated(note = "Use ExecNode::prepare() instead")]
pub fn prebuild_joins(_root: &mut Box<dyn BatchIterator>) -> Result<()> {
    // This function is deprecated - ExecNode::prepare() handles this now
    // Return Ok to maintain backward compatibility during migration
    Ok(())
}

