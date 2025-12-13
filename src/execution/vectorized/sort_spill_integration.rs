/// Integration of SortSpill with VectorSortOperator
/// 
/// This module provides the integration layer to enable external merge sort
/// with automatic spilling when memory limits are exceeded.
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::execution::vectorized::sort::VectorSortOperator;
use crate::spill::sort_spill::SortSpill;
use crate::error::EngineResult;
use crate::query::plan::OrderByExpr;
use crate::config::EngineConfig;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, debug, warn};

/// Sort operator with integrated spill support
pub struct VectorSortOperatorWithSpill {
    /// Base sort operator
    base_operator: VectorSortOperator,
    
    /// Spill manager for external merge sort
    spill_manager: Arc<Mutex<SortSpill>>,
    
    /// Memory limit for in-memory sort (bytes)
    memory_limit: usize,
    
    /// Whether sorting was spilled
    spilled: bool,
    
    /// Configuration
    config: EngineConfig,
}

impl VectorSortOperatorWithSpill {
    /// Create a new sort operator with spill support
    pub fn new(
        input: Arc<Mutex<Box<dyn VectorOperator>>>,
        order_by: Vec<OrderByExpr>,
        config: EngineConfig,
    ) -> Self {
        let base_operator = VectorSortOperator::new(input, order_by);
        
        // Initialize spill manager
        let spill_manager = Arc::new(Mutex::new(SortSpill::new(
            config.spill.spill_dir.clone(),
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
    
    /// Sort batches with spill support (external merge sort)
    pub async fn sort_batches_with_spill(&mut self) -> EngineResult<()> {
        info!(
            memory_limit = self.memory_limit,
            "Sorting batches with spill support"
        );
        
        // Collect input batches
        let mut input_batches: Vec<VectorBatch> = Vec::new();
        let mut total_memory = 0;
        
        {
            let mut input_op = self.base_operator.input.lock().await;
            loop {
                let batch = input_op.next_batch().await?;
                let Some(batch) = batch else {
                    break;
                };
                
                let batch_size = self.estimate_batch_size(&batch);
                total_memory += batch_size;
                
                // Check if we need to spill
                if total_memory > self.memory_limit {
                    if !self.spilled {
                        warn!(
                            total_memory = total_memory,
                            limit = self.memory_limit,
                            "Sort exceeds memory limit, switching to external merge sort"
                        );
                        self.spilled = true;
                    }
                    
                    // Spill batch to sorted run
                    let mut spill_mgr = self.spill_manager.lock().await;
                    spill_mgr.add_batch(&batch)?;
                } else {
                    // Keep in memory for in-memory sort
                    input_batches.push(batch);
                }
            }
        }
        
        // Finalize spill runs if any
        if self.spilled {
            let mut spill_mgr = self.spill_manager.lock().await;
            spill_mgr.finalize_current_run()?;
            
            // Merge all runs
            let merged_batches = spill_mgr.merge_runs()?;
            
            // Combine with in-memory batches
            input_batches.extend(merged_batches);
        }
        
        // Sort all batches (in-memory or merged from disk)
        // Use base operator's sort logic if available, or sort directly
        if !input_batches.is_empty() {
            // For now, store sorted batches in base operator
            // TODO: Integrate with VectorSortOperator's sort_batches() method
            // For external merge sort, batches from disk are already sorted
            // We just need to merge them with in-memory batches
        }
        
        info!(
            total_batches = input_batches.len(),
            spilled = self.spilled,
            "Sort complete"
        );
        
        Ok(())
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
}

// Note: This is a foundation for spill integration
// Full implementation requires:
// 1. Exposing VectorSortOperator internals (input_batches, sort_batches)
// 2. Integrating with VectorSortOperator's prepare() and next_batch() methods
// 3. Proper multi-way merge with sort key comparison
