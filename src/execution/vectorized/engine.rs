/// VectorExecutionEngine: Async execution engine for vectorized operators
/// 
/// This is the Phase 1 replacement for ExecutionEngine, designed for:
/// - Async execution loop
/// - Parallel operator execution
/// - Streaming results
/// - Better resource utilization
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::error::EngineResult;
use crate::config::EngineConfig;
use std::sync::Arc;
use tracing::{info, debug, warn, error};

/// Vector execution engine - async, vectorized query execution
pub struct VectorExecutionEngine {
    /// Engine configuration
    config: EngineConfig,
    
    /// Root operator
    root_operator: Option<Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>>,
}

impl VectorExecutionEngine {
    /// Create a new vector execution engine
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config,
            root_operator: None,
        }
    }
    
    /// Set the root operator
    pub fn with_root_operator(mut self, operator: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>) -> Self {
        self.root_operator = Some(operator);
        self
    }
    
    /// Execute query with async execution loop
    /// 
    /// This method:
    /// - Prepares the root operator
    /// - Executes async loop calling next_batch()
    /// - Collects all batches
    /// - Returns results
    pub async fn execute(&mut self) -> EngineResult<Vec<VectorBatch>> {
        let root = self.root_operator.as_ref()
            .ok_or_else(|| crate::error::EngineError::execution(
                "Root operator not set"
            ))?;
        
        info!("Starting vector execution engine");
        
        // Prepare phase
        {
            let mut op = root.lock().await;
            op.prepare().await?;
        }
        
        debug!("Root operator prepared");
        
        // Execution phase - async loop
        let mut batches = Vec::new();
        let mut total_rows = 0;
        let mut iteration_count = 0;
        let max_iterations = self.config.execution.max_iterations;
        let max_time_secs = self.config.execution.max_execution_time_secs;
        let start_time = std::time::Instant::now();
        
        loop {
            // Check timeout
            if start_time.elapsed().as_secs() > max_time_secs {
                warn!(
                    elapsed_secs = start_time.elapsed().as_secs(),
                    max_secs = max_time_secs,
                    "Query execution timeout"
                );
                return Err(crate::error::EngineError::cancellation(format!(
                    "Query execution exceeded {} seconds",
                    max_time_secs
                )));
            }
            
            // Check iteration limit
            if iteration_count > max_iterations {
                warn!(
                    iterations = iteration_count,
                    max_iterations = max_iterations,
                    "Query execution iteration limit exceeded"
                );
                return Err(crate::error::EngineError::cancellation(format!(
                    "Query execution exceeded {} iterations",
                    max_iterations
                )));
            }
            
            // Get next batch (async)
            let batch_result = {
                let mut op = root.lock().await;
                op.next_batch().await
            };
            
            match batch_result? {
                Some(batch) => {
                    iteration_count += 1;
                    total_rows += batch.row_count;
                    
                    debug!(
                        batch_rows = batch.row_count,
                        total_rows = total_rows,
                        batch_count = batches.len() + 1,
                        "Received vector batch"
                    );
                    
                    batches.push(batch);
                }
                None => {
                    // Operator exhausted
                    debug!(
                        total_batches = batches.len(),
                        total_rows = total_rows,
                        "Query execution complete"
                    );
                    break;
                }
            }
        }
        
        info!(
            total_batches = batches.len(),
            total_rows = total_rows,
            execution_time_ms = start_time.elapsed().as_millis(),
            "Vector execution engine completed"
        );
        
        Ok(batches)
    }
    
    /// Execute query with streaming (returns batches as they arrive)
    /// 
    /// This is useful for large result sets where we don't want to
    /// buffer all batches in memory.
    pub async fn execute_streaming(
        &mut self,
        mut callback: impl FnMut(VectorBatch) -> EngineResult<()> + Send,
    ) -> EngineResult<()> {
        let root = self.root_operator.as_ref()
            .ok_or_else(|| crate::error::EngineError::execution(
                "Root operator not set"
            ))?;
        
        info!("Starting streaming vector execution");
        
        // Prepare phase
        {
            let mut op = root.lock().await;
            op.prepare().await?;
        }
        
        // Execution phase - stream batches
        let mut total_rows = 0;
        let start_time = std::time::Instant::now();
        let max_time_secs = self.config.execution.max_execution_time_secs;
        
        loop {
            // Check timeout
            if start_time.elapsed().as_secs() > max_time_secs {
                return Err(crate::error::EngineError::cancellation(format!(
                    "Query execution exceeded {} seconds",
                    max_time_secs
                )));
            }
            
            // Get next batch
            let batch_result = {
                let mut op = root.lock().await;
                op.next_batch().await
            };
            
            match batch_result? {
                Some(batch) => {
                    total_rows += batch.row_count;
                    callback(batch)?; // Stream batch to callback
                }
                None => {
                    // Operator exhausted
                    break;
                }
            }
        }
        
        info!(
            total_rows = total_rows,
            execution_time_ms = start_time.elapsed().as_millis(),
            "Streaming execution complete"
        );
        
        Ok(())
    }
}

