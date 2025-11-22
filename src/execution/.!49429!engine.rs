use crate::execution::operators::build_operator_with_llm_limits;
use crate::execution::batch::{ExecutionBatch, BatchIterator};
use crate::query::plan::QueryPlan;
use crate::hypergraph::graph::HyperGraph;
use anyhow::Result;
use std::sync::Arc;

/// Execution engine - executes query plans
pub struct ExecutionEngine {
    graph: std::sync::Arc<HyperGraph>,
    /// Batch size for processing
    batch_size: usize,
}

impl ExecutionEngine {
    pub fn new(graph: HyperGraph) -> Self {
        Self {
            graph: std::sync::Arc::new(graph),
            batch_size: 8192, // Process 8K rows at a time (SIMD-friendly)
        }
    }
    
    pub fn from_arc(graph: std::sync::Arc<HyperGraph>) -> Self {
        Self {
            graph,
            batch_size: 8192,
        }
    }
    
    /// Execute a query plan with performance timing
    pub fn execute(&self, plan: &QueryPlan) -> Result<QueryResult> {
        self.execute_with_timeout(plan, None)
    }
    
    /// Execute a query plan with optional timeout (for LLM protocol)
    pub fn execute_with_timeout(&self, plan: &QueryPlan, max_time_ms: Option<u64>) -> Result<QueryResult> {
        self.execute_with_llm_limits(plan, max_time_ms, None)
    }
    
    /// Execute a query plan with LLM protocol limits (timeout and max_scan_rows)
    pub fn execute_with_llm_limits(&self, plan: &QueryPlan, max_time_ms: Option<u64>, max_scan_rows: Option<u64>) -> Result<QueryResult> {
        self.execute_with_llm_limits_and_ctes(plan, max_time_ms, max_scan_rows, None)
    }
    
    /// Execute a query plan with LLM protocol limits and CTE results cache
    pub fn execute_with_llm_limits_and_ctes(
        &self,
        plan: &QueryPlan,
        max_time_ms: Option<u64>,
        max_scan_rows: Option<u64>,
        cte_results: Option<&std::collections::HashMap<String, Vec<crate::execution::batch::ExecutionBatch>>>,
    ) -> Result<QueryResult> {
        self.execute_with_subquery_executor(plan, max_time_ms, max_scan_rows, cte_results, None)
    }
    
    /// Execute a query plan with LLM protocol limits, CTE results, and subquery executor
    pub fn execute_with_subquery_executor(
        &self,
        plan: &QueryPlan,
        max_time_ms: Option<u64>,
        max_scan_rows: Option<u64>,
        cte_results: Option<&std::collections::HashMap<String, Vec<crate::execution::batch::ExecutionBatch>>>,
        subquery_executor: Option<std::sync::Arc<dyn crate::query::expression::SubqueryExecutor>>,
    ) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        
        // Build execution operator tree with LLM limits, CTE results, and subquery executor
        let build_start = std::time::Instant::now();
        let mut root_op = crate::execution::operators::build_operator_with_subquery_executor(
            &plan.root,
            self.graph.clone(),
            max_scan_rows,
            cte_results,
            subquery_executor,
            Some(&plan.table_aliases),
        )?;
        let build_time = build_start.elapsed();
        
        // Execute pipeline with early termination optimization
        let exec_start = std::time::Instant::now();
        let mut batches = vec![];
        let mut total_rows = 0;
        while let Some(batch) = root_op.next()? {
            // Check timeout (LLM protocol: max_time_ms)
            if let Some(max_time) = max_time_ms {
                let elapsed_ms = start.elapsed().as_millis() as u64;
                if elapsed_ms >= max_time {
                    eprintln!("ExecutionEngine: Timeout reached ({}ms >= {}ms), stopping execution", elapsed_ms, max_time);
                    break;
                }
            }
            
            total_rows += batch.row_count;
            // Debug logging disabled to prevent memory issues in IDEs
            // eprintln!("ExecutionEngine: Got batch with {} rows (total so far: {})", batch.row_count, total_rows);
            batches.push(batch);
            
            // Early termination: if we have a small result set and no more batches needed,
            // we can stop early (this is a heuristic - actual LIMIT is handled by operators)
            // This helps when LIMIT is very small (e.g., LIMIT 10)
            if total_rows > 0 && batches.len() > 10 {
                // If we have many small batches, we might be done
                // But let the operators handle LIMIT, so we don't stop here
            }
        }
        let exec_time = exec_start.elapsed();
        
        // Collect results
        let collect_start = std::time::Instant::now();
        let row_count = batches.iter().map(|b| b.row_count).sum();
        // Debug logging disabled to prevent memory issues in IDEs
        // eprintln!("ExecutionEngine: Total row_count from batches: {} (calculated: {})", row_count, total_rows);
        let collect_time = collect_start.elapsed();
        let total_time = start.elapsed();
        
        // Only print timing summary (not per-batch details)
