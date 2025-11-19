use crate::execution::operators::build_operator;
use crate::execution::batch::{ExecutionBatch, BatchIterator};
use crate::query::plan::QueryPlan;
use crate::hypergraph::graph::HyperGraph;
use anyhow::Result;
use std::sync::Arc;
use rayon::prelude::*;

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
        let start = std::time::Instant::now();
        
        // Build execution operator tree
        let build_start = std::time::Instant::now();
        let mut root_op = build_operator(&plan.root, self.graph.clone())?;
        let build_time = build_start.elapsed();
        
        // Execute pipeline with early termination optimization
        let exec_start = std::time::Instant::now();
        let mut batches = vec![];
        let mut total_rows = 0;
        while let Some(batch) = root_op.next()? {
            total_rows += batch.row_count;
            eprintln!("ExecutionEngine: Got batch with {} rows (total so far: {})", batch.row_count, total_rows);
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
        eprintln!("ExecutionEngine: Total row_count from batches: {} (calculated: {})", row_count, total_rows);
        let collect_time = collect_start.elapsed();
        let total_time = start.elapsed();
        
        eprintln!("⏱️  Execution timing:");
        eprintln!("   Build: {:?}", build_time);
        eprintln!("   Execute: {:?}", exec_time);
        eprintln!("   Collect: {:?}", collect_time);
        eprintln!("   Total: {:?} ({:.2}ms)", total_time, total_time.as_secs_f64() * 1000.0);
        eprintln!("   Rows: {}", row_count);
        if total_time.as_secs_f64() > 0.0 {
            eprintln!("   Throughput: {:.2} rows/sec", row_count as f64 / total_time.as_secs_f64());
        }
        
        Ok(QueryResult {
            batches,
            row_count,
            execution_time_ms: total_time.as_secs_f64() * 1000.0,
        })
    }
    
    /// Execute query with parallel processing
    pub fn execute_parallel(&self, plan: &QueryPlan) -> Result<QueryResult> {
        // TODO: Implement parallel execution
        // Split work across threads using rayon
        self.execute(plan)
    }
}

/// Query execution result
pub struct QueryResult {
    pub batches: Vec<ExecutionBatch>,
    pub row_count: usize,
    pub execution_time_ms: f64,
}

impl QueryResult {
    /// Convert to arrow RecordBatch for output
    pub fn to_record_batches(&self) -> Vec<arrow::record_batch::RecordBatch> {
        // TODO: Convert ExecutionBatch to RecordBatch
        vec![]
    }
}

