/// Integration layer between HypergraphSQLEngine and VectorExecutionEngine
/// 
/// This module provides functions to execute queries using the vectorized
/// execution engine as an alternative to the traditional ExecutionEngine.
use crate::execution::vectorized::{build_vector_operator_tree, VectorExecutionEngine, vector_batches_to_execution_batches};
use crate::execution::engine::QueryResult as ExecutionQueryResult;
use crate::error::EngineResult;
use crate::query::plan::QueryPlan;
use crate::hypergraph::graph::HyperGraph;
use crate::config::EngineConfig;
use std::sync::Arc;
use tracing::{info, debug};

/// Execute a query plan using the vectorized execution engine
/// 
/// This is an alternative execution path that uses the new async, vectorized
/// operator model instead of the traditional BatchIterator model.
pub async fn execute_vectorized(
    plan: &QueryPlan,
    graph: Arc<HyperGraph>,
    config: Option<EngineConfig>,
) -> EngineResult<ExecutionQueryResult> {
    info!("Executing query with vectorized execution engine");
    
    let config = config.unwrap_or_default();
    
    // Build vectorized operator tree from plan
    let root_op = build_vector_operator_tree(&plan.root, graph).await?;
    
    debug!("Vectorized operator tree built");
    
    // Create execution engine
    let mut engine = VectorExecutionEngine::new(config)
        .with_root_operator(root_op);
    
    // Execute query
    let vector_batches = engine.execute().await?;
    
    info!(
        batch_count = vector_batches.len(),
        total_rows = vector_batches.iter().map(|b| b.row_count).sum::<usize>(),
        "Vectorized query execution complete"
    );
    
    // Convert to ExecutionBatch format for QueryResult
    let execution_batches = vector_batches_to_execution_batches(vector_batches)?;
    
    // Build ExecutionQueryResult
    let row_count = execution_batches.iter().map(|b| b.row_count).sum();
    Ok(ExecutionQueryResult {
        batches: execution_batches,
        row_count,
        execution_time_ms: 0.0, // TODO: Track actual execution time
    })
}

/// Check if a query plan is supported by the vectorized execution engine
pub fn is_vectorized_supported(plan: &QueryPlan) -> bool {
    // Check if all operators in the plan are supported
    check_plan_operator_supported(&plan.root)
}

fn check_plan_operator_supported(op: &crate::query::plan::PlanOperator) -> bool {
    match op {
        crate::query::plan::PlanOperator::Scan { .. } => true,
        crate::query::plan::PlanOperator::Filter { input, .. } => {
            check_plan_operator_supported(input)
        }
        crate::query::plan::PlanOperator::Project { input, .. } => {
            check_plan_operator_supported(input)
        }
        crate::query::plan::PlanOperator::Join { left, right, .. } => {
            check_plan_operator_supported(left) && check_plan_operator_supported(right)
        }
        crate::query::plan::PlanOperator::Aggregate { input, .. } => {
            check_plan_operator_supported(input)
        }
        // Now supported
        crate::query::plan::PlanOperator::Sort { input, .. } => {
            check_plan_operator_supported(input)
        }
        crate::query::plan::PlanOperator::Limit { input, .. } => {
            check_plan_operator_supported(input)
        }
        crate::query::plan::PlanOperator::Distinct { input } => {
            check_plan_operator_supported(input)
        }
        crate::query::plan::PlanOperator::Window { .. } => false,
        crate::query::plan::PlanOperator::CTEScan { .. } => false,
        crate::query::plan::PlanOperator::DerivedTableScan { .. } => false,
        crate::query::plan::PlanOperator::BitsetJoin { .. } => false,
        crate::query::plan::PlanOperator::SetOperation { .. } => false,
        crate::query::plan::PlanOperator::Having { .. } => false,
        crate::query::plan::PlanOperator::Fused { .. } => false,
    }
}

