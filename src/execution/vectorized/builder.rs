/// VectorOperatorBuilder: Builds vectorized operator trees from query plans
/// 
/// This module provides functions to convert QueryPlan (PlanOperator) into
/// vectorized operator trees (VectorOperator). This is the integration point
/// between the query planner and the new vectorized execution engine.
use crate::execution::vectorized::{
    VectorOperator, VectorScanOperator, VectorFilterOperator, VectorProjectOperator,
    VectorJoinOperator, VectorAggregateOperator, VectorSortOperator, VectorLimitOperator,
    VectorDistinctOperator,
};
use crate::error::EngineResult;
use crate::query::plan::{PlanOperator, JoinType, JoinPredicate};
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::NodeId;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, debug};

/// Build a vectorized operator tree from a query plan
pub fn build_vector_operator_tree(
    plan: &PlanOperator,
    graph: Arc<HyperGraph>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = EngineResult<Arc<Mutex<Box<dyn VectorOperator>>>>> + Send + '_>> {
    Box::pin(build_vector_operator_tree_impl(plan, graph))
}

/// Internal implementation (recursive, boxed)
async fn build_vector_operator_tree_impl(
    plan: &PlanOperator,
    graph: Arc<HyperGraph>,
) -> EngineResult<Arc<Mutex<Box<dyn VectorOperator>>>> {
    match plan {
        PlanOperator::Scan { node_id, table, columns, limit, offset } => {
            build_scan_operator(*node_id, table.clone(), columns.clone(), graph, *limit, *offset).await
        }
        PlanOperator::Filter { input, predicates, .. } => {
            let input_op = build_vector_operator_tree(input, graph.clone()).await?;
            build_filter_operator(input_op, predicates.clone()).await
        }
        PlanOperator::Project { input, columns, expressions } => {
            let input_op = build_vector_operator_tree(input, graph.clone()).await?;
            build_project_operator(input_op, columns.clone(), expressions.clone()).await
        }
        PlanOperator::Join { left, right, join_type, predicate, .. } => {
            let left_op = build_vector_operator_tree(left, graph.clone()).await?;
            let right_op = build_vector_operator_tree(right, graph.clone()).await?;
            build_join_operator(left_op, right_op, join_type.clone(), predicate.clone()).await
        }
        PlanOperator::Aggregate { input, group_by, aggregates, .. } => {
            let input_op = build_vector_operator_tree(input, graph.clone()).await?;
            build_aggregate_operator(input_op, group_by.clone(), aggregates.clone()).await
        }
        PlanOperator::Sort { input, order_by, .. } => {
            let input_op = build_vector_operator_tree(input, graph.clone()).await?;
            build_sort_operator(input_op, order_by.clone()).await
        }
        PlanOperator::Limit { input, limit, offset } => {
            let input_op = build_vector_operator_tree(input, graph.clone()).await?;
            build_limit_operator(input_op, *limit, *offset).await
        }
        PlanOperator::Distinct { input } => {
            let input_op = build_vector_operator_tree(input, graph.clone()).await?;
            build_distinct_operator(input_op).await
        }
        _ => {
            Err(crate::error::EngineError::execution(format!(
                "PlanOperator {:?} not yet supported in vectorized execution",
                plan
            )))
        }
    }
}

/// Build a VectorScanOperator from a Scan plan operator
async fn build_scan_operator(
    node_id: NodeId,
    table: String,
    columns: Vec<String>,
    graph: Arc<HyperGraph>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> EngineResult<Arc<Mutex<Box<dyn VectorOperator>>>> {
    info!(
        node_id = node_id.0,
        table = %table,
        columns = ?columns,
        "Building vector scan operator"
    );
    
    let operator = VectorScanOperator::new(
        table,
        node_id,
        columns,
        graph,
        limit,
        offset,
        None, // max_scan_rows
    );
    
    Ok(Arc::new(Mutex::new(Box::new(operator))))
}

/// Build a VectorFilterOperator from a Filter plan operator
async fn build_filter_operator(
    input: Arc<Mutex<Box<dyn VectorOperator>>>,
    predicates: Vec<crate::query::plan::FilterPredicate>,
) -> EngineResult<Arc<Mutex<Box<dyn VectorOperator>>>> {
    info!(
        predicate_count = predicates.len(),
        "Building vector filter operator"
    );
    
    let operator = VectorFilterOperator::new(input, predicates);
    
    Ok(Arc::new(Mutex::new(Box::new(operator))))
}

/// Build a VectorProjectOperator from a Project plan operator
async fn build_project_operator(
    input: Arc<Mutex<Box<dyn VectorOperator>>>,
    columns: Vec<String>,
    expressions: Vec<crate::query::plan::ProjectionExpr>,
) -> EngineResult<Arc<Mutex<Box<dyn VectorOperator>>>> {
    info!(
        column_count = columns.len(),
        expression_count = expressions.len(),
        "Building vector project operator"
    );
    
    let operator = VectorProjectOperator::new(input, columns, expressions);
    
    Ok(Arc::new(Mutex::new(Box::new(operator))))
}

/// Build a VectorJoinOperator from a Join plan operator
async fn build_join_operator(
    left: Arc<Mutex<Box<dyn VectorOperator>>>,
    right: Arc<Mutex<Box<dyn VectorOperator>>>,
    join_type: JoinType,
    predicate: JoinPredicate,
) -> EngineResult<Arc<Mutex<Box<dyn VectorOperator>>>> {
    info!(
        join_type = ?join_type,
        predicate = ?predicate,
        "Building vector join operator"
    );
    
    let operator = VectorJoinOperator::new(left, right, join_type, predicate);
    
    Ok(Arc::new(Mutex::new(Box::new(operator))))
}

/// Build a VectorAggregateOperator from an Aggregate plan operator
async fn build_aggregate_operator(
    input: Arc<Mutex<Box<dyn VectorOperator>>>,
    group_by: Vec<String>,
    aggregates: Vec<crate::query::plan::AggregateExpr>,
) -> EngineResult<Arc<Mutex<Box<dyn VectorOperator>>>> {
    info!(
        group_by_count = group_by.len(),
        aggregate_count = aggregates.len(),
        "Building vector aggregate operator"
    );
    
    let operator = VectorAggregateOperator::new(input, group_by, aggregates);
    
    Ok(Arc::new(Mutex::new(Box::new(operator))))
}

/// Build a VectorSortOperator from a Sort plan operator
async fn build_sort_operator(
    input: Arc<Mutex<Box<dyn VectorOperator>>>,
    order_by: Vec<crate::query::plan::OrderByExpr>,
) -> EngineResult<Arc<Mutex<Box<dyn VectorOperator>>>> {
    info!(
        order_by_count = order_by.len(),
        "Building vector sort operator"
    );
    
    let operator = VectorSortOperator::new(input, order_by);
    
    Ok(Arc::new(Mutex::new(Box::new(operator))))
}

/// Build a VectorLimitOperator from a Limit plan operator
async fn build_limit_operator(
    input: Arc<Mutex<Box<dyn VectorOperator>>>,
    limit: usize,
    offset: usize,
) -> EngineResult<Arc<Mutex<Box<dyn VectorOperator>>>> {
    info!(
        limit = limit,
        offset = offset,
        "Building vector limit operator"
    );
    
    let operator = VectorLimitOperator::new(input, limit, offset);
    
    Ok(Arc::new(Mutex::new(Box::new(operator))))
}

/// Build a VectorDistinctOperator from a Distinct plan operator
async fn build_distinct_operator(
    input: Arc<Mutex<Box<dyn VectorOperator>>>,
) -> EngineResult<Arc<Mutex<Box<dyn VectorOperator>>>> {
    info!("Building vector distinct operator");
    
    let operator = VectorDistinctOperator::new(input);
    
    Ok(Arc::new(Mutex::new(Box::new(operator))))
}

/// Helper: Convert VectorBatch results to ExecutionBatch format (for backward compatibility)
pub fn vector_batches_to_execution_batches(
    vector_batches: Vec<crate::execution::vectorized::VectorBatch>,
) -> EngineResult<Vec<crate::execution::batch::ExecutionBatch>> {
    vector_batches
        .iter()
        .map(|vb| vb.to_execution_batch())
        .collect()
}

