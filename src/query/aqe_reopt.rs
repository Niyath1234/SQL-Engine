/// Automatic Mid-Execution Re-optimization (AQE++)
/// Re-plans remaining operators if actual cardinality deviates massively from estimates
use crate::query::plan::{PlanOperator, QueryPlan};
use crate::query::cascades_optimizer::CascadesOptimizer;
use crate::query::statistics::StatisticsCatalog;
use crate::query::learned_ce::LearnedCardinalityEstimator;
use std::collections::HashMap;

/// Runtime statistics for a single operator
#[derive(Clone, Debug)]
pub struct RuntimeOperatorStats {
    pub operator_id: String,
    pub estimated_rows: usize,
    pub actual_rows: usize,
    pub execution_time_ms: f64,
    pub memory_used_bytes: usize,
}

/// Detects if mid-execution reoptimization is needed
pub fn should_reoptimize(stats: &RuntimeOperatorStats) -> bool {
    if stats.estimated_rows == 0 {
        return false; // Can't compare if no estimate
    }
    
    let ratio = stats.actual_rows as f64 / stats.estimated_rows as f64;
    
    // Replan if actual deviates by more than 10x in either direction
    ratio > 10.0 || ratio < 0.1
}

/// Reoptimize remaining plan based on runtime statistics
pub fn reoptimize_remaining_plan(
    original_plan: &QueryPlan,
    runtime_stats: &HashMap<String, RuntimeOperatorStats>,
    cascades_optimizer: &mut CascadesOptimizer,
    stats: &StatisticsCatalog,
    learned_ce: &LearnedCardinalityEstimator,
) -> Result<QueryPlan> {
    tracing::info!("Mid-execution reoptimization triggered");
    
    // Extract remaining operators (not yet executed)
    let remaining_plan = extract_remaining_plan(original_plan, runtime_stats)?;
    
    // Check if plan is too trivial to replan
    if count_operators(&remaining_plan.root) < 2 {
        tracing::debug!("Plan too trivial for reoptimization, skipping");
        return Ok(original_plan.clone());
    }
    
    // Update statistics catalog with actual runtime data
    let mut updated_stats = stats.clone();
    for (op_id, op_stats) in runtime_stats {
        // Update table statistics if this is a scan operator
        if op_id.starts_with("scan:") {
            let table_name = op_id.strip_prefix("scan:").unwrap_or(op_id);
            // Update row count estimate based on actual
            if let Some(table_stats) = updated_stats.get_table_stats_mut(table_name) {
                table_stats.row_count = op_stats.actual_rows;
            } else {
                // Create new table stats if not exists
                use crate::query::statistics::TableStatistics;
                updated_stats.table_stats.insert(
                    table_name.to_string(),
                    TableStatistics {
                        row_count: op_stats.actual_rows,
                        total_size: 0,
                        column_count: 0,
                        last_updated: Some(std::time::SystemTime::now()),
                    },
                );
            }
        }
    }
    
    // Update cascades optimizer with new statistics
    // Note: CascadesOptimizer doesn't expose stats mutably, so we create a new optimizer
    // In a full implementation, we'd add a method to update statistics
    // For now, we proceed with existing optimizer (it will use updated_stats if we pass it)
    
    // Re-run cascades optimization
    // Note: In a full implementation, we'd update cascades optimizer's statistics
    // For now, we use the existing optimizer (statistics updates would require refactoring)
    match cascades_optimizer.optimize(remaining_plan.root.clone()) {
        Ok(reoptimized_root) => {
            tracing::info!("Mid-execution reoptimization successful");
            // Create new plan with reoptimized root
            // Preserve all fields from original plan
            Ok(QueryPlan {
                root: reoptimized_root,
                estimated_cost: original_plan.estimated_cost,
                estimated_cardinality: original_plan.estimated_cardinality,
                table_aliases: original_plan.table_aliases.clone(),
                vector_index_hints: original_plan.vector_index_hints.clone(),
                btree_index_hints: original_plan.btree_index_hints.clone(),
                partition_hints: original_plan.partition_hints.clone(),
                approximate_first: original_plan.approximate_first,
                wasm_jit_hints: original_plan.wasm_jit_hints.clone(),
            })
        }
        Err(e) => {
            tracing::warn!("Mid-execution reoptimization failed: {}, using original plan", e);
            Ok(original_plan.clone())
        }
    }
}

/// Extract remaining plan (operators not yet executed)
fn extract_remaining_plan(
    plan: &QueryPlan,
    runtime_stats: &HashMap<String, RuntimeOperatorStats>,
) -> Result<QueryPlan> {
    // Traverse plan tree and remove executed operators
    let remaining_root = extract_remaining_operator(&plan.root, runtime_stats)?;
    
    Ok(QueryPlan {
        root: remaining_root,
        estimated_cost: plan.estimated_cost,
        estimated_cardinality: plan.estimated_cardinality,
        table_aliases: plan.table_aliases.clone(),
        vector_index_hints: plan.vector_index_hints.clone(),
        btree_index_hints: plan.btree_index_hints.clone(),
        partition_hints: plan.partition_hints.clone(),
        approximate_first: plan.approximate_first,
        wasm_jit_hints: plan.wasm_jit_hints.clone(),
    })
}

/// Extract remaining operators from plan tree
fn extract_remaining_operator(
    op: &PlanOperator,
    runtime_stats: &HashMap<String, RuntimeOperatorStats>,
) -> Result<PlanOperator> {
    // Generate operator ID for this operator
    let op_id = generate_operator_id(op);
    
    // Check if this operator has been executed
    if runtime_stats.contains_key(&op_id) {
        // Operator has been executed - check if we can skip it
        // For leaf operators (Scan), we can't skip - they're already done
        // For intermediate operators, we return the input if it's a pass-through
        match op {
            PlanOperator::Filter { input, .. } |
            PlanOperator::Project { input, .. } |
            PlanOperator::Sort { input, .. } |
            PlanOperator::Limit { input, .. } |
            PlanOperator::Distinct { input } |
            PlanOperator::Having { input, .. } |
            PlanOperator::Window { input, .. } => {
                // Pass-through operator - continue with input
                extract_remaining_operator(input, runtime_stats)
            }
            PlanOperator::Aggregate { input, .. } => {
                // Aggregate is not pass-through - we need to keep it
                // But if input is executed, we need to handle differently
                let remaining_input = extract_remaining_operator(input, runtime_stats)?;
                Ok(PlanOperator::Aggregate {
                    input: Box::new(remaining_input),
                    group_by: match op {
                        PlanOperator::Aggregate { group_by, .. } => group_by.clone(),
                        _ => vec![],
                    },
                    group_by_aliases: match op {
                        PlanOperator::Aggregate { group_by_aliases, .. } => group_by_aliases.clone(),
                        _ => std::collections::HashMap::new(),
                    },
                    aggregates: match op {
                        PlanOperator::Aggregate { aggregates, .. } => aggregates.clone(),
                        _ => vec![],
                    },
                    having: match op {
                        PlanOperator::Aggregate { having, .. } => having.clone(),
                        _ => None,
                    },
                })
            }
            PlanOperator::Join { left, right, edge_id, join_type, predicate } => {
                // Join - check both sides
                let remaining_left = extract_remaining_operator(left, runtime_stats)?;
                let remaining_right = extract_remaining_operator(right, runtime_stats)?;
                Ok(PlanOperator::Join {
                    left: Box::new(remaining_left),
                    right: Box::new(remaining_right),
                    edge_id: *edge_id,
                    join_type: join_type.clone(),
                    predicate: predicate.clone(),
                })
            }
            _ => {
                // Leaf operator or unknown - return as-is (shouldn't happen for executed ops)
                Ok(op.clone())
            }
        }
    } else {
        // Operator not yet executed - recurse on inputs and return
        match op {
            PlanOperator::Filter { input, predicates, where_expression } => {
                Ok(PlanOperator::Filter {
                    input: Box::new(extract_remaining_operator(input, runtime_stats)?),
                    predicates: predicates.clone(),
                    where_expression: where_expression.clone(),
                })
            }
            PlanOperator::Project { input, columns, expressions } => {
                Ok(PlanOperator::Project {
                    input: Box::new(extract_remaining_operator(input, runtime_stats)?),
                    columns: columns.clone(),
                    expressions: expressions.clone(),
                })
            }
            PlanOperator::Aggregate { input, group_by, group_by_aliases, aggregates, having } => {
                Ok(PlanOperator::Aggregate {
                    input: Box::new(extract_remaining_operator(input, runtime_stats)?),
                    group_by: group_by.clone(),
                    group_by_aliases: group_by_aliases.clone(),
                    aggregates: aggregates.clone(),
                    having: having.clone(),
                })
            }
            PlanOperator::Join { left, right, edge_id, join_type, predicate } => {
                Ok(PlanOperator::Join {
                    left: Box::new(extract_remaining_operator(left, runtime_stats)?),
                    right: Box::new(extract_remaining_operator(right, runtime_stats)?),
                    edge_id: *edge_id,
                    join_type: join_type.clone(),
                    predicate: predicate.clone(),
                })
            }
            PlanOperator::Sort { input, order_by, limit, offset } => {
                Ok(PlanOperator::Sort {
                    input: Box::new(extract_remaining_operator(input, runtime_stats)?),
                    order_by: order_by.clone(),
                    limit: *limit,
                    offset: *offset,
                })
            }
            PlanOperator::Limit { input, limit, offset } => {
                Ok(PlanOperator::Limit {
                    input: Box::new(extract_remaining_operator(input, runtime_stats)?),
                    limit: *limit,
                    offset: *offset,
                })
            }
            PlanOperator::Distinct { input } => {
                Ok(PlanOperator::Distinct {
                    input: Box::new(extract_remaining_operator(input, runtime_stats)?),
                })
            }
            PlanOperator::Having { input, predicate } => {
                Ok(PlanOperator::Having {
                    input: Box::new(extract_remaining_operator(input, runtime_stats)?),
                    predicate: predicate.clone(),
                })
            }
            PlanOperator::Window { input, window_functions } => {
                Ok(PlanOperator::Window {
                    input: Box::new(extract_remaining_operator(input, runtime_stats)?),
                    window_functions: window_functions.clone(),
                })
            }
            PlanOperator::Fused { input, operations } => {
                Ok(PlanOperator::Fused {
                    input: Box::new(extract_remaining_operator(input, runtime_stats)?),
                    operations: operations.clone(),
                })
            }
            _ => Ok(op.clone()), // Leaf operators (Scan, CTEScan, etc.) - return as-is
        }
    }
}

/// Generate a unique ID for an operator (for tracking execution)
fn generate_operator_id(op: &PlanOperator) -> String {
    match op {
        PlanOperator::Scan { table, .. } => format!("scan:{}", table),
        PlanOperator::CTEScan { cte_name, .. } => format!("cte:{}", cte_name),
        PlanOperator::DerivedTableScan { derived_table_name, .. } => format!("derived:{}", derived_table_name),
        PlanOperator::Filter { .. } => format!("filter:{}", std::ptr::addr_of!(op) as usize),
        PlanOperator::Project { .. } => format!("project:{}", std::ptr::addr_of!(op) as usize),
        PlanOperator::Aggregate { .. } => format!("aggregate:{}", std::ptr::addr_of!(op) as usize),
        PlanOperator::Join { .. } => format!("join:{}", std::ptr::addr_of!(op) as usize),
        PlanOperator::Sort { .. } => format!("sort:{}", std::ptr::addr_of!(op) as usize),
        PlanOperator::Limit { .. } => format!("limit:{}", std::ptr::addr_of!(op) as usize),
        PlanOperator::Distinct { .. } => format!("distinct:{}", std::ptr::addr_of!(op) as usize),
        PlanOperator::Having { .. } => format!("having:{}", std::ptr::addr_of!(op) as usize),
        PlanOperator::Window { .. } => format!("window:{}", std::ptr::addr_of!(op) as usize),
        PlanOperator::Fused { .. } => format!("fused:{}", std::ptr::addr_of!(op) as usize),
        _ => format!("op:{}", std::ptr::addr_of!(op) as usize),
    }
}

/// Count operators in a plan tree
fn count_operators(op: &PlanOperator) -> usize {
    match op {
        PlanOperator::Scan { .. } |
        PlanOperator::CTEScan { .. } |
        PlanOperator::DerivedTableScan { .. } => 1,
        PlanOperator::Filter { input, .. } |
        PlanOperator::Project { input, .. } |
        PlanOperator::Aggregate { input, .. } |
        PlanOperator::Sort { input, .. } |
        PlanOperator::Limit { input, .. } |
        PlanOperator::Distinct { input } |
        PlanOperator::Having { input, .. } |
        PlanOperator::Window { input, .. } |
        PlanOperator::Fused { input, .. } => 1 + count_operators(input),
        PlanOperator::Join { left, right, .. } |
        PlanOperator::BitsetJoin { left, right, .. } |
        PlanOperator::SetOperation { left, right, .. } => 1 + count_operators(left) + count_operators(right),
    }
}

use anyhow::Result;

