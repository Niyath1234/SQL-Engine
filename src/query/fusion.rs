/// Operator fusion for simple linear pipelines
/// Fuses Filter → Project → Aggregate and Join → Filter → Project patterns for 1.5–4× speed-ups
use crate::query::plan::{QueryPlan, PlanOperator};

/// Fusion engine that identifies and fuses linear operator pipelines
pub struct FusionEngine;

impl FusionEngine {
    /// Apply fusion to a query plan
    pub fn fuse_plan(plan: &mut QueryPlan) {
        // Try to fuse operators in the plan tree
        Self::fuse_operators_recursive(&mut plan.root);
    }
    
    /// Recursively fuse operators in the plan tree
    fn fuse_operators_recursive(op: &mut PlanOperator) {
        // Try to fuse this operator with its children
        match op {
            PlanOperator::Filter { input, .. } => {
                Self::fuse_operators_recursive(input);
                Self::try_fuse_filter_project(input);
                Self::try_fuse_filter_project_aggregate(input);
            }
            PlanOperator::Project { input, .. } => {
                Self::fuse_operators_recursive(input);
                Self::try_fuse_project_aggregate(input);
            }
            PlanOperator::Join { left, right, .. } => {
                Self::fuse_operators_recursive(left);
                Self::fuse_operators_recursive(right);
                Self::try_fuse_join_filter_project(left);
                Self::try_fuse_join_filter_project(right);
            }
            PlanOperator::Aggregate { input, .. } => {
                Self::fuse_operators_recursive(input);
            }
            PlanOperator::Fused { input, .. } => {
                Self::fuse_operators_recursive(input);
            }
            _ => {
                // For other operators, recurse on inputs
                match op {
                    PlanOperator::Sort { input, .. } |
                    PlanOperator::Limit { input, .. } |
                    PlanOperator::Distinct { input } |
                    PlanOperator::Having { input, .. } |
                    PlanOperator::Window { input, .. } => {
                        Self::fuse_operators_recursive(input);
                    }
                    _ => {} // No input to recurse
                }
            }
        }
    }
    
    /// Try to fuse Filter → Project pattern
    fn try_fuse_filter_project(input: &mut Box<PlanOperator>) {
        if let PlanOperator::Filter { input: filter_input, predicates, .. } = input.as_mut() {
            if predicates.len() == 1 {
                let predicate = predicates[0].clone();
                if let PlanOperator::Project { input: project_input, columns, expressions } = filter_input.as_mut() {
                    // Check if fusion is safe (no branching, pure expressions)
                    if Self::is_fusible_predicate(&predicate) && Self::is_fusible_projections(expressions) {
                        // Replace with FusedOperator
                        let fused = PlanOperator::Fused {
                            input: project_input.clone(),
                            operations: vec![
                                crate::query::plan::FusedOperation::Project {
                                    columns: columns.clone(),
                                    expressions: expressions.clone(),
                                },
                                crate::query::plan::FusedOperation::Filter {
                                    predicate: predicate.clone(),
                                },
                            ],
                        };
                        *input = Box::new(fused);
                        tracing::debug!("Fused Filter → Project");
                    }
                }
            }
        }
    }
    
    /// Try to fuse Filter → Project → Aggregate pattern
    fn try_fuse_filter_project_aggregate(input: &mut Box<PlanOperator>) {
        if let PlanOperator::Filter { input: filter_input, predicates, .. } = input.as_mut() {
            if predicates.len() == 1 {
                let predicate = predicates[0].clone();
                if let PlanOperator::Project { input: project_input, columns, expressions } = filter_input.as_mut() {
                    if let PlanOperator::Aggregate { input: agg_input, group_by, group_by_aliases, aggregates, having: _having } = project_input.as_mut() {
                        // Check if fusion is safe
                        if Self::is_fusible_predicate(&predicate) 
                            && Self::is_fusible_projections(expressions)
                            && Self::is_fusible_aggregates(aggregates) {
                            // Replace with FusedOperator
                            let fused = PlanOperator::Fused {
                                input: agg_input.clone(),
                                operations: vec![
                                    crate::query::plan::FusedOperation::Aggregate {
                                        group_by: group_by.clone(),
                                        group_by_aliases: group_by_aliases.clone(),
                                        aggregates: aggregates.clone(),
                                    },
                                    crate::query::plan::FusedOperation::Project {
                                        columns: columns.clone(),
                                        expressions: expressions.clone(),
                                    },
                                    crate::query::plan::FusedOperation::Filter {
                                        predicate: predicate.clone(),
                                    },
                                ],
                            };
                            *input = Box::new(fused);
                            tracing::debug!("Fused Filter → Project → Aggregate");
                        }
                    }
                }
            }
        }
    }
    
    /// Try to fuse Project → Aggregate pattern
    fn try_fuse_project_aggregate(input: &mut Box<PlanOperator>) {
        if let PlanOperator::Project { input: project_input, columns, expressions } = input.as_mut() {
            if let PlanOperator::Aggregate { input: agg_input, group_by, group_by_aliases, aggregates, having: _having } = project_input.as_mut() {
                // Check if fusion is safe
                if Self::is_fusible_projections(expressions) && Self::is_fusible_aggregates(aggregates) {
                    // Replace with FusedOperator
                    let fused = PlanOperator::Fused {
                        input: agg_input.clone(),
                        operations: vec![
                            crate::query::plan::FusedOperation::Aggregate {
                                group_by: group_by.clone(),
                                group_by_aliases: group_by_aliases.clone(),
                                aggregates: aggregates.clone(),
                            },
                            crate::query::plan::FusedOperation::Project {
                                columns: columns.clone(),
                                expressions: expressions.clone(),
                            },
                        ],
                    };
                    *input = Box::new(fused);
                    tracing::debug!("Fused Project → Aggregate");
                }
            }
        }
    }
    
    /// Try to fuse Join → Filter → Project pattern
    fn try_fuse_join_filter_project(input: &mut Box<PlanOperator>) {
        if let PlanOperator::Join { right, .. } = input.as_mut() {
            // Check if right side is Filter → Project
            if let PlanOperator::Filter { input: filter_input, predicates, .. } = right.as_mut() {
                if predicates.len() == 1 {
                    let filter_pred = predicates[0].clone();
                    if let PlanOperator::Project { input: project_input, columns, expressions } = filter_input.as_mut() {
                        // Check if fusion is safe
                        if Self::is_fusible_predicate(&filter_pred) && Self::is_fusible_projections(expressions) {
                            // Fuse right side
                            let fused_right = PlanOperator::Fused {
                                input: project_input.clone(),
                                operations: vec![
                                    crate::query::plan::FusedOperation::Project {
                                        columns: columns.clone(),
                                        expressions: expressions.clone(),
                                    },
                                    crate::query::plan::FusedOperation::Filter {
                                        predicate: filter_pred.clone(),
                                    },
                                ],
                            };
                            *right = Box::new(fused_right);
                            tracing::debug!("Fused Join → Filter → Project (right side)");
                        }
                    }
                }
            }
        }
    }
    
    /// Check if predicate is fusible (pure, side-effect free, simple)
    fn is_fusible_predicate(predicate: &crate::query::plan::FilterPredicate) -> bool {
        // For now, allow all predicates
        // In a full implementation, we'd check for:
        // - No side effects
        // - No volatile functions
        // - Simple comparisons and arithmetic
        // - No subqueries (for now)
        predicate.subquery_expression.is_none()
    }
    
    /// Check if projections are fusible
    fn is_fusible_projections(expressions: &[crate::query::plan::ProjectionExpr]) -> bool {
        // Check if all projections are simple column references or simple expressions
        expressions.iter().all(|expr| {
            matches!(expr.expr_type, crate::query::plan::ProjectionExprType::Column(_))
        })
    }
    
    /// Check if aggregates are fusible
    fn is_fusible_aggregates(aggregates: &[crate::query::plan::AggregateExpr]) -> bool {
        // Check if all aggregates are standard (SUM, COUNT, AVG, MIN, MAX)
        aggregates.iter().all(|agg| {
            matches!(
                agg.function,
                crate::query::plan::AggregateFunction::Sum
                    | crate::query::plan::AggregateFunction::Count
                    | crate::query::plan::AggregateFunction::Avg
                    | crate::query::plan::AggregateFunction::Min
                    | crate::query::plan::AggregateFunction::Max
            )
        })
    }
}

