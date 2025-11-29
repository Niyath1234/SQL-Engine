/// Partition pruning: skip partitions that don't match query predicates
use crate::storage::partitioning::{PartitionManager, PartitionPredicate};
use crate::query::plan::*;
use crate::storage::fragment::Value;

/// Helper to get input operator from a plan operator
fn get_operator_input(op: &PlanOperator) -> Option<&PlanOperator> {
    match op {
        PlanOperator::Filter { input, .. }
        | PlanOperator::Aggregate { input, .. }
        | PlanOperator::Project { input, .. }
        | PlanOperator::Sort { input, .. }
        | PlanOperator::Limit { input, .. }
        | PlanOperator::Distinct { input }
        | PlanOperator::Having { input, .. }
        | PlanOperator::Window { input, .. } => Some(input),
        _ => None,
    }
}

/// Extract partition predicates from query plan
pub fn extract_partition_predicates(plan: &QueryPlan) -> Vec<PartitionPredicate> {
    let mut predicates = Vec::new();
    extract_from_operator(&plan.root, &mut predicates);
    predicates
}

fn extract_from_operator(op: &PlanOperator, predicates: &mut Vec<PartitionPredicate>) {
    match op {
        PlanOperator::Filter { predicates: filter_preds, .. } => {
            for pred in filter_preds {
                match &pred.operator {
                    PredicateOperator::Equals => {
                        predicates.push(PartitionPredicate::Equals {
                            column: pred.column.clone(),
                            value: pred.value.clone(),
                        });
                    }
                    PredicateOperator::GreaterThan | PredicateOperator::GreaterThanOrEqual => {
                        let min_inclusive = matches!(pred.operator, PredicateOperator::GreaterThanOrEqual);
                        predicates.push(PartitionPredicate::Range {
                            column: pred.column.clone(),
                            min: Some(pred.value.clone()),
                            max: None,
                            min_inclusive,
                            max_inclusive: false,
                        });
                    }
                    PredicateOperator::LessThan | PredicateOperator::LessThanOrEqual => {
                        let max_inclusive = matches!(pred.operator, PredicateOperator::LessThanOrEqual);
                        predicates.push(PartitionPredicate::Range {
                            column: pred.column.clone(),
                            min: None,
                            max: Some(pred.value.clone()),
                            min_inclusive: false,
                            max_inclusive,
                        });
                    }
                    PredicateOperator::In => {
                        if let Some(in_values) = &pred.in_values {
                            predicates.push(PartitionPredicate::In {
                                column: pred.column.clone(),
                                values: in_values.clone(),
                            });
                        }
                    }
                    _ => {}
                }
            }
        }
        PlanOperator::Scan { .. } => {
            // Scan operators don't have predicates directly
        }
        PlanOperator::Join { left, right, .. } => {
            extract_from_operator(left, predicates);
            extract_from_operator(right, predicates);
        }
        _ => {
            // Recursively extract from child operators
            if let Some(child) = get_operator_input(op) {
                extract_from_operator(child, predicates);
            }
        }
    }
}

/// Prune partitions based on query predicates
pub fn prune_partitions(
    partition_manager: &PartitionManager,
    predicates: &[PartitionPredicate],
) -> Vec<crate::storage::partitioning::PartitionId> {
    partition_manager.prune_partitions(predicates)
}

