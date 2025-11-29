/// Filter debloating: predicate fusion and normalization
/// Speeds up WHERE clause evaluation by merging predicates
use crate::query::plan::{PlanOperator, FilterPredicate, PredicateOperator};
use crate::storage::fragment::Value;
use std::collections::HashSet;

/// Apply filter debloating optimization
pub fn apply_filter_debloating(plan: &mut PlanOperator) {
    // Combine adjacent filters
    combine_adjacent_filters(plan);
    
    // Normalize predicates
    normalize_predicates(plan);
    
    // Reorder predicates for optimal evaluation
    reorder_predicates(plan);
}

/// Combine adjacent Filter operators into one
fn combine_adjacent_filters(op: &mut PlanOperator) {
    match op {
        PlanOperator::Filter { input, predicates, where_expression } => {
            // Recurse first
            combine_adjacent_filters(input);
            
            // If input is also a Filter, combine them
            if let PlanOperator::Filter {
                input: inner_input,
                predicates: inner_predicates,
                where_expression: inner_where_expression,
            } = input.as_mut()
            {
                // Combine predicates
                let mut combined_predicates = inner_predicates.clone();
                combined_predicates.extend(predicates.clone());
                
                // Combine WHERE expressions (if both present, use AND)
                let combined_where_expression = match (where_expression, inner_where_expression) {
                    (Some(ref outer), Some(ref inner)) => {
                        // Create AND expression
                        use crate::query::expression::Expression;
                        Some(Expression::BinaryOp {
                            left: Box::new(inner.clone()),
                            op: crate::query::expression::BinaryOperator::And,
                            right: Box::new(outer.clone()),
                        })
                    }
                    (Some(ref outer), None) => Some(outer.clone()),
                    (None, Some(ref inner)) => Some(inner.clone()),
                    (None, None) => None,
                };
                
                // Replace with combined filter
                *op = PlanOperator::Filter {
                    input: inner_input.clone(),
                    predicates: combined_predicates,
                    where_expression: combined_where_expression,
                };
                
                tracing::debug!("Combined adjacent Filter operators");
            }
        }
        _ => {
            // Recurse on inputs
            recurse_on_inputs(op, combine_adjacent_filters);
        }
    }
}

/// Normalize predicates: remove redundant and merge compatible ones
fn normalize_predicates(op: &mut PlanOperator) {
    match op {
        PlanOperator::Filter { predicates, .. } => {
            let mut normalized: Vec<FilterPredicate> = Vec::new();
            
            // Group predicates by column
            let mut by_column: std::collections::HashMap<String, Vec<FilterPredicate>> =
                std::collections::HashMap::new();
            
            for pred in predicates.iter() {
                by_column
                    .entry(pred.column.clone())
                    .or_insert_with(Vec::new)
                    .push(pred.clone());
            }
            
            // Normalize each column's predicates
            for (column, mut col_preds) in by_column {
                // Sort predicates by operator type
                col_preds.sort_by_key(|p| format!("{:?}", p.operator));
                
                // Apply normalization rules
                let normalized_col_preds = normalize_column_predicates(&column, col_preds);
                normalized.extend(normalized_col_preds);
            }
            
            *predicates = normalized;
        }
        _ => {
            recurse_on_inputs(op, normalize_predicates);
        }
    }
}

/// Normalize predicates for a single column
fn normalize_column_predicates(
    column: &str,
    mut predicates: Vec<FilterPredicate>,
) -> Vec<FilterPredicate> {
    if predicates.is_empty() {
        return vec![];
    }
    
    // Separate by operator type
    let mut greater_than = Vec::new();
    let mut less_than = Vec::new();
    let mut equals = Vec::new();
    let mut others = Vec::new();
    
    for pred in predicates {
        match pred.operator {
            PredicateOperator::GreaterThan | PredicateOperator::GreaterThanOrEqual => {
                greater_than.push(pred);
            }
            PredicateOperator::LessThan | PredicateOperator::LessThanOrEqual => {
                less_than.push(pred);
            }
            PredicateOperator::Equals => {
                equals.push(pred);
            }
            _ => others.push(pred),
        }
    }
    
    let mut result = Vec::new();
    
    // Rule: (col > 5) AND (col > 3) → (col > 5)
    if !greater_than.is_empty() {
        let max_greater = greater_than
            .iter()
            .max_by_key(|p| extract_numeric_value(&p.value))
            .cloned();
        if let Some(pred) = max_greater {
            result.push(pred);
        }
    }
    
    // Rule: (col < 10) AND (col < 7) → (col < 7)
    if !less_than.is_empty() {
        let min_less = less_than
            .iter()
            .min_by_key(|p| extract_numeric_value(&p.value))
            .cloned();
        if let Some(pred) = min_less {
            result.push(pred);
        }
    }
    
    // Equals predicates: if multiple, they're contradictory (unless IN)
    if !equals.is_empty() {
        result.extend(equals);
    }
    
    // Other predicates (IN, LIKE, etc.)
    result.extend(others);
    
    result
}

/// Extract numeric value from Value for comparison
fn extract_numeric_value(value: &Value) -> i64 {
    match value {
        Value::Int64(v) => *v,
        Value::Int32(v) => *v as i64,
        Value::Float64(v) => *v as i64,
        Value::Float32(v) => *v as i64,
        _ => 0,
    }
}

/// Reorder predicates to evaluate cheapest first
fn reorder_predicates(op: &mut PlanOperator) {
    match op {
        PlanOperator::Filter { predicates, .. } => {
            // Sort predicates by evaluation cost
            predicates.sort_by_key(|p| {
                // Cost estimation:
                // 0: Constant comparisons (cheapest)
                // 1: Selective predicates
                // 2: LIKE/pattern matching (more expensive)
                // 3: IN with many values (expensive)
                match p.operator {
                    PredicateOperator::Equals => 0,
                    PredicateOperator::GreaterThan
                    | PredicateOperator::LessThan
                    | PredicateOperator::GreaterThanOrEqual
                    | PredicateOperator::LessThanOrEqual => 0,
                    PredicateOperator::In => {
                        if let Some(ref in_values) = p.in_values {
                            if in_values.len() > 10 {
                                3
                            } else {
                                1
                            }
                        } else {
                            2
                        }
                    }
                    PredicateOperator::Like | PredicateOperator::NotLike => 2,
                    _ => 1,
                }
            });
        }
        _ => {
            recurse_on_inputs(op, reorder_predicates);
        }
    }
}

/// Recurse on operator inputs
fn recurse_on_inputs<F>(op: &mut PlanOperator, f: F)
where
    F: Fn(&mut PlanOperator),
{
    match op {
        PlanOperator::Filter { input, .. } |
        PlanOperator::Project { input, .. } |
        PlanOperator::Aggregate { input, .. } |
        PlanOperator::Sort { input, .. } |
        PlanOperator::Limit { input, .. } |
        PlanOperator::Distinct { input } |
        PlanOperator::Having { input, .. } |
        PlanOperator::Window { input, .. } |
        PlanOperator::Fused { input, .. } => {
            f(input);
        }
        PlanOperator::Join { left, right, .. } |
        PlanOperator::BitsetJoin { left, right, .. } => {
            f(left);
            f(right);
        }
        PlanOperator::SetOperation { left, right, .. } => {
            f(left);
            f(right);
        }
        _ => {}
    }
}

