/// Index selection: choose optimal indexes for query execution
use crate::storage::btree_index::BTreeIndex;
use crate::storage::fragment::Value;
use crate::query::plan::*;
use std::collections::HashMap;

/// Index selection result
pub struct IndexSelection {
    /// Selected B-tree indexes: (table, column) -> index
    pub btree_indexes: HashMap<(String, String), BTreeIndex>,
    /// Whether to use index for each scan
    pub use_index: HashMap<String, bool>,
}

impl IndexSelection {
    pub fn new() -> Self {
        Self {
            btree_indexes: HashMap::new(),
            use_index: HashMap::new(),
        }
    }

    /// Select indexes for a query plan
    pub fn select_indexes(plan: &QueryPlan, available_indexes: &HashMap<(String, String), BTreeIndex>) -> Self {
        let mut selection = Self::new();
        select_from_operator(&plan.root, available_indexes, &mut selection);
        selection
    }
}

fn select_from_operator(
    op: &PlanOperator,
    available_indexes: &HashMap<(String, String), BTreeIndex>,
    selection: &mut IndexSelection,
) {
    match op {
        PlanOperator::Scan { table, columns, .. } => {
            // Check if any column has an index
            // Note: predicates are typically in Filter operators above Scan
            // We'll check for indexes that could be useful for common patterns
            for column in columns {
                let key = (table.clone(), column.clone());
                if let Some(index) = available_indexes.get(&key) {
                    // If index exists, mark it as potentially useful
                    // Actual usage decision will be made during execution
                    selection.btree_indexes.insert(key.clone(), index.clone());
                    selection.use_index.insert(format!("{}.{}", table, column), true);
                }
            }
        }
        PlanOperator::Filter { predicates, input, .. } => {
            // Check if filter can use index
            for pred in predicates {
                // Extract table and column from predicate
                // Note: FilterPredicate may need table info - for now assume it's in the scan
                let column = &pred.column;
                // Try to find matching index by checking all available indexes
                for ((table, col), index) in available_indexes {
                    if col == column {
                        let can_use = matches!(
                            pred.operator,
                            PredicateOperator::Equals
                                | PredicateOperator::GreaterThan
                                | PredicateOperator::GreaterThanOrEqual
                                | PredicateOperator::LessThan
                                | PredicateOperator::LessThanOrEqual
                        );
                        
                        if can_use {
                            let key = (table.clone(), col.clone());
                            selection.btree_indexes.insert(key.clone(), index.clone());
                            selection.use_index.insert(format!("{}.{}", table, col), true);
                        }
                    }
                }
            }
            select_from_operator(input, available_indexes, selection);
        }
        PlanOperator::Join { left, right, .. } => {
            select_from_operator(left, available_indexes, selection);
            select_from_operator(right, available_indexes, selection);
        }
        PlanOperator::Aggregate { input, .. }
        | PlanOperator::Project { input, .. }
        | PlanOperator::Sort { input, .. }
        | PlanOperator::Limit { input, .. } => {
            select_from_operator(input, available_indexes, selection);
        }
        _ => {}
    }
}

/// Estimate index benefit for a predicate
pub fn estimate_index_benefit(
    index: &BTreeIndex,
    predicate: &crate::query::plan::FilterPredicate,
) -> f64 {
    use crate::query::plan::PredicateOperator;
    
    match &predicate.operator {
        PredicateOperator::Equals => {
            // Point lookup: very beneficial
            if let Some(rows) = index.lookup(&predicate.value) {
                // Benefit is inverse of selectivity
                let selectivity = rows.len() as f64 / index.entry_count.max(1) as f64;
                if selectivity > 0.0 {
                    return 1.0 / selectivity;
                }
            }
            1.0
        }
        PredicateOperator::GreaterThan
        | PredicateOperator::GreaterThanOrEqual
        | PredicateOperator::LessThan
        | PredicateOperator::LessThanOrEqual => {
            // Range scan: estimate selectivity
            let min = if matches!(
                predicate.operator,
                PredicateOperator::GreaterThan | PredicateOperator::GreaterThanOrEqual
            ) {
                Some(&predicate.value)
            } else {
                None
            };
            let max = if matches!(
                predicate.operator,
                PredicateOperator::LessThan | PredicateOperator::LessThanOrEqual
            ) {
                Some(&predicate.value)
            } else {
                None
            };
            
            let selectivity = index.estimate_selectivity(min, max);
            if selectivity > 0.0 && selectivity < 1.0 {
                return 1.0 / selectivity;
            }
            1.0
        }
        _ => 0.0, // Index not useful for other operators
    }
}

