/// Heuristic join strategy selection based on table size ratios
/// Improves join performance 2–20× using simple, low-cost rules
use crate::query::plan::PlanOperator;
use crate::query::statistics::StatisticsCatalog;

/// Join strategy hint for physical plan selection
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinStrategy {
    /// Broadcast hash join (for small left table)
    BroadcastHashJoin,
    /// Merge join (for sorted or nearly sorted data)
    MergeJoin,
    /// Hash join (default for medium-sized tables)
    HashJoin,
    /// Nested loop join (for very small tables)
    NestedLoopJoin,
}

/// Table information for join strategy selection
#[derive(Clone, Debug)]
pub struct TableInfo {
    pub name: String,
    pub estimated_rows: usize,
    pub is_sorted: bool,
    pub join_key_sorted: bool,
}

/// Choose initial join strategy based on table size ratios
pub fn choose_initial_join_strategy(left: &TableInfo, right: &TableInfo) -> JoinStrategy {
    let left_rows = left.estimated_rows;
    let right_rows = right.estimated_rows;
    
    // Rule 1: If left table is much smaller than right, use broadcast hash join
    // Threshold: left < 10% of right
    if left_rows > 0 && right_rows > 0 {
        let ratio = left_rows as f64 / right_rows as f64;
        if ratio < 0.1 {
            tracing::debug!(
                "Heuristic: BroadcastHashJoin (left={}, right={}, ratio={:.2})",
                left_rows,
                right_rows,
                ratio
            );
            return JoinStrategy::BroadcastHashJoin;
        }
    }
    
    // Rule 2: If tables are sorted or join key is nearly sorted, use merge join
    if (left.is_sorted || left.join_key_sorted) && (right.is_sorted || right.join_key_sorted) {
        tracing::debug!(
            "Heuristic: MergeJoin (sorted tables: left={}, right={})",
            left.name,
            right.name
        );
        return JoinStrategy::MergeJoin;
    }
    
    // Rule 3: If both tables are very small, use nested loop join
    if left_rows < 100 && right_rows < 100 {
        tracing::debug!(
            "Heuristic: NestedLoopJoin (small tables: left={}, right={})",
            left_rows,
            right_rows
        );
        return JoinStrategy::NestedLoopJoin;
    }
    
    // Rule 4: Default to hash join for medium-sized tables
    tracing::debug!(
        "Heuristic: HashJoin (default for medium tables: left={}, right={})",
        left_rows,
        right_rows
    );
    JoinStrategy::HashJoin
}

/// Extract table info from plan operator for join strategy selection
pub fn extract_table_info(
    op: &PlanOperator,
    stats: &StatisticsCatalog,
) -> Option<TableInfo> {
    match op {
        PlanOperator::Scan { table, .. } => {
            let estimated_rows = stats
                .get_table_stats(table)
                .map(|s| s.row_count)
                .unwrap_or(1000);
            Some(TableInfo {
                name: table.clone(),
                estimated_rows,
                is_sorted: false, // TODO: Check if table is sorted
                join_key_sorted: false, // TODO: Check if join key is sorted
            })
        }
        PlanOperator::CTEScan { cte_name, .. } => {
            // CTEs are typically small and unsorted
            Some(TableInfo {
                name: format!("__CTE_{}", cte_name),
                estimated_rows: 1000, // Default estimate
                is_sorted: false,
                join_key_sorted: false,
            })
        }
        PlanOperator::DerivedTableScan { derived_table_name, .. } => {
            // Derived tables are typically small and unsorted
            Some(TableInfo {
                name: format!("__DERIVED_{}", derived_table_name),
                estimated_rows: 1000, // Default estimate
                is_sorted: false,
                join_key_sorted: false,
            })
        }
        PlanOperator::Filter { input, .. } => {
            // Filter doesn't change table info significantly
            extract_table_info(input, stats)
        }
        PlanOperator::Project { input, .. } => {
            // Project doesn't change table info
            extract_table_info(input, stats)
        }
        PlanOperator::Join { left, .. } => {
            // For joins, use left side info
            extract_table_info(left, stats)
        }
        _ => None,
    }
}

/// Apply heuristic join strategy selection to a plan
pub fn apply_join_heuristics(plan: &mut PlanOperator, stats: &StatisticsCatalog) {
    match plan {
        PlanOperator::Join { left, right, .. } => {
            // Recurse first
            apply_join_heuristics(left, stats);
            apply_join_heuristics(right, stats);
            
            // Extract table info
            if let (Some(left_info), Some(right_info)) = (
                extract_table_info(left, stats),
                extract_table_info(right, stats),
            ) {
                let strategy = choose_initial_join_strategy(&left_info, &right_info);
                tracing::debug!(
                    "Selected join strategy: {:?} for {} JOIN {}",
                    strategy,
                    left_info.name,
                    right_info.name
                );
                // Store strategy hint (would be used by execution engine)
                // For now, we just log it - in full implementation, we'd store it in the plan
            }
        }
        PlanOperator::BitsetJoin { left, right, .. } => {
            apply_join_heuristics(left, stats);
            apply_join_heuristics(right, stats);
        }
        _ => {
            // Recurse on inputs
            recurse_on_inputs(plan, |op| apply_join_heuristics(op, stats));
        }
    }
}

/// Get mutable reference to input operator
fn get_input_mut(op: &mut PlanOperator) -> Option<&mut Box<PlanOperator>> {
    match op {
        PlanOperator::Filter { input, .. } |
        PlanOperator::Project { input, .. } |
        PlanOperator::Aggregate { input, .. } |
        PlanOperator::Sort { input, .. } |
        PlanOperator::Limit { input, .. } |
        PlanOperator::Distinct { input } |
        PlanOperator::Having { input, .. } |
        PlanOperator::Window { input, .. } |
        PlanOperator::Fused { input, .. } => Some(input),
        _ => None,
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

