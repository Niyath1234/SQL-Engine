/// Planner rules for deciding when to use bitset joins
use crate::query::plan::*;
use crate::storage::bitmap_index::BitmapIndex;
use crate::hypergraph::graph::HyperGraph;
use std::collections::HashMap;
use std::sync::Arc;

/// Statistics for join planning
pub struct JoinStatistics {
    /// Estimated domain size for each column: (table, column) -> domain_size
    pub domain_sizes: HashMap<(String, String), usize>,
    /// Available bitmap indexes: (table, column) -> index
    pub bitmap_indexes: HashMap<(String, String), BitmapIndex>,
    /// Threshold for using bitset join (default: 1M)
    pub bitset_threshold: usize,
}

impl JoinStatistics {
    pub fn new() -> Self {
        Self {
            domain_sizes: HashMap::new(),
            bitmap_indexes: HashMap::new(),
            bitset_threshold: 1_000_000,
        }
    }

    /// Estimate domain size for a column
    pub fn estimate_domain(&self, table: &str, column: &str) -> usize {
        self.domain_sizes
            .get(&(table.to_string(), column.to_string()))
            .copied()
            .unwrap_or(10_000_000) // Default: assume large domain
    }

    /// Check if bitmap index exists for a column
    pub fn has_bitmap_index(&self, table: &str, column: &str) -> bool {
        self.bitmap_indexes
            .contains_key(&(table.to_string(), column.to_string()))
    }

    /// Get bitmap index for a column
    pub fn get_bitmap_index(&self, table: &str, column: &str) -> Option<&BitmapIndex> {
        self.bitmap_indexes
            .get(&(table.to_string(), column.to_string()))
    }

    /// Decide whether to use bitset join for a join predicate
    pub fn should_use_bitset_join(&self, predicate: &JoinPredicate) -> bool {
        let left_domain = self.estimate_domain(&predicate.left.0, &predicate.left.1);
        let right_domain = self.estimate_domain(&predicate.right.0, &predicate.right.1);

        // Use bitset join if:
        // 1. Both domains are below threshold
        // 2. Bitmap indexes exist for both columns
        let domains_ok = left_domain < self.bitset_threshold && right_domain < self.bitset_threshold;
        let indexes_exist = self.has_bitmap_index(&predicate.left.0, &predicate.left.1)
            && self.has_bitmap_index(&predicate.right.0, &predicate.right.1);

        domains_ok && indexes_exist
    }
}

impl Default for JoinStatistics {
    fn default() -> Self {
        Self::new()
    }
}

/// Planner rule: convert regular joins to bitset joins when beneficial
pub fn apply_bitset_join_rule(
    plan: &mut QueryPlan,
    stats: &JoinStatistics,
    graph: Arc<HyperGraph>,
) {
    // CRITICAL: Validate plan structure before optimization
    let had_joins = count_joins_in_plan(&plan.root) > 0;
    convert_joins_to_bitset(&mut plan.root, stats, graph);
    // CRITICAL: Validate plan structure after optimization - joins must be preserved
    let has_joins_after = count_joins_in_plan(&plan.root) > 0;
    if had_joins && !has_joins_after {
        panic!("CRITICAL BUG: apply_bitset_join_rule removed all joins from plan. This must never happen.");
    }
}

fn count_joins_in_plan(op: &crate::query::plan::PlanOperator) -> usize {
    use crate::query::plan::PlanOperator;
    match op {
        PlanOperator::Join { left, right, .. } | PlanOperator::BitsetJoin { left, right, .. } => {
            1 + count_joins_in_plan(left) + count_joins_in_plan(right)
        }
        PlanOperator::Filter { input, .. }
        | PlanOperator::Aggregate { input, .. }
        | PlanOperator::Project { input, .. }
        | PlanOperator::Sort { input, .. }
        | PlanOperator::Limit { input, .. }
        | PlanOperator::Distinct { input }
        | PlanOperator::Having { input, .. }
        | PlanOperator::Window { input, .. } => count_joins_in_plan(input),
        _ => 0,
    }
}

fn convert_joins_to_bitset(
    op: &mut PlanOperator,
    stats: &JoinStatistics,
    _graph: Arc<HyperGraph>,
) {
    // Use a match that doesn't borrow op while mutating it
    let should_convert = match op {
        PlanOperator::Join { predicate, .. } => {
            stats.should_use_bitset_join(predicate)
        }
        _ => false,
    };
    
    if should_convert {
        // Replace the operator
        if let PlanOperator::Join {
            left,
            right,
            edge_id,
            join_type,
            predicate,
        } = std::mem::replace(op, PlanOperator::Distinct { input: Box::new(PlanOperator::Scan { node_id: crate::hypergraph::node::NodeId(0), table: String::new(), columns: vec![], limit: None, offset: None }) })
        {
            *op = PlanOperator::BitsetJoin {
                left: left.clone(),
                right: right.clone(),
                edge_id: edge_id.clone(),
                join_type: join_type.clone(),
                predicate: predicate.clone(),
                use_bitmap: true,
            };
            
            // Recursively process children
            if let PlanOperator::BitsetJoin { left, right, .. } = op {
                convert_joins_to_bitset(left, stats, _graph.clone());
                convert_joins_to_bitset(right, stats, _graph.clone());
            }
        } else {
            // CRITICAL: If replace failed, the Join was lost! This should never happen.
            panic!("CRITICAL BUG: convert_joins_to_bitset failed to extract Join operator. Plan structure may be corrupted.");
        }
    } else {
        // Recursively process children without converting
        match op {
            PlanOperator::Join { left, right, .. } | PlanOperator::BitsetJoin { left, right, .. } => {
                convert_joins_to_bitset(left, stats, _graph.clone());
                convert_joins_to_bitset(right, stats, _graph.clone());
            }
            PlanOperator::Limit { input, .. } | PlanOperator::Sort { input, .. } | PlanOperator::Project { input, .. } | 
            PlanOperator::Filter { input, .. } | PlanOperator::Aggregate { input, .. } | PlanOperator::Distinct { input } => {
                // CRITICAL: Recursively process input to preserve join structure
                // VALIDATION: Check input type before recursion
                let input_was_join = matches!(input.as_ref(), PlanOperator::Join { .. } | PlanOperator::BitsetJoin { .. });
                convert_joins_to_bitset(input, stats, _graph.clone());
                // VALIDATION: Ensure Join wasn't replaced
                if input_was_join && matches!(input.as_ref(), PlanOperator::Scan { .. }) {
                    panic!("CRITICAL BUG: convert_joins_to_bitset replaced Join with Scan. This must never happen.");
                }
            }
            _ => {
                // Recursively process children
                if let Some(child) = get_operator_input_mut(op) {
                    convert_joins_to_bitset(child, stats, _graph.clone());
                }
            }
        }
    }
}

/// Helper to get mutable reference to operator input
fn get_operator_input_mut(op: &mut PlanOperator) -> Option<&mut Box<PlanOperator>> {
    match op {
        PlanOperator::Filter { input, .. }
        | PlanOperator::Aggregate { input, .. }
        | PlanOperator::Project { input, .. }
        | PlanOperator::Sort { input, .. }
        | PlanOperator::Limit { input, .. }
        | PlanOperator::Distinct { input }
        | PlanOperator::Having { input, .. }
        | PlanOperator::Window { input, .. } => Some(input),
        PlanOperator::Join { left, right, .. } | PlanOperator::BitsetJoin { left, right, .. } => {
            // Return left for recursion (right will be handled separately)
            Some(left)
        }
        PlanOperator::SetOperation { left, right, .. } => Some(left),
        _ => None,
    }
}

/// Hypergraph optimizer integration: identify multiway join opportunities
pub fn identify_multiway_bitset_joins(
    graph: &HyperGraph,
    stats: &JoinStatistics,
) -> Vec<Vec<(String, String)>> {
    // Find subgraphs with small domains that could benefit from bitset joins
    // This is a simplified version - full implementation would analyze hypergraph structure
    
    let mut opportunities = Vec::new();
    
    // For now, return empty - full implementation would:
    // 1. Find connected components in hypergraph
    // 2. Check if all join keys have small domains
    // 3. Check if bitmap indexes exist
    // 4. Return list of join key sets that could use multiway bitset join
    
    opportunities
}

