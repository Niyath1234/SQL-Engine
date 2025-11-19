use crate::hypergraph::node::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// An edge in the hypergraph represents a join relationship
/// Edges connect nodes and store join predicates
/// Note: Cannot fully serialize due to MaterializedJoin containing non-serializable types
#[derive(Clone, Debug)]
pub struct HyperEdge {
    /// Unique edge ID
    pub id: EdgeId,
    
    /// Source node ID
    pub source: NodeId,
    
    /// Target node ID
    pub target: NodeId,
    
    /// Join type
    pub join_type: JoinType,
    
    /// Join predicate (e.g., "left.region_id = right.id")
    pub predicate: JoinPredicate,
    
    /// Statistics about this edge
    pub stats: EdgeStatistics,
    
    /// Whether this edge is materialized (precomputed)
    pub is_materialized: bool,
    
    /// Cached join result (if materialized)
    pub materialized_result: Option<MaterializedJoin>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EdgeId(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinPredicate {
    /// Left side: (table, column)
    pub left: (String, String),
    
    /// Right side: (table, column)
    pub right: (String, String),
    
    /// Operator (usually Equals)
    pub operator: PredicateOperator,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PredicateOperator {
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeStatistics {
    /// Estimated cardinality of join result
    pub cardinality: usize,
    
    /// Selectivity estimate (0.0 to 1.0)
    pub selectivity: f64,
    
    /// Average fan-out (rows in target per row in source)
    pub avg_fanout: f64,
    
    /// Last update timestamp
    pub last_updated: u64,
}

/// Materialized join result (for precomputed joins)
#[derive(Clone, Debug)]
pub struct MaterializedJoin {
    /// Map from left key to list of right row indices
    pub join_map: HashMap<crate::storage::fragment::Value, Vec<usize>>,
    
    /// Total number of result rows
    pub result_count: usize,
}

impl HyperEdge {
    pub fn new(
        id: EdgeId,
        source: NodeId,
        target: NodeId,
        join_type: JoinType,
        predicate: JoinPredicate,
    ) -> Self {
        Self {
            id,
            source,
            target,
            join_type,
            predicate,
            stats: EdgeStatistics {
                cardinality: 0,
                selectivity: 1.0,
                avg_fanout: 1.0,
                last_updated: 0,
            },
            is_materialized: false,
            materialized_result: None,
        }
    }
    
    /// Materialize this edge (precompute join result)
    pub fn materialize(&mut self) {
        // TODO: Precompute join and store in materialized_result
        self.is_materialized = true;
    }
    
    /// Check if this edge matches a given predicate
    pub fn matches_predicate(&self, left_table: &str, left_col: &str, right_table: &str, right_col: &str) -> bool {
        self.predicate.left.0 == left_table
            && self.predicate.left.1 == left_col
            && self.predicate.right.0 == right_table
            && self.predicate.right.1 == right_col
    }
}

