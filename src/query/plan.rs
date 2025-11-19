use crate::hypergraph::node::NodeId;
use crate::hypergraph::edge::EdgeId;
use crate::storage::fragment::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Query execution plan - represents how to execute a query
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryPlan {
    /// Root operator of the plan tree
    pub root: PlanOperator,
    
    /// Estimated cost
    pub estimated_cost: f64,
    
    /// Estimated cardinality
    pub estimated_cardinality: usize,
}

/// Plan operator - represents a single operation in the query plan
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PlanOperator {
    /// Scan a table/column
    Scan {
        node_id: NodeId,
        table: String,
        columns: Vec<String>,
        limit: Option<usize>,  // Push LIMIT down to scan for early termination
        offset: Option<usize>, // Push OFFSET down to scan
    },
    
    /// Join two relations
    Join {
        left: Box<PlanOperator>,
        right: Box<PlanOperator>,
        edge_id: EdgeId,
        join_type: JoinType,
        predicate: JoinPredicate,
    },
    
    /// Filter rows
    Filter {
        input: Box<PlanOperator>,
        predicates: Vec<FilterPredicate>,
    },
    
    /// Aggregate rows
    Aggregate {
        input: Box<PlanOperator>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
        having: Option<String>, // HAVING clause predicate (simplified for now)
    },
    
    /// Project columns
    Project {
        input: Box<PlanOperator>,
        columns: Vec<String>,
    },
    
    /// Sort rows
    Sort {
        input: Box<PlanOperator>,
        order_by: Vec<OrderByExpr>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    
    /// Limit rows
    Limit {
        input: Box<PlanOperator>,
        limit: usize,
        offset: usize,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinPredicate {
    pub left: (String, String),
    pub right: (String, String),
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
    Like,
    NotLike,
    In,
    NotIn,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilterPredicate {
    pub column: String,
    pub operator: PredicateOperator,
    pub value: Value,
    /// For IN/NOT IN: list of values to check against
    pub in_values: Option<Vec<Value>>,
    /// For LIKE/NOT LIKE: pattern string (supports % and _ wildcards)
    pub pattern: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub column: String,
    pub alias: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AggregateFunction {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    CountDistinct,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderByExpr {
    pub column: String,
    pub ascending: bool,
}

impl QueryPlan {
    pub fn new(root: PlanOperator) -> Self {
        Self {
            root,
            estimated_cost: 0.0,
            estimated_cardinality: 0,
        }
    }
    
    /// Optimize this plan (apply transformations)
    pub fn optimize(&mut self) {
        // Apply predicate pushdown
        self.pushdown_predicates();
        
        // Apply projection pushdown
        self.pushdown_projections();
        
        // Reorder joins
        self.reorder_joins();
    }
    
    fn pushdown_predicates(&mut self) {
        // TODO: Push filters as early as possible
    }
    
    fn pushdown_projections(&mut self) {
        // TODO: Only project needed columns
    }
    
    fn reorder_joins(&mut self) {
        // TODO: Reorder joins for optimal cost
    }
}

