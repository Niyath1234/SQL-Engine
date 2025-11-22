use crate::hypergraph::node::NodeId;
use crate::hypergraph::edge::EdgeId;
use crate::storage::fragment::Value;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Query execution plan - represents how to execute a query
#[derive(Clone, Debug)]
pub struct QueryPlan {
    /// Root operator of the plan tree
    pub root: PlanOperator,
    
    /// Estimated cost
    pub estimated_cost: f64,
    
    /// Estimated cardinality
    pub estimated_cardinality: usize,
    
    /// Table alias mapping: alias -> actual table name (e.g., "d" -> "documents")
    pub table_aliases: std::collections::HashMap<String, String>,
    
    /// Vector index usage hints: (table, column) -> whether to use vector index
    pub vector_index_hints: std::collections::HashMap<(String, String), bool>,
}

/// Plan operator - represents a single operation in the query plan
#[derive(Clone, Debug)]
pub enum PlanOperator {
    /// Scan a table/column
    Scan {
        node_id: NodeId,
        table: String,
        columns: Vec<String>,
        limit: Option<usize>,  // Push LIMIT down to scan for early termination
        offset: Option<usize>, // Push OFFSET down to scan
    },
    
    /// Scan a CTE (Common Table Expression) - reads from cached results
    CTEScan {
        cte_name: String,
        columns: Vec<String>,
        limit: Option<usize>,
        offset: Option<usize>,
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
        /// Expressions to evaluate for projection (for CAST, functions, etc.)
        expressions: Vec<ProjectionExpr>,
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
    
    /// Set operation (UNION, INTERSECT, EXCEPT)
    SetOperation {
        left: Box<PlanOperator>,
        right: Box<PlanOperator>,
        operation: SetOperationType,
    },
    
    /// DISTINCT operator
    Distinct {
        input: Box<PlanOperator>,
    },
    
    /// HAVING operator - filters groups after aggregation
    Having {
        input: Box<PlanOperator>,
        predicate: crate::query::expression::Expression,
    },
    
    /// Window operator - computes window functions
    Window {
        input: Box<PlanOperator>,
        window_functions: Vec<WindowFunctionExpr>,
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
    IsNull,
    IsNotNull,
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

#[derive(Clone, Debug)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub column: String,
    pub alias: Option<String>,
    /// Target data type if column is wrapped in CAST expression
    pub cast_type: Option<arrow::datatypes::DataType>,
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

#[derive(Clone, Debug)]
pub struct ProjectionExpr {
    /// Column name or alias for the output
    pub alias: String,
    /// Type of expression
    pub expr_type: ProjectionExprType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SetOperationType {
    Union,
    UnionAll,
    Intersect,
    Except,
}

#[derive(Clone, Debug)]
pub enum ProjectionExprType {
    /// Simple column reference
    Column(String),
    /// CAST expression
    Cast {
        column: String,
        target_type: arrow::datatypes::DataType,
    },
    /// CASE expression (evaluated at runtime)
    Case(crate::query::expression::Expression),
    /// Function expression (evaluated at runtime)
    Function(crate::query::expression::Expression),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderByExpr {
    pub column: String,
    pub ascending: bool,
}

/// Window function expression
#[derive(Clone, Debug)]
pub struct WindowFunctionExpr {
    pub function: WindowFunction,
    pub column: Option<String>, // Column for aggregate windows
    pub alias: Option<String>,
    pub partition_by: Vec<String>,
    pub order_by: Vec<OrderByExpr>,
    pub frame: Option<WindowFrame>,
}

#[derive(Clone, Debug)]
pub enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    Lag { offset: usize },
    Lead { offset: usize },
    SumOver,
    AvgOver,
    MinOver,
    MaxOver,
    CountOver,
    FirstValue,
    LastValue,
}

#[derive(Clone, Debug)]
pub struct WindowFrame {
    pub frame_type: FrameType,
    pub start: FrameBound,
    pub end: Option<FrameBound>,
}

#[derive(Clone, Debug)]
pub enum FrameType {
    Rows,
    Range,
}

#[derive(Clone, Debug)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(usize),
    CurrentRow,
    Following(usize),
    UnboundedFollowing,
}

impl QueryPlan {
    pub fn new(root: PlanOperator) -> Self {
        Self {
            root,
            estimated_cost: 0.0,
            estimated_cardinality: 0,
            table_aliases: std::collections::HashMap::new(),
            vector_index_hints: std::collections::HashMap::new(),
        }
    }
    
    pub fn with_table_aliases(mut self, table_aliases: std::collections::HashMap<String, String>) -> Self {
        self.table_aliases = table_aliases;
        self
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
    
    /// Push filters down the plan tree to execute them as early as possible
    /// This reduces the number of rows processed in later operators
    fn pushdown_predicates(&mut self) {
        self.root = self.pushdown_predicates_recursive(self.root.clone());
    }
    
    fn pushdown_predicates_recursive(&self, op: PlanOperator) -> PlanOperator {
        match op {
            PlanOperator::Filter { input, predicates } => {
                let optimized_input = self.pushdown_predicates_recursive(*input);
                
                // Try to push predicates down to the input operator
                match optimized_input {
                    PlanOperator::Scan { node_id, table, ref columns, limit, offset } => {
                        // Push predicates that reference columns in this scan
                        let columns_set: HashSet<String> = columns.iter().cloned().collect();
                        let (pushable_predicates, remaining_predicates): (Vec<_>, Vec<_>) = 
                            predicates.into_iter().partition(|p| {
                                // Check if predicate column is in the scan's columns
                                columns_set.contains(&p.column)
                            });
                        
                        // If all predicates can be pushed down, we keep Filter close to Scan
                        // (In full implementation, we'd integrate into Scan operator)
                        if remaining_predicates.is_empty() && !pushable_predicates.is_empty() {
                            PlanOperator::Filter {
                                input: Box::new(PlanOperator::Scan { 
                                    node_id, table, columns: columns.clone(), limit, offset 
                                }),
                                predicates: pushable_predicates,
                            }
                        } else if !pushable_predicates.is_empty() {
                            // Some predicates pushed down, some remain
                            PlanOperator::Filter {
                                input: Box::new(PlanOperator::Scan { 
                                    node_id, table, columns: columns.clone(), limit, offset 
                                }),
                                predicates: remaining_predicates,
                            }
                        } else {
                            // No predicates can be pushed down
                            PlanOperator::Filter {
                                input: Box::new(PlanOperator::Scan { 
                                    node_id, table, columns: columns.clone(), limit, offset 
                                }),
                                predicates: remaining_predicates,
                            }
                        }
                    }
                    PlanOperator::Join { left, right, edge_id, join_type, predicate } => {
                        // For joins, push predicates to the appropriate side
                        let left_columns = self.get_columns_from_operator(&left);
                        let right_columns = self.get_columns_from_operator(&right);
                        
                        let (left_predicates, right_predicates, remaining_predicates): 
                            (Vec<_>, Vec<_>, Vec<_>) = predicates.into_iter().fold(
                                (Vec::new(), Vec::new(), Vec::new()),
                                |(mut left, mut right, mut remain), p| {
                                    if left_columns.contains(&p.column) {
                                        left.push(p);
                                    } else if right_columns.contains(&p.column) {
                                        right.push(p);
                                    } else {
                                        remain.push(p);
                                    }
                                    (left, right, remain)
                                }
                            );
                        
                        let optimized_left = if !left_predicates.is_empty() {
                            Box::new(PlanOperator::Filter {
                                input: left,
                                predicates: left_predicates,
                            })
                        } else {
                            left
                        };
                        
                        let optimized_right = if !right_predicates.is_empty() {
                            Box::new(PlanOperator::Filter {
                                input: right,
                                predicates: right_predicates,
                            })
                        } else {
                            right
                        };
                        
                        if remaining_predicates.is_empty() {
                            // All predicates pushed down
                            PlanOperator::Join {
                                left: optimized_left,
                                right: optimized_right,
                                edge_id,
                                join_type,
                                predicate,
                            }
                        } else {
                            // Wrap join with remaining predicates
                            PlanOperator::Filter {
                                input: Box::new(PlanOperator::Join {
                                    left: optimized_left,
                                    right: optimized_right,
                                    edge_id,
                                    join_type,
                                    predicate,
                                }),
                                predicates: remaining_predicates,
                            }
                        }
                    }
                    other => {
                        // For other operators, push predicates as far as possible
                        PlanOperator::Filter {
                            input: Box::new(other),
                            predicates,
                        }
                    }
                }
            }
            PlanOperator::Project { input, columns, expressions } => {
                // Push predicates through projections
                PlanOperator::Project {
                    input: Box::new(self.pushdown_predicates_recursive(*input)),
                    columns,
                    expressions,
                }
            }
            PlanOperator::Aggregate { input, group_by, aggregates, having } => {
                // Can't push predicates through aggregation (they become HAVING)
                PlanOperator::Aggregate {
                    input: Box::new(self.pushdown_predicates_recursive(*input)),
                    group_by,
                    aggregates,
                    having,
                }
            }
            PlanOperator::Join { left, right, edge_id, join_type, predicate } => {
                PlanOperator::Join {
                    left: Box::new(self.pushdown_predicates_recursive(*left)),
                    right: Box::new(self.pushdown_predicates_recursive(*right)),
                    edge_id,
                    join_type,
                    predicate,
                }
            }
            PlanOperator::Sort { input, order_by, limit, offset } => {
                PlanOperator::Sort {
                    input: Box::new(self.pushdown_predicates_recursive(*input)),
                    order_by,
                    limit,
                    offset,
                }
            }
            PlanOperator::Limit { input, limit, offset } => {
                PlanOperator::Limit {
                    input: Box::new(self.pushdown_predicates_recursive(*input)),
                    limit,
                    offset,
                }
            }
            PlanOperator::Distinct { input } => {
                PlanOperator::Distinct {
                    input: Box::new(self.pushdown_predicates_recursive(*input)),
                }
            }
            PlanOperator::SetOperation { left, right, operation } => {
                PlanOperator::SetOperation {
                    left: Box::new(self.pushdown_predicates_recursive(*left)),
                    right: Box::new(self.pushdown_predicates_recursive(*right)),
                    operation,
                }
            }
            PlanOperator::Having { input, predicate } => {
                PlanOperator::Having {
                    input: Box::new(self.pushdown_predicates_recursive(*input)),
                    predicate,
                }
            }
            other => other, // Scan and other leaf operators
        }
    }
    
    /// Get column names from an operator (used for predicate pushdown)
    fn get_columns_from_operator(&self, op: &PlanOperator) -> HashSet<String> {
        match op {
            PlanOperator::Scan { columns, .. } => columns.iter().cloned().collect(),
            PlanOperator::Project { columns, .. } => columns.iter().cloned().collect(),
            PlanOperator::Aggregate { group_by, .. } => group_by.iter().cloned().collect(),
            PlanOperator::Join { left, right, .. } => {
                let mut cols = self.get_columns_from_operator(left);
                cols.extend(self.get_columns_from_operator(right));
                cols
            }
            _ => std::collections::HashSet::new(),
        }
    }
    
    /// Push projections down the plan tree to only project needed columns
    /// This reduces the amount of data processed in intermediate operators
    fn pushdown_projections(&mut self) {
        // First, determine which columns are actually needed from the root
        let required_columns = self.get_required_columns(&self.root);
        
        // Then push projections down based on required columns
        self.root = self.pushdown_projections_recursive(self.root.clone(), &required_columns);
    }
    
    /// Get columns that are actually needed from this operator
    fn get_required_columns(&self, op: &PlanOperator) -> HashSet<String> {
        match op {
            PlanOperator::Project { columns, .. } => {
                columns.iter().cloned().collect()
            }
            PlanOperator::Aggregate { group_by, aggregates, .. } => {
                let mut required = group_by.iter().cloned().collect::<HashSet<_>>();
                for agg in aggregates {
                    required.insert(agg.column.clone());
                }
                required
            }
            PlanOperator::Sort { order_by, .. } => {
                order_by.iter().map(|o| o.column.clone()).collect()
            }
            PlanOperator::Filter { predicates, .. } => {
                predicates.iter().map(|p| p.column.clone()).collect()
            }
            PlanOperator::Join { left, right, .. } => {
                let mut required = self.get_required_columns(left);
                required.extend(self.get_required_columns(right));
                required
            }
            PlanOperator::Scan { columns, .. } => {
                columns.iter().cloned().collect()
            }
            _ => std::collections::HashSet::new(),
        }
    }
    
    fn pushdown_projections_recursive(
        &self,
        op: PlanOperator,
        required_columns: &HashSet<String>,
    ) -> PlanOperator {
        match op {
            PlanOperator::Scan { node_id, table, ref columns, limit, offset } => {
                // Only scan required columns
                let filtered_columns: Vec<String> = columns
                    .iter()
                    .filter(|col| required_columns.contains(*col))
                    .cloned()
                    .collect();
                
                // If no columns needed, still scan at least one column (to avoid empty scans)
                let final_columns = if filtered_columns.is_empty() && !required_columns.is_empty() {
                    // Fallback: scan first required column if available
                    vec![required_columns.iter().next().cloned().unwrap_or_else(|| {
                        columns.first().cloned().unwrap_or_else(|| "id".to_string())
                    })]
                } else if filtered_columns.is_empty() {
                    columns.clone() // Keep original if no requirements
                } else {
                    filtered_columns
                };
                
                PlanOperator::Scan {
                    node_id,
                    table,
                    columns: final_columns,
                    limit,
                    offset,
                }
            }
            PlanOperator::Project { input, columns, expressions } => {
                // Determine which columns are needed from input
                let input_required: HashSet<String> = columns
                    .iter()
                    .cloned()
                    .collect();
                
                // Add columns referenced in expressions
                let mut expr_required = input_required.clone();
                for expr in &expressions {
                    match &expr.expr_type {
                        ProjectionExprType::Column(col) => {
                            expr_required.insert(col.clone());
                        }
                        _ => {} // Other expression types would require deeper analysis
                    }
                }
                
                // Optimize input with required columns
                let optimized_input = self.pushdown_projections_recursive(*input, &expr_required);
                
                // Only keep columns that are in the required set
                let filtered_columns: Vec<String> = columns
                    .into_iter()
                    .filter(|col| required_columns.contains(col))
                    .collect();
                
                let filtered_expressions: Vec<ProjectionExpr> = expressions
                    .into_iter()
                    .filter(|expr| required_columns.contains(&expr.alias))
                    .collect();
                
                PlanOperator::Project {
                    input: Box::new(optimized_input),
                    columns: filtered_columns,
                    expressions: filtered_expressions,
                }
            }
            PlanOperator::Join { left, right, edge_id, join_type, predicate } => {
                // For joins, need columns from both sides for join condition and output
                let join_cols: HashSet<String> = HashSet::from_iter(vec![
                    predicate.left.1.clone(),
                    predicate.right.1.clone(),
                ]);
                
                let mut left_required = required_columns.clone();
                left_required.insert(predicate.left.1.clone());
                
                let mut right_required = required_columns.clone();
                right_required.insert(predicate.right.1.clone());
                
                PlanOperator::Join {
                    left: Box::new(self.pushdown_projections_recursive(*left, &left_required)),
                    right: Box::new(self.pushdown_projections_recursive(*right, &right_required)),
                    edge_id,
                    join_type,
                    predicate,
                }
            }
            PlanOperator::Filter { input, predicates } => {
                // Filters need the columns referenced in predicates
                let mut filter_required = required_columns.clone();
                for pred in &predicates {
                    filter_required.insert(pred.column.clone());
                }
                
                PlanOperator::Filter {
                    input: Box::new(self.pushdown_projections_recursive(*input, &filter_required)),
                    predicates,
                }
            }
            PlanOperator::Aggregate { input, group_by, aggregates, having } => {
                // Aggregates need group_by columns and aggregate columns
                let mut agg_required: HashSet<String> = group_by.iter().cloned().collect();
                for agg in &aggregates {
                    agg_required.insert(agg.column.clone());
                }
                
                PlanOperator::Aggregate {
                    input: Box::new(self.pushdown_projections_recursive(*input, &agg_required)),
                    group_by,
                    aggregates,
                    having,
                }
            }
            PlanOperator::Sort { input, order_by, limit, offset } => {
                // Sort needs order_by columns
                let mut sort_required = required_columns.clone();
                for ord in &order_by {
                    sort_required.insert(ord.column.clone());
                }
                
                PlanOperator::Sort {
                    input: Box::new(self.pushdown_projections_recursive(*input, &sort_required)),
                    order_by,
                    limit,
                    offset,
                }
            }
            other => other, // Other operators pass through
        }
    }
    
    fn reorder_joins(&mut self) {
        // TODO: Reorder joins for optimal cost
        // This requires cost-based optimization which is more complex
        // The optimizer.rs already has join reordering logic that should be integrated here
    }
}

