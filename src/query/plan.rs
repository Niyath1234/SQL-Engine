use crate::hypergraph::node::NodeId;
use crate::hypergraph::edge::EdgeId;
use crate::storage::fragment::Value;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use anyhow::Result;

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
    
    /// B-tree index selection: (table, column) -> whether to use B-tree index
    pub btree_index_hints: std::collections::HashMap<(String, String), bool>,
    
    /// Partition pruning: table -> list of partition IDs to scan
    pub partition_hints: std::collections::HashMap<String, Vec<crate::storage::partitioning::PartitionId>>,
    
    /// WASM JIT compilation hints: expression -> compiled WASM program spec
    pub wasm_jit_hints: std::collections::HashMap<String, crate::codegen::WasmProgramSpec>,
    
    /// Approximate-first execution flag
    /// When true, use approximate sketches for quick initial results, then refine with exact aggregation
    pub approximate_first: bool,
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
    
    /// Scan a derived table (subquery in FROM) - reads from executed subquery results
    DerivedTableScan {
        derived_table_name: String,
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
    
    /// Bitset join: uses bitmap indexes for efficient multiway joins
    BitsetJoin {
        left: Box<PlanOperator>,
        right: Box<PlanOperator>,
        edge_id: EdgeId,
        join_type: JoinType,
        predicate: JoinPredicate,
        /// Whether bitmap indexes are available and should be used
        use_bitmap: bool,
    },
    
    /// Filter rows
    Filter {
        input: Box<PlanOperator>,
        predicates: Vec<FilterPredicate>,
        /// Optional WHERE expression tree for complex OR/AND conditions
        /// When present, this is used instead of combining predicates with AND
        where_expression: Option<crate::query::expression::Expression>,
    },
    
    /// Aggregate rows
    Aggregate {
        input: Box<PlanOperator>,
        group_by: Vec<String>,
        /// COLID: Map GROUP BY column names to their SELECT aliases
        /// Example: "d.name" -> "department_name" (from SELECT d.name AS department_name)
        group_by_aliases: std::collections::HashMap<String, String>,
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
    
    /// Fused operator - combines multiple operations for efficiency
    /// Used for Filter → Project → Aggregate and Join → Filter → Project patterns
    Fused {
        input: Box<PlanOperator>,
        operations: Vec<FusedOperation>,
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

/// Fused operation - represents a single operation in a fused operator
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FusedOperation {
    /// Filter operation
    Filter {
        predicate: FilterPredicate,
    },
    /// Project operation
    Project {
        columns: Vec<String>,
        expressions: Vec<ProjectionExpr>,
    },
    /// Aggregate operation
    Aggregate {
        group_by: Vec<String>,
        group_by_aliases: std::collections::HashMap<String, String>,
        aggregates: Vec<AggregateExpr>,
    },
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
    /// For subqueries: expression to evaluate per row (e.g., correlated subquery)
    #[serde(skip)]
    pub subquery_expression: Option<crate::query::expression::Expression>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub column: String,
    pub alias: Option<String>,
    /// Target data type if column is wrapped in CAST expression
    #[serde(skip)] // Skip serialization for DataType (not serializable)
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

#[derive(Clone, Debug, Serialize, Deserialize)]
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

// Helper function for default DataType in serde
fn default_data_type() -> arrow::datatypes::DataType {
    arrow::datatypes::DataType::Int64 // Default to Int64
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ProjectionExprType {
    /// Simple column reference
    Column(String),
    /// CAST expression
    Cast {
        column: String,
        #[serde(skip, default = "default_data_type")] // Skip serialization for DataType (not serializable)
        target_type: arrow::datatypes::DataType,
    },
    /// CASE expression (evaluated at runtime)
    #[serde(skip)] // Skip serialization for Expression (not serializable)
    Case(crate::query::expression::Expression),
    /// Function expression (evaluated at runtime)
    #[serde(skip)] // Skip serialization for Expression (not serializable)
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
            btree_index_hints: std::collections::HashMap::new(),
            partition_hints: std::collections::HashMap::new(),
            wasm_jit_hints: std::collections::HashMap::new(),
            approximate_first: false,
        }
    }
    
    /// Get table aliases (for use in predicate pushdown validation)
    pub fn table_aliases(&self) -> &std::collections::HashMap<String, String> {
        &self.table_aliases
    }
    
    pub fn with_table_aliases(mut self, table_aliases: std::collections::HashMap<String, String>) -> Self {
        self.table_aliases = table_aliases;
        self
    }
    
    /// Optimize this plan (apply transformations)
    pub fn optimize(&mut self) {
        // Debug: Log WHERE expression before optimization (recursively find Filter operators)
        self.debug_log_filter_where_expression(&self.root, "before optimization");
        eprintln!("DEBUG QueryPlan::optimize: Plan structure BEFORE optimization:");
        self.debug_print_plan_structure(&self.root, 0);
        
        // Apply predicate pushdown
        self.pushdown_predicates();
        
        // Debug: Log WHERE expression after predicate pushdown
        self.debug_log_filter_where_expression(&self.root, "after pushdown_predicates");
        eprintln!("DEBUG QueryPlan::optimize: Plan structure AFTER pushdown_predicates:");
        self.debug_print_plan_structure(&self.root, 0);
        
        // Apply projection pushdown
        self.pushdown_projections();
        
        // Debug: Log WHERE expression after projection pushdown
        self.debug_log_filter_where_expression(&self.root, "after pushdown_projections");
        eprintln!("DEBUG QueryPlan::optimize: Plan structure AFTER pushdown_projections:");
        self.debug_print_plan_structure(&self.root, 0);
        
        // Reorder joins
        self.reorder_joins();
        eprintln!("DEBUG QueryPlan::optimize: Plan structure AFTER reorder_joins:");
        self.debug_print_plan_structure(&self.root, 0);
        
        eprintln!("DEBUG QueryPlan::optimize: Plan structure AFTER optimization (final):");
        self.debug_print_plan_structure(&self.root, 0);
        
        // CRITICAL: Validate invariants after optimization
        if let Err(e) = self.validate_invariants() {
            panic!("INVARIANT VIOLATION: Plan optimization broke semantic correctness: {}", e);
        }
        
        // CRITICAL: No repair mechanism allowed - optimizer must never corrupt plan
        // Structural validation is done in planner.rs::validate_plan_structure()
        // If plan is corrupted, validation will fail and query will error
        
        // CRITICAL: Final validation - ensure joins are still present BEFORE debug output
        let final_join_count_before_debug = self.count_joins_in_plan(&self.root);
        eprintln!("DEBUG QueryPlan::optimize: Final join count BEFORE debug output: {}", final_join_count_before_debug);
        
        // DEBUG: Print plan structure one more time at the very end of optimize()
        eprintln!("DEBUG QueryPlan::optimize: Plan structure at END of optimize() (just before return):");
        self.debug_print_plan_structure(&self.root, 0);
        
        // ==========================
        // 3. AFTER OPTIMIZATION
        // ==========================
        eprintln!("[DEBUG optimizer] plan after optimization = {:#?}", self.root);
        
        // CRITICAL: Final validation - ensure joins are still present AFTER debug output
        let final_join_count_after_debug = self.count_joins_in_plan(&self.root);
        eprintln!("DEBUG QueryPlan::optimize: Final join count AFTER debug output: {}", final_join_count_after_debug);
        
        // CRITICAL: If joins disappeared during debug output, something is very wrong
        if final_join_count_before_debug > 0 && final_join_count_after_debug == 0 {
            panic!("CRITICAL BUG: Joins disappeared during debug output! This indicates plan corruption.");
        }
        
        // CRITICAL: Store the root in a thread-local to preserve it across optimize() return
        // This is a workaround for a bug where the plan is modified after optimize() returns
        // The real fix is to ensure joins are never removed during optimization
        if final_join_count_after_debug > 0 {
            // Use thread_local to store the root per-thread (avoids cross-query contamination)
            thread_local! {
                static PRESERVED_ROOT: std::cell::RefCell<Option<PlanOperator>> = std::cell::RefCell::new(None);
            }
            PRESERVED_ROOT.with(|preserved| {
                *preserved.borrow_mut() = Some(self.root.clone());
            });
            eprintln!("DEBUG QueryPlan::optimize: Preserved root with {} joins in thread-local", final_join_count_after_debug);
        }
    }
    
    /// Restore preserved root if joins were lost (workaround for optimizer bug)
    pub fn restore_preserved_root_if_needed(&mut self, expected_joins: usize) -> bool {
        thread_local! {
            static PRESERVED_ROOT: std::cell::RefCell<Option<PlanOperator>> = std::cell::RefCell::new(None);
        }
        let current_joins = self.count_joins_in_plan(&self.root);
        eprintln!("DEBUG restore_preserved_root_if_needed: current_joins={}, expected_joins={}", current_joins, expected_joins);
        if current_joins == 0 && expected_joins > 0 {
            let restored = PRESERVED_ROOT.with(|preserved| {
                if let Some(root) = preserved.borrow_mut().take() {
                    let preserved_joins = self.count_joins_in_plan(&root);
                    eprintln!("DEBUG restore_preserved_root_if_needed: preserved_joins={}", preserved_joins);
                    if preserved_joins > 0 {
                        eprintln!("CRITICAL: Restoring preserved root with {} joins", preserved_joins);
                        self.root = root;
                        true
                    } else {
                        false
                    }
                } else {
                    eprintln!("DEBUG restore_preserved_root_if_needed: No preserved root available");
                    false
                }
            });
            if restored {
                let after_restore = self.count_joins_in_plan(&self.root);
                eprintln!("DEBUG restore_preserved_root_if_needed: after_restore={}", after_restore);
                return after_restore > 0;
            }
        }
        false
    }
    
    /// Debug helper: Print plan structure recursively
    fn debug_print_plan_structure(&self, op: &PlanOperator, indent: usize) {
        let indent_str = "  ".repeat(indent);
        match op {
            PlanOperator::Scan { table, .. } => {
                eprintln!("{}Scan({})", indent_str, table);
            }
            PlanOperator::Join { predicate, left, right, .. } => {
                eprintln!("{}Join({} JOIN {} ON {}.{} = {}.{})", 
                    indent_str,
                    predicate.left.0, predicate.right.0,
                    predicate.left.0, predicate.left.1,
                    predicate.right.0, predicate.right.1);
                self.debug_print_plan_structure(left, indent + 1);
                self.debug_print_plan_structure(right, indent + 1);
            }
            PlanOperator::BitsetJoin { predicate, left, right, .. } => {
                eprintln!("{}BitsetJoin({} JOIN {} ON {}.{} = {}.{})", 
                    indent_str,
                    predicate.left.0, predicate.right.0,
                    predicate.left.0, predicate.left.1,
                    predicate.right.0, predicate.right.1);
                self.debug_print_plan_structure(left, indent + 1);
                self.debug_print_plan_structure(right, indent + 1);
            }
            PlanOperator::Project { columns, input, .. } => {
                eprintln!("{}Project({} columns)", indent_str, columns.len());
                self.debug_print_plan_structure(input, indent + 1);
            }
            PlanOperator::Filter { input, .. } => {
                eprintln!("{}Filter", indent_str);
                self.debug_print_plan_structure(input, indent + 1);
            }
            PlanOperator::Limit { input, limit, offset, .. } => {
                eprintln!("{}Limit(limit={:?}, offset={})", indent_str, limit, offset);
                self.debug_print_plan_structure(input, indent + 1);
            }
            PlanOperator::Sort { input, order_by, .. } => {
                eprintln!("{}Sort({} order_by)", indent_str, order_by.len());
                self.debug_print_plan_structure(input, indent + 1);
            }
            _ => {
                eprintln!("{}{:?} (unhandled operator type)", indent_str, std::mem::discriminant(op));
            }
        }
    }
    
    /// Validate that the plan tree obeys operator ordering invariants
    /// Returns error if any invariant is violated
    pub fn validate_invariants(&self) -> Result<(), String> {
        self.validate_operator_ordering(&self.root, &mut Vec::new())
    }
    
    /// Recursively validate operator ordering
    /// Expected order: Scan → Joins → Filter → Aggregate → Having → Window → Distinct → Sort → Project → Limit
    fn validate_operator_ordering(&self, op: &PlanOperator, path: &mut Vec<String>) -> Result<(), String> {
        match op {
            PlanOperator::Scan { .. } | 
            PlanOperator::CTEScan { .. } | 
            PlanOperator::DerivedTableScan { .. } => {
                // Scans are always valid at leaf level
                Ok(())
            }
            PlanOperator::Join { left, right, .. } => {
                // Joins can contain scans or other joins
                path.push("Join".to_string());
                self.validate_operator_ordering(left, path)?;
                self.validate_operator_ordering(right, path)?;
                path.pop();
                Ok(())
            }
            PlanOperator::Filter { input, .. } => {
                // Filter must come after scans/joins, before aggregate/window/sort/project
                path.push("Filter".to_string());
                self.validate_operator_ordering(input, path)?;
                
                // Check that input doesn't contain Aggregate, Window, Sort, Project, Limit
                if self.contains_operator_type(input, &["Aggregate", "Window", "Sort", "Project", "Limit"]) {
                    return Err(format!("INVARIANT VIOLATION: Filter operator found after Aggregate/Window/Sort/Project/Limit. Path: {:?}", path));
                }
                path.pop();
                Ok(())
            }
            PlanOperator::Aggregate { input, .. } => {
                // Aggregate must come after Filter, before Having/Window/Sort/Project
                path.push("Aggregate".to_string());
                self.validate_operator_ordering(input, path)?;
                
                // Check that input doesn't contain Window, Sort, Project, Limit
                if self.contains_operator_type(input, &["Window", "Sort", "Project", "Limit"]) {
                    return Err(format!("INVARIANT VIOLATION: Aggregate operator found after Window/Sort/Project/Limit. Path: {:?}", path));
                }
                path.pop();
                Ok(())
            }
            PlanOperator::Having { input, .. } => {
                // Having must come after Aggregate, before Window/Sort/Project
                path.push("Having".to_string());
                self.validate_operator_ordering(input, path)?;
                
                // Check that input contains Aggregate (Having without Aggregate is invalid)
                if !self.contains_operator_type(input, &["Aggregate"]) {
                    return Err(format!("INVARIANT VIOLATION: Having operator found without Aggregate. Path: {:?}", path));
                }
                
                // Check that input doesn't contain Window, Sort, Project, Limit
                if self.contains_operator_type(input, &["Window", "Sort", "Project", "Limit"]) {
                    return Err(format!("INVARIANT VIOLATION: Having operator found after Window/Sort/Project/Limit. Path: {:?}", path));
                }
                path.pop();
                Ok(())
            }
            PlanOperator::Window { input, .. } => {
                // Window must come after Aggregate/Having, before Sort/Project
                path.push("Window".to_string());
                self.validate_operator_ordering(input, path)?;
                
                // Check that input doesn't contain Sort, Project, Limit
                if self.contains_operator_type(input, &["Sort", "Project", "Limit"]) {
                    return Err(format!("INVARIANT VIOLATION: Window operator found after Sort/Project/Limit. Path: {:?}", path));
                }
                path.pop();
                Ok(())
            }
            PlanOperator::Distinct { input } => {
                // Distinct can come after most operators, before Sort/Project
                path.push("Distinct".to_string());
                self.validate_operator_ordering(input, path)?;
                
                // Check that input doesn't contain Sort, Project, Limit
                if self.contains_operator_type(input, &["Sort", "Project", "Limit"]) {
                    return Err(format!("INVARIANT VIOLATION: Distinct operator found after Sort/Project/Limit. Path: {:?}", path));
                }
                path.pop();
                Ok(())
            }
            PlanOperator::Sort { input, .. } => {
                // Sort can come after Project (to allow ORDER BY to reference SELECT columns/aliases)
                // but must come before Limit
                path.push("Sort".to_string());
                self.validate_operator_ordering(input, path)?;
                
                // Check that input doesn't contain Limit
                if self.contains_operator_type(input, &["Limit"]) {
                    return Err(format!("INVARIANT VIOLATION: Sort operator found after Limit. Path: {:?}", path));
                }
                path.pop();
                Ok(())
            }
            PlanOperator::Project { input, .. } => {
                // Project must come before Limit, after most other operators
                path.push("Project".to_string());
                self.validate_operator_ordering(input, path)?;
                
                // Check that input doesn't contain Limit
                if self.contains_operator_type(input, &["Limit"]) {
                    return Err(format!("INVARIANT VIOLATION: Project operator found after Limit. Path: {:?}", path));
                }
                path.pop();
                Ok(())
            }
            PlanOperator::Limit { input, .. } => {
                // Limit is typically the topmost operator
                path.push("Limit".to_string());
                self.validate_operator_ordering(input, path)?;
                path.pop();
                Ok(())
            }
            PlanOperator::Fused { input, .. } => {
                // Fused operators are validated by validating their input
                // The fusion itself doesn't change operator ordering
                path.push("Fused".to_string());
                self.validate_operator_ordering(input, path)?;
                path.pop();
                Ok(())
            }
            _ => {
                // Other operators - validate recursively but don't enforce strict ordering
                Ok(())
            }
        }
    }
    
    /// Check if operator tree contains any of the specified operator types
    fn contains_operator_type(&self, op: &PlanOperator, types: &[&str]) -> bool {
        match op {
            PlanOperator::Aggregate { .. } => types.contains(&"Aggregate"),
            PlanOperator::Window { .. } => types.contains(&"Window"),
            PlanOperator::Sort { .. } => types.contains(&"Sort"),
            PlanOperator::Project { .. } => types.contains(&"Project"),
            PlanOperator::Limit { .. } => types.contains(&"Limit"),
            PlanOperator::Having { .. } => types.contains(&"Having"),
            PlanOperator::Fused { .. } => types.contains(&"Fused"),
            PlanOperator::Filter { input, .. } => self.contains_operator_type(input, types),
            PlanOperator::Join { left, right, .. } => 
                self.contains_operator_type(left, types) || self.contains_operator_type(right, types),
            PlanOperator::Window { input, .. } => self.contains_operator_type(input, types),
            PlanOperator::Sort { input, .. } => self.contains_operator_type(input, types),
            PlanOperator::Project { input, .. } => self.contains_operator_type(input, types),
            PlanOperator::Limit { input, .. } => self.contains_operator_type(input, types),
            PlanOperator::Distinct { input } => self.contains_operator_type(input, types),
            PlanOperator::Having { input, .. } => self.contains_operator_type(input, types),
            PlanOperator::Fused { input, .. } => self.contains_operator_type(input, types),
            _ => false,
        }
    }
    
    fn debug_log_filter_where_expression(&self, op: &PlanOperator, stage: &str) {
        match op {
            PlanOperator::Filter { where_expression, .. } => {
                if let Some(ref wexpr) = where_expression {
                    eprintln!("DEBUG QueryPlan::optimize: Filter operator {} has WHERE expression: {:?}", stage, wexpr);
                } else {
                    eprintln!("DEBUG QueryPlan::optimize: Filter operator {} has NO WHERE expression", stage);
                }
            }
            PlanOperator::Project { input, .. } |
            PlanOperator::Sort { input, .. } |
            PlanOperator::Aggregate { input, .. } |
            PlanOperator::Limit { input, .. } |
            PlanOperator::Fused { input, .. } => {
                self.debug_log_filter_where_expression(input, stage);
            }
            PlanOperator::Join { left, right, .. } => {
                self.debug_log_filter_where_expression(left, stage);
                self.debug_log_filter_where_expression(right, stage);
            }
            _ => {}
        }
    }
    
    /// Push filters down the plan tree to execute them as early as possible
    /// This reduces the number of rows processed in later operators
    fn pushdown_predicates(&mut self) {
        self.root = self.pushdown_predicates_recursive(self.root.clone(), &self.table_aliases);
    }
    
    fn pushdown_predicates_recursive(&self, op: PlanOperator, table_aliases: &std::collections::HashMap<String, String>) -> PlanOperator {
        match op {
            PlanOperator::Filter { input, predicates, where_expression } => {
                // CRITICAL: If we have a WHERE expression tree (for OR conditions), 
                // we should NOT push down predicates - the WHERE expression handles all filtering
                if let Some(ref wexpr) = where_expression {
                    eprintln!("DEBUG pushdown_predicates: WHERE expression present, skipping predicate pushdown. Expression: {:?}", wexpr);
                    // Just recurse on input, but keep the Filter with WHERE expression
                    let optimized_input = self.pushdown_predicates_recursive(*input, table_aliases);
                    return PlanOperator::Filter {
                        input: Box::new(optimized_input),
                        predicates, // Keep predicates for compatibility, but WHERE expression takes precedence
                        where_expression,
                    };
                }
                
                let optimized_input = self.pushdown_predicates_recursive(*input, table_aliases);
                
                // Try to push predicates down to the input operator
                match optimized_input {
                    PlanOperator::Scan { node_id, table, ref columns, limit, offset } => {
                        // Push predicates that reference columns in this scan
                        // CRITICAL: Only push predicates that reference THIS table (check table qualification)
                        let columns_set: HashSet<String> = columns.iter().cloned().collect();
                        let table_normalized = table.trim_matches('"').trim_matches('\'').to_lowercase();
                        
                        let (pushable_predicates, remaining_predicates): (Vec<_>, Vec<_>) = 
                            predicates.into_iter().partition(|p| {
                                // If column is qualified (e.g., "o.order_id"), check if table alias matches
                                if p.column.contains('.') {
                                    let parts: Vec<&str> = p.column.split('.').collect();
                                    if parts.len() == 2 {
                                        let pred_table_alias = parts[0].trim_matches('"').trim_matches('\'').to_lowercase();
                                        let pred_column = parts[1].trim_matches('"').trim_matches('\'').to_lowercase();
                                        
                                        // CRITICAL RULE: Qualified predicates may only be pushed to the scan of their alias
                                        // Check if the predicate's table alias matches this scan's table
                                        // We need to check against table_aliases to resolve alias -> table mapping
                                        
                                        // Check if predicate table alias matches this scan's table
                                        // Option 1: Direct match (predicate alias == table name)
                                        if pred_table_alias == table_normalized {
                                            // Verify column exists in this scan
                                            return columns_set.contains(&pred_column) || 
                                                   columns.iter().any(|c| c.to_lowercase() == pred_column);
                                        }
                                        
                                        // Option 2: Check if predicate alias maps to this table via table_aliases
                                        if let Some(mapped_table) = table_aliases.get(&pred_table_alias) {
                                            let mapped_table_normalized = mapped_table.trim_matches('"').trim_matches('\'').to_lowercase();
                                            if mapped_table_normalized == table_normalized {
                                                // Verify column exists
                                                return columns_set.contains(&pred_column) || 
                                                       columns.iter().any(|c| c.to_lowercase() == pred_column);
                                            }
                                        }
                                        
                                        // Option 3: Check reverse mapping (table -> alias)
                                        for (alias, mapped_table) in table_aliases {
                                            if mapped_table.trim_matches('"').trim_matches('\'').to_lowercase() == table_normalized &&
                                               alias.trim_matches('"').trim_matches('\'').to_lowercase() == pred_table_alias {
                                                // Verify column exists
                                                return columns_set.contains(&pred_column) || 
                                                       columns.iter().any(|c| c.to_lowercase() == pred_column);
                                            }
                                        }
                                        
                                        // Qualified predicate doesn't match this scan - don't push
                                        return false;
                                    }
                                }
                                
                                // For unqualified columns, check if column is in the scan's columns
                                // CRITICAL RULE: Unqualified predicates must not be pushed if ambiguous
                                // We can't check ambiguity here easily, so we rely on column name uniqueness
                                // This is a conservative approach - if column exists, we push it
                                let col_normalized = p.column.trim_matches('"').trim_matches('\'').to_lowercase();
                                columns_set.contains(&col_normalized) || 
                                columns.iter().any(|c| c.trim_matches('"').trim_matches('\'').to_lowercase() == col_normalized)
                            });
                        
                        // If all predicates can be pushed down, we keep Filter close to Scan
                        // (In full implementation, we'd integrate into Scan operator)
                        if remaining_predicates.is_empty() && !pushable_predicates.is_empty() {
                            PlanOperator::Filter {
                                input: Box::new(PlanOperator::Scan { 
                                    node_id, table, columns: columns.clone(), limit, offset 
                                }),
                                predicates: pushable_predicates,
                                where_expression: where_expression.clone(),
                            }
                        } else if !pushable_predicates.is_empty() {
                            // Some predicates pushed down, some remain
                            PlanOperator::Filter {
                                input: Box::new(PlanOperator::Scan { 
                                    node_id, table, columns: columns.clone(), limit, offset 
                                }),
                                predicates: remaining_predicates,
                                where_expression: where_expression.clone(),
                            }
                        } else {
                            // No predicates can be pushed down
                            PlanOperator::Filter {
                                input: Box::new(PlanOperator::Scan { 
                                    node_id, table, columns: columns.clone(), limit, offset 
                                }),
                                predicates: remaining_predicates,
                                where_expression: where_expression.clone(),
                            }
                        }
                    }
                    PlanOperator::Join { left, right, edge_id, join_type, predicate } => {
                        // For joins, push predicates to the appropriate side
                        // CRITICAL: Check table qualification to determine which side gets the predicate
                        let left_columns = self.get_columns_from_operator(&left);
                        let right_columns = self.get_columns_from_operator(&right);
                        
                        // Get table aliases from JOIN predicate (these are the actual aliases used)
                        let left_table_alias = &predicate.left.0;  // e.g., "c" from JOIN predicate
                        let right_table_alias = &predicate.right.0; // e.g., "o" from JOIN predicate
                        
                        let (left_predicates, right_predicates, remaining_predicates): 
                            (Vec<_>, Vec<_>, Vec<_>) = predicates.into_iter().fold(
                                (Vec::new(), Vec::new(), Vec::new()),
                                |(mut left_preds, mut right_preds, mut remain), p| {
                                    // If column is qualified (e.g., "o.order_id"), check table alias
                                    if p.column.contains('.') {
                                        let parts: Vec<&str> = p.column.split('.').collect();
                                        if parts.len() == 2 {
                                            let pred_table_alias = parts[0];
                                            let pred_column = parts[1];
                                            
                                            // Check if predicate table alias matches left or right table alias
                                            if pred_table_alias == left_table_alias {
                                                left_preds.push(p);
                                            } else if pred_table_alias == right_table_alias {
                                                right_preds.push(p);
                                            } else {
                                                // Table alias doesn't match either side - keep after JOIN
                                                remain.push(p);
                                            }
                                        } else {
                                            // Malformed qualified name - keep after JOIN
                                            remain.push(p);
                                        }
                                    } else {
                                        // Unqualified column - try to match by column name only
                                        // This is less safe but needed for backward compatibility
                                        if left_columns.contains(&p.column) && !right_columns.contains(&p.column) {
                                            left_preds.push(p);
                                        } else if right_columns.contains(&p.column) && !left_columns.contains(&p.column) {
                                            right_preds.push(p);
                                        } else {
                                            // Ambiguous or not found - keep after JOIN
                                            remain.push(p);
                                        }
                                    }
                                    (left_preds, right_preds, remain)
                                }
                            );
                        
                        let optimized_left = if !left_predicates.is_empty() {
                            Box::new(PlanOperator::Filter {
                                input: left,
                                predicates: left_predicates,
                                where_expression: where_expression.clone(),
                            })
                        } else {
                            left
                        };
                        
                        let optimized_right = if !right_predicates.is_empty() {
                            Box::new(PlanOperator::Filter {
                                input: right,
                                predicates: right_predicates,
                                where_expression: where_expression.clone(),
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
                                where_expression: where_expression.clone(),
                            }
                        }
                    }
                    other => {
                        // For other operators, push predicates as far as possible
                        PlanOperator::Filter {
                            input: Box::new(other),
                            predicates,
                            where_expression: where_expression.clone(),
                        }
                    }
                }
            }
            PlanOperator::Project { input, columns, expressions } => {
                // Push predicates through projections
                PlanOperator::Project {
                    input: Box::new(self.pushdown_predicates_recursive(*input, table_aliases)),
                    columns,
                    expressions,
                }
            }
            PlanOperator::Aggregate { input, group_by, group_by_aliases, aggregates, having } => {
                // Can't push predicates through aggregation (they become HAVING)
                PlanOperator::Aggregate {
                    input: Box::new(self.pushdown_predicates_recursive(*input, table_aliases)),
                    group_by,
                    group_by_aliases, // COLID: Preserve GROUP BY aliases
                    aggregates,
                    having,
                }
            }
            PlanOperator::Fused { input, operations } => {
                // For fused operators, recurse on input but keep operations unchanged
                PlanOperator::Fused {
                    input: Box::new(self.pushdown_predicates_recursive(*input, table_aliases)),
                    operations,
                }
            }
            PlanOperator::Join { left, right, edge_id, join_type, predicate } => {
                PlanOperator::Join {
                    left: Box::new(self.pushdown_predicates_recursive(*left, table_aliases)),
                    right: Box::new(self.pushdown_predicates_recursive(*right, table_aliases)),
                    edge_id,
                    join_type,
                    predicate,
                }
            }
            PlanOperator::Limit { input, limit, offset } => {
                // CRITICAL: Preserve Limit operator and recurse on input
                // VALIDATION: Check input type before recursion to detect corruption
                let input_was_join = matches!(input.as_ref(), PlanOperator::Join { .. } | PlanOperator::BitsetJoin { .. });
                let optimized_input = self.pushdown_predicates_recursive(*input, table_aliases);
                // VALIDATION: Ensure we didn't accidentally replace Join with Scan
                if matches!(&optimized_input, PlanOperator::Scan { .. }) && input_was_join {
                    panic!("CRITICAL BUG: pushdown_predicates replaced Join with Scan in Limit input. This must never happen.");
                }
                PlanOperator::Limit {
                    input: Box::new(optimized_input),
                    limit,
                    offset,
                }
            }
            PlanOperator::Sort { input, order_by, limit, offset } => {
                // CRITICAL: Preserve Sort operator and recurse on input
                PlanOperator::Sort {
                    input: Box::new(self.pushdown_predicates_recursive(*input, table_aliases)),
                    order_by,
                    limit,
                    offset,
                }
            }
            PlanOperator::Distinct { input } => {
                PlanOperator::Distinct {
                    input: Box::new(self.pushdown_predicates_recursive(*input, table_aliases)),
                }
            }
            other => {
                // CRITICAL: For any other operator, preserve it as-is (don't modify)
                // This prevents accidental replacement of Join operators
                other
            }
            PlanOperator::SetOperation { left, right, operation } => {
                PlanOperator::SetOperation {
                    left: Box::new(self.pushdown_predicates_recursive(*left, table_aliases)),
                    right: Box::new(self.pushdown_predicates_recursive(*right, table_aliases)),
                    operation,
                }
            }
            PlanOperator::Having { input, predicate } => {
                PlanOperator::Having {
                    input: Box::new(self.pushdown_predicates_recursive(*input, table_aliases)),
                    predicate,
                }
            }
            PlanOperator::Fused { input, operations } => {
                // For fused operators, recurse on input but keep operations unchanged
                PlanOperator::Fused {
                    input: Box::new(self.pushdown_predicates_recursive(*input, table_aliases)),
                    operations,
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
    
    /// Get table name/alias from an operator (used for qualified predicate pushdown)
    fn get_table_name_from_operator(&self, op: &PlanOperator) -> Option<String> {
        match op {
            PlanOperator::Scan { table, .. } => Some(table.clone()),
            PlanOperator::Join { left, right, .. } => {
                // For JOINs, we can't easily determine which table an alias refers to
                // Return None to be conservative
                None
            }
            _ => None,
        }
    }
    
    /// Push projections down the plan tree to only project needed columns
    /// This reduces the amount of data processed in intermediate operators
    pub fn pushdown_projections(&mut self) {
        // First, determine which columns are actually needed from the root
        let required_columns = self.get_required_columns(&self.root);
        eprintln!("DEBUG pushdown_projections: Root operator type BEFORE: {:?}, required_columns: {:?}", 
            std::mem::discriminant(&self.root),
            required_columns.iter().take(10).collect::<Vec<_>>());
        // Debug: Check what columns the Project actually has
        if let PlanOperator::Project { columns, expressions, .. } = &self.root {
            eprintln!("DEBUG pushdown_projections: Project has {} columns, {} expressions: {:?}", 
                columns.len(), expressions.len(), columns);
            // CRITICAL FIX: If get_required_columns returned empty but Project has columns/expressions,
            // we need to extract them manually to prevent plan corruption
            if required_columns.is_empty() && (!columns.is_empty() || !expressions.is_empty()) {
                eprintln!("DEBUG pushdown_projections: WARNING - required_columns is empty but Project has data. Extracting columns manually.");
                // Extract columns from both columns and expressions
                let mut manual_required: HashSet<String> = columns.iter().cloned().collect();
                for expr in expressions {
                    manual_required.insert(expr.alias.clone());
                    if let crate::query::plan::ProjectionExprType::Column(col) = &expr.expr_type {
                        manual_required.insert(col.clone());
                    }
                }
                
                // CRITICAL: Also extract join keys from any Join operators in the plan
                // This ensures joins are preserved during projection pushdown
                self.extract_join_keys_from_plan(&self.root, &mut manual_required);
                
                eprintln!("DEBUG pushdown_projections: Using manually extracted {} required columns (including join keys)", manual_required.len());
                // CRITICAL: Validate Join count before and after to detect corruption
                let joins_before = self.count_joins_in_plan(&self.root);
                eprintln!("DEBUG pushdown_projections: Join count BEFORE manual extraction: {}", joins_before);
                // Use manually extracted columns
                let new_root = self.pushdown_projections_recursive(self.root.clone(), &manual_required);
                let joins_after = self.count_joins_in_plan(&new_root);
                eprintln!("DEBUG pushdown_projections: Join count AFTER manual extraction: {}", joins_after);
                if joins_before > 0 && joins_after == 0 {
                    panic!("CRITICAL BUG: pushdown_projections_recursive removed all joins! This must never happen.");
                }
                self.root = new_root;
                return;
            }
        }
        
        // CRITICAL FIX: If required_columns is empty and we can't extract them, skip optimization
        // to prevent plan corruption (joins being removed)
        if required_columns.is_empty() {
            eprintln!("DEBUG pushdown_projections: WARNING - required_columns is empty. Skipping projection pushdown to preserve plan structure.");
            return; // Skip optimization to preserve joins
        }
        
        // Then push projections down based on required columns
        let new_root = self.pushdown_projections_recursive(self.root.clone(), &required_columns);
        eprintln!("DEBUG pushdown_projections: Root operator type AFTER recursive call: {:?}", 
            std::mem::discriminant(&new_root));
        eprintln!("DEBUG pushdown_projections: New root structure:");
        self.debug_print_plan_structure(&new_root, 0);
        self.root = new_root;
        eprintln!("DEBUG pushdown_projections: self.root structure AFTER assignment:");
        self.debug_print_plan_structure(&self.root, 0);
    }
    
    /// Get columns that are actually needed from this operator
    fn get_required_columns(&self, op: &PlanOperator) -> HashSet<String> {
        match op {
            PlanOperator::Project { columns, expressions, .. } => {
                // CRITICAL FIX: Project may have columns in both `columns` and `expressions`
                // We need to collect from both to get all required columns
                eprintln!("DEBUG get_required_columns: Project has {} columns, {} expressions", columns.len(), expressions.len());
                let mut required: HashSet<String> = columns.iter().cloned().collect();
                // Also add columns from expressions (aliases and column references)
                for expr in expressions {
                    required.insert(expr.alias.clone());
                    if let ProjectionExprType::Column(col) = &expr.expr_type {
                        required.insert(col.clone());
                    }
                }
                eprintln!("DEBUG get_required_columns: Returning {} required columns: {:?}", required.len(), required.iter().take(10).collect::<Vec<_>>());
                required
            }
            PlanOperator::Aggregate { group_by, aggregates, .. } => {
                let mut required = group_by.iter().cloned().collect::<HashSet<_>>();
                for agg in aggregates {
                    required.insert(agg.column.clone());
                }
                required
            }
            PlanOperator::Sort { input, order_by, .. } => {
                // CRITICAL FIX: Sort operator outputs ALL columns from its input, not just ORDER BY columns
                // ORDER BY columns are just used for sorting, but all input columns are passed through
                // So we need to get required columns from the input operator, not just ORDER BY columns
                let mut required = self.get_required_columns(input);
                // Also include ORDER BY columns in case they're not already in the input
                for o in order_by {
                    required.insert(o.column.clone());
                }
                required
            }
            PlanOperator::Filter { input, predicates, where_expression } => {
                // CRITICAL FIX: Filter operator outputs ALL columns from its input, not just predicate columns
                // Predicate columns are just used for filtering, but all input columns are passed through
                // So we need to get required columns from the input operator, not just predicate columns
                let mut required = self.get_required_columns(input);
                // Also include predicate columns in case they're not already in the input
                for p in predicates {
                    required.insert(p.column.clone());
                }
                // CRITICAL: Also extract columns from WHERE expression tree if present
                if let Some(ref wexpr) = where_expression {
                    let expr_columns = Self::extract_columns_from_expression(wexpr);
                    required.extend(expr_columns);
                }
                required
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
                eprintln!("DEBUG pushdown_projections_recursive: Processing Project with {} columns", columns.len());
                eprintln!("  Project input operator type BEFORE optimization: {:?}", 
                    std::mem::discriminant(input.as_ref()));
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
                
                // CRITICAL: If input is a Join, we must preserve join keys
                // Check if input is a Join and add join keys to required columns
                let mut final_required = expr_required.clone();
                let input_is_join = matches!(input.as_ref(), PlanOperator::Join { .. });
                if let PlanOperator::Join { predicate, .. } = input.as_ref() {
                    // Always include join keys to preserve join structure
                    final_required.insert(format!("{}.{}", predicate.left.0, predicate.left.1));
                    final_required.insert(format!("{}.{}", predicate.right.0, predicate.right.1));
                    final_required.insert(predicate.left.1.clone());
                    final_required.insert(predicate.right.1.clone());
                    eprintln!("  Project input is Join - adding join keys to required columns");
                }
                
                // Optimize input with required columns (including join keys if it's a Join)
                let optimized_input = self.pushdown_projections_recursive(*input, &final_required);
                eprintln!("  Project input operator type AFTER optimization: {:?}", 
                    std::mem::discriminant(&optimized_input));
                
                // VALIDATION: Ensure Join wasn't replaced with Scan
                if input_is_join && matches!(&optimized_input, PlanOperator::Scan { .. }) {
                    panic!("CRITICAL BUG: pushdown_projections_recursive replaced Join with Scan in Project input. This must never happen.");
                }
                
                // Only keep columns that are in the required set
                // CRITICAL FIX: If required_columns is empty, preserve all columns to maintain plan structure
                let filtered_columns: Vec<String> = if required_columns.is_empty() {
                    // Preserve all columns when required_columns is empty (happens at root level)
                    columns.clone()
                } else {
                    columns.into_iter().filter(|col| required_columns.contains(col)).collect()
                };
                
                // CRITICAL FIX: Filter expressions based on both alias and the column name in the expression
                // This ensures that if an expression has alias "name" but column "name", it's kept
                // If required_columns is empty, preserve all expressions
                let filtered_expressions: Vec<ProjectionExpr> = if required_columns.is_empty() {
                    expressions.clone()
                } else {
                    expressions.into_iter().filter(|expr| {
                        // Keep if alias is in required columns
                        if required_columns.contains(&expr.alias) {
                            return true;
                        }
                        // Also keep if the column name in the expression is in required columns
                        match &expr.expr_type {
                            ProjectionExprType::Column(col) => required_columns.contains(col),
                            _ => false, // For other expression types, only check alias
                        }
                    }).collect()
                };
                
                PlanOperator::Project {
                    input: Box::new(optimized_input),
                    columns: filtered_columns,
                    expressions: filtered_expressions,
                }
            }
            PlanOperator::Join { left, right, edge_id, join_type, predicate } => {
                // CRITICAL: Projection pushdown must NEVER replace Join with Scan
                // Rule: Join operators are preserved, only their inputs are optimized
                // Rule: Join keys are ALWAYS included in required columns (cannot be dropped)
                
                // Build required columns for left and right sides
                // Must include:
                // 1. Columns required by parent operators
                // 2. Join key columns (left and right) - CRITICAL: cannot be dropped
                // 3. ALL columns from both sides if required_columns is empty (preserve join structure)
                let mut left_required = required_columns.clone();
                let mut right_required = required_columns.clone();
                
                // CRITICAL FIX: If required_columns is empty, we need to preserve ALL columns from both sides
                // to maintain the join structure. Otherwise, joins might be incorrectly optimized away.
                if required_columns.is_empty() {
                    // Get all columns from left and right to preserve join structure
                    let left_cols = self.get_columns_from_operator(left.as_ref());
                    let right_cols = self.get_columns_from_operator(right.as_ref());
                    left_required.extend(left_cols);
                    right_required.extend(right_cols);
                }
                
                // Always add join keys (required for join to work)
                // Add qualified join key: "table.column"
                left_required.insert(format!("{}.{}", predicate.left.0, predicate.left.1));
                // Also add unqualified if needed
                left_required.insert(predicate.left.1.clone());
                
                // Add qualified join key: "table.column"
                right_required.insert(format!("{}.{}", predicate.right.0, predicate.right.1));
                // Also add unqualified if needed
                right_required.insert(predicate.right.1.clone());
                
                // Recursively optimize left and right inputs
                // CRITICAL: The result MUST still be a Join operator (never Scan)
                let left_clone = (*left).clone();
                let right_clone = (*right).clone();
                let optimized_left = self.pushdown_projections_recursive(left_clone, &left_required);
                let optimized_right = self.pushdown_projections_recursive(right_clone, &right_required);
                
                // VALIDATION: Ensure we didn't accidentally replace Join with Scan
                // This should never happen, but we check to fail fast
                if matches!(optimized_left, PlanOperator::Scan { .. }) && 
                   matches!(left.as_ref(), PlanOperator::Join { .. }) {
                    panic!("CRITICAL BUG: Projection pushdown replaced Join with Scan on left side. This must never happen.");
                }
                if matches!(optimized_right, PlanOperator::Scan { .. }) && 
                   matches!(right.as_ref(), PlanOperator::Join { .. }) {
                    panic!("CRITICAL BUG: Projection pushdown replaced Join with Scan on right side. This must never happen.");
                }
                
                // Return Join operator (preserved structure)
                PlanOperator::Join {
                    left: Box::new(optimized_left),
                    right: Box::new(optimized_right),
                    edge_id,
                    join_type,
                    predicate,
                }
            }
            PlanOperator::Filter { input, predicates, where_expression } => {
                // Filters need the columns referenced in predicates AND in WHERE expression tree
                let mut filter_required = required_columns.clone();
                for pred in &predicates {
                    filter_required.insert(pred.column.clone());
                }
                
                // CRITICAL: Also extract columns from WHERE expression tree if present
                if let Some(ref wexpr) = where_expression {
                    let expr_columns = Self::extract_columns_from_expression(wexpr);
                    for col in expr_columns {
                        filter_required.insert(col);
                    }
                }
                
                PlanOperator::Filter {
                    input: Box::new(self.pushdown_projections_recursive(*input, &filter_required)),
                    predicates,
                    where_expression: where_expression.clone(),
                }
            }
            PlanOperator::Aggregate { input, group_by, group_by_aliases, aggregates, having } => {
                // Aggregates need group_by columns and aggregate columns
                let mut agg_required: HashSet<String> = group_by.iter().cloned().collect();
                for agg in &aggregates {
                    agg_required.insert(agg.column.clone());
                }
                
                PlanOperator::Aggregate {
                    input: Box::new(self.pushdown_projections_recursive(*input, &agg_required)),
                    group_by,
                    group_by_aliases, // COLID: Preserve GROUP BY aliases
                    aggregates,
                    having,
                }
            }
            PlanOperator::Fused { input, operations } => {
                // For fused operators, determine required columns from operations
                let mut fused_required = required_columns.clone();
                // Extract columns from fused operations (iterate by reference to avoid move)
                for op in &operations {
                    match op {
                        FusedOperation::Filter { predicate } => {
                            fused_required.insert(predicate.column.clone());
                        }
                        FusedOperation::Project { columns, .. } => {
                            fused_required.extend(columns.iter().cloned());
                        }
                        FusedOperation::Aggregate { group_by, aggregates, .. } => {
                            fused_required.extend(group_by.iter().cloned());
                            for agg in aggregates {
                                fused_required.insert(agg.column.clone());
                            }
                        }
                    }
                }
                PlanOperator::Fused {
                    input: Box::new(self.pushdown_projections_recursive(*input, &fused_required)),
                    operations,
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
            PlanOperator::Window { input, window_functions } => {
                // Window needs input columns plus window function columns
                let mut window_required = required_columns.clone();
                for wf in &window_functions {
                    if let Some(ref col) = wf.column {
                        window_required.insert(col.clone());
                    }
                    window_required.extend(wf.partition_by.iter().cloned());
                    for ord in &wf.order_by {
                        window_required.insert(ord.column.clone());
                    }
                }
                PlanOperator::Window {
                    input: Box::new(self.pushdown_projections_recursive(*input, &window_required)),
                    window_functions,
                }
            }
            PlanOperator::Distinct { input } => {
                PlanOperator::Distinct {
                    input: Box::new(self.pushdown_projections_recursive(*input, required_columns)),
                }
            }
            PlanOperator::CTEScan { cte_name, columns, limit, offset } => {
                // Only scan required columns from CTE
                let filtered_columns: Vec<String> = columns
                    .iter()
                    .filter(|col| required_columns.contains(*col))
                    .cloned()
                    .collect();
                PlanOperator::CTEScan {
                    cte_name,
                    columns: if filtered_columns.is_empty() { columns } else { filtered_columns },
                    limit,
                    offset,
                }
            }
            PlanOperator::DerivedTableScan { derived_table_name, columns, limit, offset } => {
                // Only scan required columns from derived table
                let filtered_columns: Vec<String> = columns
                    .iter()
                    .filter(|col| required_columns.contains(*col))
                    .cloned()
                    .collect();
                PlanOperator::DerivedTableScan {
                    derived_table_name,
                    columns: if filtered_columns.is_empty() { columns } else { filtered_columns },
                    limit,
                    offset,
                }
            }
            PlanOperator::BitsetJoin { left, right, edge_id, join_type, predicate, use_bitmap } => {
                // Bitset joins need join columns
                let mut left_required = required_columns.clone();
                left_required.insert(predicate.left.1.clone());
                let mut right_required = required_columns.clone();
                right_required.insert(predicate.right.1.clone());
                PlanOperator::BitsetJoin {
                    left: Box::new(self.pushdown_projections_recursive(*left, &left_required)),
                    right: Box::new(self.pushdown_projections_recursive(*right, &right_required)),
                    edge_id,
                    join_type,
                    predicate,
                    use_bitmap,
                }
            }
            PlanOperator::Limit { input, limit, offset } => {
                // CRITICAL: Preserve Limit and recurse on input to process Project/Join underneath
                PlanOperator::Limit {
                    input: Box::new(self.pushdown_projections_recursive(*input, required_columns)),
                    limit,
                    offset,
                }
            }
            other => other, // Other operators pass through
        }
    }
    
    /// Reorder joins for optimal cost (DuckDB/Presto-style)
    /// 
    /// # Outer Join Restrictions
    /// - Do NOT reorder across outer joins (LEFT/RIGHT/FULL)
    /// - Preserve textual order for any join involving LEFT/RIGHT/FULL
    /// - Only INNER joins can be freely reordered via join graph
    /// 
    /// # Current Implementation
    /// Join reordering is handled by the join graph algorithm in planner.rs::build_join_tree_from_graph()
    /// This function is a placeholder for future cost-based join reordering.
    fn reorder_joins(&mut self) {
        eprintln!("DEBUG reorder_joins: Plan structure BEFORE (inside function):");
        self.debug_print_plan_structure(&self.root, 0);
        // TODO: Reorder joins for optimal cost
        // This requires cost-based optimization which is more complex
        // The optimizer.rs already has join reordering logic that should be integrated here
        // NOTE: For outer joins, we must preserve textual order (not implemented yet)
        eprintln!("DEBUG reorder_joins: Plan structure AFTER (inside function, no changes):");
        self.debug_print_plan_structure(&self.root, 0);
    }
    
    /// Count Join operators in plan tree
    fn count_joins_in_plan(&self, op: &PlanOperator) -> usize {
        match op {
            PlanOperator::Join { left, right, .. } | PlanOperator::BitsetJoin { left, right, .. } => {
                1 + self.count_joins_in_plan(left) + self.count_joins_in_plan(right)
            }
            PlanOperator::Filter { input, .. }
            | PlanOperator::Aggregate { input, .. }
            | PlanOperator::Project { input, .. }
            | PlanOperator::Sort { input, .. }
            | PlanOperator::Limit { input, .. }
            | PlanOperator::Distinct { input }
            | PlanOperator::Having { input, .. }
            | PlanOperator::Window { input, .. } => self.count_joins_in_plan(input),
            _ => 0,
        }
    }
    
    /// Extract join keys from Join operators in the plan tree
    fn extract_join_keys_from_plan(&self, op: &PlanOperator, required: &mut HashSet<String>) {
        match op {
            PlanOperator::Join { predicate, left, right, .. } | PlanOperator::BitsetJoin { predicate, left, right, .. } => {
                // Add join keys (both qualified and unqualified)
                required.insert(format!("{}.{}", predicate.left.0, predicate.left.1));
                required.insert(format!("{}.{}", predicate.right.0, predicate.right.1));
                required.insert(predicate.left.1.clone());
                required.insert(predicate.right.1.clone());
                // Recurse on children
                self.extract_join_keys_from_plan(left, required);
                self.extract_join_keys_from_plan(right, required);
            }
            PlanOperator::Filter { input, .. }
            | PlanOperator::Aggregate { input, .. }
            | PlanOperator::Project { input, .. }
            | PlanOperator::Sort { input, .. }
            | PlanOperator::Limit { input, .. }
            | PlanOperator::Distinct { input }
            | PlanOperator::Having { input, .. }
            | PlanOperator::Window { input, .. } => {
                self.extract_join_keys_from_plan(input, required);
            }
            _ => {}
        }
    }
    
    /// Extract column names from an Expression tree
    fn extract_columns_from_expression(expr: &crate::query::expression::Expression) -> HashSet<String> {
        use crate::query::expression::Expression;
        let mut columns = HashSet::new();
        
        match expr {
            Expression::Column(name, _) => {
                columns.insert(name.clone());
            }
            Expression::BinaryOp { left, right, .. } => {
                columns.extend(Self::extract_columns_from_expression(left));
                columns.extend(Self::extract_columns_from_expression(right));
            }
            Expression::UnaryOp { expr, .. } => {
                columns.extend(Self::extract_columns_from_expression(expr));
            }
            Expression::Function { args, .. } => {
                for arg in args {
                    columns.extend(Self::extract_columns_from_expression(arg));
                }
            }
            Expression::Case { operand, conditions, else_result, .. } => {
                if let Some(op) = operand {
                    columns.extend(Self::extract_columns_from_expression(op));
                }
                for (cond, result) in conditions {
                    columns.extend(Self::extract_columns_from_expression(cond));
                    columns.extend(Self::extract_columns_from_expression(result));
                }
                if let Some(else_res) = else_result {
                    columns.extend(Self::extract_columns_from_expression(else_res));
                }
            }
            Expression::In { expr, list, .. } => {
                columns.extend(Self::extract_columns_from_expression(expr));
                for item in list {
                    columns.extend(Self::extract_columns_from_expression(item));
                }
            }
            Expression::Cast { expr, .. } => {
                columns.extend(Self::extract_columns_from_expression(expr));
            }
            Expression::NullIf { expr1, expr2 } => {
                columns.extend(Self::extract_columns_from_expression(expr1));
                columns.extend(Self::extract_columns_from_expression(expr2));
            }
            Expression::Coalesce(exprs) => {
                for expr in exprs {
                    columns.extend(Self::extract_columns_from_expression(expr));
                }
            }
            Expression::Literal(_) | Expression::Subquery(_) | Expression::Exists(_) => {
                // No columns to extract from literals or subqueries
            }
        }
        
        columns
    }
}

