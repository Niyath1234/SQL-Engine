/// Cascades-style memo structure for query optimization
/// Based on the Cascades optimizer framework
use crate::query::plan::PlanOperator;
use std::collections::HashMap;

/// A group represents a set of equivalent logical or physical expressions
/// All expressions in a group produce the same logical result
#[derive(Clone, Debug)]
pub struct Group {
    /// Group ID
    pub id: GroupId,
    
    /// Logical properties (schema, cardinality, etc.)
    pub logical_props: LogicalProperties,
    
    /// Physical properties (sort order, distribution, etc.)
    pub physical_props: PhysicalProperties,
    
    /// Expressions in this group (logical and physical)
    pub expressions: Vec<Expression>,
    
    /// Best cost for this group (for each physical property requirement)
    pub best_costs: HashMap<PhysicalPropertySet, Cost>,
    
    /// Best expression for each physical property requirement
    pub best_expressions: HashMap<PhysicalPropertySet, ExpressionId>,
}

/// Unique identifier for a group
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GroupId(pub usize);

/// Unique identifier for an expression within a group
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ExpressionId {
    pub group_id: GroupId,
    pub expr_index: usize,
}

/// Logical properties of an expression
#[derive(Clone, Debug)]
pub struct LogicalProperties {
    /// Output schema
    pub schema: Vec<String>,
    
    /// Estimated cardinality
    pub cardinality: f64,
    
    /// Estimated number of distinct values per column
    pub distinct_values: HashMap<String, f64>,
    
    /// Nullability per column
    pub nullable: HashMap<String, bool>,
}

/// Physical properties of an expression
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PhysicalProperties {
    /// Sort order (if any)
    pub sort_order: Option<Vec<SortKey>>,
    
    /// Distribution (if any)
    pub distribution: Option<Distribution>,
    
    /// Partitioning (if any)
    pub partitioning: Option<Partitioning>,
}

/// Set of physical property requirements
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PhysicalPropertySet {
    pub sort_order: Option<Vec<SortKey>>,
    pub distribution: Option<Distribution>,
    pub partitioning: Option<Partitioning>,
}

/// Sort key (column + direction)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SortKey {
    pub column: String,
    pub ascending: bool,
}

/// Data distribution
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Distribution {
    Single,      // Single partition
    Hash(Vec<String>), // Hash partitioned on columns
    Range(Vec<String>), // Range partitioned on columns
    Broadcast,    // Broadcast to all nodes
}

/// Partitioning information
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Partitioning {
    pub strategy: PartitionStrategy,
    pub columns: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PartitionStrategy {
    Hash,
    Range,
    List,
}

/// An expression in the memo
#[derive(Clone, Debug)]
pub struct Expression {
    /// Expression ID
    pub id: ExpressionId,
    
    /// Operator (logical or physical)
    pub operator: PlanOperator,
    
    /// Input groups (for operators with children)
    pub inputs: Vec<GroupId>,
    
    /// Is this a logical or physical expression?
    pub is_logical: bool,
    
    /// Cost estimate
    pub cost: Cost,
    
    /// Why was this expression created? (for explainability)
    pub derivation_reason: Option<String>,
}

/// Cost model: multi-objective cost
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct Cost {
    /// Latency in milliseconds
    pub latency_ms: f64,
    
    /// Memory penalty (bytes)
    pub memory_penalty: f64,
    
    /// Spill penalty (bytes spilled to disk)
    pub spill_penalty: f64,
    
    /// I/O cost (bytes read/written)
    pub io_cost: f64,
    
    /// Cloud cost (if applicable)
    pub cloud_cost: f64,
    
    /// Total cost (weighted sum)
    pub total: f64,
}

impl Cost {
    pub fn new() -> Self {
        Self {
            latency_ms: 0.0,
            memory_penalty: 0.0,
            spill_penalty: 0.0,
            io_cost: 0.0,
            cloud_cost: 0.0,
            total: 0.0,
        }
    }
    
    /// Compute total cost with weights
    pub fn compute_total(&mut self) {
        // Weighted sum: prioritize latency, then memory, then I/O
        self.total = 
            self.latency_ms * 1.0 +
            self.memory_penalty * 0.1 +
            self.spill_penalty * 0.5 +
            self.io_cost * 0.2 +
            self.cloud_cost * 0.01;
    }
    
    pub fn add(&mut self, other: &Cost) {
        self.latency_ms += other.latency_ms;
        self.memory_penalty += other.memory_penalty;
        self.spill_penalty += other.spill_penalty;
        self.io_cost += other.io_cost;
        self.cloud_cost += other.cloud_cost;
        self.compute_total();
    }
}

/// The memo structure - central data structure for cascades optimizer
#[derive(Clone, Debug)]
pub struct Memo {
    /// Groups indexed by ID
    pub groups: HashMap<GroupId, Group>,
    
    /// Next group ID to assign
    pub next_group_id: usize,
    
    /// Next expression ID to assign
    pub next_expr_id: usize,
    
    /// Optimization rules applied
    pub applied_rules: Vec<RuleApplication>,
    
    /// Plans discarded and why
    pub discarded_plans: Vec<DiscardedPlan>,
}

/// Record of a rule application
#[derive(Clone, Debug)]
pub struct RuleApplication {
    pub rule_name: String,
    pub input_expression: ExpressionId,
    pub output_expressions: Vec<ExpressionId>,
    pub timestamp: std::time::SystemTime,
}

/// Record of a discarded plan
#[derive(Clone, Debug)]
pub struct DiscardedPlan {
    pub expression_id: ExpressionId,
    pub reason: String,
    pub better_alternative: Option<ExpressionId>,
    pub cost_difference: f64,
}

impl Memo {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
            next_group_id: 0,
            next_expr_id: 0,
            applied_rules: Vec::new(),
            discarded_plans: Vec::new(),
        }
    }
    
    /// Create a new group
    pub fn new_group(&mut self, logical_props: LogicalProperties) -> GroupId {
        let group_id = GroupId(self.next_group_id);
        self.next_group_id += 1;
        
        let group = Group {
            id: group_id,
            logical_props,
            physical_props: PhysicalProperties {
                sort_order: None,
                distribution: None,
                partitioning: None,
            },
            expressions: Vec::new(),
            best_costs: HashMap::new(),
            best_expressions: HashMap::new(),
        };
        
        self.groups.insert(group_id, group);
        group_id
    }
    
    /// Add an expression to a group
    pub fn add_expression(&mut self, group_id: GroupId, expression: Expression) -> ExpressionId {
        let group = self.groups.get_mut(&group_id).expect("Group not found");
        let expr_index = group.expressions.len();
        
        let expr_id = ExpressionId {
            group_id,
            expr_index,
        };
        
        let mut expr = expression;
        expr.id = expr_id;
        group.expressions.push(expr);
        
        expr_id
    }
    
    /// Get the best expression for a given physical property requirement
    pub fn get_best_expression(&self, group_id: GroupId, req: &PhysicalPropertySet) -> Option<ExpressionId> {
        self.groups.get(&group_id)
            .and_then(|group| group.best_expressions.get(req).copied())
    }
    
    /// Record that a plan was discarded
    pub fn discard_plan(&mut self, expr_id: ExpressionId, reason: String, better: Option<ExpressionId>, cost_diff: f64) {
        self.discarded_plans.push(DiscardedPlan {
            expression_id: expr_id,
            reason,
            better_alternative: better,
            cost_difference: cost_diff,
        });
    }
    
    /// Record a rule application
    pub fn record_rule(&mut self, rule_name: String, input: ExpressionId, outputs: Vec<ExpressionId>) {
        self.applied_rules.push(RuleApplication {
            rule_name,
            input_expression: input,
            output_expressions: outputs,
            timestamp: std::time::SystemTime::now(),
        });
    }
    
    /// Get explainability information
    pub fn explain(&self, expr_id: ExpressionId) -> String {
        let mut explanation = String::new();
        
        // Find the expression
        if let Some(group) = self.groups.get(&expr_id.group_id) {
            if let Some(expr) = group.expressions.get(expr_id.expr_index) {
                explanation.push_str(&format!("Expression {}:\n", expr_id.expr_index));
                explanation.push_str(&format!("  Operator: {:?}\n", expr.operator));
                explanation.push_str(&format!("  Cost: {:?}\n", expr.cost));
                if let Some(ref reason) = expr.derivation_reason {
                    explanation.push_str(&format!("  Derived from: {}\n", reason));
                }
            }
        }
        
        // Find discarded alternatives
        let discarded: Vec<_> = self.discarded_plans.iter()
            .filter(|d| d.expression_id == expr_id)
            .collect();
        if !discarded.is_empty() {
            explanation.push_str("  Discarded alternatives:\n");
            for d in discarded {
                explanation.push_str(&format!("    - {} (cost diff: {})\n", d.reason, d.cost_difference));
            }
        }
        
        explanation
    }
}

impl Default for Memo {
    fn default() -> Self {
        Self::new()
    }
}

