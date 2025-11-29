/// Advanced query optimizer with hypergraph-based join reordering
use crate::query::plan::*;
use crate::query::expression::Expression;
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::NodeId;
use crate::hypergraph::edge::EdgeId;
use crate::hypergraph::path::HyperPath;
use std::collections::{HashMap, HashSet};
use anyhow::Result;

/// Cost model for query optimization
pub struct CostModel {
    /// Cost per row scanned
    scan_cost_per_row: f64,
    /// Cost per row joined
    join_cost_per_row: f64,
    /// Cost per row filtered
    filter_cost_per_row: f64,
    /// Cost per row aggregated
    aggregate_cost_per_row: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            scan_cost_per_row: 1.0,
            join_cost_per_row: 10.0,
            filter_cost_per_row: 2.0,
            aggregate_cost_per_row: 5.0,
        }
    }
}

impl CostModel {
    /// Estimate cost of a plan operator
    pub fn estimate_cost(&self, op: &PlanOperator, stats: &PlanStatistics) -> f64 {
        self.estimate_cost_with_index_hints(op, stats, &HashMap::new())
    }
    
    /// Estimate cost of a plan operator with vector index hints
    pub fn estimate_cost_with_index_hints(
        &self,
        op: &PlanOperator,
        stats: &PlanStatistics,
        vector_index_hints: &HashMap<(String, String), bool>,
    ) -> f64 {
        self.estimate_cost_with_index_hints_and_graph(op, stats, vector_index_hints, None)
    }
    
    /// Estimate cost of a plan operator with vector index hints and graph access
    pub fn estimate_cost_with_index_hints_and_graph(
        &self,
        op: &PlanOperator,
        stats: &PlanStatistics,
        vector_index_hints: &HashMap<(String, String), bool>,
        graph: Option<&HyperGraph>,
    ) -> f64 {
        match op {
            PlanOperator::BitsetJoin { left, right, .. } => {
                // Bitset join cost: similar to regular join but with bitmap index benefits
                let left_cost = self.estimate_cost_with_index_hints_and_graph(left, stats, vector_index_hints, graph);
                let right_cost = self.estimate_cost_with_index_hints_and_graph(right, stats, vector_index_hints, graph);
                // Bitset join is cheaper: O(n) instead of O(n*m) for hash join
                left_cost + right_cost + stats.cardinality as f64 * self.join_cost_per_row * 0.1
            }
            PlanOperator::Scan { table, columns, node_id, .. } => {
                // OPTIMIZATION: Check for vector indexes
                let mut has_vector_index = false;
                for col in columns {
                    if vector_index_hints.get(&(table.clone(), col.clone())).copied().unwrap_or(false) {
                        has_vector_index = true;
                        break;
                    }
                }
                
                // OPTIMIZATION: Use size_bytes for I/O cost estimation
                let io_cost = if let Some(g) = graph {
                    if let Some(node) = g.get_node(*node_id) {
                        // I/O cost based on data size: larger data = more I/O
                        let size_mb = (node.stats.size_bytes as f64) / (1024.0 * 1024.0);
                        size_mb * 0.1  // 0.1 cost per MB (configurable)
                    } else {
                        0.0
                    }
                } else {
                    0.0
                };
                
                let base_scan_cost = if has_vector_index {
                    // Vector index scan is much cheaper: O(log n) instead of O(n)
                    // Use logarithmic cost model for vector index scans
                    let log_n = (stats.cardinality as f64).ln().max(1.0);
                    log_n * self.scan_cost_per_row * 0.1  // 10x cheaper than linear scan
                } else {
                    stats.cardinality as f64 * self.scan_cost_per_row
                };
                
                base_scan_cost + io_cost  // Add I/O cost to scan cost
            }
            PlanOperator::CTEScan { .. } => {
                // CTE scan is similar to regular scan but from memory cache
                stats.cardinality as f64 * self.scan_cost_per_row * 0.5  // Faster than disk scan
            }
            PlanOperator::DerivedTableScan { .. } => {
                // Derived table scan is similar to CTE scan - reads from memory cache
                stats.cardinality as f64 * self.scan_cost_per_row * 0.5  // Faster than disk scan
            }
            PlanOperator::Join { left, right, .. } => {
                let left_cost = self.estimate_cost(left, stats);
                let right_cost = self.estimate_cost(right, stats);
                let join_output_size = stats.cardinality as f64;
                left_cost + right_cost + join_output_size * self.join_cost_per_row
            }
            PlanOperator::Filter { input, predicates, .. } => {
                let input_cost = self.estimate_cost_with_index_hints(input, stats, vector_index_hints);
                
                // Check if filter contains vector operations
                let mut has_vector_op = false;
                for pred in predicates {
                    // Vector operations are typically in WHERE clauses with VECTOR_SIMILARITY
                    // For now, we'll detect this through the column name pattern or predicate value
                    // In a full implementation, we'd analyze the expression tree
                    if let Some(ref in_values) = pred.in_values {
                        // Check if predicate involves vector operations
                        // This is a simplified check - full implementation would analyze expressions
                    }
                }
                
                if has_vector_op {
                    // Vector filter operations can use indexes
                    let log_n = (stats.cardinality as f64).ln().max(1.0);
                    input_cost + log_n * self.filter_cost_per_row * 0.1
                } else {
                    input_cost + stats.cardinality as f64 * self.filter_cost_per_row
                }
            }
            PlanOperator::Aggregate { input, .. } => {
                let input_cost = self.estimate_cost(input, stats);
                input_cost + stats.cardinality as f64 * self.aggregate_cost_per_row
            }
            PlanOperator::Project { input, expressions, .. } => {
                let input_cost = self.estimate_cost_with_index_hints(input, stats, vector_index_hints);
                
                // Check if projection contains vector operations (VECTOR_SIMILARITY, VECTOR_DISTANCE)
                let mut has_vector_op = false;
                let mut can_use_index = false;
                for expr in expressions {
                    if HypergraphOptimizer::expression_has_vector_operations(&expr.expr_type) {
                        has_vector_op = true;
                        // Check if we can use vector index for this operation
                        let (table, column) = HypergraphOptimizer::extract_vector_column_from_expression(&expr.expr_type);
                        if let (Some(t), Some(c)) = (table, column) {
                            if vector_index_hints.get(&(t, c)).copied().unwrap_or(false) {
                                can_use_index = true;
                            }
                        }
                    }
                }
                
                if has_vector_op {
                    if can_use_index {
                        // Vector index search: O(log n) cost
                        let log_n = (stats.cardinality as f64).ln().max(1.0);
                        input_cost + log_n * self.filter_cost_per_row * 0.1
                    } else {
                        // Linear scan: O(n) cost
                        input_cost + stats.cardinality as f64 * self.filter_cost_per_row * 0.5
                    }
                } else {
                    input_cost
                }
            }
            PlanOperator::Sort { input, .. } => {
                let input_cost = self.estimate_cost(input, stats);
                input_cost + stats.cardinality as f64 * (stats.cardinality as f64).log2()
            }
            PlanOperator::Limit { input, .. } => {
                self.estimate_cost(input, stats)
            }
            PlanOperator::SetOperation { left, right, .. } => {
                let left_cost = self.estimate_cost(left, stats);
                let right_cost = self.estimate_cost(right, stats);
                left_cost + right_cost + stats.cardinality as f64 * self.join_cost_per_row
            }
            PlanOperator::Distinct { input, .. } => {
                let input_cost = self.estimate_cost(input, stats);
                input_cost + stats.cardinality as f64 * self.aggregate_cost_per_row
            }
            PlanOperator::Having { input, .. } => {
                let input_cost = self.estimate_cost(input, stats);
                input_cost + stats.cardinality as f64 * self.filter_cost_per_row
            }
            PlanOperator::Window { input, .. } => {
                let input_cost = self.estimate_cost(input, stats);
                // Window functions require sorting/partitioning, more expensive than filter
                input_cost + stats.cardinality as f64 * (stats.cardinality as f64).log2() * 0.5
            }
            PlanOperator::Fused { input, operations } => {
                // Fused operators are typically faster (1.5-4x speedup)
                let input_cost = self.estimate_cost(input, stats);
                // Apply operations cost (fused operations are cheaper than separate)
                let mut fused_cost = input_cost;
                for op in operations {
                    match op {
                        crate::query::plan::FusedOperation::Filter { .. } => {
                            fused_cost += stats.cardinality as f64 * 0.00005; // Very fast when fused
                        }
                        crate::query::plan::FusedOperation::Project { .. } => {
                            fused_cost += stats.cardinality as f64 * 0.0001; // Fast when fused
                        }
                        crate::query::plan::FusedOperation::Aggregate { group_by, .. } => {
                            fused_cost += group_by.len() as f64 * 0.001; // Faster when fused
                        }
                    }
                }
                fused_cost * 0.5  // 50% cost reduction from fusion
            }
        }
    }
    
    /// Estimate cardinality of a plan operator
    /// This uses default estimates - for actual statistics, use HypergraphOptimizer
    pub fn estimate_cardinality(&self, op: &PlanOperator) -> usize {
        match op {
            PlanOperator::Scan { .. } => {
                // Default estimate - actual statistics should come from HypergraphOptimizer
                1000
            }
            PlanOperator::CTEScan { .. } => {
                // CTE cardinality should be known from execution
                // For now, use same default as regular scan
                1000
            }
            PlanOperator::DerivedTableScan { .. } => {
                // Derived table cardinality should be known from execution
                // For now, use same default as regular scan
                1000
            }
            PlanOperator::Join { left, right, .. } | PlanOperator::BitsetJoin { left, right, .. } => {
                self.estimate_cardinality(left) + self.estimate_cardinality(right)
            }
            PlanOperator::Filter { input, predicates, .. } => {
                // Estimate selectivity based on predicate type
                // For equality predicates: lower selectivity (more selective)
                // For range predicates: higher selectivity
                let base_cardinality = self.estimate_cardinality(input);
                
                let selectivity = if predicates.is_empty() {
                    1.0 // No filter, return all rows
                } else {
                    // Calculate selectivity based on predicate operators
                    let mut total_selectivity = 1.0;
                    for pred in predicates {
                        let pred_selectivity = match pred.operator {
                            PredicateOperator::Equals => 0.05, // 5% selectivity for equality
                            PredicateOperator::NotEquals => 0.95, // 95% for NOT equals
                            PredicateOperator::LessThan | PredicateOperator::LessThanOrEqual => 0.3,
                            PredicateOperator::GreaterThan | PredicateOperator::GreaterThanOrEqual => 0.3,
                            PredicateOperator::In => {
                                // Selectivity depends on number of values in IN clause
                                if let Some(ref in_values) = pred.in_values {
                                    0.1 * (in_values.len() as f64 / 100.0).min(1.0)
                                } else {
                                    0.05
                                }
                            }
                            PredicateOperator::NotIn => 0.9,
                            PredicateOperator::Like | PredicateOperator::NotLike => 0.2, // Pattern matching
                            PredicateOperator::IsNull => 0.01, // Rare
                            PredicateOperator::IsNotNull => 0.99,
                        };
                        // Combine selectivities (assume independence)
                        total_selectivity *= pred_selectivity;
                    }
                    total_selectivity.max(0.01).min(1.0) // Clamp between 1% and 100%
                };
                
                (base_cardinality as f64 * selectivity) as usize
            }
            PlanOperator::Fused { input, operations } => {
                // Fused operators apply operations in sequence
                let mut card = self.estimate_cardinality(input);
                for op in operations {
                    match op {
                        crate::query::plan::FusedOperation::Filter { .. } => {
                            // Filter reduces cardinality
                            card = (card as f64 * 0.1) as usize; // Default 10% selectivity
                        }
                        crate::query::plan::FusedOperation::Project { .. } => {
                            // Project doesn't change cardinality
                        }
                        crate::query::plan::FusedOperation::Aggregate { group_by, .. } => {
                            // Aggregate reduces to number of groups
                            card = group_by.len();
                        }
                    }
                }
                card
            }
            PlanOperator::Aggregate { input, group_by, .. } => {
                let input_cardinality = self.estimate_cardinality(input);
                
                // Estimate groups based on group_by columns
                // If no group_by, it's a single group
                if group_by.is_empty() {
                    return 1;
                }
                
                // Try to get cardinality estimates from hypergraph nodes
                // For now, use a heuristic based on input cardinality and number of group columns
                // More group columns = fewer groups (higher selectivity)
                let group_selectivity = 1.0 / (group_by.len() as f64 * 10.0).max(1.0);
                let estimated_groups = (input_cardinality as f64 * group_selectivity).max(1.0) as usize;
                
                // Cap at input cardinality (can't have more groups than rows)
                estimated_groups.min(input_cardinality)
            }
            PlanOperator::Project { input, .. } => {
                self.estimate_cardinality(input)
            }
            PlanOperator::Sort { input, .. } => {
                self.estimate_cardinality(input)
            }
            PlanOperator::Limit { input, .. } => {
                self.estimate_cardinality(input)
            }
            PlanOperator::SetOperation { left, right, .. } => {
                self.estimate_cardinality(left).max(self.estimate_cardinality(right))
            }
            PlanOperator::Distinct { input, .. } => {
                self.estimate_cardinality(input)
            }
            PlanOperator::Having { input, .. } => {
                // HAVING filters groups - assume 50% selectivity
                (self.estimate_cardinality(input) as f64 * 0.5) as usize
            }
            PlanOperator::Window { input, .. } => {
                // Window functions don't change cardinality, just add columns
                self.estimate_cardinality(input)
            }
            PlanOperator::Fused { input, operations } => {
                // Fused operators apply operations in sequence
                let mut card = self.estimate_cardinality(input);
                for op in operations {
                    match op {
                        crate::query::plan::FusedOperation::Filter { .. } => {
                            // Filter reduces cardinality
                            card = (card as f64 * 0.1) as usize; // Default 10% selectivity
                        }
                        crate::query::plan::FusedOperation::Project { .. } => {
                            // Project doesn't change cardinality
                        }
                        crate::query::plan::FusedOperation::Aggregate { group_by, .. } => {
                            // Aggregate reduces to number of groups
                            card = group_by.len();
                        }
                    }
                }
                card
            }
        }
    }
}

/// Statistics for plan optimization
pub struct PlanStatistics {
    pub cardinality: usize,
    pub selectivity: f64,
    pub avg_fanout: f64,
}

/// Hypergraph-based join reordering optimizer
pub struct HypergraphOptimizer {
    graph: HyperGraph,
    cost_model: CostModel,
}

impl HypergraphOptimizer {
    pub fn new(graph: HyperGraph) -> Self {
        Self {
            graph,
            cost_model: CostModel::default(),
        }
    }
    
    /// Detect vector operations in a query plan and return vector index hints
    pub fn detect_vector_operations(&self, plan: &QueryPlan) -> HashMap<(String, String), bool> {
        let mut vector_index_hints = HashMap::new();
        Self::analyze_operator_for_vector_ops(&plan.root, &mut vector_index_hints, &self.graph);
        vector_index_hints
    }
    
    /// Analyze an operator for vector operations
    fn analyze_operator_for_vector_ops(
        op: &PlanOperator,
        hints: &mut HashMap<(String, String), bool>,
        graph: &HyperGraph,
    ) {
        match op {
            PlanOperator::Scan { table, columns, .. } => {
                // Check if any columns have vector indexes
                for col in columns {
                    if Self::has_vector_index(graph, table, col) {
                        hints.insert((table.clone(), col.clone()), true);
                    }
                }
            }
            PlanOperator::Project { input, expressions, .. } => {
                // Analyze expressions for vector operations
                for expr in expressions {
                    let (table, column) = Self::extract_vector_column_from_expression(&expr.expr_type);
                    if let (Some(t), Some(c)) = (table, column) {
                        if Self::has_vector_index(graph, &t, &c) {
                            hints.insert((t, c), true);
                        }
                    }
                }
                Self::analyze_operator_for_vector_ops(input, hints, graph);
            }
            PlanOperator::Filter { input, .. } => {
                Self::analyze_operator_for_vector_ops(input, hints, graph);
            }
            PlanOperator::Join { left, right, .. } => {
                Self::analyze_operator_for_vector_ops(left, hints, graph);
                Self::analyze_operator_for_vector_ops(right, hints, graph);
            }
            PlanOperator::Aggregate { input, .. } => {
                Self::analyze_operator_for_vector_ops(input, hints, graph);
            }
            PlanOperator::Sort { input, .. } => {
                Self::analyze_operator_for_vector_ops(input, hints, graph);
            }
            PlanOperator::Limit { input, .. } => {
                Self::analyze_operator_for_vector_ops(input, hints, graph);
            }
            PlanOperator::SetOperation { left, right, .. } => {
                Self::analyze_operator_for_vector_ops(left, hints, graph);
                Self::analyze_operator_for_vector_ops(right, hints, graph);
            }
            PlanOperator::Distinct { input, .. } => {
                Self::analyze_operator_for_vector_ops(input, hints, graph);
            }
            PlanOperator::BitsetJoin { left, right, .. } => {
                Self::analyze_operator_for_vector_ops(left, hints, graph);
                Self::analyze_operator_for_vector_ops(right, hints, graph);
            }
            PlanOperator::Having { input, .. } => {
                Self::analyze_operator_for_vector_ops(input, hints, graph);
            }
            PlanOperator::Window { input, .. } => {
                Self::analyze_operator_for_vector_ops(input, hints, graph);
            }
            PlanOperator::CTEScan { .. } => {
                // CTEs don't have vector indexes (they're cached results)
            }
            PlanOperator::DerivedTableScan { .. } => {
                // Derived tables don't have vector indexes (they're cached results)
            }
            PlanOperator::Fused { input, .. } => {
                Self::analyze_operator_for_vector_ops(input, hints, graph);
            }
        }
    }
    
    /// Check if a column has a vector index
    fn has_vector_index(graph: &HyperGraph, table: &str, column: &str) -> bool {
        if let Some(col_node) = graph.get_node_by_table_column(table, column) {
            // Check if any fragment has a vector index
            for fragment in &col_node.fragments {
                if fragment.vector_index.is_some() {
                    return true;
                }
            }
        }
        false
    }
    
    /// Extract table and column from a vector operation expression
    fn extract_vector_column_from_expression(expr_type: &ProjectionExprType) -> (Option<String>, Option<String>) {
        match expr_type {
            ProjectionExprType::Function(expr) => {
                Self::extract_vector_column_from_expression_tree(expr)
            }
            _ => (None, None),
        }
    }
    
    /// Extract table and column from an expression tree
    fn extract_vector_column_from_expression_tree(expr: &Expression) -> (Option<String>, Option<String>) {
        match expr {
            Expression::Function { name, args } => {
                // Check if this is a vector function
                if name == "VECTOR_SIMILARITY" || name == "VECTOR_DISTANCE" {
                    // Extract column from first argument (typically the column being searched)
                    if let Some(first_arg) = args.first() {
                        match first_arg {
                            Expression::Column(col, table) => {
                                return (table.clone(), Some(col.clone()));
                            }
                            _ => {}
                        }
                    }
                }
                // Recursively check arguments
                for arg in args {
                    let (t, c) = Self::extract_vector_column_from_expression_tree(arg);
                    if t.is_some() && c.is_some() {
                        return (t, c);
                    }
                }
                (None, None)
            }
            Expression::Column(col, table) => {
                (table.clone(), Some(col.clone()))
            }
            Expression::BinaryOp { left, right, .. } => {
                let (t1, c1) = Self::extract_vector_column_from_expression_tree(left);
                if t1.is_some() && c1.is_some() {
                    return (t1, c1);
                }
                Self::extract_vector_column_from_expression_tree(right)
            }
            Expression::UnaryOp { expr, .. } => {
                Self::extract_vector_column_from_expression_tree(expr)
            }
            Expression::Cast { expr, .. } => {
                Self::extract_vector_column_from_expression_tree(expr)
            }
            Expression::Case { operand, conditions, else_result, .. } => {
                if let Some(op) = operand {
                    let (t, c) = Self::extract_vector_column_from_expression_tree(op);
                    if t.is_some() && c.is_some() {
                        return (t, c);
                    }
                }
                for (cond, result) in conditions {
                    let (t1, c1) = Self::extract_vector_column_from_expression_tree(cond);
                    if t1.is_some() && c1.is_some() {
                        return (t1, c1);
                    }
                    let (t2, c2) = Self::extract_vector_column_from_expression_tree(result);
                    if t2.is_some() && c2.is_some() {
                        return (t2, c2);
                    }
                }
                if let Some(else_expr) = else_result {
                    Self::extract_vector_column_from_expression_tree(else_expr)
                } else {
                    (None, None)
                }
            }
            _ => (None, None),
        }
    }
    
    /// Check if an expression contains vector operations
    fn expression_has_vector_operations(expr_type: &ProjectionExprType) -> bool {
        match expr_type {
            ProjectionExprType::Function(expr) => {
                Self::expression_tree_has_vector_operations(expr)
            }
            _ => false,
        }
    }
    
    /// Check if an expression tree contains vector operations
    fn expression_tree_has_vector_operations(expr: &Expression) -> bool {
        match expr {
            Expression::Function { name, .. } => {
                name == "VECTOR_SIMILARITY" || name == "VECTOR_DISTANCE"
            }
            Expression::BinaryOp { left, right, .. } => {
                Self::expression_tree_has_vector_operations(left) ||
                Self::expression_tree_has_vector_operations(right)
            }
            Expression::UnaryOp { expr, .. } => {
                Self::expression_tree_has_vector_operations(expr)
            }
            Expression::Cast { expr, .. } => {
                Self::expression_tree_has_vector_operations(expr)
            }
            Expression::Case { operand, conditions, else_result, .. } => {
                if let Some(op) = operand {
                    if Self::expression_tree_has_vector_operations(op) {
                        return true;
                    }
                }
                for (cond, result) in conditions {
                    if Self::expression_tree_has_vector_operations(cond) ||
                       Self::expression_tree_has_vector_operations(result) {
                        return true;
                    }
                }
                if let Some(else_expr) = else_result {
                    Self::expression_tree_has_vector_operations(else_expr)
                } else {
                    false
                }
            }
            _ => false,
        }
    }
    
    /// Optimize join order using hypergraph structure
    pub fn optimize_joins(&self, path: &HyperPath) -> Result<Vec<EdgeId>> {
        // Build join graph from path
        let join_graph = self.build_join_graph(path)?;
        
        // Find optimal join order using dynamic programming
        let optimal_order = self.find_optimal_order(&join_graph)?;
        
        Ok(optimal_order)
    }
    
    /// Build join graph representation
    fn build_join_graph(&self, path: &HyperPath) -> Result<JoinGraph> {
        let mut nodes = HashSet::new();
        let mut edges = vec![];
        
        // Collect all nodes and edges from path
        for &node_id in &path.nodes {
            nodes.insert(node_id);
        }
        
        for &edge_id in &path.edges {
            if let Some(edge) = self.graph.get_edge(edge_id) {
                edges.push(edge_id);
            }
        }
        
        Ok(JoinGraph { nodes, edges })
    }
    
    /// Find optimal join order using dynamic programming
    fn find_optimal_order(&self, join_graph: &JoinGraph) -> Result<Vec<EdgeId>> {
        // Simplified DP: try all permutations of edges
        // Full implementation would use DP with memoization
        
        let mut best_order: Vec<EdgeId> = join_graph.edges.iter().cloned().collect();
        let mut best_cost = f64::INFINITY;
        
        // Try different orderings (simplified - just use current order)
        // Full implementation would enumerate all permutations
        let cost = self.estimate_join_order_cost(&best_order)?;
        if cost < best_cost {
            best_cost = cost;
        }
        
        Ok(best_order)
    }
    
    /// Enumerate all valid join orders
    fn enumerate_orders<F>(&self, remaining: &[EdgeId], current: &mut Vec<EdgeId>, f: &mut F) -> Result<()>
    where
        F: FnMut(&[EdgeId]) -> Result<()>,
    {
        if remaining.is_empty() {
            f(current)?;
            return Ok(());
        }
        
        for (i, &edge_id) in remaining.iter().enumerate() {
            // Check if this edge can be added (dependencies satisfied)
            if self.can_add_edge(current, edge_id)? {
                current.push(edge_id);
                let mut new_remaining = remaining.to_vec();
                new_remaining.remove(i);
                self.enumerate_orders(&new_remaining, current, f)?;
                current.pop();
            }
        }
        
        Ok(())
    }
    
    /// Check if an edge can be added to current order
    fn can_add_edge(&self, current: &[EdgeId], edge_id: EdgeId) -> Result<bool> {
        // Check if all required nodes are already joined
        if let Some(edge) = self.graph.get_edge(edge_id) {
            // For now, allow any order
            // Full implementation would check node dependencies
            Ok(true)
        } else {
            Ok(false)
        }
    }
    
    /// Estimate cost of a join order
    fn estimate_join_order_cost(&self, order: &[EdgeId]) -> Result<f64> {
        let mut cost = 0.0;
        let mut current_size = 1.0;
        
        for &edge_id in order {
            if let Some(edge) = self.graph.get_edge(edge_id) {
                // Estimate output size using actual statistics when available
                let fanout = if edge.stats.avg_fanout > 0.0 {
                    edge.stats.avg_fanout
                } else {
                    1.0 // Default fanout
                };
                let output_size = current_size * fanout;
                cost += current_size * self.cost_model.join_cost_per_row;
                current_size = output_size;
            }
        }
        
        Ok(cost)
    }
    
    /// Estimate cardinality using actual statistics from hypergraph nodes
    /// This provides better estimates than the default CostModel
    pub fn estimate_cardinality(&self, op: &PlanOperator) -> usize {
        match op {
            PlanOperator::Scan { node_id, .. } => {
                // OPTIMIZATION: Use actual statistics from hypergraph node (including size_bytes)
                if let Some(node) = self.graph.get_node(*node_id) {
                    // Try to use node statistics first
                    let stats_row_count = node.stats.row_count;
                    if stats_row_count > 0 {
                        return stats_row_count;
                    }
                    
                    // Fallback: calculate from fragments
                    let fragment_row_count: usize = node.fragments.iter().map(|f| f.len()).sum();
                    if fragment_row_count > 0 {
                        return fragment_row_count;
                    }
                    
                    // Use cardinality estimate from stats
                    if node.stats.cardinality > 0 {
                        return node.stats.cardinality;
                    }
                    
                    // OPTIMIZATION: Use size_bytes to estimate rows if available
                    // Estimate: ~8 bytes per row on average (for numeric data)
                    if node.stats.size_bytes > 0 {
                        let estimated_rows = node.stats.size_bytes / 8;
                        if estimated_rows > 0 {
                            return estimated_rows;
                        }
                    }
                }
                
                // Default estimate if no statistics available
                self.cost_model.estimate_cardinality(op)
            }
            _ => {
                // For other operators, use cost model but with improved selectivity
                self.cost_model.estimate_cardinality(op)
            }
        }
    }
}

struct JoinGraph {
    nodes: HashSet<NodeId>,
    edges: Vec<EdgeId>,
}

/// Cardinality estimator with histograms
pub struct CardinalityEstimator {
    /// Histograms per column
    histograms: HashMap<(String, String), Histogram>,
}

impl CardinalityEstimator {
    pub fn new() -> Self {
        Self {
            histograms: HashMap::new(),
        }
    }
    
    /// Estimate selectivity of a filter
    pub fn estimate_filter_selectivity(
        &self,
        table: &str,
        column: &str,
        predicate: &crate::query::plan::FilterPredicate,
    ) -> f64 {
        // Use histogram to estimate selectivity
        if let Some(hist) = self.histograms.get(&(table.to_string(), column.to_string())) {
            hist.estimate_selectivity(predicate)
        } else {
            // Default selectivity
            0.1
        }
    }
    
    /// Estimate join cardinality
    pub fn estimate_join_cardinality(
        &self,
        left_table: &str,
        left_column: &str,
        right_table: &str,
        right_column: &str,
        left_size: usize,
        right_size: usize,
    ) -> usize {
        // Use histograms to estimate join size
        let left_card = self.get_cardinality(left_table, left_column);
        let right_card = self.get_cardinality(right_table, right_column);
        
        // Simplified: assume uniform distribution
        if left_card > 0 && right_card > 0 {
            (left_size * right_size) / left_card.max(right_card)
        } else {
            left_size.min(right_size)
        }
    }
    
    fn get_cardinality(&self, table: &str, column: &str) -> usize {
        self.histograms
            .get(&(table.to_string(), column.to_string()))
            .map(|h| h.cardinality())
            .unwrap_or(1000)
    }
}

/// Simple histogram for cardinality estimation
pub struct Histogram {
    buckets: Vec<(crate::storage::fragment::Value, usize)>,
    total_count: usize,
}

impl Histogram {
    pub fn new(buckets: Vec<(crate::storage::fragment::Value, usize)>, total_count: usize) -> Self {
        Self { buckets, total_count }
    }
    
    pub fn estimate_selectivity(&self, predicate: &crate::query::plan::FilterPredicate) -> f64 {
        match predicate {
            crate::query::plan::FilterPredicate { operator: crate::query::plan::PredicateOperator::Equals, value, .. } => {
                // Find bucket with this value
                if let Some((_, count)) = self.buckets.iter().find(|(v, _)| v == value) {
                    *count as f64 / self.total_count as f64
                } else {
                    0.0
                }
            }
            _ => {
                // Default selectivity for range predicates
                0.1
            }
        }
    }
    
    pub fn cardinality(&self) -> usize {
        self.buckets.len()
    }
}

