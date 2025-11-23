use crate::query::plan::*;
use crate::query::parser::ParsedQuery;
use crate::query::cte::CTEContext;
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::NodeId;
use crate::hypergraph::edge::EdgeId;
use anyhow::Result;
use std::sync::Arc;

/// Query planner - converts parsed query to execution plan
#[derive(Clone)]
pub struct QueryPlanner {
    graph: Arc<HyperGraph>,
    cte_context: Option<CTEContext>,
}

impl QueryPlanner {
    pub fn new(graph: HyperGraph) -> Self {
        Self { 
            graph: Arc::new(graph),
            cte_context: None,
        }
    }
    
    pub fn from_arc(graph: Arc<HyperGraph>) -> Self {
        Self { 
            graph,
            cte_context: None,
        }
    }
    
    pub fn with_cte_context(mut self, cte_context: CTEContext) -> Self {
        self.cte_context = Some(cte_context);
        self
    }
    
    /// Plan a query
    pub fn plan(&self, parsed: &ParsedQuery) -> Result<QueryPlan> {
        self.plan_with_ast(parsed, None)
    }
    
    /// Plan a query with optional AST (for HAVING clause and window function parsing)
    pub fn plan_with_ast(&self, parsed: &ParsedQuery, ast: Option<&sqlparser::ast::Statement>) -> Result<QueryPlan> {
        // Plan normally first
        let mut plan = self.plan_internal(parsed)?;
        
        // Add Window operator if window functions are present
        if !parsed.window_functions.is_empty() {
            // Convert WindowFunctionInfo to WindowFunctionExpr
            use crate::query::plan::{WindowFunctionExpr, WindowFunction, OrderByExpr};
            
            let window_exprs: Vec<WindowFunctionExpr> = parsed.window_functions.iter().map(|wf_info| {
                // Convert WindowFunctionType to WindowFunction
                let win_func = match &wf_info.function {
                    crate::query::parser::WindowFunctionType::RowNumber => WindowFunction::RowNumber,
                    crate::query::parser::WindowFunctionType::Rank => WindowFunction::Rank,
                    crate::query::parser::WindowFunctionType::DenseRank => WindowFunction::DenseRank,
                    crate::query::parser::WindowFunctionType::Lag { offset } => WindowFunction::Lag { offset: *offset },
                    crate::query::parser::WindowFunctionType::Lead { offset } => WindowFunction::Lead { offset: *offset },
                    crate::query::parser::WindowFunctionType::SumOver => WindowFunction::SumOver,
                    crate::query::parser::WindowFunctionType::AvgOver => WindowFunction::AvgOver,
                    crate::query::parser::WindowFunctionType::MinOver => WindowFunction::MinOver,
                    crate::query::parser::WindowFunctionType::MaxOver => WindowFunction::MaxOver,
                    crate::query::parser::WindowFunctionType::CountOver => WindowFunction::CountOver,
                    crate::query::parser::WindowFunctionType::FirstValue => WindowFunction::FirstValue,
                    crate::query::parser::WindowFunctionType::LastValue => WindowFunction::LastValue,
                };
                
                // Convert OrderByInfo to OrderByExpr
                let order_by: Vec<OrderByExpr> = wf_info.order_by.iter().map(|o| {
                    OrderByExpr {
                        column: o.column.clone(),
                        ascending: o.ascending,
                    }
                }).collect();
                
                WindowFunctionExpr {
                    function: win_func,
                    column: wf_info.column.clone(),
                    alias: wf_info.alias.clone(),
                    partition_by: wf_info.partition_by.clone(),
                    order_by,
                    frame: wf_info.frame.as_ref().map(|f| {
                        crate::query::plan::WindowFrame {
                            frame_type: match f.frame_type {
                                crate::query::parser::FrameType::Rows => crate::query::plan::FrameType::Rows,
                                crate::query::parser::FrameType::Range => crate::query::plan::FrameType::Range,
                            },
                            start: match &f.start {
                                crate::query::parser::FrameBound::UnboundedPreceding => crate::query::plan::FrameBound::UnboundedPreceding,
                                crate::query::parser::FrameBound::Preceding(n) => crate::query::plan::FrameBound::Preceding(*n),
                                crate::query::parser::FrameBound::CurrentRow => crate::query::plan::FrameBound::CurrentRow,
                                crate::query::parser::FrameBound::Following(n) => crate::query::plan::FrameBound::Following(*n),
                                crate::query::parser::FrameBound::UnboundedFollowing => crate::query::plan::FrameBound::UnboundedFollowing,
                            },
                            end: f.end.as_ref().map(|e| {
                                match e {
                                    crate::query::parser::FrameBound::UnboundedPreceding => crate::query::plan::FrameBound::UnboundedPreceding,
                                    crate::query::parser::FrameBound::Preceding(n) => crate::query::plan::FrameBound::Preceding(*n),
                                    crate::query::parser::FrameBound::CurrentRow => crate::query::plan::FrameBound::CurrentRow,
                                    crate::query::parser::FrameBound::Following(n) => crate::query::plan::FrameBound::Following(*n),
                                    crate::query::parser::FrameBound::UnboundedFollowing => crate::query::plan::FrameBound::UnboundedFollowing,
                                }
                            }),
                        }
                    }),
                }
            }).collect();
            
            // Add Window operator BEFORE final projection but AFTER aggregation
            // Window functions are computed after GROUP BY but before final SELECT projection
            let window_op = PlanOperator::Window {
                input: Box::new(plan.root),
                window_functions: window_exprs,
            };
            plan.root = window_op;
        }
        
        // Add HAVING operator if present and AST is available (HAVING comes after aggregation)
        if let (Some(having_str), Some(ast_stmt)) = (&parsed.having, ast) {
            if !having_str.is_empty() {
                // Extract HAVING expression from AST
                if let sqlparser::ast::Statement::Query(query) = ast_stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
                        if let Some(having_expr) = &select.having {
                            // Convert SQL AST expression to our Expression type
                            use crate::query::ast_to_expression::sql_expr_to_expression;
                            match sql_expr_to_expression(having_expr) {
                                Ok(mut having_expression) => {
                                    // Rewrite aggregate function calls in HAVING to column references
                                    // After aggregation, COUNT(*) becomes a column named "COUNT" or alias
                                    having_expression = self.rewrite_having_expression(having_expression, &parsed.aggregates);
                                    
                                    // Wrap current plan with HavingOperator (after window functions)
                                    let having_op = PlanOperator::Having {
                                        input: Box::new(plan.root),
                                        predicate: having_expression,
                                    };
                                    plan.root = having_op;
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to parse HAVING clause: {}, skipping", e);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        plan.optimize();
        Ok(plan)
    }
    
    /// Internal plan method (moved from plan())
    fn plan_internal(&self, parsed: &ParsedQuery) -> Result<QueryPlan> {
        // Check if this is a set operation (UNION, INTERSECT, EXCEPT)
        if let Some(set_op_str) = parsed.tables.first() {
            if set_op_str.starts_with("__SET_OP_") {
                return self.plan_set_operation(parsed);
            }
        }
        
        // 0. Handle CTEs - check if any tables are actually CTEs
        let mut actual_tables = parsed.tables.clone();
        let mut cte_tables = std::collections::HashSet::new();
        if let Some(ref cte_ctx) = self.cte_context {
            // Identify CTE tables but keep them in the list
            for table in &parsed.tables {
                if cte_ctx.contains(table) {
                    cte_tables.insert(table.clone());
                }
            }
            // Don't filter out CTEs - we'll handle them in build_plan_tree
        }
        
        // 1. Find nodes for all non-CTE tables
        let non_cte_tables: Vec<String> = actual_tables.iter()
            .filter(|t| !cte_tables.contains(*t))
            .cloned()
            .collect();
        let table_nodes = if non_cte_tables.is_empty() && !cte_tables.is_empty() {
            // Only CTE tables - return empty nodes list
            vec![]
        } else {
            self.find_table_nodes(&non_cte_tables)?
        };
        
        // 2. Find join paths between tables
        let join_path = self.find_join_path(&parsed.joins, &table_nodes)?;
        
        // 3. Build plan tree
        let mut root = self.build_plan_tree(parsed, &table_nodes, &join_path)?;
        
        // 3.5. Add DISTINCT if needed
        if parsed.distinct {
            root = PlanOperator::Distinct {
                input: Box::new(root),
            };
        }
        
        // 4. Create plan
        let mut plan = QueryPlan::new(root).with_table_aliases(parsed.table_aliases.clone());
        
        // 4.5. Detect vector operations and add index hints
        use crate::query::optimizer::HypergraphOptimizer;
        let optimizer = HypergraphOptimizer::new((*self.graph).clone());
        plan.vector_index_hints = optimizer.detect_vector_operations(&plan);
        
        // 5. Optimize plan
        plan.optimize();
        
        Ok(plan)
    }
    
    /// Plan a set operation (UNION, INTERSECT, EXCEPT)
    fn plan_set_operation(&self, parsed: &ParsedQuery) -> Result<QueryPlan> {
        // Extract set operation type from first table
        let set_op_type = if let Some(set_op_str) = parsed.tables.first() {
            if set_op_str == "__SET_OP_UNION_ALL__" {
                SetOperationType::UnionAll
            } else if set_op_str == "__SET_OP_UNION__" {
                SetOperationType::Union
            } else if set_op_str == "__SET_OP_INTERSECT__" {
                SetOperationType::Intersect
            } else if set_op_str == "__SET_OP_EXCEPT__" {
                SetOperationType::Except
            } else {
                anyhow::bail!("Unknown set operation: {}", set_op_str);
            }
        } else {
            anyhow::bail!("Set operation marker not found");
        };
        
        // The parser has already extracted left and right sides
        // We need to plan them separately. Since we don't have the AST here,
        // we'll need to reconstruct the queries from the parsed info.
        // For now, we'll use a simpler approach: plan based on the parsed query structure
        
        // The parsed query contains info from the left side, and the right side tables
        // are in parsed.tables[1..]. We need to plan both sides.
        // However, without the full AST, we can't properly reconstruct the queries.
        // So we'll need to pass the AST to the planner or store it in ParsedQuery.
        
        // For now, let's check if we can extract enough info from parsed
        // The issue is that parsed only has info from the left side, not the right.
        // We need to modify the parser to store both sides, or pass AST to planner.
        
        // TEMPORARY: Return an error indicating we need AST
        // The proper fix would be to either:
        // 1. Store left/right ParsedQuery in ParsedQuery struct
        // 2. Pass AST to planner.plan() method
        anyhow::bail!("Set operations require full query AST - needs engine integration")
    }
    
    /// Rewrite HAVING expression to convert aggregate function calls to column references
    fn rewrite_having_expression(&self, expr: crate::query::expression::Expression, aggregates: &[crate::query::parser::AggregateInfo]) -> crate::query::expression::Expression {
        use crate::query::expression::Expression;
        
        match expr {
            Expression::Function { name, args } => {
                // Check if this is an aggregate function that should be converted to column reference
                let upper_name = name.to_uppercase();
                if upper_name == "COUNT" || upper_name == "SUM" || upper_name == "AVG" || upper_name == "MIN" || upper_name == "MAX" {
                    // Check if there's a matching aggregate with alias
                    for agg in aggregates {
                        let agg_name = match agg.function {
                            crate::query::parser::AggregateFunction::Count => "COUNT",
                            crate::query::parser::AggregateFunction::Sum => "SUM",
                            crate::query::parser::AggregateFunction::Avg => "AVG",
                            crate::query::parser::AggregateFunction::Min => "MIN",
                            crate::query::parser::AggregateFunction::Max => "MAX",
                            crate::query::parser::AggregateFunction::CountDistinct => "COUNT",
                        };
                        
                        if upper_name == agg_name {
                            // Use alias if available (case-insensitive), otherwise use function name
                            // Try to find the actual column name from schema - prefer alias but fall back to function name
                            if let Some(ref alias) = agg.alias {
                                // Use the alias as-is (preserve original case)
                                return Expression::Column(alias.clone(), None);
                            } else {
                                // No alias - use function name (uppercase like "COUNT", "SUM")
                                return Expression::Column(upper_name.clone(), None);
                            }
                        }
                    }
                    // No matching aggregate found - use function name as column name
                    Expression::Column(upper_name, None)
                } else {
                    // Not an aggregate function - keep as function call but rewrite args
                    Expression::Function {
                        name,
                        args: args.into_iter().map(|a| self.rewrite_having_expression(a, aggregates)).collect(),
                    }
                }
            }
            Expression::BinaryOp { left, op, right } => {
                Expression::BinaryOp {
                    left: Box::new(self.rewrite_having_expression(*left, aggregates)),
                    op,
                    right: Box::new(self.rewrite_having_expression(*right, aggregates)),
                }
            }
            Expression::UnaryOp { op, expr } => {
                Expression::UnaryOp {
                    op,
                    expr: Box::new(self.rewrite_having_expression(*expr, aggregates)),
                }
            }
            Expression::Cast { expr, data_type } => {
                Expression::Cast {
                    expr: Box::new(self.rewrite_having_expression(*expr, aggregates)),
                    data_type,
                }
            }
            Expression::Case { operand, conditions, else_result } => {
                Expression::Case {
                    operand: operand.map(|e| Box::new(self.rewrite_having_expression(*e, aggregates))),
                    conditions: conditions.into_iter().map(|(c, r)| {
                        (self.rewrite_having_expression(c, aggregates), self.rewrite_having_expression(r, aggregates))
                    }).collect(),
                    else_result: else_result.map(|e| Box::new(self.rewrite_having_expression(*e, aggregates))),
                }
            }
            other => other, // Literals, columns, etc. don't need rewriting
        }
    }
    
    /// Plan a set operation with AST (called from engine)
    pub fn plan_set_operation_with_ast(&self, ast: &sqlparser::ast::Statement, parsed: &ParsedQuery) -> Result<QueryPlan> {
        use sqlparser::ast::*;
        
        // Extract set operation from AST
        let (set_op, left_body, right_body, is_union_all) = if let Statement::Query(query) = ast {
            if let SetExpr::SetOperation { op, left, right, set_quantifier } = &*query.body {
                let is_all = matches!(set_quantifier, sqlparser::ast::SetQuantifier::All);
                (op, left.clone(), right.clone(), is_all)
            } else {
                anyhow::bail!("Not a set operation");
            }
        } else {
            anyhow::bail!("Not a query statement");
        };
        
        // Determine set operation type
        let set_op_type = match set_op {
            sqlparser::ast::SetOperator::Union => {
                if is_union_all {
                    SetOperationType::UnionAll
                } else {
                    SetOperationType::Union
                }
            }
            sqlparser::ast::SetOperator::Intersect => SetOperationType::Intersect,
            sqlparser::ast::SetOperator::Except => SetOperationType::Except,
        };
        
        // Parse and plan left side
        let left_ast = Statement::Query(Box::new(Query {
            body: left_body,
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
            locks: vec![],
            with: None,
            for_clause: None,
            limit_by: vec![],
        }));
        let left_parsed = crate::query::parser_enhanced::extract_query_info_enhanced(&left_ast)?;
        let left_plan = self.plan(&left_parsed)?;
        
        // Parse and plan right side
        let right_ast = Statement::Query(Box::new(Query {
            body: right_body,
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
            locks: vec![],
            with: None,
            for_clause: None,
            limit_by: vec![],
        }));
        let right_parsed = crate::query::parser_enhanced::extract_query_info_enhanced(&right_ast)?;
        let right_plan = self.plan(&right_parsed)?;
        
        // Combine with SetOperation
        let root = PlanOperator::SetOperation {
            left: Box::new(left_plan.root),
            right: Box::new(right_plan.root),
            operation: set_op_type,
        };
        
        let mut plan = QueryPlan::new(root);
        plan.optimize();
        Ok(plan)
    }
    
    fn find_table_nodes(&self, tables: &[String]) -> Result<Vec<NodeId>> {
        let mut nodes = vec![];
        
        if tables.is_empty() {
            anyhow::bail!("No tables in query");
        }
        
        // OPTIMIZATION: Use O(1) table lookup instead of iterating all nodes
        for table in tables {
            // Normalize table name (remove quotes, handle case)
            let normalized_table = table.trim_matches('"').trim_matches('\'').to_lowercase();
            tracing::debug!("Normalized table name: '{}'", normalized_table);
            
            // EDGE CASE 2 FIX: Check CTE context FIRST before hypergraph lookup
            // This allows nested CTEs to reference earlier CTEs during materialization
            let is_cte = if let Some(ref cte_ctx) = self.cte_context {
                cte_ctx.contains(&normalized_table)
            } else {
                false
            };
            
            if is_cte {
                tracing::debug!("Table '{}' is a CTE, will be handled by CTEScan operator", normalized_table);
                // Don't add to nodes - CTEScan will handle it
            } else {
                // Use O(1) lookup via table_index
                if let Some(table_node) = self.graph.get_table_node(&normalized_table) {
                    nodes.push(table_node.id);
                    tracing::debug!("Found table node: {:?}", table_node.id);
                } else {
                    // Fallback: try case-insensitive lookup
                    let mut found = false;
                    for entry in self.graph.table_index.iter() {
                        let (table_name, node_id) = (entry.key(), entry.value());
                        if table_name.to_lowercase() == normalized_table {
                            if let Some(node) = self.graph.get_node(*node_id) {
                                nodes.push(node.id);
                                found = true;
                                tracing::debug!("Found table node (case-insensitive): {:?}", node.id);
                                break;
                            }
                        }
                    }
                    
                    if !found {
                        // EDGE CASE 2 FIX: Check if this is a CTE before failing
                        if let Some(ref cte_ctx) = self.cte_context {
                            if cte_ctx.contains(&normalized_table) {
                                tracing::debug!("Table '{}' is a CTE, will be handled by CTEScan operator", normalized_table);
                                // Don't add to nodes - CTEScan will handle it
                                continue;
                            }
                        }
                        // Collect available tables for error message
                        let mut all_table_names = Vec::new();
                        for entry in self.graph.table_index.iter() {
                            all_table_names.push(entry.key().clone());
                        }
                        anyhow::bail!("Table '{}' not found in hypergraph and is not a CTE. Available tables: {:?}", table, all_table_names);
                    }
                }
            }
        }
        
        Ok(nodes)
    }
    
    fn find_join_path(&self, joins: &[crate::query::parser::JoinInfo], table_nodes: &[NodeId]) -> Result<Vec<EdgeId>> {
        let mut edges = vec![];
        
        for join in joins {
            // Find edge matching this join predicate
            // We need to find an edge that connects the two tables with the matching columns
            
            // Find nodes for the join columns
            let left_node = self.graph.get_node_by_table_column(&join.left_table, &join.left_column);
            let right_node = self.graph.get_node_by_table_column(&join.right_table, &join.right_column);
            
            if let (Some(left_node), Some(right_node)) = (left_node, right_node) {
                let mut found_edge = false;
                
                // Try to find an edge between these nodes that matches the predicate
                // Check outgoing edges from left node
                let outgoing_edges = self.graph.get_outgoing_edges(left_node.id);
                for edge in outgoing_edges {
                    if edge.target == right_node.id && 
                       edge.matches_predicate(&join.left_table, &join.left_column, &join.right_table, &join.right_column) {
                        edges.push(edge.id);
                        found_edge = true;
                        break;
                    }
                }
                
                // If not found in outgoing, check incoming edges
                if !found_edge {
                    let incoming_edges = self.graph.get_incoming_edges(left_node.id);
                    for edge in incoming_edges {
                        if edge.source == right_node.id && 
                           edge.matches_predicate(&join.right_table, &join.right_column, &join.left_table, &join.left_column) {
                            edges.push(edge.id);
                            found_edge = true;
                            break;
                        }
                    }
                }
                
                // If still not found, check edges from right to left
                if !found_edge {
                    let outgoing_from_right = self.graph.get_outgoing_edges(right_node.id);
                    for edge in outgoing_from_right {
                        if edge.target == left_node.id && 
                           edge.matches_predicate(&join.right_table, &join.right_column, &join.left_table, &join.left_column) {
                            edges.push(edge.id);
                            found_edge = true;
                            break;
                        }
                    }
                }
                
                // If no edge found at column level, try table-level edges
                if !found_edge {
                    if let (Some(left_table_node), Some(right_table_node)) = (
                        self.graph.get_table_node(&join.left_table),
                        self.graph.get_table_node(&join.right_table)
                    ) {
                        // Check outgoing edges from left table node
                        let table_edges = self.graph.get_outgoing_edges(left_table_node.id);
                        for edge in table_edges {
                            if edge.target == right_table_node.id {
                                edges.push(edge.id);
                                found_edge = true;
                                break;
                            }
                        }
                    }
                }
                
                // If still not found, warn but continue
                // The join operator will handle the join without a pre-defined edge
                if !found_edge {
                    tracing::warn!(
                        "No edge found for join: {}.{} = {}.{} - join will proceed without pre-defined edge",
                        join.left_table, join.left_column,
                        join.right_table, join.right_column
                    );
                }
            }
        }
        
        Ok(edges)
    }
    
    fn build_plan_tree(
        &self,
        parsed: &ParsedQuery,
        table_nodes: &[NodeId],
        join_path: &[EdgeId],
    ) -> Result<PlanOperator> {
        // Only push LIMIT/OFFSET down to scan when there's NO aggregation
        // When there's GROUP BY or aggregates, LIMIT must be applied AFTER aggregation
        let has_aggregation = !parsed.aggregates.is_empty() || !parsed.group_by.is_empty();
        let scan_limit = if has_aggregation {
            None  // Don't push LIMIT to scan when aggregating - must apply after GROUP BY
        } else {
            parsed.limit  // Can push LIMIT down to scan for optimization when no aggregation
        };
        let scan_offset = if has_aggregation {
            None  // Don't push OFFSET to scan when aggregating
        } else {
            parsed.offset
        };
        
        // Start with scan operators - check if first table is a CTE
        let first_table = &parsed.tables[0];
        let mut current_op = {
            // Check if this is a CTE reference first (before checking table_nodes)
            if let Some(ref cte_ctx) = self.cte_context {
                if cte_ctx.contains(first_table) {
                    // This is a CTE - create CTEScan operator
                    PlanOperator::CTEScan {
                        cte_name: first_table.clone(),
                        columns: parsed.columns.clone(),
                        limit: scan_limit,
                        offset: scan_offset,
                    }
                } else if let Some(node_id) = table_nodes.first() {
                    // Regular table scan
                    PlanOperator::Scan {
                        node_id: *node_id,
                        table: first_table.clone(),
                        columns: parsed.columns.clone(),
                        limit: scan_limit,  // Only pushed when no aggregation
                        offset: scan_offset,
                    }
                } else {
                    anyhow::bail!("Table '{}' not found and is not a CTE", first_table);
                }
            } else if let Some(node_id) = table_nodes.first() {
                // No CTE context - regular table scan
                PlanOperator::Scan {
                    node_id: *node_id,
                    table: first_table.clone(),
                    columns: parsed.columns.clone(),
                    limit: scan_limit,  // Only pushed when no aggregation
                    offset: scan_offset,
                }
            } else {
                anyhow::bail!("No tables in query");
            }
        };
        
        // Add joins
        // CRITICAL FIX: Create JOIN operators even if join_path is empty (no hypergraph edge)
        // The JOIN operator can work without a pre-defined edge using hash joins
        let num_joins = parsed.joins.len().max(join_path.len());
        
        for i in 0..num_joins {
            if i + 1 < table_nodes.len() {
                let right_table = &parsed.tables[i + 1];
                let right_op = if let Some(ref cte_ctx) = self.cte_context {
                    if cte_ctx.contains(right_table) {
                        // Right table is a CTE
                        PlanOperator::CTEScan {
                            cte_name: right_table.clone(),
                            columns: parsed.columns.clone(),
                            limit: None,  // Joins don't push LIMIT to right side
                            offset: None,
                        }
                    } else {
                        // Regular table scan
                        PlanOperator::Scan {
                            node_id: table_nodes[i + 1],
                            table: right_table.clone(),
                            columns: parsed.columns.clone(),
                            limit: None,  // Joins don't push LIMIT to right side (would be incorrect)
                            offset: None,
                        }
                    }
                } else {
                    // No CTE context - regular table scan
                    PlanOperator::Scan {
                        node_id: table_nodes[i + 1],
                        table: right_table.clone(),
                        columns: parsed.columns.clone(),
                        limit: None,  // Joins don't push LIMIT to right side (would be incorrect)
                        offset: None,
                    }
                };
                
                // Extract join predicate and type from parsed query
                let join_info = parsed.joins.get(i).cloned();
                let (join_type, join_predicate) = if let Some(join) = join_info {
                    let jt = match join.join_type {
                        crate::query::parser::JoinType::Inner => JoinType::Inner,
                        crate::query::parser::JoinType::Left => JoinType::Left,
                        crate::query::parser::JoinType::Right => JoinType::Right,
                        crate::query::parser::JoinType::Full => JoinType::Full,
                    };
                    let pred = JoinPredicate {
                        left: (join.left_table, join.left_column),
                        right: (join.right_table, join.right_column),
                        operator: PredicateOperator::Equals,
                    };
                    (jt, pred)
                } else {
                    // Default to inner join if not specified
                    let pred = JoinPredicate {
                        left: ("".to_string(), "".to_string()),
                        right: ("".to_string(), "".to_string()),
                        operator: PredicateOperator::Equals,
                    };
                    (JoinType::Inner, pred)
                };
                
                // Use edge_id from join_path if available, otherwise use EdgeId(0) (no edge)
                let edge_id = join_path.get(i).copied().unwrap_or(crate::hypergraph::edge::EdgeId(0));
                
                current_op = PlanOperator::Join {
                    left: Box::new(current_op),
                    right: Box::new(right_op),
                    edge_id,
                    join_type,
                    predicate: join_predicate,
                };
            }
        }
        
        // Add filters
        if !parsed.filters.is_empty() {
            let predicates = parsed.filters.iter().map(|f| {
                // Convert FilterInfo to FilterPredicate
                let column = if f.table.is_empty() {
                    f.column.clone()
                } else {
                    format!("{}.{}", f.table, f.column)
                };
                
                let operator = match f.operator {
                    crate::query::parser::FilterOperator::Equals => PredicateOperator::Equals,
                    crate::query::parser::FilterOperator::NotEquals => PredicateOperator::NotEquals,
                    crate::query::parser::FilterOperator::LessThan => PredicateOperator::LessThan,
                    crate::query::parser::FilterOperator::LessThanOrEqual => PredicateOperator::LessThanOrEqual,
                    crate::query::parser::FilterOperator::GreaterThan => PredicateOperator::GreaterThan,
                    crate::query::parser::FilterOperator::GreaterThanOrEqual => PredicateOperator::GreaterThanOrEqual,
                    crate::query::parser::FilterOperator::Like => PredicateOperator::Like,
                    crate::query::parser::FilterOperator::NotLike => PredicateOperator::NotLike,
                    crate::query::parser::FilterOperator::In => PredicateOperator::In,
                    crate::query::parser::FilterOperator::NotIn => PredicateOperator::NotIn,
                    crate::query::parser::FilterOperator::IsNull => PredicateOperator::IsNull,
                    crate::query::parser::FilterOperator::IsNotNull => PredicateOperator::IsNotNull,
                    _ => PredicateOperator::Equals, // Default for Between, etc.
                };
                
                // Convert value string to Value enum
                let value = if !f.value.is_empty() {
                    // Try to parse as number first
                    if let Ok(i) = f.value.parse::<i64>() {
                        crate::storage::fragment::Value::Int64(i)
                    } else if let Ok(fl) = f.value.parse::<f64>() {
                        crate::storage::fragment::Value::Float64(fl)
                    } else {
                        crate::storage::fragment::Value::String(f.value.clone())
                    }
                } else {
                    crate::storage::fragment::Value::String(String::new())
                };
                
                // Convert in_values from Vec<String> to Vec<Value>
                let in_values = f.in_values.as_ref().map(|vals| {
                    vals.iter().map(|v| {
                        if let Ok(i) = v.parse::<i64>() {
                            crate::storage::fragment::Value::Int64(i)
                        } else if let Ok(fl) = v.parse::<f64>() {
                            crate::storage::fragment::Value::Float64(fl)
                        } else {
                            crate::storage::fragment::Value::String(v.clone())
                        }
                    }).collect()
                });
                
                FilterPredicate {
                    column,
                    operator,
                    value,
                    pattern: f.pattern.clone(),
                    in_values,
                    subquery_expression: f.subquery_expression.clone(),
                }
            }).collect();
            
            current_op = PlanOperator::Filter {
                input: Box::new(current_op),
                predicates,
            };
        }
        
        // Add aggregates
        if !parsed.aggregates.is_empty() || !parsed.group_by.is_empty() {
            let aggregates = parsed.aggregates.iter().map(|a| {
                AggregateExpr {
                    function: match a.function {
                        crate::query::parser::AggregateFunction::Sum => AggregateFunction::Sum,
                        crate::query::parser::AggregateFunction::Count => AggregateFunction::Count,
                        crate::query::parser::AggregateFunction::Avg => AggregateFunction::Avg,
                        crate::query::parser::AggregateFunction::Min => AggregateFunction::Min,
                        crate::query::parser::AggregateFunction::Max => AggregateFunction::Max,
                        crate::query::parser::AggregateFunction::CountDistinct => AggregateFunction::CountDistinct,
                    },
                    column: a.column.clone(),
                    alias: a.alias.clone(),
                    cast_type: a.cast_type.clone(),
                }
            }).collect();
            
            current_op = PlanOperator::Aggregate {
                input: Box::new(current_op),
                group_by: parsed.group_by.clone(),
                aggregates,
                having: parsed.having.clone(),
            };
            
            // HAVING clause will be added as HavingOperator after aggregate
            // This is handled in plan_with_ast() method when AST is available
        }
        
        // Add projection
        // IMPORTANT: Before creating ProjectOperator, add columns needed for ORDER BY to the projection
        // SQL allows ORDER BY to reference columns not in SELECT, so we need to preserve them
        let mut projection_columns = parsed.columns.clone();
        let mut projection_expressions = parsed.projection_expressions.clone();
        
        // Collect columns needed for ORDER BY that aren't already in the projection
        if !parsed.order_by.is_empty() {
            for order_expr in &parsed.order_by {
                let order_col = &order_expr.column;
                
                // Check if this column is already in the projection
                // Strategy 1: Check if exact column name matches
                let exact_match = projection_columns.contains(order_col);
                
                // Strategy 2: Check if column name matches any projection expression's column
                let column_match = projection_expressions.iter().any(|expr| {
                    match &expr.expr_type {
                        crate::query::parser::ProjectionExprTypeInfo::Column(col) => col == order_col,
                        _ => false,
                    }
                });
                
                // Strategy 3: Check if ORDER BY column matches an alias (e.g., ORDER BY e1.salary when SELECT has e1.salary AS emp_salary)
                // This handles the case where ORDER BY uses qualified name but SELECT has an alias
                let alias_match = projection_expressions.iter().any(|expr| {
                    // Check if the alias matches the ORDER BY column (unqualified)
                    let order_unqualified = if order_col.contains('.') {
                        order_col.split('.').last().unwrap_or(order_col)
                    } else {
                        order_col
                    };
                    expr.alias.to_uppercase() == order_unqualified.to_uppercase() ||
                    expr.alias.to_uppercase() == order_col.to_uppercase()
                });
                
                // Strategy 4: Check if ORDER BY uses unqualified name that matches qualified column in projection
                // (e.g., ORDER BY salary when SELECT has e1.salary AS emp_salary)
                let unqualified_match = if !order_col.contains('.') {
                    projection_expressions.iter().any(|expr| {
                        match &expr.expr_type {
                            crate::query::parser::ProjectionExprTypeInfo::Column(col) => {
                                // Check if the column's unqualified part matches
                                if col.contains('.') {
                                    col.split('.').last().unwrap_or(col).to_uppercase() == order_col.to_uppercase()
                                } else {
                                    col.to_uppercase() == order_col.to_uppercase()
                                }
                            }
                            _ => false,
                        }
                    })
                } else {
                    false
                };
                
                let already_in_projection = exact_match || column_match || alias_match || unqualified_match;
                
                if !already_in_projection {
                    // Add this column to projection so it's available for ORDER BY
                    projection_columns.push(order_col.clone());
                    projection_expressions.push(crate::query::parser::ProjectionExprInfo {
                        alias: order_col.clone(), // Use column name as alias so it's available for ORDER BY
                        expr_type: crate::query::parser::ProjectionExprTypeInfo::Column(order_col.clone()),
                    });
                }
            }
        }
        
        if !projection_columns.is_empty() || !projection_expressions.is_empty() {
            // Convert projection expressions to plan format
            let expressions: Vec<crate::query::plan::ProjectionExpr> = projection_expressions.iter().map(|expr| {
                crate::query::plan::ProjectionExpr {
                    alias: expr.alias.clone(),
                    expr_type: match &expr.expr_type {
                        crate::query::parser::ProjectionExprTypeInfo::Column(col) => {
                            crate::query::plan::ProjectionExprType::Column(col.clone())
                        }
                        crate::query::parser::ProjectionExprTypeInfo::Cast { column, target_type } => {
                            crate::query::plan::ProjectionExprType::Cast {
                                column: column.clone(),
                                target_type: target_type.clone(),
                            }
                        }
                        crate::query::parser::ProjectionExprTypeInfo::Case(case_expr) => {
                            crate::query::plan::ProjectionExprType::Case(case_expr.clone())
                        }
                        crate::query::parser::ProjectionExprTypeInfo::Expression(func_expr) => {
                            crate::query::plan::ProjectionExprType::Function(func_expr.clone())
                        }
                    }
                }
            }).collect();
            
            eprintln!("DEBUG planner: Creating Project operator with columns: {:?}, expressions: {:?}", 
                projection_columns, 
                expressions.iter().map(|e| match &e.expr_type {
                    crate::query::plan::ProjectionExprType::Column(c) => format!("Column({})", c),
                    _ => format!("{:?}", e.expr_type),
                }).collect::<Vec<_>>());
            current_op = PlanOperator::Project {
                input: Box::new(current_op),
                columns: projection_columns,
                expressions,
            };
        }
        
        // Add sort
        if !parsed.order_by.is_empty() {
            let order_by: Vec<OrderByExpr> = parsed.order_by.iter().map(|o| {
                OrderByExpr {
                    column: o.column.clone(),
                    ascending: o.ascending,
                }
            }).collect();
            
            current_op = PlanOperator::Sort {
                input: Box::new(current_op),
                order_by,
                limit: parsed.limit,
                offset: parsed.offset,
            };
        }
        
        // Add limit
        if let Some(limit) = parsed.limit {
            current_op = PlanOperator::Limit {
                input: Box::new(current_op),
                limit,
                offset: parsed.offset.unwrap_or(0),
            };
        }
        
        Ok(current_op)
    }
}

