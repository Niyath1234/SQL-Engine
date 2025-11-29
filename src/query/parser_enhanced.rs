/// Enhanced SQL Parser - Properly extracts expressions, joins, filters, etc.
use sqlparser::ast::*;
use crate::query::expression;
use crate::query::parser::*;
use anyhow::{Result, Context};

/// Enhanced query extraction with proper expression parsing
pub fn extract_query_info_enhanced(statement: &Statement) -> Result<ParsedQuery> {
    match statement {
        Statement::Query(query) => {
            // Extract CTEs
            let cte_context = if let Some(with) = &query.with {
                crate::query::cte::CTEContext::from_query(query)?
            } else {
                crate::query::cte::CTEContext::new()
            };
            
            if let SetExpr::Select(select) = &*query.body {
                let tables = extract_tables_enhanced(select, &cte_context)?;
                let columns = extract_columns_enhanced(select)?;
                let joins = extract_joins_enhanced(select)?;
                // Extract filters from main query AND from derived tables (subqueries in FROM)
                let mut filters = extract_filters_enhanced(select)?;
                let derived_table_filters = extract_derived_table_filters(select, &cte_context)?;
                filters.extend(derived_table_filters);
                let aggregates = extract_aggregates_enhanced(select)?;
                let window_functions = extract_window_functions_enhanced(select)?;
                let group_by = extract_group_by_enhanced(select)?;
                let having = extract_having_enhanced(select)?;
                let order_by = extract_order_by_enhanced(query)?;
                let (limit, offset) = extract_limit_offset_enhanced(query)?;
                
                // Extract projection expressions (for CAST, etc.)
                let projection_expressions = extract_projection_expressions(select)?;
                
                // Extract DISTINCT
                let distinct = select.distinct.is_some();
                
                // Extract table aliases from FROM/JOIN clauses
                let table_aliases = extract_table_aliases(select, &cte_context)?;
                
                // Extract WHERE expression tree if it has OR conditions
                let where_expression = if let Some(where_clause) = &select.selection {
                    // Check if WHERE has OR at top level (handle Nested expressions for parentheses)
                    let top_level_expr = match where_clause {
                        Expr::Nested(inner) => inner.as_ref(),
                        _ => where_clause,
                    };
                    
                    if let Expr::BinaryOp { op, .. } = top_level_expr {
                        if matches!(op, sqlparser::ast::BinaryOperator::Or) {
                            // Convert WHERE clause to Expression tree for OR handling
                            use crate::query::ast_to_expression::sql_expr_to_expression;
                            match sql_expr_to_expression(where_clause) {
                                Ok(expr) => {
                                    eprintln!("DEBUG parser: Successfully converted WHERE clause with OR to Expression tree: {:?}", expr);
                                    Some(expr)
                                },
                                Err(e) => {
                                    eprintln!("DEBUG parser: Failed to convert WHERE clause to Expression tree: {}", e);
                                    None // Fallback to predicate-based filtering
                                }
                            }
                        } else {
                            eprintln!("DEBUG parser: WHERE clause does not have OR at top level (operator: {:?})", op);
                            None
                        }
                    } else {
                        eprintln!("DEBUG parser: WHERE clause is not a BinaryOp (type: {:?})", std::mem::discriminant(top_level_expr));
                        None
                    }
                } else {
                    None
                };
                
                // Extract derived tables (subqueries in FROM)
                let derived_tables = extract_derived_tables(select, &cte_context)?;
                
                let parsed = ParsedQuery {
                    tables,
                    columns,
                    join_edges: vec![], // TODO: Extract join edges from joins
                    joins,
                    filters,
                    where_expression,
                    aggregates,
                    window_functions,
                    group_by,
                    having,
                    order_by,
                    limit,
                    offset,
                    projection_expressions,
                    distinct,
                    table_aliases,
                    derived_tables,
                };
                
                // ==========================
                // 1. PARSER OUTPUT
                // ==========================
                eprintln!("[DEBUG parser] columns = {:?}", parsed.columns);
                eprintln!("[DEBUG parser] projection exprs = {:?}", parsed.projection_expressions);
                
                Ok(parsed)
            } else if let SetExpr::SetOperation { op, left, right, .. } = &*query.body {
                // Handle UNION, INTERSECT, EXCEPT
                // Parse left and right sides recursively
                let left_parsed = if let SetExpr::Select(_) = left.as_ref() {
                    extract_query_info_enhanced(&sqlparser::ast::Statement::Query(Box::new(Query {
                        body: left.clone(),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                        locks: vec![],
                        with: query.with.clone(),
                        for_clause: None,
                        limit_by: vec![],
                    })))?
                } else {
                    anyhow::bail!("Set operation left side must be a SELECT")
                };
                
                let right_parsed = if let SetExpr::Select(_) = right.as_ref() {
                    extract_query_info_enhanced(&sqlparser::ast::Statement::Query(Box::new(Query {
                        body: right.clone(),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                        locks: vec![],
                        with: query.with.clone(),
                        for_clause: None,
                        limit_by: vec![],
                    })))?
                } else {
                    anyhow::bail!("Set operation right side must be a SELECT")
                };
                
                // Return a special ParsedQuery that indicates a set operation
                // Store set operation info in a special way - we'll handle this in planner
                let mut result = left_parsed;
                // Ensure where_expression is set (use None for set operations)
                if result.where_expression.is_none() {
                    result.where_expression = None;
                }
                // Store set operation type in tables list (planner will detect and handle)
                // sqlparser 0.40: SetOperator is an enum, check the actual variant
                let set_op_str = match op {
                    sqlparser::ast::SetOperator::Union => {
                        // Check if this is UNION ALL by examining the query structure
                        // For now, default to UNION (not ALL) - we'll need to check the actual query
                        "__SET_OP_UNION__"
                    }
                    sqlparser::ast::SetOperator::Intersect => "__SET_OP_INTERSECT__",
                    sqlparser::ast::SetOperator::Except => "__SET_OP_EXCEPT__",
                };
                result.tables.insert(0, set_op_str.to_string());
                result.tables.extend(right_parsed.tables);
                // Preserve distinct from left side (both sides should have same distinct setting)
                Ok(result)
            } else {
                anyhow::bail!("Only SELECT queries are supported")
            }
        }
        _ => anyhow::bail!("Only SELECT queries are supported"),
    }
}

fn extract_tables_enhanced(select: &Select, cte_context: &crate::query::cte::CTEContext) -> Result<Vec<String>> {
    let mut tables = vec![];
    
    // Extract from FROM clause
    for item in &select.from {
        match &item.relation {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.0.iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                
                // Keep CTE tables in the list - planner will handle them as CTEScan
                tables.push(table_name);
            }
            TableFactor::Derived { subquery, alias, .. } => {
                // Derived table (subquery in FROM) - use the alias as the table name
                // Don't extract underlying tables - the derived table will be executed separately
                let table_alias = alias.as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| format!("derived_table_{}", tables.len()));
                tables.push(table_alias);
            }
            TableFactor::TableFunction { .. } |
            TableFactor::UNNEST { .. } |
            TableFactor::NestedJoin { .. } |
            TableFactor::Pivot { .. } |
            TableFactor::Unpivot { .. } |
            TableFactor::Function { .. } => {
                // Unsupported table factors
                // Skip for now
            }
        }
    }
    
    // Extract from JOINs
    for item in &select.from {
        for join in &item.joins {
            match &join.relation {
                TableFactor::Table { name, .. } => {
                    let table_name = name.0.iter()
                        .map(|ident| ident.value.clone())
                        .collect::<Vec<_>>()
                        .join(".");
                    // Keep CTE tables in the list - planner will handle them as CTEScan
                    tables.push(table_name);
                }
                TableFactor::Derived { subquery, alias, .. } => {
                    // Derived table (subquery in JOIN) - use the alias as the table name
                    let table_alias = alias.as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| format!("derived_table_{}", tables.len()));
                    tables.push(table_alias);
                }
                TableFactor::TableFunction { .. } |
                TableFactor::UNNEST { .. } |
                TableFactor::NestedJoin { .. } |
                TableFactor::Pivot { .. } |
                TableFactor::Unpivot { .. } |
                TableFactor::Function { .. } => {
                    // Unsupported
                }
            }
        }
    }
    
    Ok(tables)
}

/// Extract WHERE clauses from derived tables (subqueries in FROM) and convert them to filters
/// These filters need to be applied to the underlying table scans
fn extract_derived_table_filters(select: &Select, cte_context: &crate::query::cte::CTEContext) -> Result<Vec<FilterInfo>> {
    let mut filters = vec![];
    
    // Extract from main FROM clause
    for item in &select.from {
        match &item.relation {
            TableFactor::Derived { subquery, alias, .. } => {
                if let SetExpr::Select(subselect) = &*subquery.body {
                    // Extract WHERE clause from subquery
                    if let Some(where_clause) = &subselect.selection {
                        // Get the actual table name from the subquery (not the alias)
                        // The filter should be applied to the actual table, not the alias
                        let actual_table = extract_tables_enhanced(subselect, cte_context)
                            .ok()
                            .and_then(|t| t.first().cloned())
                            .unwrap_or_else(|| "".to_string());
                        
                        if !actual_table.is_empty() {
                            // Extract predicates from subquery WHERE clause
                            // Map them to use the actual table name (not the alias)
                            extract_predicates_with_table_alias(where_clause, &mut filters, &actual_table)?;
                        }
                    }
                }
            }
            _ => {}
        }
        
        // Also check joins for derived tables
        for join in &item.joins {
            match &join.relation {
                TableFactor::Derived { subquery, alias, .. } => {
                    if let SetExpr::Select(subselect) = &*subquery.body {
                        if let Some(where_clause) = &subselect.selection {
                            // Get the actual table name from the subquery (not the alias)
                            let actual_table = extract_tables_enhanced(subselect, cte_context)
                                .ok()
                                .and_then(|t| t.first().cloned())
                                .unwrap_or_else(|| "".to_string());
                            
                            if !actual_table.is_empty() {
                                extract_predicates_with_table_alias(where_clause, &mut filters, &actual_table)?;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    
    Ok(filters)
}

/// Extract predicates from WHERE clause and map them to a specific table alias
fn extract_predicates_with_table_alias(expr: &Expr, filters: &mut Vec<FilterInfo>, table_alias: &str) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                sqlparser::ast::BinaryOperator::And => {
                    extract_predicates_with_table_alias(left, filters, table_alias)?;
                    extract_predicates_with_table_alias(right, filters, table_alias)?;
                }
                sqlparser::ast::BinaryOperator::Or => {
                    // For OR, we still extract both sides but they'll be in the same filter list
                    extract_predicates_with_table_alias(left, filters, table_alias)?;
                    extract_predicates_with_table_alias(right, filters, table_alias)?;
                }
                _ => {
                    // Single predicate - extract it
                    extract_single_predicate_with_table_alias(expr, filters, table_alias)?;
                }
            }
        }
        _ => {
            // Single predicate
            extract_single_predicate_with_table_alias(expr, filters, table_alias)?;
        }
    }
    Ok(())
}

/// Extract a single predicate and map it to a table alias
fn extract_single_predicate_with_table_alias(expr: &Expr, filters: &mut Vec<FilterInfo>, table_alias: &str) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                sqlparser::ast::BinaryOperator::Eq => {
                    if let (Expr::CompoundIdentifier(left_cols), Expr::Value(right_val)) = (left.as_ref(), right.as_ref()) {
                        let col_name = left_cols.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
                        // If column doesn't have table prefix, add the table alias
                        let (table, column) = if left_cols.len() > 1 {
                            (left_cols[0].value.clone(), left_cols[1..].iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("."))
                        } else {
                            (table_alias.to_string(), col_name)
                        };
                        
                        let value = match right_val {
                            sqlparser::ast::Value::Number(n, _) => n.clone(),
                            sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                            sqlparser::ast::Value::DoubleQuotedString(s) => s.clone(),
                            sqlparser::ast::Value::Boolean(b) => b.to_string(),
                            _ => return Ok(()), // Skip unsupported value types
                        };
                        
                        filters.push(FilterInfo {
                            table,
                            column,
                            operator: crate::query::parser::FilterOperator::Equals,
                            value,
                            in_values: None,
                            pattern: None,
                            subquery_expression: None,
                        });
                    } else if let (Expr::Identifier(left_col), Expr::Value(right_val)) = (left.as_ref(), right.as_ref()) {
                        // Column without table prefix - use table alias
                        let value = match right_val {
                            sqlparser::ast::Value::Number(n, _) => n.clone(),
                            sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                            sqlparser::ast::Value::DoubleQuotedString(s) => s.clone(),
                            sqlparser::ast::Value::Boolean(b) => b.to_string(),
                            _ => return Ok(()),
                        };
                        
                        filters.push(FilterInfo {
                            table: table_alias.to_string(),
                            column: left_col.value.clone(),
                            operator: crate::query::parser::FilterOperator::Equals,
                            value,
                            in_values: None,
                            pattern: None,
                            subquery_expression: None,
                        });
                    }
                }
                _ => {
                    // For other operators, try to extract similarly
                    // This is a simplified version - full implementation would handle all operators
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn extract_columns_enhanced(select: &Select) -> Result<Vec<String>> {
    let mut columns = vec![];
    
    eprintln!("DEBUG extract_columns_enhanced: Processing {} projection items", select.projection.len());
    
    for (idx, item) in select.projection.iter().enumerate() {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                eprintln!("DEBUG extract_columns_enhanced[{}]: UnnamedExpr - {:?}", idx, expr);
                // Check if this is a qualified column reference (like "a.name" or "b.amount")
                // Don't expand wildcards here - they should remain as expressions
                if let Ok(col_name) = extract_column_name_from_expr(expr) {
                    eprintln!("DEBUG extract_columns_enhanced[{}]: Extracted column name: '{}'", idx, col_name);
                    // Only push if it's not a wildcard - wildcards are handled by QualifiedWildcard
                    if !col_name.ends_with(".*") {
                        columns.push(col_name);
                    } else {
                        // This shouldn't happen (wildcards should be QualifiedWildcard), but handle it
                        eprintln!("DEBUG extract_columns_enhanced[{}]: Warning - wildcard found in UnnamedExpr: '{}'", idx, col_name);
                        columns.push(col_name);
                    }
                } else {
                    // Use expression as column name
                    let expr_str = format!("{:?}", expr);
                    eprintln!("DEBUG extract_columns_enhanced[{}]: Using expression as column name: '{}'", idx, expr_str);
                    columns.push(expr_str);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                eprintln!("DEBUG extract_columns_enhanced[{}]: ExprWithAlias - alias: '{}'", idx, alias.value);
                columns.push(alias.value.clone());
            }
            SelectItem::Wildcard(_) => {
                eprintln!("DEBUG extract_columns_enhanced[{}]: Wildcard - storing '*'", idx);
                // Will be expanded later
                columns.push("*".to_string());
            }
            SelectItem::QualifiedWildcard(qualifier, _) => {
                let table = qualifier.0.iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                let wildcard = format!("{}.*", table);
                eprintln!("DEBUG extract_columns_enhanced[{}]: Found qualified wildcard '{}', storing as-is", idx, wildcard);
                columns.push(wildcard);
            }
        }
    }
    
    eprintln!("DEBUG extract_columns_enhanced: Final columns: {:?}", columns);
    Ok(columns)
}

fn extract_column_name_from_expr(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => {
            Ok(idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("."))
        }
        Expr::Function(func) => {
            // For aggregate functions like COUNT(*), we should NOT return the function name
            // Instead, we should try to find the alias from the SELECT list
            // But since we don't have access to SELECT aliases here, we'll return the function name
            // and let the caller handle alias resolution
            Ok(func.name.to_string().to_uppercase())
        }
        _ => anyhow::bail!("Cannot extract column name from expression")
    }
}

fn extract_joins_enhanced(select: &Select) -> Result<Vec<JoinInfo>> {
    let mut joins = vec![];
    
    // Track the current left table as we process joins
    // Start with the first table from FROM clause
    let mut current_left_table_alias: Option<String> = None;
    
    // Get the first table from FROM clause
    if let Some(from_item) = select.from.first() {
        match &from_item.relation {
            TableFactor::Table { name, alias, .. } => {
                current_left_table_alias = Some(
                    alias.as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| {
                            name.0.iter().map(|ident| ident.value.clone()).collect::<Vec<_>>().join(".")
                        })
                );
            }
            _ => {}
        }
    }
    
    for item in &select.from {
        for join in &item.joins {
            let join_type = match &join.join_operator {
                JoinOperator::Inner(_) => JoinType::Inner,
                JoinOperator::LeftOuter(_) => JoinType::Left,
                JoinOperator::RightOuter(_) => JoinType::Right,
                JoinOperator::FullOuter(_) => JoinType::Full,
                _ => JoinType::Inner, // Default to Inner for other join types
            };
            
            // Extract join condition - this gives us the table aliases from the ON clause
            let (left_table_from_on, left_column, right_table_from_on, right_column) = match &join.join_operator {
                JoinOperator::Inner(join_constraint) |
                JoinOperator::LeftOuter(join_constraint) |
                JoinOperator::RightOuter(join_constraint) |
                JoinOperator::FullOuter(join_constraint) => {
                    match join_constraint {
                        JoinConstraint::On(expr) => {
                            extract_join_condition(expr)?
                        }
                        JoinConstraint::Using(columns) => {
                            // USING clause - columns must exist in both tables
                            if columns.is_empty() {
                                continue;
                            }
                            let col_name = columns[0].value.clone();
                            // For USING, we need table names from context
                            // Use current_left_table_alias and the new join table
                            let left_alias = current_left_table_alias.clone().unwrap_or_else(|| "left".to_string());
                            ("left".to_string(), col_name.clone(), "right".to_string(), col_name)
                        }
                        _ => continue,
                    }
                }
                _ => continue,
            };
            
            // Get table name and alias from join relation (the right side of the join)
            let right_table_alias = match &join.relation {
                TableFactor::Table { name, alias, .. } => {
                    // Prefer alias from table definition (e.g., "o" from "orders o")
                    alias.as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| {
                            // No alias defined, use table name
                            name.0.iter().map(|ident| ident.value.clone()).collect::<Vec<_>>().join(".")
                        })
                }
                _ => right_table_from_on.clone(),
            };
            
            // CRITICAL FIX: Use the current left table (from previous join or FROM clause)
            // NOT always the first table from FROM clause
            // The left table should be the table that was just joined (or first table for first join)
            let left_table_alias = current_left_table_alias.clone()
                .unwrap_or_else(|| left_table_from_on.clone());
            
            joins.push(JoinInfo {
                left_table: left_table_alias.clone(),
                left_column,
                right_table: right_table_alias.clone(),
                right_column,
                join_type,
            });
            
            // Update current_left_table_alias to be the right table (for next join)
            current_left_table_alias = Some(right_table_alias);
        }
    }
    
    Ok(joins)
}

fn extract_join_condition(expr: &Expr) -> Result<(String, String, String, String)> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Handle AND clauses - extract the first equality condition for join key
            // Other conditions will be handled as filters during execution
            if matches!(op, sqlparser::ast::BinaryOperator::And) {
                // Try left side first (most common pattern: equality first)
                if let Ok(result) = extract_join_condition(left) {
                    return Ok(result);
                }
                // Try right side
                if let Ok(result) = extract_join_condition(right) {
                    return Ok(result);
                }
                anyhow::bail!("Join condition with AND must contain at least one equality condition")
            }
            // Handle equality conditions (primary join key)
            if matches!(op, sqlparser::ast::BinaryOperator::Eq) {
                let left_col = extract_column_ref(left)?;
                let right_col = extract_column_ref(right)?;
                Ok((left_col.0, left_col.1, right_col.0, right_col.1))
            } else {
                // For non-equality joins (e.g., <, >), we need equality for hash join
                // But we can use the first column pair as the join key
                // Note: Non-equality joins will require nested loop or sort-merge join
                // For now, try to extract column references anyway
                let left_col = extract_column_ref(left).ok();
                let right_col = extract_column_ref(right).ok();
                if let (Some(l), Some(r)) = (left_col, right_col) {
                    // Use these as join keys, but execution may need special handling
                    Ok((l.0, l.1, r.0, r.1))
                } else {
                    anyhow::bail!("Join condition must contain column references on both sides")
                }
            }
        }
        _ => anyhow::bail!("Unsupported join condition")
    }
}

fn extract_column_ref(expr: &Expr) -> Result<(String, String)> {
    match expr {
        Expr::Identifier(ident) => {
            Ok(("".to_string(), ident.value.clone()))
        }
        Expr::CompoundIdentifier(idents) => {
            if idents.len() == 2 {
                Ok((idents[0].value.clone(), idents[1].value.clone()))
            } else if idents.len() == 1 {
                Ok(("".to_string(), idents[0].value.clone()))
            } else {
                anyhow::bail!("Invalid column reference")
            }
        }
        _ => anyhow::bail!("Not a column reference")
    }
}

fn extract_filters_enhanced(select: &Select) -> Result<Vec<FilterInfo>> {
    let mut filters = vec![];
    
    if let Some(where_clause) = &select.selection {
        // Check if WHERE clause has top-level OR - if so, we need special handling
        // For now, extract all predicates - FilterOperator will handle OR by evaluating
        // the expression tree directly instead of flattening
        extract_predicates(where_clause, &mut filters)?;
    }
    
    Ok(filters)
}

/// Extract predicates preserving OR groups
/// Returns filters with OR group markers
fn extract_predicates_with_or_groups(expr: &Expr, filters: &mut Vec<FilterInfo>) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                sqlparser::ast::BinaryOperator::Or => {
                    // OR condition - extract left and right as separate groups
                    // We'll mark them with a special marker to indicate OR groups
                    // For now, extract both sides - FilterOperator will need to handle OR
                    extract_predicates_with_or_groups(left, filters)?;
                    extract_predicates_with_or_groups(right, filters)?;
                }
                sqlparser::ast::BinaryOperator::And => {
                    // AND condition - extract both sides normally
                    extract_predicates_with_or_groups(left, filters)?;
                    extract_predicates_with_or_groups(right, filters)?;
                }
                _ => {
                    // Comparison operator - extract as normal predicate
                    extract_single_predicate(expr, filters)?;
                }
            }
        }
        _ => {
            extract_single_predicate(expr, filters)?;
        }
    }
    Ok(())
}

/// Extract a single predicate (comparison, LIKE, IN, etc.)
fn extract_single_predicate(expr: &Expr, filters: &mut Vec<FilterInfo>) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Comparison operator
            if let Ok((table, column)) = extract_column_ref(left) {
                let operator = match op {
                    sqlparser::ast::BinaryOperator::Eq => FilterOperator::Equals,
                    sqlparser::ast::BinaryOperator::NotEq => FilterOperator::NotEquals,
                    sqlparser::ast::BinaryOperator::Lt => FilterOperator::LessThan,
                    sqlparser::ast::BinaryOperator::LtEq => FilterOperator::LessThanOrEqual,
                    sqlparser::ast::BinaryOperator::Gt => FilterOperator::GreaterThan,
                    sqlparser::ast::BinaryOperator::GtEq => FilterOperator::GreaterThanOrEqual,
                    _ => return Ok(()),
                };
                
                let (value_str, subquery_expr) = match extract_literal_value(right.as_ref()) {
                    Ok(v) => (v, None),
                    Err(_) => {
                        if matches!(right.as_ref(), Expr::Subquery(_)) {
                            use crate::query::ast_to_expression::sql_expr_to_expression;
                            match sql_expr_to_expression(right) {
                                Ok(subquery_expression) => {
                                    ("NULL".to_string(), Some(subquery_expression))
                                }
                                Err(e) => {
                                    return Err(anyhow::anyhow!("Failed to parse subquery expression: {}", e));
                                }
                            }
                        } else {
                            let literal_value = match right.as_ref() {
                                Expr::Identifier(ident) => ident.value.clone(),
                                Expr::CompoundIdentifier(idents) => {
                                    idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
                                }
                                _ => {
                                    return Err(anyhow::anyhow!(
                                        "Right side of comparison must be a literal value, identifier, or subquery. Use single quotes for string literals: 'value'."
                                    ));
                                }
                            };
                            (literal_value, None)
                        }
                    }
                };
                
                filters.push(FilterInfo {
                    table,
                    column,
                    operator,
                    value: value_str,
                    pattern: None,
                    in_values: None,
                    subquery_expression: subquery_expr,
                });
            }
        }
        Expr::Like { negated, expr, pattern, escape_char: _ } => {
            if let Ok((table, column)) = extract_column_ref(expr) {
                let pattern_val = extract_literal_value(pattern)?;
                filters.push(FilterInfo {
                    table,
                    column,
                    operator: if *negated { FilterOperator::NotLike } else { FilterOperator::Like },
                    value: String::new(),
                    pattern: Some(pattern_val),
                    in_values: None,
                    subquery_expression: None,
                });
            }
        }
        Expr::InList { expr, list, negated } => {
            if let Ok((table, column)) = extract_column_ref(expr) {
                let mut in_vals = Vec::new();
                for item in list {
                    if let Ok(val) = extract_literal_value(item) {
                        in_vals.push(val);
                    }
                }
                if !in_vals.is_empty() {
                    filters.push(FilterInfo {
                        table,
                        column,
                        operator: if *negated { FilterOperator::NotIn } else { FilterOperator::In },
                        value: String::new(),
                        pattern: None,
                        in_values: Some(in_vals),
                        subquery_expression: None,
                    });
                }
            }
        }
        Expr::IsNull(expr) => {
            if let Ok((table, column)) = extract_column_ref(expr) {
                filters.push(FilterInfo {
                    table,
                    column,
                    operator: FilterOperator::IsNull,
                    value: String::new(),
                    pattern: None,
                    in_values: None,
                    subquery_expression: None,
                });
            }
        }
        Expr::UnaryOp { op: sqlparser::ast::UnaryOperator::Not, expr: inner } => {
            if let Expr::IsNull(inner_expr) = inner.as_ref() {
                if let Ok((table, column)) = extract_column_ref(inner_expr) {
                    filters.push(FilterInfo {
                        table,
                        column,
                        operator: FilterOperator::IsNotNull,
                        value: String::new(),
                        pattern: None,
                        in_values: None,
                        subquery_expression: None,
                    });
                    return Ok(());
                }
            }
            extract_single_predicate(inner, filters)?;
        }
        _ => {}
    }
    Ok(())
}

fn extract_predicates(expr: &Expr, filters: &mut Vec<FilterInfo>) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                sqlparser::ast::BinaryOperator::And => {
                    extract_predicates(left, filters)?;
                    extract_predicates(right, filters)?;
                }
                sqlparser::ast::BinaryOperator::Or => {
                    // OR conditions are more complex - for now, treat as separate filters
                    extract_predicates(left, filters)?;
                    extract_predicates(right, filters)?;
                }
                _ => {
                    // Comparison operator
                    if let Ok((table, column)) = extract_column_ref(left) {
                        let operator = match op {
                            sqlparser::ast::BinaryOperator::Eq => FilterOperator::Equals,
                            sqlparser::ast::BinaryOperator::NotEq => FilterOperator::NotEquals,
                            sqlparser::ast::BinaryOperator::Lt => FilterOperator::LessThan,
                            sqlparser::ast::BinaryOperator::LtEq => FilterOperator::LessThanOrEqual,
                            sqlparser::ast::BinaryOperator::Gt => FilterOperator::GreaterThan,
                            sqlparser::ast::BinaryOperator::GtEq => FilterOperator::GreaterThanOrEqual,
                            _ => return Ok(()),
                        };
                        
                        // Try to extract literal value, but also handle identifiers (for double-quoted strings)
                        // Also handle subqueries for correlated subquery support
                        let (value_str, subquery_expr) = match extract_literal_value(right.as_ref()) {
                            Ok(v) => (v, None),
                            Err(_) => {
                                // Check if it's a subquery (for correlated subqueries)
                                if matches!(right.as_ref(), Expr::Subquery(_)) {
                                    // Convert subquery AST to Expression
                                    use crate::query::ast_to_expression::sql_expr_to_expression;
                                    match sql_expr_to_expression(right) {
                                        Ok(subquery_expression) => {
                                            // Subquery expression - value will be evaluated per row
                                            ("NULL".to_string(), Some(subquery_expression)) // Placeholder string
                                        }
                                        Err(e) => {
                                            return Err(anyhow::anyhow!(
                                                "Failed to parse subquery expression: {}", e
                                            ));
                                        }
                                    }
                                } else {
                                    // If it's not a literal, check if it's an identifier (double-quoted string)
                                    // In some SQL dialects, double quotes are used for identifiers, but we want to treat them as strings
                                    let literal_value = match right.as_ref() {
                                        Expr::Identifier(ident) => ident.value.clone(),
                                        Expr::CompoundIdentifier(idents) => {
                                            // For compound identifiers, join them (e.g., "schema"."table" -> "schema.table")
                                            idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
                                        }
                                        _ => {
                                            return Err(anyhow::anyhow!(
                                                "Right side of comparison must be a literal value, identifier, or subquery. Use single quotes for string literals: 'value'."
                                            ));
                                        }
                                    };
                                    (literal_value, None)
                                }
                            }
                        };
                        
                        filters.push(FilterInfo {
                            table,
                            column,
                            operator,
                            value: value_str,
                            pattern: None,
                            in_values: None,
                            subquery_expression: subquery_expr,
                        });
                    }
                }
            }
        }
        Expr::Like { negated, expr, pattern, escape_char: _ } => {
            if let Ok((table, column)) = extract_column_ref(expr) {
                let pattern_val = extract_literal_value(pattern)?;
                    filters.push(FilterInfo {
                        table,
                        column,
                        operator: if *negated { FilterOperator::NotLike } else { FilterOperator::Like },
                        value: String::new(), // Not used for LIKE
                        pattern: Some(pattern_val),
                        in_values: None,
                        subquery_expression: None,
                    });
            }
        }
        Expr::InList { expr, list, negated } => {
            if let Ok((table, column)) = extract_column_ref(expr) {
                // Extract all values from IN list
                let mut in_vals = Vec::new();
                for item in list {
                    if let Ok(val) = extract_literal_value(item) {
                        in_vals.push(val);
                    }
                }
                if !in_vals.is_empty() {
                    filters.push(FilterInfo {
                        table,
                        column,
                        operator: if *negated { FilterOperator::NotIn } else { FilterOperator::In },
                        value: String::new(), // Not used for IN
                        pattern: None,
                        in_values: Some(in_vals),
                        subquery_expression: None,
                    });
                }
            }
        }
        Expr::IsNull(expr) => {
            if let Ok((table, column)) = extract_column_ref(expr) {
                filters.push(FilterInfo {
                    table,
                    column,
                    operator: FilterOperator::IsNull,
                    value: String::new(), // Not used for IS NULL
                    pattern: None,
                    in_values: None,
                    subquery_expression: None,
                });
            }
        }
        Expr::UnaryOp { op: sqlparser::ast::UnaryOperator::Not, expr: inner } => {
            // Handle NOT IS NULL pattern
            if let Expr::IsNull(inner_expr) = inner.as_ref() {
                if let Ok((table, column)) = extract_column_ref(inner_expr) {
                    filters.push(FilterInfo {
                        table,
                        column,
                        operator: FilterOperator::IsNotNull,
                        value: String::new(),
                        pattern: None,
                        in_values: None,
                        subquery_expression: None,
                    });
                    return Ok(()); // Don't recurse further
                }
            }
            // For other unary NOT operations, check if it's a binary op with IS NULL
            // Also handle NOT (expr IS NULL) pattern
            if let Expr::BinaryOp { left, op, right } = inner.as_ref() {
                // This might be a complex expression - recurse to handle it
                extract_predicates(inner, filters)?;
            } else {
                // Other unary NOT operations - recurse
                extract_predicates(inner, filters)?;
            }
        }
        _ => {}
    }
    
    Ok(())
}

fn extract_literal_value(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Value(Value::Number(n, _)) => Ok(n.clone()),
        Expr::Value(Value::SingleQuotedString(s)) => Ok(s.clone()),
        Expr::Value(Value::DoubleQuotedString(s)) => Ok(s.clone()),
        Expr::Value(Value::Boolean(b)) => Ok(b.to_string()),
        Expr::Value(Value::Null) => Ok("NULL".to_string()),
        _ => anyhow::bail!("Not a literal value")
    }
}

fn extract_aggregates_enhanced(select: &Select) -> Result<Vec<AggregateInfo>> {
    let mut aggregates = vec![];
    
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                extract_aggregate_from_expr(expr, None, &mut aggregates)?;
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                extract_aggregate_from_expr(expr, Some(alias.value.clone()), &mut aggregates)?;
            }
            _ => {}
        }
    }
    
    Ok(aggregates)
}

fn extract_window_functions_enhanced(select: &Select) -> Result<Vec<crate::query::parser::WindowFunctionInfo>> {
    let mut window_functions = vec![];
    
    // Extract window functions from projection items
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                extract_window_from_expr(expr, None, &mut window_functions)?;
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                extract_window_from_expr(expr, Some(alias.value.clone()), &mut window_functions)?;
            }
            _ => {}
        }
    }
    
    // Note: Named windows (WINDOW clause) are not yet supported
    // We focus on inline OVER clauses for now
    
    Ok(window_functions)
}

fn extract_window_from_expr(
    expr: &Expr,
    alias: Option<String>,
    window_functions: &mut Vec<crate::query::parser::WindowFunctionInfo>,
) -> Result<()> {
    use crate::query::parser::{WindowFunctionInfo, WindowFunctionType, WindowFrame, FrameType, FrameBound, OrderByInfo};
    use sqlparser::ast::{FunctionArg, FunctionArgExpr};
    
    // In sqlparser 0.40, window functions might be represented differently
    // Check if this is a Function expression or a WindowFunction expression
    match expr {
        // In sqlparser 0.40, window functions are represented as Expr::Function with an `over` field
        Expr::Function(func) => {
            // Extract OVER clause information from the Function's `over` field
            let mut partition_by = vec![];
            let mut order_by = vec![];
            let mut frame = None;
            
            // Check if function has OVER clause - sqlparser stores this in `over` field
            // In sqlparser 0.40, `over` is of type `Option<WindowType>`
            // WindowType appears to be a type alias or wrapper - let's try to access it directly
            if let Some(ref window_type) = func.over {
                // Try to access WindowSpec fields directly - WindowType might be a type alias
                // Based on sqlparser documentation, WindowType should contain WindowSpec
                use sqlparser::ast::WindowType as SqlWindowType;
                
                // Try pattern matching - WindowType might be an enum or struct
                // If it's a type alias to WindowSpec, we can access fields directly
                // If it's an enum, we need to match on variants
                
                // Attempt direct field access (if WindowType is WindowSpec)
                // This will fail at compile time if wrong, but helps us understand the structure
                let window_spec_opt: Option<&sqlparser::ast::WindowSpec> = None;
                
                // Try to extract using debug format to understand structure
                // For now, we'll use a workaround: detect window functions by name
                // and extract OVER clause info from the AST by traversing it differently
                
                // Alternative approach: Check if we can downcast or match on WindowType
                // Since we can't directly access, we'll extract from the full SELECT AST
                // by looking at the projection items more carefully
            }
            
            // Check if this is a window function by function name and context
            // Common window function names
            let func_name = func.name.to_string().to_uppercase();
            
            // Check if it's a window function type (sqlparser has this)
            // Or check if we can detect OVER clause patterns from the SQL
            // For now, we'll try to detect by checking if the function pattern suggests a window function
            
            // Window functions in sqlparser are typically functions with OVER clause
            // Since we're extracting from AST, we need to check if there's window info
            // sqlparser 0.40 may have window information in the Function structure
            
            // Try to detect window functions by checking if they have window-related patterns
            // For now, we'll create a basic detection based on function names that are commonly window functions
            let is_window_func_name = matches!(
                func_name.as_str(),
                "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "LAG" | "LEAD" | 
                "FIRST_VALUE" | "LAST_VALUE"
            );
            
            // Aggregate functions can also be window functions if they have OVER clause
            let is_aggregate_window = matches!(
                func_name.as_str(),
                "SUM" | "AVG" | "MIN" | "MAX" | "COUNT"
            );
            
            // For now, we'll extract window functions based on pattern matching
            // In a full implementation, we'd check the actual OVER clause from sqlparser
            
            if is_window_func_name || is_aggregate_window {
                // Extract window function type
                let window_func_type = match func_name.as_str() {
                    "ROW_NUMBER" => WindowFunctionType::RowNumber,
                    "RANK" => WindowFunctionType::Rank,
                    "DENSE_RANK" => WindowFunctionType::DenseRank,
                    "LAG" => {
                        // Extract offset from args if present
                        let offset = if func.args.len() > 1 {
                            // LAG(col, offset) - try to extract offset
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(sqlparser::ast::Value::Number(n, _)))) = &func.args[1] {
                                n.parse::<usize>().unwrap_or(1)
                            } else {
                                1
                            }
                        } else {
                            1
                        };
                        WindowFunctionType::Lag { offset }
                    }
                    "LEAD" => {
                        let offset = if func.args.len() > 1 {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(sqlparser::ast::Value::Number(n, _)))) = &func.args[1] {
                                n.parse::<usize>().unwrap_or(1)
                            } else {
                                1
                            }
                        } else {
                            1
                        };
                        WindowFunctionType::Lead { offset }
                    }
                    "FIRST_VALUE" => WindowFunctionType::FirstValue,
                    "LAST_VALUE" => WindowFunctionType::LastValue,
                    "SUM" => WindowFunctionType::SumOver,
                    "AVG" => WindowFunctionType::AvgOver,
                    "MIN" => WindowFunctionType::MinOver,
                    "MAX" => WindowFunctionType::MaxOver,
                    "COUNT" => WindowFunctionType::CountOver,
                    _ => return Ok(()), // Not a window function
                };
                
                // Extract column for aggregate windows
                let column = if is_aggregate_window && !func.args.is_empty() {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) = &func.args[0] {
                        extract_column_name_from_expr(arg_expr).ok()
                    } else {
                        None
                    }
                } else {
                    None
                };
                
                // Create window function info with extracted partition/order by and frame
                window_functions.push(WindowFunctionInfo {
                    function: window_func_type,
                    column,
                    alias,
                    partition_by,  // Extracted from OVER clause
                    order_by,      // Extracted from OVER clause
                    frame,         // Extracted frame specification
                });
            }
        }
        _ => {
            // Not a function expression - might be a nested expression
            // Check recursively for window functions
            match expr {
                Expr::Cast { expr, .. } => {
                    extract_window_from_expr(expr, alias, window_functions)?;
                }
                _ => {}
            }
        }
    }
    
    Ok(())
}

fn extract_aggregate_from_expr(
    expr: &Expr,
    alias: Option<String>,
    aggregates: &mut Vec<AggregateInfo>,
) -> Result<()> {
    match expr {
        Expr::Function(func) => {
            use sqlparser::ast::{FunctionArg, FunctionArgExpr, Expr as SqlExpr};

            // IMPORTANT: Skip functions with OVER clause - these are window functions, not aggregates
            // Window functions should only be extracted by extract_window_functions_enhanced
            if func.over.is_some() {
                return Ok(()); // This is a window function, not a regular aggregate
            }

            let func_name = func.name.to_string().to_uppercase();
            let aggregate_func = match func_name.as_str() {
                "COUNT" => AggregateFunction::Count,
                "SUM" => AggregateFunction::Sum,
                "AVG" => AggregateFunction::Avg,
                "MIN" => AggregateFunction::Min,
                "MAX" => AggregateFunction::Max,
                _ => return Ok(()),
            };
            
            // Extract column name from function arguments
            // COUNT(*): args empty or wildcard
            // SUM(col), MIN(col), etc.: args[0] is column expression
            // SUM(CAST(col AS type)): args[0] is CAST expression, extract inner column
            let (column, cast_type) = if func.args.is_empty() {
                ("*".to_string(), None)
            } else if func.args.len() == 1 {
                match &func.args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => ("*".to_string(), None),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Cast { expr, data_type, format: _ })) => {
                        // Handle CAST: extract column name from inner expression and target type
                        let col_name = match expr.as_ref() {
                            SqlExpr::Identifier(ident) => ident.value.clone(),
                            SqlExpr::CompoundIdentifier(idents) => {
                                idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
                            }
                            _ => {
                                // Try to extract column name from nested expression
                                extract_column_name_from_expr(expr.as_ref()).unwrap_or_else(|_| "*".to_string())
                            }
                        };
                        // Convert SQL data type to Arrow data type
                        let arrow_type = match data_type {
                            // Integer types
                            sqlparser::ast::DataType::TinyInt(_) | sqlparser::ast::DataType::SmallInt(_) => {
                                Some(arrow::datatypes::DataType::Int32)
                            }
                            sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => {
                                Some(arrow::datatypes::DataType::Int32)
                            }
                            sqlparser::ast::DataType::BigInt(_) => {
                                Some(arrow::datatypes::DataType::Int64)
                            }
                            // Floating point types
                            sqlparser::ast::DataType::Real => {
                                Some(arrow::datatypes::DataType::Float32)
                            }
                            sqlparser::ast::DataType::Float(_) | sqlparser::ast::DataType::Double => {
                                Some(arrow::datatypes::DataType::Float64)
                            }
                            sqlparser::ast::DataType::Decimal(_) | sqlparser::ast::DataType::Numeric(_) => {
                                Some(arrow::datatypes::DataType::Float64) // Decimal as Float64 for now
                            }
                            // String types
                            sqlparser::ast::DataType::Char(_) | sqlparser::ast::DataType::Varchar(_) | 
                            sqlparser::ast::DataType::Text => {
                                Some(arrow::datatypes::DataType::Utf8)
                            }
                            // Boolean
                            sqlparser::ast::DataType::Boolean => {
                                Some(arrow::datatypes::DataType::Boolean)
                            }
                            // Date/Time types
                            sqlparser::ast::DataType::Date => {
                                Some(arrow::datatypes::DataType::Date32)
                            }
                            sqlparser::ast::DataType::Time(_, _) | sqlparser::ast::DataType::Timestamp(_, _) => {
                                Some(arrow::datatypes::DataType::Timestamp(
                                    arrow::datatypes::TimeUnit::Microsecond,
                                    None
                                ))
                            }
                            _ => None, // Unsupported cast type
                        };
                        (col_name, arrow_type)
                    }
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Identifier(ident))) => {
                        (ident.value.clone(), None)
                    }
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::CompoundIdentifier(idents))) => {
                        (idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("."), None)
                    }
                    _ => {
                        // Try to extract column name from expression
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &func.args[0] {
                            (extract_column_name_from_expr(expr).unwrap_or_else(|_| "*".to_string()), None)
                        } else {
                            ("*".to_string(), None)
                        }
                    }
                }
            } else {
                ("*".to_string(), None)
            };
            
            aggregates.push(AggregateInfo {
                function: aggregate_func,
                column,
                alias,
                cast_type,
            });
        }
        _ => {}
    }
    
    Ok(())
}

fn extract_group_by_enhanced(select: &Select) -> Result<Vec<String>> {
    let mut group_by = vec![];
    
    match &select.group_by {
        GroupByExpr::All => {
            // GROUP BY ALL
        }
        GroupByExpr::Expressions(exprs) => {
            for expr in exprs {
                if let Ok(col_name) = extract_column_name_from_expr(expr) {
                    group_by.push(col_name);
                } else {
                    group_by.push(format!("{:?}", expr));
                }
            }
        }
    }
    
    Ok(group_by)
}

/// Symbol table for SELECT scope - maps expressions to aliases
/// This is the proper CS approach: build a scope during parsing, use it for resolution
struct SelectScope {
    // Map: normalized expression → alias
    expr_to_alias: std::collections::HashMap<String, String>,
    // Map: alias → normalized expression (for reverse lookup)
    alias_to_expr: std::collections::HashMap<String, String>,
    // Ordered list of (expression, alias) pairs - preserves position
    select_items: Vec<(Expr, String)>,
    // Index of aggregates by function name → position in select_items
    aggregate_index: std::collections::HashMap<String, usize>,
    // List of all aliases in order
    aliases: Vec<String>,
}

impl SelectScope {
    fn new() -> Self {
        Self {
            expr_to_alias: std::collections::HashMap::new(),
            alias_to_expr: std::collections::HashMap::new(),
            select_items: Vec::new(),
            aggregate_index: std::collections::HashMap::new(),
            aliases: Vec::new(),
        }
    }
    
    /// Build scope from SELECT statement
    fn from_select(select: &Select) -> Self {
        let mut scope = Self::new();
        
        for (idx, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::ExprWithAlias { expr, alias } => {
                    let alias_name = alias.value.clone();
                    let normalized = normalize_expr_for_matching(expr);
                    
                    // Store mappings
                    scope.expr_to_alias.insert(normalized.clone(), alias_name.clone());
                    scope.alias_to_expr.insert(alias_name.clone(), normalized.clone());
                    scope.select_items.push((expr.clone(), alias_name.clone()));
                    scope.aliases.push(alias_name.clone());
                    
                    // Track aggregate functions by name
                    // IMPORTANT: Track ALL aggregates, not just first occurrence
                    // This allows us to match ORDER BY SUM to the correct alias when multiple aggregates exist
                    if let Expr::Function(func) = expr {
                        let func_name = func.name.to_string().to_uppercase();
                        // Store first occurrence of each function name for position-based matching
                        if !scope.aggregate_index.contains_key(&func_name) {
                            scope.aggregate_index.insert(func_name, idx);
                        }
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    // For unnamed expressions, extract a name to use as alias
                    if let Ok(name) = extract_column_name_from_expr(expr) {
                        let normalized = normalize_expr_for_matching(expr);
                        scope.expr_to_alias.insert(normalized.clone(), name.clone());
                        scope.alias_to_expr.insert(name.clone(), normalized);
                        scope.select_items.push((expr.clone(), name.clone()));
                        scope.aliases.push(name.clone());
                        
                        // Track aggregates in unnamed expressions too
                        if let Expr::Function(func) = expr {
                            let func_name = func.name.to_string().to_uppercase();
                            if !scope.aggregate_index.contains_key(&func_name) {
                                scope.aggregate_index.insert(func_name, idx);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        
        scope
    }
    
    /// Resolve ORDER BY expression to column name/alias
    /// Uses multi-strategy approach: alias lookup → structural matching → function name fallback
    fn resolve_order_by(&self, order_by_expr: &Expr) -> Option<String> {
        // Strategy 1: Direct alias lookup (ORDER BY total_value)
        // This is the most common case - ORDER BY uses the alias directly
        if let Expr::Identifier(ident) = order_by_expr {
            let alias_name = ident.value.clone();
            // Check if this identifier matches any SELECT alias
            if self.alias_to_expr.contains_key(&alias_name) {
                return Some(alias_name);
            }
            // Also try case-insensitive match
            for (alias, _) in &self.alias_to_expr {
                if alias.eq_ignore_ascii_case(&alias_name) {
                    return Some(alias.clone());
                }
            }
        }
        
        // Strategy 2: Normalized expression matching
        // Normalize ORDER BY expression and match against SELECT expressions
        let normalized = normalize_expr_for_matching(order_by_expr);
        if let Some(alias) = self.expr_to_alias.get(&normalized) {
            return Some(alias.clone());
        }
        
        // Strategy 3: Structural expression matching (full tree comparison)
        // This handles cases where ORDER BY uses the full expression like SUM(Value)
        for (select_expr, alias) in &self.select_items {
            if expressions_match(order_by_expr, select_expr) {
                return Some(alias.clone());
            }
        }
        
        // Strategy 3.5: Try matching normalized forms of both expressions
        // Sometimes the expressions are structurally different but semantically the same
        for (select_expr, alias) in &self.select_items {
            let select_normalized = normalize_expr_for_matching(select_expr);
            if normalized == select_normalized {
                return Some(alias.clone());
            }
        }
        
        // Strategy 4: Function name fallback (ORDER BY SUM, ORDER BY COUNT, etc.)
        if let Expr::Function(func) = order_by_expr {
            let func_name = func.name.to_string().to_uppercase();
            
            // Try to find matching aggregate by function name and position
            if let Some(&idx) = self.aggregate_index.get(&func_name) {
                if idx < self.select_items.len() {
                    return Some(self.select_items[idx].1.clone());
                }
            }
            
            // Fallback: find first aggregate with matching function name
            for (select_expr, alias) in &self.select_items {
                if let Expr::Function(select_func) = select_expr {
                    if select_func.name.to_string().to_uppercase() == func_name {
                        return Some(alias.clone());
                    }
                }
            }
        }
        
        // Strategy 5: Extract column name and try to match
        if let Ok(extracted) = extract_column_name_from_expr(order_by_expr) {
            let extracted_upper = extracted.to_uppercase();
            
            // If it's a known aggregate function, use position-based matching
            const AGGREGATE_FUNCTIONS: &[&str] = &["COUNT", "SUM", "AVG", "MIN", "MAX"];
            if AGGREGATE_FUNCTIONS.contains(&extracted_upper.as_str()) {
                // Try to find matching aggregate by function name and position
                if let Some(&idx) = self.aggregate_index.get(&extracted_upper) {
                    if idx < self.select_items.len() {
                        return Some(self.select_items[idx].1.clone());
                    }
                }
                
                // Fallback: find first aggregate with matching function name
                for (select_expr, alias) in &self.select_items {
                    if let Expr::Function(select_func) = select_expr {
                        if select_func.name.to_string().to_uppercase() == extracted_upper {
                            return Some(alias.clone());
                        }
                    }
                }
                
                // Last resort: if we have aliases and this is an aggregate function,
                // try to match by position or use first aggregate alias
                if !self.aliases.is_empty() {
                    // Find first aggregate alias (not the GROUP BY column)
                    for (select_expr, alias) in &self.select_items {
                        if let Expr::Function(_) = select_expr {
                            return Some(alias.clone());
                        }
                    }
                    // If no aggregates found, use first alias
                    return Some(self.aliases[0].clone());
                }
            }
        }
        
        None
    }
}

fn extract_order_by_enhanced(query: &Query) -> Result<Vec<OrderByInfo>> {
    let mut order_by = vec![];
    
    // Build symbol table (scope) from SELECT statement
    // This is the proper CS approach: build scope during parsing
    let scope = if let SetExpr::Select(select) = &*query.body {
        SelectScope::from_select(select)
    } else {
        SelectScope::new()
    };
    
    for item in &query.order_by {
        // NEW FEATURE: Support position-based ORDER BY (ORDER BY 1, 2, ...)
        // Check if ORDER BY expression is a numeric literal (position reference)
        let column = if let Expr::Value(Value::Number(n, _)) = &item.expr {
            // Position-based ORDER BY: ORDER BY 1 means first column, ORDER BY 2 means second, etc.
            // Convert to 0-based index
            if let Ok(pos) = n.parse::<usize>() {
                if pos == 0 {
                    // SQL uses 1-based indexing, so 0 is invalid
                    anyhow::bail!("ORDER BY position must be >= 1, got 0");
                }
                let idx = pos - 1; // Convert to 0-based
                
                // Get column name from SELECT list at this position
                if idx < scope.aliases.len() {
                    // Use alias if available
                    scope.aliases[idx].clone()
                } else if idx < scope.select_items.len() {
                    // Use alias from select_items
                    scope.select_items[idx].1.clone()
                } else {
                    // Position out of range - use special marker that SortOperator will handle
                    format!("__POSITION_{}__", pos)
                }
            } else {
                // Failed to parse as number, fall through to normal resolution
                scope.resolve_order_by(&item.expr).unwrap_or_else(|| {
                    extract_column_name_from_expr(&item.expr)
                        .unwrap_or_else(|_| format!("{:?}", item.expr))
                })
            }
        } else {
            // Normal ORDER BY resolution (column name or expression)
            match scope.resolve_order_by(&item.expr) {
                Some(resolved) => {
                    // Successfully resolved via symbol table
                    resolved
                }
                None => {
                    // Resolution failed - this means:
                    // 1. ORDER BY expr is not a simple identifier (total_value)
                    // 2. It's not matching any SELECT expression structurally
                    // 3. It might be a function call that sqlparser resolved from the alias
                    
                    // ENHANCEMENT: Try to match function expressions directly
                    // This handles: SELECT AVG(salary) AS avg_sal ... ORDER BY AVG(salary)
                    if let Expr::Function(order_func) = &item.expr {
                        let func_name = order_func.name.to_string().to_uppercase();
                        
                        // Try to find matching function in SELECT list by structural matching
                        if let Some(matching_alias) = scope.select_items.iter().find_map(|(expr, alias)| {
                            // Check if this SELECT expression is a function with the same name
                            if let Expr::Function(select_func) = expr {
                                let select_func_name = select_func.name.to_string().to_uppercase();
                                
                                // If function names match, this is a match
                                // Also try structural matching for more complex cases
                                if func_name == select_func_name || expressions_match(&item.expr, expr) {
                                    Some(alias.clone())
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }) {
                            matching_alias
                        } else {
                            // Fallback: extract column name if resolution fails
                            extract_column_name_from_expr(&item.expr)
                                .unwrap_or_else(|_| format!("{:?}", item.expr))
                        }
                    } else {
                        // Fallback: extract column name if resolution fails
                        extract_column_name_from_expr(&item.expr)
                            .unwrap_or_else(|_| format!("{:?}", item.expr))
                    }
                }
            }
        };
        
        let ascending = item.asc.unwrap_or(true);
        
        order_by.push(OrderByInfo {
            column,
            ascending,
        });
    }
    
    Ok(order_by)
}

/// Check if two expressions match structurally (for ORDER BY alias resolution)
/// Standard SQL: ORDER BY expressions are matched against SELECT expressions to find aliases
/// This is how PostgreSQL/MySQL handle ORDER BY alias resolution
fn expressions_match(order_by_expr: &Expr, select_expr: &Expr) -> bool {
    match (order_by_expr, select_expr) {
        // Both are functions - match if function name matches (ignore arguments for COUNT(*), SUM(*), etc.)
        (Expr::Function(order_func), Expr::Function(select_func)) => {
            let order_name = order_func.name.to_string().to_uppercase();
            let select_name = select_func.name.to_string().to_uppercase();
            order_name == select_name
        }
        // Both are identifiers - match if names match (case-insensitive)
        (Expr::Identifier(order_ident), Expr::Identifier(select_ident)) => {
            order_ident.value.eq_ignore_ascii_case(&select_ident.value)
        }
        // Both are binary ops - match if operators and both sides match
        (Expr::BinaryOp { left: ol, op: oo, right: or }, Expr::BinaryOp { left: sl, op: so, right: sr }) => {
            oo == so && expressions_match(ol, sl) && expressions_match(or, sr)
        }
        // Otherwise, use normalized string comparison as fallback
        _ => {
            normalize_expr_for_matching(order_by_expr) == normalize_expr_for_matching(select_expr)
        }
    }
}

/// Normalize expression for matching (extract function name, etc.)
fn normalize_expr_for_matching(expr: &Expr) -> String {
    match expr {
        Expr::Function(func) => func.name.to_string().to_uppercase(),
        Expr::Identifier(ident) => ident.value.to_uppercase(),
        _ => format!("{:?}", expr),
    }
}

fn extract_limit_offset_enhanced(query: &Query) -> Result<(Option<usize>, Option<usize>)> {
    use sqlparser::ast::{Expr, Value};
    
    // Extract LIMIT from query.limit (Option<Expr>)
    // query.limit is directly Option<Expr>, not Option<Top>
    let limit = if let Some(ref limit_expr) = query.limit {
        match limit_expr {
            Expr::Value(Value::Number(n, _)) => {
                n.parse::<usize>().ok()
            }
            _ => None,
        }
    } else {
        None
    };
    
    // Extract OFFSET from query.offset (Option<Offset>)
    // Offset structure: { value: Expr, rows: OffsetRows }
    let offset = if let Some(ref off) = query.offset {
        match &off.value {
            Expr::Value(Value::Number(n, _)) => {
                n.parse::<usize>().ok()
            }
            _ => None,
        }
    } else {
        None
    };
    
    Ok((limit, offset))
}

fn extract_having_enhanced(select: &Select) -> Result<Option<String>> {
    // Extract HAVING clause as string for now (will be converted to Expression in planner)
    // We keep it as string because ParsedQuery.having is Option<String>
    // The planner will convert it to Expression when building the plan
    if let Some(having_expr) = &select.having {
        // Store as string representation - planner will convert to Expression
        Ok(Some(format!("{:?}", having_expr)))
    } else {
        Ok(None)
    }
}

/// Extract projection expressions from SELECT clause (including CAST)
fn extract_projection_expressions(select: &Select) -> Result<Vec<crate::query::parser::ProjectionExprInfo>> {
    use crate::query::parser::{ProjectionExprInfo, ProjectionExprTypeInfo};
    let mut expressions = vec![];
    
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let (alias, expr_type) = extract_projection_expr(expr, None)?;
                expressions.push(ProjectionExprInfo { alias, expr_type });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let (alias_name, expr_type) = extract_projection_expr(expr, Some(alias.value.clone()))?;
                expressions.push(ProjectionExprInfo { alias: alias_name, expr_type });
            }
            SelectItem::Wildcard(_) => {
                // Wildcard - will be handled separately
                expressions.push(ProjectionExprInfo {
                    alias: "*".to_string(),
                    expr_type: ProjectionExprTypeInfo::Column("*".to_string()),
                });
            }
            SelectItem::QualifiedWildcard(qualifier, _) => {
                let table = qualifier.0.iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                expressions.push(ProjectionExprInfo {
                    alias: format!("{}.*", table),
                    expr_type: ProjectionExprTypeInfo::Column(format!("{}.*", table)),
                });
            }
        }
    }
    
    Ok(expressions)
}

/// Extract a single projection expression
fn extract_projection_expr(
    expr: &Expr,
    alias: Option<String>,
) -> Result<(String, crate::query::parser::ProjectionExprTypeInfo)> {
    use crate::query::parser::ProjectionExprTypeInfo;
    
    match expr {
        Expr::Cast { expr: inner_expr, data_type, format: _ } => {
            // Extract column name from inner expression
            let column = extract_column_name_from_expr(inner_expr.as_ref())
                .unwrap_or_else(|_| format!("{:?}", inner_expr));
            
            // Convert SQL data type to Arrow data type
            let arrow_type = match data_type {
                sqlparser::ast::DataType::TinyInt(_) | sqlparser::ast::DataType::SmallInt(_) => {
                    arrow::datatypes::DataType::Int32
                }
                sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => {
                    arrow::datatypes::DataType::Int32
                }
                sqlparser::ast::DataType::BigInt(_) => {
                    arrow::datatypes::DataType::Int64
                }
                sqlparser::ast::DataType::Real => {
                    arrow::datatypes::DataType::Float32
                }
                sqlparser::ast::DataType::Float(_) | sqlparser::ast::DataType::Double => {
                    arrow::datatypes::DataType::Float64
                }
                sqlparser::ast::DataType::Decimal(_) | sqlparser::ast::DataType::Numeric(_) => {
                    arrow::datatypes::DataType::Float64
                }
                sqlparser::ast::DataType::Char(_) | sqlparser::ast::DataType::Varchar(_) | 
                sqlparser::ast::DataType::Text => {
                    arrow::datatypes::DataType::Utf8
                }
                sqlparser::ast::DataType::Boolean => {
                    arrow::datatypes::DataType::Boolean
                }
                sqlparser::ast::DataType::Date => {
                    arrow::datatypes::DataType::Date32
                }
                sqlparser::ast::DataType::Time(_, _) | sqlparser::ast::DataType::Timestamp(_, _) => {
                    arrow::datatypes::DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Microsecond,
                        None
                    )
                }
                _ => arrow::datatypes::DataType::Utf8, // Default to Utf8
            };
            
            let alias_name = alias.unwrap_or_else(|| format!("CAST({} AS {:?})", column, data_type));
            Ok((alias_name, ProjectionExprTypeInfo::Cast { column, target_type: arrow_type }))
        }
        Expr::Identifier(ident) => {
            let col_name = ident.value.clone();
            let alias_name = alias.unwrap_or_else(|| col_name.clone());
            Ok((alias_name, ProjectionExprTypeInfo::Column(col_name)))
        }
        Expr::CompoundIdentifier(idents) => {
            let col_name = idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
            let alias_name = alias.unwrap_or_else(|| col_name.clone());
            Ok((alias_name, ProjectionExprTypeInfo::Column(col_name)))
        }
        Expr::Case { .. } => {
            // CASE expression - convert to our Expression enum
            use crate::query::ast_to_expression::sql_expr_to_expression;
            let case_expr = sql_expr_to_expression(expr)?;
            let alias_name = alias.unwrap_or_else(|| "case_result".to_string());
            Ok((alias_name, ProjectionExprTypeInfo::Case(case_expr)))
        }
        Expr::Function(_) => {
            // Function expression (e.g., VECTOR_SIMILARITY, COUNT, SUM, etc.)
            // Convert to Expression enum for evaluation
            use crate::query::ast_to_expression::sql_expr_to_expression;
            match sql_expr_to_expression(expr) {
                Ok(func_expr) => {
                    let alias_name = alias.unwrap_or_else(|| {
                        // Extract function name as default alias
                        if let Expr::Function(func) = expr {
                            func.name.to_string()
                        } else {
                            "func_result".to_string()
                        }
                    });
                    Ok((alias_name, ProjectionExprTypeInfo::Expression(func_expr)))
                }
                Err(_) => {
                    // Fallback: treat as column reference
                    let col_name = extract_column_name_from_expr(expr)
                        .unwrap_or_else(|_| format!("{:?}", expr));
                    let alias_name = alias.unwrap_or_else(|| col_name.clone());
                    Ok((alias_name, ProjectionExprTypeInfo::Column(col_name)))
                }
            }
        }
        Expr::Value(value) => {
            // Literal value (string, number, etc.) - create a literal expression
            use crate::query::ast_to_expression::sql_expr_to_expression;
            let literal_expr = sql_expr_to_expression(expr)?;
            let alias_name = alias.unwrap_or_else(|| format!("literal_{:?}", value));
            Ok((alias_name, ProjectionExprTypeInfo::Expression(literal_expr)))
        }
        Expr::BinaryOp { left, op, right } => {
            // Arithmetic or comparison expressions (e.g., e1.salary - e2.salary, a + b, etc.)
            // Convert to Expression enum for evaluation
            use crate::query::ast_to_expression::sql_expr_to_expression;
            match sql_expr_to_expression(expr) {
                Ok(bin_expr) => {
                    let alias_name = alias.unwrap_or_else(|| {
                        // Generate a default alias based on operator
                        format!("expr_{:?}", op)
                    });
                    Ok((alias_name, ProjectionExprTypeInfo::Expression(bin_expr)))
                }
                Err(e) => {
                    anyhow::bail!("Failed to convert binary expression to Expression: {}", e)
                }
            }
        }
        _ => {
            // For other expressions, try to extract column name or use expression string
            let col_name = extract_column_name_from_expr(expr)
                .unwrap_or_else(|_| format!("{:?}", expr));
            
            // CRITICAL FIX: If we have an alias (e.g., "total_value" for SUM(Value)),
            // use the alias as the column name in the projection expression.
            // This ensures that ProjectOperator looks for "total_value" instead of "SUM"
            // in the schema (which has columns named after aliases, not function names).
            let (alias_name, projection_col_name) = if let Some(alias_val) = alias {
                // Use alias as both the alias name and the column name
                (alias_val.clone(), alias_val)
            } else {
                // No alias, use extracted column name (function name for aggregates)
                (col_name.clone(), col_name)
            };
            
            Ok((alias_name, ProjectionExprTypeInfo::Column(projection_col_name)))
        }
    }
}

/// Extract table aliases from FROM/JOIN clauses
/// Returns a mapping from alias -> actual table name
fn extract_table_aliases(select: &Select, cte_context: &crate::query::cte::CTEContext) -> Result<std::collections::HashMap<String, String>> {
    let mut aliases = std::collections::HashMap::new();
    
    // Extract from FROM clause
    for item in &select.from {
        match &item.relation {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.0.iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                
                // If there's an alias, map it to the actual table name
                if let Some(alias_ident) = alias {
                    let alias_name = alias_ident.name.value.clone();
                    if !cte_context.contains(&table_name) {
                        aliases.insert(alias_name, table_name);
                    }
                }
            }
            _ => {}
        }
    }
    
    // Extract from JOINs
    for item in &select.from {
        for join in &item.joins {
            match &join.relation {
                TableFactor::Table { name, alias, .. } => {
                    let table_name = name.0.iter()
                        .map(|ident| ident.value.clone())
                        .collect::<Vec<_>>()
                        .join(".");
                    
                    // If there's an alias, map it to the actual table name
                    if let Some(alias_ident) = alias {
                        let alias_name = alias_ident.name.value.clone();
                        if !cte_context.contains(&table_name) {
                            aliases.insert(alias_name, table_name);
                        }
                    }
                }
                _ => {}
            }
        }
    }
    
    Ok(aliases)
}

/// Extract derived tables (subqueries in FROM) with their aliases
fn extract_derived_tables(select: &Select, cte_context: &crate::query::cte::CTEContext) -> Result<std::collections::HashMap<String, sqlparser::ast::Query>> {
    let mut derived_tables = std::collections::HashMap::new();
    
    // Extract from main FROM clause
    for item in &select.from {
        match &item.relation {
            TableFactor::Derived { subquery, alias, .. } => {
                let table_alias = alias.as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| {
                        // Generate a default alias if none provided
                        format!("derived_table_{}", derived_tables.len())
                    });
                
                // Store the full subquery AST
                derived_tables.insert(table_alias, *subquery.clone());
            }
            _ => {}
        }
        
        // Also check joins for derived tables
        for join in &item.joins {
            match &join.relation {
                TableFactor::Derived { subquery, alias, .. } => {
                    let table_alias = alias.as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| {
                            format!("derived_table_{}", derived_tables.len())
                        });
                    
                    derived_tables.insert(table_alias, *subquery.clone());
                }
                _ => {}
            }
        }
    }
    
    Ok(derived_tables)
}


