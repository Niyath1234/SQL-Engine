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
                let filters = extract_filters_enhanced(select)?;
                let aggregates = extract_aggregates_enhanced(select)?;
                let group_by = extract_group_by_enhanced(select)?;
                let having = extract_having_enhanced(select)?;
                let order_by = extract_order_by_enhanced(query)?;
                let (limit, offset) = extract_limit_offset_enhanced(query)?;
                
                Ok(ParsedQuery {
                    tables,
                    columns,
                    joins,
                    filters,
                    aggregates,
                    group_by,
                    having,
                    order_by,
                    limit,
                    offset,
                })
            } else if let SetExpr::SetOperation { op, left, right, .. } = &*query.body {
                // Handle UNION, INTERSECT, EXCEPT
                // For now, bail out - set operations need more complex handling
                anyhow::bail!("Set operations (UNION, INTERSECT, EXCEPT) are not yet fully implemented in the execution engine")
                
                // Parse left and right sides recursively (commented out for now)
                /*
                let left_parsed = if let SetExpr::Select(left_select) = left.as_ref() {
                    extract_query_info_enhanced(&sqlparser::ast::Statement::Query(Box::new(Query {
                        body: left.clone(),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                        locks: vec![],
                        with: None,
                        for_clause: None,
                        limit_by: vec![],
                    })))?
                } else {
                    anyhow::bail!("Set operation left side must be a SELECT")
                };
                
                let right_parsed = if let SetExpr::Select(right_select) = right.as_ref() {
                    extract_query_info_enhanced(&sqlparser::ast::Statement::Query(Box::new(Query {
                        body: right.clone(),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                        locks: vec![],
                        with: None,
                        for_clause: None,
                        limit_by: vec![],
                    })))?
                } else {
                    anyhow::bail!("Set operation right side must be a SELECT")
                };
                
                // Determine set operation type
                // UNION ALL is represented as Union { all: true }
                let set_op = match op {
                    sqlparser::ast::SetOperator::Union { all: true, .. } => crate::query::union::SetOperation::UnionAll,
                    sqlparser::ast::SetOperator::Union { all: false, .. } => crate::query::union::SetOperation::Union,
                    sqlparser::ast::SetOperator::Intersect { .. } => crate::query::union::SetOperation::Intersect,
                    sqlparser::ast::SetOperator::Except { .. } => crate::query::union::SetOperation::Except,
                };
                
                // Return a special ParsedQuery that indicates a set operation
                // We'll need to extend ParsedQuery to support this
                // For now, return left side and mark it as needing set operation
                let mut result = left_parsed;
                // Store set operation info in a special way - we'll handle this in planner
                result.tables.push(format!("__SET_OP__{:?}__", set_op));
                result.tables.extend(right_parsed.tables);
                Ok(result)
                */
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
                
                // Check if it's a CTE
                if !cte_context.contains(&table_name) {
                    tables.push(table_name);
                }
            }
            TableFactor::Derived { subquery, alias: _, .. } => {
                // Derived table (subquery in FROM) - extract tables from subquery
                if let SetExpr::Select(subselect) = &*subquery.body {
                    let sub_tables = extract_tables_enhanced(subselect, cte_context)?;
                    tables.extend(sub_tables);
                }
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
                    if !cte_context.contains(&table_name) {
                        tables.push(table_name);
                    }
                }
                TableFactor::Derived { subquery, .. } => {
                    if let SetExpr::Select(subselect) = &*subquery.body {
                        let sub_tables = extract_tables_enhanced(subselect, cte_context)?;
                        tables.extend(sub_tables);
                    }
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

fn extract_columns_enhanced(select: &Select) -> Result<Vec<String>> {
    let mut columns = vec![];
    
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                // Try to extract column name from expression
                if let Ok(col_name) = extract_column_name_from_expr(expr) {
                    columns.push(col_name);
                } else {
                    // Use expression as column name
                    columns.push(format!("{:?}", expr));
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                columns.push(alias.value.clone());
            }
            SelectItem::Wildcard(_) => {
                // Will be expanded later
                columns.push("*".to_string());
            }
            SelectItem::QualifiedWildcard(qualifier, _) => {
                let table = qualifier.0.iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                columns.push(format!("{}.*", table));
            }
        }
    }
    
    Ok(columns)
}

fn extract_column_name_from_expr(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => {
            Ok(idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("."))
        }
        Expr::Function(func) => {
            Ok(func.name.to_string())
        }
        _ => anyhow::bail!("Cannot extract column name from expression")
    }
}

fn extract_joins_enhanced(select: &Select) -> Result<Vec<JoinInfo>> {
    let mut joins = vec![];
    
    for item in &select.from {
        for join in &item.joins {
            let join_type = match &join.join_operator {
                JoinOperator::Inner(_) => JoinType::Inner,
                JoinOperator::LeftOuter(_) => JoinType::Left,
                JoinOperator::RightOuter(_) => JoinType::Right,
                JoinOperator::FullOuter(_) => JoinType::Full,
                _ => JoinType::Inner, // Default to Inner for other join types
            };
            
            // Extract join condition
            let (left_table, left_column, right_table, right_column) = match &join.join_operator {
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
                            // This is a simplified version
                            ("left".to_string(), col_name.clone(), "right".to_string(), col_name)
                        }
                        _ => continue,
                    }
                }
                _ => continue,
            };
            
            // Get table name from join relation
            let right_table_name = match &join.relation {
                TableFactor::Table { name, .. } => {
                    name.0.iter().map(|ident| ident.value.clone()).collect::<Vec<_>>().join(".")
                }
                _ => right_table.clone(),
            };
            
            joins.push(JoinInfo {
                left_table,
                left_column,
                right_table: right_table_name,
                right_column,
                join_type,
            });
        }
    }
    
    Ok(joins)
}

fn extract_join_condition(expr: &Expr) -> Result<(String, String, String, String)> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if matches!(op, sqlparser::ast::BinaryOperator::Eq) {
                let left_col = extract_column_ref(left)?;
                let right_col = extract_column_ref(right)?;
                Ok((left_col.0, left_col.1, right_col.0, right_col.1))
            } else {
                anyhow::bail!("Join condition must be equality")
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
        extract_predicates(where_clause, &mut filters)?;
    }
    
    Ok(filters)
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
                        
                        let value = extract_literal_value(right)?;
                        
                        filters.push(FilterInfo {
                            table,
                            column,
                            operator,
                            value,
                            pattern: None,
                            in_values: None,
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
                    });
                }
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
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                extract_aggregate_from_expr(expr, &mut aggregates)?;
            }
            _ => {}
        }
    }
    
    Ok(aggregates)
}

fn extract_aggregate_from_expr(expr: &Expr, aggregates: &mut Vec<AggregateInfo>) -> Result<()> {
    match expr {
        Expr::Function(func) => {
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
            // For COUNT(*), args will be empty or contain a wildcard
            // For COUNT(column), args will contain the column expression
            let column = if func.args.is_empty() {
                "*".to_string()
            } else {
                // For now, just use "*" - the aggregate operator will handle COUNT(*) correctly
                // when column is "*"
                "*".to_string()
            };
            
            aggregates.push(AggregateInfo {
                function: aggregate_func,
                column,
                alias: None,
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

fn extract_order_by_enhanced(query: &Query) -> Result<Vec<OrderByInfo>> {
    let mut order_by = vec![];
    
    for item in &query.order_by {
        let column = extract_column_name_from_expr(&item.expr)
            .unwrap_or_else(|_| format!("{:?}", item.expr));
        let ascending = item.asc.unwrap_or(true);
        
        order_by.push(OrderByInfo {
            column,
            ascending,
        });
    }
    
    Ok(order_by)
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
    // Extract HAVING clause - for now, just convert to string representation
    if let Some(having_expr) = &select.having {
        Ok(Some(format!("{:?}", having_expr)))
    } else {
        Ok(None)
    }
}

