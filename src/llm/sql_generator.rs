//! SQL Generator - Converts structured plan to SQL

use crate::llm::plan_generator::StructuredPlan;
use crate::worldstate::policies::QueryPolicy;

/// SQL Generator - Converts structured plan to executable SQL
pub struct SQLGenerator;

impl SQLGenerator {
    pub fn new() -> Self {
        Self
    }
    
    /// Generate SQL from structured plan
    pub fn generate_sql(&self, plan: &StructuredPlan, policy: &QueryPolicy) -> String {
        // Collect all tables involved
        let mut all_tables = std::collections::HashSet::new();
        for dataset in &plan.datasets {
            all_tables.insert(dataset.table.as_str());
        }
        for join in &plan.joins {
            all_tables.insert(join.left_table.as_str());
            all_tables.insert(join.right_table.as_str());
        }

        // Aggregation aliases must NOT be qualified as table.columns
        let agg_aliases: std::collections::HashSet<&str> = plan.aggregations
            .iter()
            .map(|a| a.alias.as_str())
            .collect();
        
        // Also extract tables from qualified column names in projections and group_by
        for proj in &plan.projections {
            if let Some((table, _)) = proj.split_once('.') {
                all_tables.insert(table);
            }
        }
        for col in &plan.group_by {
            if let Some((table, _)) = col.split_once('.') {
                all_tables.insert(table);
            }
        }
        
        // Helper to qualify a column name
        let qualify_column = |col: &str, default_table: Option<&str>| -> String {
            if col.contains('.') {
                col.to_string()
            } else if agg_aliases.contains(col) {
                // e.g. total_sales (alias)
                col.to_string()
            } else {
                // Try to find the table that contains this column
                // Common column name patterns
                let table_hint = if col == "id" || col == "name" || col == "email" || col == "description" {
                    // These could be in multiple tables, use default or first table
                    default_table.or_else(|| all_tables.iter().next().copied()).unwrap_or(&plan.datasets[0].table)
                } else if col == "order_id" {
                    // order_id could be in orders or order_items - check which table is in query
                    if all_tables.contains("orders") {
                        "orders"
                    } else {
                        "order_items"
                    }
                } else if col.contains("order") || col == "order_date" || col == "status" || col == "total_amount" || col == "customer_id" {
                    "orders"
                } else if col.contains("product") || col == "product_id" || col == "price" || col == "category_id" {
                    "products"
                } else if col.contains("category") {
                    "categories"
                } else if col.contains("customer") || col == "customer_id" {
                    "customers"
                } else if col == "quantity" {
                    "order_items"
                } else {
                    default_table.or_else(|| all_tables.iter().next().copied()).unwrap_or(&plan.datasets[0].table)
                };
                
                // Check if this table is actually in the query
                if all_tables.contains(table_hint) {
                    format!("{}.{}", table_hint, col)
                } else {
                    // Fallback: use first table
                    if let Some(first_table) = all_tables.iter().next() {
                        format!("{}.{}", first_table, col)
                    } else {
                        col.to_string()
                    }
                }
            }
        };
        
        let mut sql = String::new();
        
        // SELECT clause
        sql.push_str("SELECT ");
        
        // Build SELECT items: aggregations first, then projections
        let mut select_items = Vec::new();
        
        // Add aggregations (e.g., SUM(orders.total_amount) AS total_sales)
        for agg in &plan.aggregations {
            let qualified_expr = qualify_agg_expr(&agg.expr, &qualify_column);
            if agg.alias.trim().is_empty() {
                select_items.push(qualified_expr);
            } else if qualified_expr.to_lowercase().contains(" as ") {
                // If model already included AS, don't double-alias
                select_items.push(qualified_expr);
            } else {
                select_items.push(format!("{} AS {}", qualified_expr, agg.alias));
            }
        }
        
        // Add projections (non-aggregated columns)
        if !plan.projections.is_empty() {
            let qualified_projections: Vec<String> = plan.projections.iter()
                .map(|proj| qualify_column(proj, None))
                .collect();
            select_items.extend(qualified_projections);
        }
        
        if select_items.is_empty() {
            sql.push_str("*");
        } else {
            sql.push_str(&select_items.join(", "));
        }
        
        // FROM clause
        if !plan.datasets.is_empty() {
            sql.push_str(" FROM ");
            sql.push_str(&plan.datasets[0].table);
        }
        
        // JOIN clauses
        // Build joins in a connected order so we never reference a table that hasn't been introduced yet.
        if !plan.joins.is_empty() && !plan.datasets.is_empty() {
            let mut joined: std::collections::HashSet<&str> = std::collections::HashSet::new();
            joined.insert(plan.datasets[0].table.as_str());

            // Track remaining joins by index
            let mut remaining: Vec<usize> = (0..plan.joins.len()).collect();
            let mut progress = true;
            while progress && !remaining.is_empty() {
                progress = false;
                let mut next_remaining = Vec::new();
                for idx in remaining {
                    let j = &plan.joins[idx];
                    let left_in = joined.contains(j.left_table.as_str());
                    let right_in = joined.contains(j.right_table.as_str());

                    match (left_in, right_in) {
                        (true, false) => {
                            sql.push_str(&format!(
                                " {} JOIN {} ON {}.{} = {}.{}",
                                j.join_type.to_uppercase(),
                                j.right_table,
                                j.left_table,
                                j.left_key,
                                j.right_table,
                                j.right_key
                            ));
                            joined.insert(j.right_table.as_str());
                            progress = true;
                        }
                        (false, true) => {
                            // Reverse the join direction to attach the missing left_table
                            sql.push_str(&format!(
                                " {} JOIN {} ON {}.{} = {}.{}",
                                j.join_type.to_uppercase(),
                                j.left_table,
                                j.right_table,
                                j.right_key,
                                j.left_table,
                                j.left_key
                            ));
                            joined.insert(j.left_table.as_str());
                            progress = true;
                        }
                        (true, true) => {
                            // Both already present; skip (avoid duplicate joins)
                            progress = true;
                        }
                        (false, false) => {
                            next_remaining.push(idx);
                        }
                    }
                }
                remaining = next_remaining;
            }
            // If any joins remain disconnected, we intentionally omit them (they're either unnecessary or invalid).
        }
        
        // WHERE clause
        let mut filters = Vec::new();
        for dataset in &plan.datasets {
            for filter in &dataset.filters {
                // Qualify column names in filters (simplified - would need SQL parsing)
                let qualified_filter = if filter.contains('.') {
                    filter.clone()
                } else {
                    // Try to qualify first column reference
                    let parts: Vec<&str> = filter.split_whitespace().collect();
                    if !parts.is_empty() {
                        let first_part = parts[0];
                        let qualified_first = qualify_column(first_part, Some(&dataset.table));
                        format!("{} {}", qualified_first, parts[1..].join(" "))
                    } else {
                        filter.clone()
                    }
                };
                filters.push(qualified_filter);
            }
            if let Some(ref time_constraint) = dataset.time_constraints {
                filters.push(time_constraint.clone());
            }
        }
        if !filters.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&filters.join(" AND "));
        }
        
        // GROUP BY clause
        if !plan.group_by.is_empty() {
            sql.push_str(" GROUP BY ");
            let qualified_group_by: Vec<String> = plan.group_by.iter()
                .map(|col| qualify_column(col, None))
                .collect();
            sql.push_str(&qualified_group_by.join(", "));
        }
        
        // ORDER BY clause
        if let Some(ref order_by) = plan.order_by {
            sql.push_str(" ORDER BY ");
            let order_clauses: Vec<String> = order_by.iter()
                .map(|o| {
                    // Extract column name (strip table prefix if present, e.g., "customers.total_sales" -> "total_sales")
                    let col_name = if let Some((_, col)) = o.column.split_once('.') {
                        col
                    } else {
                        o.column.as_str()
                    };
                    
                    // Check if this is an aggregation alias (should not be qualified)
                    let col = if agg_aliases.contains(col_name) {
                        // It's an aggregation alias - use just the alias name (e.g., "total_sales")
                        col_name.to_string()
                    } else {
                        // It's a regular column - qualify it
                        qualify_column(&o.column, None)
                    };
                    format!("{} {}", col, o.direction)
                })
                .collect();
            sql.push_str(&order_clauses.join(", "));
        }
        
        // LIMIT clause (enforced by policy)
        if let Some(max_rows) = policy.max_rows {
            sql.push_str(&format!(" LIMIT {}", max_rows));
        } else {
            // Default limit for safety
            sql.push_str(" LIMIT 10000");
        }
        
        sql
    }
}

impl Default for SQLGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// Try to qualify simple aggregation expressions like:
/// - SUM(total_amount) -> SUM(orders.total_amount) (if resolvable by qualify_column)
/// - COUNT(*) stays as-is
fn qualify_agg_expr(expr: &str, qualify_column: &impl Fn(&str, Option<&str>) -> String) -> String {
    let trimmed = expr.trim();
    let upper = trimmed.to_uppercase();
    for f in ["SUM(", "AVG(", "MIN(", "MAX(", "COUNT("] {
        if upper.starts_with(f) && trimmed.ends_with(')') {
            let inner = trimmed[f.len()..trimmed.len() - 1].trim();
            if inner == "*" || inner.contains('.') || inner.is_empty() {
                return trimmed.to_string();
            }
            let qualified_inner = qualify_column(inner, None);
            return format!("{}{})", &trimmed[..f.len()], qualified_inner);
        }
    }
    trimmed.to_string()
}

