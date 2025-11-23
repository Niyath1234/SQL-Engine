/// Wildcard Expansion Module
/// Expands SELECT * and table.* wildcards into explicit column lists
use crate::query::parser::ParsedQuery;
use crate::query::plan::QueryPlan;
use crate::hypergraph::graph::HyperGraph;
use anyhow::Result;
use std::sync::Arc;

/// Expand wildcards in a query plan
/// This should be called BEFORE CTE materialization to ensure
/// materialized CTEs have fully expanded column lists
pub fn expand_wildcards_in_plan(
    plan: &mut QueryPlan,
    graph: &Arc<HyperGraph>,
) -> Result<()> {
    // Expand wildcards in the main query plan
    expand_wildcards_in_operator(&mut plan.root, graph)?;
    Ok(())
}

/// Expand wildcards in a plan operator recursively
fn expand_wildcards_in_operator(
    op: &mut crate::query::plan::PlanOperator,
    graph: &Arc<HyperGraph>,
) -> Result<()> {
    match op {
        crate::query::plan::PlanOperator::Project { columns, .. } => {
            // Expand wildcards in projection columns
            if columns.iter().any(|c| c == "*" || c.ends_with(".*")) {
                // Need to expand wildcards - but we need the input schema
                // This is tricky because we're in planning phase
                // For now, we'll expand during execution, but mark that expansion is needed
                // TODO: Get input schema from previous operator to expand here
            }
        }
        crate::query::plan::PlanOperator::Scan { columns, .. } => {
            // Expand wildcards in scan columns
            expand_scan_wildcards(columns, graph)?;
        }
        crate::query::plan::PlanOperator::CTEScan { columns, .. } => {
            // CTE wildcards should already be expanded during materialization
            // But if not, expand them here
            // Note: CTE schema is not available at planning time, so this is a placeholder
            // Actual expansion happens during CTE materialization
        }
        crate::query::plan::PlanOperator::Join { left, right, .. } => {
            expand_wildcards_in_operator(left, graph)?;
            expand_wildcards_in_operator(right, graph)?;
        }
        crate::query::plan::PlanOperator::Filter { input, .. } => {
            expand_wildcards_in_operator(input, graph)?;
        }
        crate::query::plan::PlanOperator::Aggregate { input, .. } => {
            expand_wildcards_in_operator(input, graph)?;
        }
        crate::query::plan::PlanOperator::Sort { input, .. } => {
            expand_wildcards_in_operator(input, graph)?;
        }
        crate::query::plan::PlanOperator::Window { input, .. } => {
            expand_wildcards_in_operator(input, graph)?;
        }
        crate::query::plan::PlanOperator::Limit { input, .. } => {
            expand_wildcards_in_operator(input, graph)?;
        }
        crate::query::plan::PlanOperator::Distinct { input, .. } => {
            expand_wildcards_in_operator(input, graph)?;
        }
        crate::query::plan::PlanOperator::Having { input, .. } => {
            expand_wildcards_in_operator(input, graph)?;
        }
        _ => {}
    }
    Ok(())
}

/// Expand wildcards in scan columns based on table schema
fn expand_scan_wildcards(
    columns: &mut Vec<String>,
    graph: &Arc<HyperGraph>,
) -> Result<()> {
    let mut expanded_columns = Vec::new();
    let mut has_wildcard = false;
    
    for col in columns.iter() {
        if col == "*" {
            // Unqualified wildcard - expand to all columns from all tables
            // This is complex and requires knowing which tables are in the query
            // For now, we'll leave it and expand at execution time
            has_wildcard = true;
            expanded_columns.push(col.clone());
        } else if col.ends_with(".*") {
            // Qualified wildcard (e.g., "e.*")
            // NOTE: Wildcard expansion at planning time requires table schema access
            // which isn't available here. Expansion happens at runtime in ProjectOperator.
            // Keep wildcard as-is - it will be expanded during execution.
            expanded_columns.push(col.clone());
            has_wildcard = true;
        } else {
            // Regular column - keep as-is
            expanded_columns.push(col.clone());
        }
    }
    
    if has_wildcard {
        *columns = expanded_columns;
    }
    
    Ok(())
}

/// Expand wildcards in parsed query columns based on available schema
/// This is used during CTE materialization to expand wildcards before execution
pub fn expand_wildcards_in_parsed_query(
    parsed: &mut ParsedQuery,
    graph: &Arc<HyperGraph>,
) -> Result<()> {
    let mut expanded_columns = Vec::new();
    let mut has_wildcard = false;
    
    for col in &parsed.columns {
        if col == "*" {
            // Unqualified wildcard - need to expand based on tables in query
            // For CTEs, we need to expand based on the CTE's input schema
            // This is handled during CTE execution
            has_wildcard = true;
            expanded_columns.push(col.clone());
        } else if col.ends_with(".*") {
            // Qualified wildcard (e.g., "e.*")
            let table_alias = col.strip_suffix(".*").unwrap();
            
            // Try to find table by alias or name
            let table_name = parsed.table_aliases.get(table_alias)
                .cloned()
                .unwrap_or_else(|| table_alias.to_string());
            
            // NOTE: Wildcard expansion at planning time requires table schema access
            // which isn't available here. Expansion happens at runtime in ProjectOperator.
            // Keep wildcard as-is - it will be expanded during CTE execution.
            expanded_columns.push(col.clone());
            has_wildcard = true;
        } else {
            // Regular column - keep as-is
            expanded_columns.push(col.clone());
        }
    }
    
    if has_wildcard {
        eprintln!("DEBUG expand_wildcards_in_parsed_query: Expanded {} columns to {} columns", 
            parsed.columns.len(), expanded_columns.len());
        parsed.columns = expanded_columns;
    }
    
    Ok(())
}

/// Expand wildcards in columns based on a schema
/// This is used during execution when we have the actual schema
pub fn expand_wildcards_from_schema(
    columns: &[String],
    schema: &arrow::datatypes::Schema,
    table_aliases: &std::collections::HashMap<String, String>,
) -> Result<Vec<String>> {
    let mut expanded_columns = Vec::new();
    
    for col in columns {
        if col == "*" {
            // Unqualified wildcard - expand to all columns in schema
            for field in schema.fields() {
                expanded_columns.push(field.name().to_string());
            }
        } else if col.ends_with(".*") {
            // Qualified wildcard (e.g., "e.*")
            let table_alias = col.strip_suffix(".*").unwrap();
            
            // Find columns that match this table alias
            for field in schema.fields() {
                let field_name = field.name();
                // Check if field is qualified with this table alias
                if field_name.starts_with(&format!("{}.", table_alias)) {
                    expanded_columns.push(field_name.to_string());
                } else if let Some(actual_table) = table_aliases.get(table_alias) {
                    // Check if field is qualified with actual table name
                    if field_name.starts_with(&format!("{}.", actual_table)) {
                        expanded_columns.push(field_name.to_string());
                    }
                }
            }
        } else {
            // Regular column - keep as-is
            expanded_columns.push(col.clone());
        }
    }
    
    Ok(expanded_columns)
}

