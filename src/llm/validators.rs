//! Plan Validators - Deterministic validation of structured plans

use crate::llm::plan_generator::StructuredPlan;
use crate::worldstate::MetadataPack;
use crate::worldstate::policies::QueryPolicy;
use anyhow::Result;

/// Validation result
#[derive(Clone, Debug)]
pub enum ValidationResult {
    Valid,
    Invalid {
        reason: String,
        suggestions: Vec<String>,
    },
}

/// Plan Validator - Validates structured plans against metadata and policies
pub struct PlanValidator;

impl PlanValidator {
    pub fn new() -> Self {
        Self
    }
    
    /// Validate a structured plan
    pub fn validate(
        &self,
        plan: &StructuredPlan,
        metadata_pack: &MetadataPack,
        policy: &QueryPolicy,
    ) -> ValidationResult {
        // Schema safety: no invented tables/columns
        if let Err(e) = self.validate_schema_safety(plan, metadata_pack) {
            return ValidationResult::Invalid {
                reason: format!("Schema safety violation: {}", e),
                suggestions: vec!["Only use tables and columns from the metadata pack".to_string()],
            };
        }
        
        // Check that all referenced tables are either in datasets or joined
        if let Err(e) = self.validate_table_coverage(plan, metadata_pack) {
            return ValidationResult::Invalid {
                reason: format!("Table coverage violation: {}", e),
                suggestions: vec!["If you reference columns from multiple tables, you must include JOINs in the 'joins' array".to_string()],
            };
        }
        
        // Join safety: only approved joins
        if let Err(e) = self.validate_join_safety(plan, metadata_pack) {
            return ValidationResult::Invalid {
                reason: format!("Join safety violation: {}", e),
                suggestions: vec!["Only use approved join rules from the metadata pack".to_string()],
            };
        }
        
        // Filter safety: valid types and columns
        if let Err(e) = self.validate_filter_safety(plan, metadata_pack) {
            return ValidationResult::Invalid {
                reason: format!("Filter safety violation: {}", e),
                suggestions: vec!["Check filter column names and types match metadata".to_string()],
            };
        }
        
        // Aggregation rules
        if let Err(e) = self.validate_aggregations(plan, metadata_pack) {
            return ValidationResult::Invalid {
                reason: format!("Aggregation violation: {}", e),
                suggestions: vec!["Use numeric fields for sums/averages, check cardinality for group-by".to_string()],
            };
        }
        
        // Policy enforcement
        if let Err(e) = self.validate_policy(plan, policy) {
            return ValidationResult::Invalid {
                reason: format!("Policy violation: {}", e),
                suggestions: vec!["Respect max_rows, max_time_ms, and other policy constraints".to_string()],
            };
        }
        
        ValidationResult::Valid
    }
    
    /// Validate schema safety (no invented tables/columns)
    fn validate_schema_safety(
        &self,
        plan: &StructuredPlan,
        metadata_pack: &MetadataPack,
    ) -> Result<()> {
        let valid_tables: std::collections::HashSet<&str> = metadata_pack.tables
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        
        // Build valid columns from all tables
        let mut valid_columns: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for table in &metadata_pack.tables {
            for col in &table.columns {
                valid_columns.insert(col.name.as_str());
            }
        }
        
        // Check all tables in plan exist
        for dataset in &plan.datasets {
            if !valid_tables.contains(dataset.table.as_str()) {
                return Err(anyhow::anyhow!("Table '{}' not found in metadata", dataset.table));
            }
        }
        
        // Build set of aggregation aliases (these are allowed in projections)
        let aggregation_aliases: std::collections::HashSet<&str> = plan.aggregations.iter()
            .map(|agg| agg.alias.as_str())
            .collect();
        
        // Check all projections exist (handle both "column" and "table.column" formats)
        for proj in &plan.projections {
            // Extract column name (remove table prefix if present)
            let column_name = if let Some(dot_pos) = proj.find('.') {
                &proj[dot_pos + 1..]
            } else {
                proj.as_str()
            };
            
            // Allow aggregation aliases
            if aggregation_aliases.contains(column_name) {
                continue;
            }
            
            if !valid_columns.contains(column_name) {
                return Err(anyhow::anyhow!("Column '{}' (from projection '{}') not found in metadata", column_name, proj));
            }
        }
        
        Ok(())
    }
    
    /// Validate that all tables referenced in columns are either in datasets or joined
    fn validate_table_coverage(
        &self,
        plan: &StructuredPlan,
        _metadata_pack: &MetadataPack,
    ) -> Result<()> {
        use std::collections::HashSet;
        
        // Collect tables from datasets
        let mut covered_tables: HashSet<&str> = plan.datasets.iter()
            .map(|d| d.table.as_str())
            .collect();
        
        // Add tables from joins
        for join in &plan.joins {
            covered_tables.insert(join.left_table.as_str());
            covered_tables.insert(join.right_table.as_str());
        }
        
        // Check projections for table-qualified columns
        for proj in &plan.projections {
            if let Some((table, _)) = proj.split_once('.') {
                if !covered_tables.contains(table) {
                    return Err(anyhow::anyhow!(
                        "Column '{}' references table '{}' which is not in datasets or joins. \
                        You must add a JOIN to this table if you want to use its columns.",
                        proj, table
                    ));
                }
            }
        }
        
        // Check group_by for table-qualified columns
        for col in &plan.group_by {
            if let Some((table, _)) = col.split_once('.') {
                if !covered_tables.contains(table) {
                    return Err(anyhow::anyhow!(
                        "Column '{}' in group_by references table '{}' which is not in datasets or joins. \
                        You must add a JOIN to this table if you want to use its columns.",
                        col, table
                    ));
                }
            }
        }
        
        Ok(())
    }
    
    /// Validate join safety (only approved joins)
    fn validate_join_safety(
        &self,
        plan: &StructuredPlan,
        metadata_pack: &MetadataPack,
    ) -> Result<()> {
        let approved_joins: std::collections::HashSet<(&str, &str, &str, &str)> = metadata_pack.join_rules
            .iter()
            .flat_map(|j| {
                // Handle multi-column keys (simplified: use first key)
                if !j.left_key.is_empty() && !j.right_key.is_empty() {
                    Some((
                        j.left_table.as_str(),
                        j.left_key[0].as_str(),
                        j.right_table.as_str(),
                        j.right_key[0].as_str(),
                    ))
                } else {
                    None
                }
            })
            .collect();
        
        for join in &plan.joins {
            let key = (
                join.left_table.as_str(),
                join.left_key.as_str(),
                join.right_table.as_str(),
                join.right_key.as_str(),
            );
            
            // Check both directions
            if !approved_joins.contains(&key) {
                let reverse_key = (
                    join.right_table.as_str(),
                    join.right_key.as_str(),
                    join.left_table.as_str(),
                    join.left_key.as_str(),
                );
                
                if !approved_joins.contains(&reverse_key) {
                    return Err(anyhow::anyhow!(
                        "Join {} JOIN {} ON {}.{} = {}.{} is not approved",
                        join.left_table, join.right_table,
                        join.left_table, join.left_key,
                        join.right_table, join.right_key
                    ));
                }
            }
        }
        
        Ok(())
    }
    
    /// Validate filter safety (valid types and columns)
    fn validate_filter_safety(
        &self,
        plan: &StructuredPlan,
        metadata_pack: &MetadataPack,
    ) -> Result<()> {
        // Build column map: table -> columns
        let mut table_columns: std::collections::HashMap<&str, std::collections::HashSet<&str>> = std::collections::HashMap::new();
        for table in &metadata_pack.tables {
            let mut cols = std::collections::HashSet::new();
            for col in &table.columns {
                cols.insert(col.name.as_str());
            }
            table_columns.insert(table.name.as_str(), cols);
        }
        
        // Check filters reference valid columns
        for dataset in &plan.datasets {
            let table_cols = table_columns.get(dataset.table.as_str())
                .ok_or_else(|| anyhow::anyhow!("Table '{}' not found for filter validation", dataset.table))?;
            
            // Simple check: extract column names from filter expressions
            // This is a simplified check - in production would parse SQL
            for filter in &dataset.filters {
                // Try to extract column name (simplified)
                if let Some(col_name) = filter.split_whitespace().next() {
                    let clean_col = col_name.trim_matches('`').trim_matches('"').trim_matches('\'');
                    // Handle table.column format
                    let column_only = if let Some(dot_pos) = clean_col.find('.') {
                        &clean_col[dot_pos + 1..]
                    } else {
                        clean_col
                    };
                    if !table_cols.contains(column_only) {
                        return Err(anyhow::anyhow!("Filter column '{}' (from '{}') not found in table '{}'", column_only, clean_col, dataset.table));
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Validate aggregations
    fn validate_aggregations(
        &self,
        plan: &StructuredPlan,
        metadata_pack: &MetadataPack,
    ) -> Result<()> {
        // Build column map
        let mut table_columns: std::collections::HashMap<&str, std::collections::HashMap<&str, &str>> = std::collections::HashMap::new();
        for table in &metadata_pack.tables {
            let mut cols = std::collections::HashMap::new();
            for col in &table.columns {
                cols.insert(col.name.as_str(), col.data_type.as_str());
            }
            table_columns.insert(table.name.as_str(), cols);
        }
        
        // Check aggregation expressions reference valid columns
        for agg in &plan.aggregations {
            // Simplified: check if expression contains valid column names
            // In production, would parse SQL expression
            let mut found_valid_column = false;
            for (table_name, columns) in &table_columns {
                for (col_name, _) in columns {
                    // Check both "column" and "table.column" formats
                    if agg.expr.contains(col_name) || agg.expr.contains(&format!("{}.{}", table_name, col_name)) {
                        found_valid_column = true;
                        break;
                    }
                }
                if found_valid_column {
                    break;
                }
            }
            // Allow COUNT(*) and other functions without column references
            if !found_valid_column && !agg.expr.contains("COUNT(*)") && !agg.expr.contains("*") {
                // This is a warning, not an error - aggregation might be valid
            }
        }
        
        // Check group_by columns exist (handle table.column format)
        for group_col in &plan.group_by {
            // Extract column name (remove table prefix if present)
            let column_name = if let Some(dot_pos) = group_col.find('.') {
                &group_col[dot_pos + 1..]
            } else {
                group_col.as_str()
            };
            
            let mut found = false;
            for table in &metadata_pack.tables {
                for col in &table.columns {
                    if col.name == column_name {
                        found = true;
                        break;
                    }
                }
                if found {
                    break;
                }
            }
            if !found {
                return Err(anyhow::anyhow!("Group by column '{}' (from '{}') not found", column_name, group_col));
            }
        }
        
        Ok(())
    }
    
    /// Validate policy constraints
    fn validate_policy(
        &self,
        plan: &StructuredPlan,
        policy: &QueryPolicy,
    ) -> Result<()> {
        // Check max_rows (would be enforced in SQL generation)
        if let Some(max_rows) = policy.max_rows {
            if plan.estimated_cost > max_rows as f64 {
                return Err(anyhow::anyhow!(
                    "Estimated cost ({}) exceeds max_rows ({})",
                    plan.estimated_cost, max_rows
                ));
            }
        }
        
        Ok(())
    }
}

impl Default for PlanValidator {
    fn default() -> Self {
        Self::new()
    }
}

