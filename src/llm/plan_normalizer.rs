//! Plan Normalizer - deterministic cleanup/repair of LLM structured plans
//!
//! Goal: make the system foolproof by deterministically mapping/qualifying columns
//! against the execution-truth schema and rewriting only when unambiguous.

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};

use crate::llm::StructuredPlan;
use crate::worldstate::MetadataPack;

#[derive(Default)]
pub struct PlanNormalizer;

impl PlanNormalizer {
    pub fn new() -> Self {
        Self
    }

    /// Normalize a structured plan:
    /// - Qualify unqualified columns when unambiguous across covered tables
    /// - Allow aggregation aliases in projections/group_by/order_by
    /// - Rewrite common qualified alias mistakes when unambiguous (e.g. orders.order_id -> orders.id)
    /// - Validate join keys exist; rewrite join key *_id -> id only when table lacks *_id but has id
    pub fn normalize(&self, plan: &mut StructuredPlan, metadata_pack: &MetadataPack) -> Result<()> {
        eprintln!("DEBUG normalizer::normalize: CALLED");
        let schema = SchemaIndex::from_metadata_pack(metadata_pack);
        let covered_tables = covered_tables(plan);
        let join_rule_index = JoinRuleIndex::from_metadata_pack(metadata_pack);

        // Precompute aggregation aliases so we don't treat them like real columns.
        let agg_aliases: HashSet<String> = plan.aggregations.iter().map(|a| a.alias.clone()).collect();
        eprintln!("DEBUG normalizer::normalize: agg_aliases: {:?}", agg_aliases);

        // MVP RULE: Single-table query detection and correction - RUN FIRST!
        // This removes unnecessary joins BEFORE we try to normalize them, preventing errors
        // If all referenced columns are from ONE table, use that table directly (no joins needed)
        eprintln!("DEBUG normalizer: About to call fix_single_table_queries (FIRST, before join normalization)...");
        eprintln!("DEBUG normalizer: Plan state BEFORE fix_single_table_queries - datasets: {:?}, joins: {:?}, projections: {:?}", 
            plan.datasets.iter().map(|d| d.table.clone()).collect::<Vec<_>>(),
            plan.joins.iter().map(|j| format!("{}->{}", j.left_table, j.right_table)).collect::<Vec<_>>(),
            plan.projections);
        let was_single_table_fixed = self.fix_single_table_queries(plan, &schema, &agg_aliases, metadata_pack)?;
        eprintln!("DEBUG normalizer: fix_single_table_queries returned: {}", was_single_table_fixed);
        eprintln!("DEBUG normalizer: Plan state AFTER fix_single_table_queries - datasets: {:?}, joins: {:?}, projections: {:?}", 
            plan.datasets.iter().map(|d| d.table.clone()).collect::<Vec<_>>(),
            plan.joins.iter().map(|j| format!("{}->{}", j.left_table, j.right_table)).collect::<Vec<_>>(),
            plan.projections);

        // Normalize join keys (AFTER removing unnecessary joins via fix_single_table_queries)
        for join in &mut plan.joins {
            // Enforce join keys from approved join rules (single source of truth for joins).
            // This prevents the LLM from inventing/swapping join keys like orders.id = order_items.id.
            join_rule_index.apply_to_join(join);

            self.normalize_join_key(&schema, &covered_tables, &join.left_table, &mut join.left_key)?;
            self.normalize_join_key(&schema, &covered_tables, &join.right_table, &mut join.right_key)?;
        }

        // Normalize projections
        for col in &mut plan.projections {
            self.normalize_column_ref(&schema, &covered_tables, &agg_aliases, col)
                .with_context(|| format!("Failed to normalize projection '{}'", col))?;
        }

        // Normalize group_by
        for col in &mut plan.group_by {
            self.normalize_column_ref(&schema, &covered_tables, &agg_aliases, col)
                .with_context(|| format!("Failed to normalize group_by '{}'", col))?;
        }

        // Normalize order_by columns
        // CRITICAL: Aggregation aliases should NOT be qualified as table.column
        if let Some(ref mut order_by) = plan.order_by {
            for o in order_by {
                // Extract column name (strip table prefix if present)
                let col_name = if let Some((_, col)) = o.column.split_once('.') {
                    col
                } else {
                    o.column.as_str()
                };
                
                // If it's an aggregation alias, unqualify it (remove table prefix if present)
                if agg_aliases.contains(col_name) {
                    // It's an aggregation alias - ensure it's unqualified (just the alias name)
                    o.column = col_name.to_string();
                } else {
                    // Only normalize if it's NOT an aggregation alias
                    self.normalize_column_ref(&schema, &covered_tables, &agg_aliases, &mut o.column)
                        .with_context(|| format!("Failed to normalize order_by '{}'", o.column))?;
                }
            }
        }
        
        // CRITICAL: Before removing joins, ensure all referenced tables have joins
        // If a table is referenced in projections/group_by but not in datasets or joins, we need to add a join
        // (Only for multi-table queries - single-table queries are handled above)
        // MVP: Skip ensure_required_joins if fix_single_table_queries already fixed it (no joins needed)
        if !was_single_table_fixed {
            eprintln!("DEBUG normalizer: Calling ensure_required_joins (multi-table query)...");
            eprintln!("DEBUG normalizer: Before ensure_required_joins - datasets: {:?}, joins: {:?}, projections: {:?}", 
                plan.datasets.iter().map(|d| d.table.clone()).collect::<Vec<_>>(),
                plan.joins.iter().map(|j| format!("{}->{}", j.left_table, j.right_table)).collect::<Vec<_>>(),
                plan.projections);
            self.ensure_required_joins(plan, &schema, &agg_aliases, metadata_pack)?;
            eprintln!("DEBUG normalizer: After ensure_required_joins - datasets: {:?}, joins: {:?}", 
                plan.datasets.iter().map(|d| d.table.clone()).collect::<Vec<_>>(),
                plan.joins.iter().map(|j| format!("{}->{}", j.left_table, j.right_table)).collect::<Vec<_>>());
        } else {
            eprintln!("DEBUG normalizer: Skipping ensure_required_joins (single-table query already fixed)");
        }
        
        // CRITICAL: Do NOT remove joins that were just added by ensure_required_joins
        // Only remove joins where NEITHER table is referenced in any way
        // For MVP: Skip join removal entirely to ensure correctness
        // self.remove_unnecessary_joins(plan, &schema, &agg_aliases)?;
        
        // MVP: Skip remove_unnecessary_datasets for single-table queries (already fixed by fix_single_table_queries)
        // Only run if we have multiple tables (multi-table query)
        let dataset_count = plan.datasets.len();
        if dataset_count > 1 || !plan.joins.is_empty() {
            // Multi-table query - safe to remove unnecessary datasets
            self.remove_unnecessary_datasets(plan, &schema, &agg_aliases)?;
        } else {
            // Single-table query - already fixed, don't mess with it
            eprintln!("DEBUG normalizer: Skipping remove_unnecessary_datasets for single-table query (already fixed)");
        }
        
        // After removing datasets, remove any joins that reference removed datasets
        let remaining_datasets: HashSet<String> = plan.datasets.iter().map(|d| d.table.clone()).collect();
        let initial_join_count = plan.joins.len();
        plan.joins.retain(|join| {
            remaining_datasets.contains(&join.left_table) && remaining_datasets.contains(&join.right_table)
        });
        if plan.joins.len() < initial_join_count {
            eprintln!("DEBUG normalizer: Removed {} join(s) referencing removed datasets. Remaining: {:?}", 
                initial_join_count - plan.joins.len(),
                plan.joins.iter().map(|j| format!("{}→{}", j.left_table, j.right_table)).collect::<Vec<_>>());
        }

        Ok(())
    }
    
    /// MVP RULE: Fix single-table queries - if all columns reference ONE table, use that table directly
    /// This prevents LLM from hallucinating unnecessary joins
    /// 
    /// Algorithm:
    /// Step 1: Collect projected tables from projections, group_by, filters (NOT aggregations)
    /// Step 2: Ignore columns that are aggregation aliases
    /// Step 3: Detect table references from "table.column" format
    /// Step 4: If exactly one real table appears → single-table query
    ///   → Overwrite datasets[0].table with that table
    ///   → Clear all aggregations unless query explicitly asks for them
    ///   → Remove all projections referencing other tables
    /// Step 5: Keep hypergraph-aware (validate table exists in hypergraph)
    /// 
    /// Returns: true if a single-table fix was applied, false otherwise
    fn fix_single_table_queries(
        &self,
        plan: &mut StructuredPlan,
        _schema: &SchemaIndex,
        agg_aliases: &HashSet<String>,
        metadata_pack: &MetadataPack,
    ) -> Result<bool> {
        use std::collections::HashSet;
        
        eprintln!("DEBUG fix_single_table_queries: CALLED");
        eprintln!("DEBUG fix_single_table_queries: projections: {:?}, agg_aliases: {:?}", plan.projections, agg_aliases);
        
        // Step 1: Collect projected tables from projections, group_by, filters (NOT aggregations)
        let mut projected_tables = HashSet::new();
        
        // From projections (Step 2: Ignore aggregation aliases)
        for proj in &plan.projections {
            if agg_aliases.contains(proj) {
                eprintln!("DEBUG fix_single_table_queries: Skipping projection '{}' (aggregation alias)", proj);
                continue;
            }
            // Step 3: Detect table references from "table.column" format
            if let Some((table, _)) = split_qualified(proj) {
                eprintln!("DEBUG fix_single_table_queries: Found table '{}' from projection '{}'", table, proj);
                projected_tables.insert(table.to_string());
            } else if let Some((table, _)) = proj.split_once('.') {
                eprintln!("DEBUG fix_single_table_queries: Found table '{}' from projection '{}' (split_once)", table, proj);
                projected_tables.insert(table.to_string());
            }
        }
        
        // From group_by
        for col in &plan.group_by {
            if agg_aliases.contains(col) {
                continue;
            }
            if let Some((table, _)) = split_qualified(col) {
                projected_tables.insert(table.to_string());
            } else if let Some((table, _)) = col.split_once('.') {
                projected_tables.insert(table.to_string());
            }
        }
        
        // From filters
        for dataset in &plan.datasets {
            for filter in &dataset.filters {
                for word in filter.split_whitespace() {
                    if let Some((table, _)) = word.split_once('.') {
                        projected_tables.insert(table.to_string());
                    }
                }
            }
        }
        
        eprintln!("DEBUG fix_single_table_queries: Step 1-3 complete - projected_tables: {:?}", projected_tables);
        eprintln!("DEBUG fix_single_table_queries: Current datasets: {:?}", plan.datasets.iter().map(|d| d.table.clone()).collect::<Vec<_>>());
        eprintln!("DEBUG fix_single_table_queries: Current joins: {:?}", plan.joins.iter().map(|j| format!("{}->{}", j.left_table, j.right_table)).collect::<Vec<_>>());
        
        // Step 4: If exactly one real table appears → single-table query
        // OR: If no qualified columns but only one dataset → single-table query (unqualified columns assumed from that dataset)
        // OR: If all projections are unqualified AND there's only one dataset → single-table query (even if joins exist, they're unnecessary)
        let single_table = if projected_tables.len() == 1 {
            Some(projected_tables.iter().next().unwrap().clone())
        } else if projected_tables.len() == 0 && plan.datasets.len() == 1 {
            // MVP: If all projections are unqualified AND there's only one dataset, treat as single-table
            // This handles cases like "List all customers" where LLM generates unqualified "name" but correct dataset "customers"
            // Even if joins exist, they're unnecessary for unqualified columns from a single dataset
            eprintln!("DEBUG fix_single_table_queries: No qualified columns found, but single dataset - treating as single-table (will remove joins)");
            Some(plan.datasets[0].table.clone())
        } else {
            None
        };
        
        if let Some(single_table) = single_table {
            // Step 5: Hypergraph-aware - validate table exists in hypergraph
            let table_exists = metadata_pack.tables.iter().any(|t| t.name == single_table);
            if !table_exists {
                eprintln!("DEBUG fix_single_table_queries: Table '{}' not found in hypergraph, skipping fix", single_table);
                return Ok(false);
            }
            
            eprintln!("DEBUG fix_single_table_queries: ✅ Single-table query detected - all projected columns reference '{}'", single_table);
            eprintln!("DEBUG fix_single_table_queries: Fixing plan...");
            
            // Step 4a: Overwrite datasets[0].table with that table
            plan.datasets.clear();
            plan.datasets.push(crate::llm::plan_generator::DatasetInfo {
                table: single_table.clone(),
                filters: vec![],
                time_constraints: None,
            });
            
            // Step 4b: Clear all aggregations (unless query explicitly asks for them - for MVP, clear them)
            // TODO: In future, detect if aggregations are explicitly requested vs hallucinated
            if !plan.aggregations.is_empty() {
                eprintln!("DEBUG fix_single_table_queries: Clearing {} aggregation(s) for single-table query", plan.aggregations.len());
                plan.aggregations.clear();
            }
            
            // Step 4c: Remove all projections referencing other tables (keep only single_table projections)
            let initial_proj_count = plan.projections.len();
            plan.projections.retain(|proj| {
                // Keep aggregation aliases (they'll be empty now, but safe to keep)
                if agg_aliases.contains(proj) {
                    return false; // Remove aggregation aliases since we cleared aggregations
                }
                // Keep projections that reference the single table
                if let Some((table, _)) = split_qualified(proj) {
                    table == single_table
                } else if let Some((table, _)) = proj.split_once('.') {
                    table == single_table
                } else {
                    // Unqualified column - keep it (assumed to be from single table)
                    true
                }
            });
            if plan.projections.len() < initial_proj_count {
                eprintln!("DEBUG fix_single_table_queries: Removed {} projection(s) referencing other tables", 
                    initial_proj_count - plan.projections.len());
            }
            
            // Remove all joins (not needed for single-table query)
            if !plan.joins.is_empty() {
                eprintln!("DEBUG fix_single_table_queries: Removing {} unnecessary join(s)", plan.joins.len());
                plan.joins.clear();
            }
            
            eprintln!("DEBUG fix_single_table_queries: ✅ Fixed plan - datasets: [{}], joins: [], projections: {:?}", 
                single_table, plan.projections);
            return Ok(true); // Indicate that fix was applied
        } else if projected_tables.len() == 0 {
            // No table references found - might be aggregation-only query
            eprintln!("DEBUG fix_single_table_queries: No table references found in projections/group_by - keeping current plan");
        } else {
            // Multiple tables referenced - this is a multi-table query (joins needed)
            eprintln!("DEBUG fix_single_table_queries: Multi-table query detected - {} tables: {:?}", 
                projected_tables.len(), projected_tables);
        }
        
        Ok(false) // No fix was applied
    }
    
    /// Ensure all referenced tables have joins (add missing joins if needed)
    fn ensure_required_joins(
        &self,
        plan: &mut StructuredPlan,
        _schema: &SchemaIndex,
        agg_aliases: &HashSet<String>,
        metadata_pack: &MetadataPack,
    ) -> Result<()> {
        use std::collections::HashSet;
        
        // Collect all tables referenced in columns
        let mut referenced_tables = HashSet::new();
        
        // Tables from datasets (base tables)
        for dataset in &plan.datasets {
            referenced_tables.insert(dataset.table.clone());
        }
        
        // Tables from projections - CRITICAL: Parse table.column patterns correctly
        for proj in &plan.projections {
            if agg_aliases.contains(proj) {
                continue;
            }
            // Try split_qualified first (handles qualified columns)
            if let Some((table, _)) = split_qualified(proj) {
                referenced_tables.insert(table.to_string());
                eprintln!("DEBUG ensure_required_joins: Found table '{}' from projection '{}' (via split_qualified)", table, proj);
            } else {
                // Fallback: try direct split_once for table.column patterns
                if let Some((table, _)) = proj.split_once('.') {
                    referenced_tables.insert(table.to_string());
                    eprintln!("DEBUG ensure_required_joins: Found table '{}' from projection '{}' (via split_once)", table, proj);
                }
            }
        }
        
        // Tables from group_by
        for col in &plan.group_by {
            if agg_aliases.contains(col) {
                continue;
            }
            if let Some((table, _)) = split_qualified(col) {
                referenced_tables.insert(table.to_string());
            }
        }
        
        // Tables from aggregations (e.g., "SUM(orders.total_amount)" -> "orders")
        for agg in &plan.aggregations {
            // Extract table.column patterns even inside function calls
            let expr = &agg.expr;
            let parts: Vec<&str> = expr.split(|c: char| !c.is_alphanumeric() && c != '_' && c != '.').collect();
            for part in parts {
                if let Some((table, col)) = part.split_once('.') {
                    if !table.is_empty() && !col.is_empty() {
                        referenced_tables.insert(table.to_string());
                    }
                }
            }
        }
        
        // Tables from filters
        for dataset in &plan.datasets {
            for filter in &dataset.filters {
                for word in filter.split_whitespace() {
                    if let Some((table, _)) = word.split_once('.') {
                        referenced_tables.insert(table.to_string());
                    }
                }
            }
        }
        
        // Tables from order_by
        if let Some(ref order_by) = plan.order_by {
            for o in order_by {
                if agg_aliases.contains(&o.column) {
                    continue;
                }
                if let Some((table, _)) = split_qualified(&o.column) {
                    referenced_tables.insert(table.to_string());
                }
            }
        }
        
        // Tables already covered by joins
        let mut joined_tables = HashSet::new();
        for dataset in &plan.datasets {
            joined_tables.insert(dataset.table.clone());
        }
        for join in &plan.joins {
            joined_tables.insert(join.left_table.clone());
            joined_tables.insert(join.right_table.clone());
        }
        
        // Find tables that are referenced but not joined
        let missing_tables: Vec<String> = referenced_tables.iter()
            .filter(|t| !joined_tables.contains(*t))
            .cloned()
            .collect();
        
        if !missing_tables.is_empty() {
            eprintln!("DEBUG normalizer: Found {} missing table(s): {:?}", missing_tables.len(), missing_tables);
            
            // Try to find join rules to connect missing tables to existing tables
            for missing_table in &missing_tables {
                eprintln!("DEBUG normalizer: Looking for join rule to connect missing table: {}", missing_table);
                // Find a join rule that connects this table to an existing table
                let mut found_join = false;
                for rule in &metadata_pack.join_rules {
                    let can_join_left = joined_tables.contains(&rule.left_table) && rule.right_table == *missing_table;
                    let can_join_right = joined_tables.contains(&rule.right_table) && rule.left_table == *missing_table;
                    eprintln!("DEBUG normalizer: Checking rule {} → {}: can_join_left={}, can_join_right={}, joined_tables={:?}", 
                        rule.left_table, rule.right_table, can_join_left, can_join_right, joined_tables);
                    
                    if can_join_left {
                        found_join = true;
                        eprintln!("DEBUG normalizer: MATCH FOUND - can_join_left=true for rule {} → {}", rule.left_table, rule.right_table);
                        eprintln!("DEBUG normalizer: Adding missing join: {} JOIN {} (via rule)", rule.left_table, rule.right_table);
                        // Ensure right_table (missing_table) is in datasets
                        if !plan.datasets.iter().any(|d| d.table == rule.right_table) {
                            eprintln!("DEBUG normalizer: Adding missing dataset: {}", rule.right_table);
                            plan.datasets.push(crate::llm::plan_generator::DatasetInfo {
                                table: rule.right_table.clone(),
                                filters: vec![],
                                time_constraints: None,
                            });
                        }
                        plan.joins.push(crate::llm::plan_generator::JoinInfo {
                            left_table: rule.left_table.clone(),
                            left_key: rule.left_key.first().cloned().unwrap_or_else(|| "id".to_string()),
                            right_table: rule.right_table.clone(),
                            right_key: rule.right_key.first().cloned().unwrap_or_else(|| "id".to_string()),
                            join_type: rule.join_type.clone(),
                            justification: format!("Auto-added to connect referenced table {}", missing_table),
                        });
                        joined_tables.insert(missing_table.clone());
                        break;
                    } else if can_join_right {
                        found_join = true;
                        eprintln!("DEBUG normalizer: Adding missing join: {} JOIN {} (via rule)", rule.right_table, rule.left_table);
                        // Ensure left_table (missing_table) is in datasets
                        if !plan.datasets.iter().any(|d| d.table == rule.left_table) {
                            eprintln!("DEBUG normalizer: Adding missing dataset: {}", rule.left_table);
                            plan.datasets.push(crate::llm::plan_generator::DatasetInfo {
                                table: rule.left_table.clone(),
                                filters: vec![],
                                time_constraints: None,
                            });
                        }
                        plan.joins.push(crate::llm::plan_generator::JoinInfo {
                            left_table: rule.right_table.clone(),
                            left_key: rule.right_key.first().cloned().unwrap_or_else(|| "id".to_string()),
                            right_table: rule.left_table.clone(),
                            right_key: rule.left_key.first().cloned().unwrap_or_else(|| "id".to_string()),
                            join_type: rule.join_type.clone(),
                            justification: format!("Auto-added to connect referenced table {}", missing_table),
                        });
                        joined_tables.insert(missing_table.clone());
                        break;
                    }
                }
                
                // If no join rule found, try to infer from hypergraph edges
                if !found_join {
                    eprintln!("DEBUG normalizer: No join rule found, trying hypergraph edges for: {}", missing_table);
                    for edge in &metadata_pack.hypergraph_edges {
                        let can_join_from = joined_tables.contains(&edge.from_table) && edge.to_table == *missing_table;
                        let can_join_to = joined_tables.contains(&edge.to_table) && edge.from_table == *missing_table;
                        
                        if can_join_from {
                            eprintln!("DEBUG normalizer: Adding join from hypergraph edge: {} → {} (via {})", 
                                edge.from_table, edge.to_table, edge.via_column);
                            // Add missing dataset
                            if !plan.datasets.iter().any(|d| d.table == edge.to_table) {
                                plan.datasets.push(crate::llm::plan_generator::DatasetInfo {
                                    table: edge.to_table.clone(),
                                    filters: vec![],
                                    time_constraints: None,
                                });
                            }
                            // Add join
                            plan.joins.push(crate::llm::plan_generator::JoinInfo {
                                left_table: edge.from_table.clone(),
                                left_key: edge.via_column.clone(),
                                right_table: edge.to_table.clone(),
                                right_key: "id".to_string(), // Assume right side uses id
                                join_type: "inner".to_string(),
                                justification: format!("Auto-added from hypergraph edge to connect {}", missing_table),
                            });
                            joined_tables.insert(missing_table.clone());
                            found_join = true;
                            break;
                        } else if can_join_to {
                            eprintln!("DEBUG normalizer: Adding join from hypergraph edge: {} → {} (via {})", 
                                edge.to_table, edge.from_table, edge.via_column);
                            // Add missing dataset
                            if !plan.datasets.iter().any(|d| d.table == edge.from_table) {
                                plan.datasets.push(crate::llm::plan_generator::DatasetInfo {
                                    table: edge.from_table.clone(),
                                    filters: vec![],
                                    time_constraints: None,
                                });
                            }
                            // Add join
                            plan.joins.push(crate::llm::plan_generator::JoinInfo {
                                left_table: edge.to_table.clone(),
                                left_key: edge.via_column.clone(),
                                right_table: edge.from_table.clone(),
                                right_key: "id".to_string(),
                                join_type: "inner".to_string(),
                                justification: format!("Auto-added from hypergraph edge to connect {}", missing_table),
                            });
                            joined_tables.insert(missing_table.clone());
                            found_join = true;
                            break;
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Remove datasets (base tables) that aren't referenced and aren't needed for joins
    fn remove_unnecessary_datasets(
        &self,
        plan: &mut StructuredPlan,
        _schema: &SchemaIndex,
        agg_aliases: &HashSet<String>,
    ) -> Result<()> {
        // First, collect all tables that are actually referenced in columns (not just in datasets)
        let mut referenced_tables = HashSet::new();
        
        // Tables from projections (excluding aggregation aliases)
        for proj in &plan.projections {
            if agg_aliases.contains(proj) {
                continue;
            }
            if let Some((table, _)) = split_qualified(proj) {
                referenced_tables.insert(table.to_string());
            }
        }
        
        // Tables from group_by
        for col in &plan.group_by {
            if agg_aliases.contains(col) {
                continue;
            }
            if let Some((table, _)) = split_qualified(col) {
                referenced_tables.insert(table.to_string());
            }
        }
        
        // Tables from aggregations (e.g., "SUM(orders.total_amount)" -> "orders")
        for agg in &plan.aggregations {
            // Extract table.column patterns even inside function calls
            let expr = &agg.expr;
            let parts: Vec<&str> = expr.split(|c: char| !c.is_alphanumeric() && c != '_' && c != '.').collect();
            for part in parts {
                if let Some((table, col)) = part.split_once('.') {
                    if !table.is_empty() && !col.is_empty() {
                        referenced_tables.insert(table.to_string());
                    }
                }
            }
        }
        
        // Tables from filters
        for dataset in &plan.datasets {
            for filter in &dataset.filters {
                for word in filter.split_whitespace() {
                    if let Some((table, _)) = word.split_once('.') {
                        referenced_tables.insert(table.to_string());
                    }
                }
            }
        }
        
        // Tables from order_by
        if let Some(ref order_by) = plan.order_by {
            for o in order_by {
                if agg_aliases.contains(&o.column) {
                    continue;
                }
                if let Some((table, _)) = split_qualified(&o.column) {
                    referenced_tables.insert(table.to_string());
                }
            }
        }
        
        // Tables from joins (both sides are needed for the join to work)
        for join in &plan.joins {
            referenced_tables.insert(join.left_table.clone());
            referenced_tables.insert(join.right_table.clone());
        }
        
        // Keep only datasets that are actually referenced in columns OR needed for joins
        let initial_dataset_count = plan.datasets.len();
        plan.datasets.retain(|dataset| {
            let keep = referenced_tables.contains(&dataset.table);
            if !keep {
                eprintln!("DEBUG normalizer: Removing unnecessary dataset: {} (not referenced in any columns and not needed for joins)", 
                    dataset.table);
            }
            keep
        });
        if plan.datasets.len() < initial_dataset_count {
            eprintln!("DEBUG normalizer: Removed {} unnecessary dataset(s). Remaining: {:?}", 
                initial_dataset_count - plan.datasets.len(),
                plan.datasets.iter().map(|d| d.table.clone()).collect::<Vec<_>>());
        }
        
        Ok(())
    }

    /// Remove joins where neither table is referenced in projections, group_by, filters, or order_by
    fn remove_unnecessary_joins(
        &self,
        plan: &mut StructuredPlan,
        schema: &SchemaIndex,
        agg_aliases: &HashSet<String>,
    ) -> Result<()> {
        // Collect all tables that are actually referenced
        let mut referenced_tables = HashSet::new();
        
        // Tables from datasets (base tables)
        for dataset in &plan.datasets {
            referenced_tables.insert(dataset.table.clone());
        }
        
        // Tables from projections
        for proj in &plan.projections {
            if agg_aliases.contains(proj) {
                continue; // Skip aggregation aliases
            }
            // Try split_qualified first
            if let Some((table, _)) = split_qualified(proj) {
                referenced_tables.insert(table.to_string());
            } else {
                // Fallback: try direct split_once for table.column patterns
                if let Some((table, _)) = proj.split_once('.') {
                    referenced_tables.insert(table.to_string());
                }
            }
        }
        
        // Tables from group_by
        for col in &plan.group_by {
            if agg_aliases.contains(col) {
                continue;
            }
            if let Some((table, _)) = split_qualified(col) {
                referenced_tables.insert(table.to_string());
            }
        }
        
        // Tables from filters
        for dataset in &plan.datasets {
            for filter in &dataset.filters {
                // Simple heuristic: extract table names from filter expressions
                // Look for patterns like "table.column" or just "column" (assume from dataset.table)
                for word in filter.split_whitespace() {
                    if let Some((table, _)) = word.split_once('.') {
                        referenced_tables.insert(table.to_string());
                    }
                }
            }
        }
        
        // Tables from order_by
        if let Some(ref order_by) = plan.order_by {
            for o in order_by {
                if agg_aliases.contains(&o.column) {
                    continue;
                }
                if let Some((table, _)) = split_qualified(&o.column) {
                    referenced_tables.insert(table.to_string());
                }
            }
        }
        
        // Tables from aggregations (e.g., "SUM(orders.total_amount)" -> "orders")
        for agg in &plan.aggregations {
            // Extract table.column patterns even inside function calls
            let expr = &agg.expr;
            let parts: Vec<&str> = expr.split(|c: char| !c.is_alphanumeric() && c != '_' && c != '.').collect();
            for part in parts {
                if let Some((table, col)) = part.split_once('.') {
                    if !table.is_empty() && !col.is_empty() {
                        referenced_tables.insert(table.to_string());
                    }
                }
            }
        }
        
        // MVP FIX: Keep joins if EITHER table is referenced (both sides needed for join to work)
        // This ensures joins added by ensure_required_joins are not removed
        let initial_join_count = plan.joins.len();
        plan.joins.retain(|join| {
            let left_referenced = referenced_tables.contains(&join.left_table);
            let right_referenced = referenced_tables.contains(&join.right_table);
            // Keep if EITHER table is referenced (both sides are needed for the join)
            let keep = left_referenced || right_referenced;
            if !keep {
                eprintln!("DEBUG normalizer: Removing unnecessary join: {} JOIN {} (left_referenced: {}, right_referenced: {})", 
                    join.left_table, join.right_table, left_referenced, right_referenced);
            } else {
                eprintln!("DEBUG normalizer: Keeping join: {} JOIN {} (left_referenced: {}, right_referenced: {})", 
                    join.left_table, join.right_table, left_referenced, right_referenced);
            }
            keep
        });
        if plan.joins.len() < initial_join_count {
            eprintln!("DEBUG normalizer: Removed {} unnecessary join(s). Remaining: {:?}", 
                initial_join_count - plan.joins.len(),
                plan.joins.iter().map(|j| format!("{}→{}", j.left_table, j.right_table)).collect::<Vec<_>>());
        }
        
        Ok(())
    }

    fn normalize_join_key(
        &self,
        schema: &SchemaIndex,
        covered_tables: &HashSet<String>,
        table: &str,
        key: &mut String,
    ) -> Result<()> {
        if !covered_tables.contains(table) {
            // If the plan references a table not in covered_tables, let validator deal with it.
            return Ok(());
        }

        if schema.table_has_column(table, key) {
            return Ok(());
        }

        // Rewrite only when the key is a common alias and the rewrite is unambiguous.
        // Example: orders.order_id is wrong if orders has id and doesn't have order_id.
        if key.ends_with("_id") && schema.table_has_column(table, "id") {
            *key = "id".to_string();
            return Ok(());
        }

        Err(anyhow::anyhow!(
            "Join key '{}' not found in table '{}'. Available columns: {:?}",
            key,
            table,
            schema.columns_for_table(table)
        ))
    }

    fn normalize_column_ref(
        &self,
        schema: &SchemaIndex,
        covered_tables: &HashSet<String>,
        agg_aliases: &HashSet<String>,
        col: &mut String,
    ) -> Result<()> {
        // Aggregation alias? Always allow.
        if agg_aliases.contains(col) {
            return Ok(());
        }

        if let Some((table, column)) = split_qualified(col) {
            // If user/LLM qualifies an aggregation alias (e.g. orders.total_sales), strip the qualifier.
            if agg_aliases.contains(column) {
                *col = column.to_string();
                return Ok(());
            }

            // Qualified reference: enforce table coverage and try safe rewrite.
            if !covered_tables.contains(table) {
                return Err(anyhow::anyhow!(
                    "Column '{}' references table '{}' which is not covered by datasets/joins",
                    col,
                    table
                ));
            }

            if schema.table_has_column(table, column) {
                return Ok(());
            }

            // Safe rewrite: <table>.<something_id> -> <table>.id when table has id and lacks the alias column.
            if column.ends_with("_id") && schema.table_has_column(table, "id") {
                *col = format!("{}.id", table);
                return Ok(());
            }

            return Err(anyhow::anyhow!(
                "Column '{}' not found. Table '{}' columns: {:?}",
                col,
                table,
                schema.columns_for_table(table)
            ));
        }

        // Unqualified column: deterministically map to a single table among covered tables.
        let candidates = schema.tables_with_column_in(col, covered_tables);
        match candidates.len() {
            1 => {
                let t = candidates.iter().next().unwrap();
                *col = format!("{}.{}", t, col);
                Ok(())
            }
            0 => {
                // Optional unqualified alias rewrite: <entity>_id -> <entity_plural>.id ONLY if unambiguous.
                if col.ends_with("_id") {
                    let entity = col.trim_end_matches("_id");
                    let table_guess = pluralize_simple(entity);
                    if covered_tables.contains(&table_guess)
                        && schema.table_has_column(&table_guess, "id")
                        && !schema.table_has_column(&table_guess, col)
                    {
                        *col = format!("{}.id", table_guess);
                        return Ok(());
                    }
                }

                Err(anyhow::anyhow!(
                    "Column '{}' not found in covered tables {:?}",
                    col,
                    covered_tables
                ))
            }
            _ => Err(anyhow::anyhow!(
                "Column '{}' is ambiguous across tables {:?}. Please qualify it (e.g. table.{})",
                col,
                candidates,
                col
            )),
        }
    }
}

struct JoinRuleIndex {
    // (left_table, right_table) -> (left_key, right_key)
    rules: HashMap<(String, String), (String, String)>,
}

impl JoinRuleIndex {
    fn from_metadata_pack(pack: &MetadataPack) -> Self {
        let mut rules = HashMap::new();
        for r in &pack.join_rules {
            if let (Some(lk), Some(rk)) = (r.left_key.first(), r.right_key.first()) {
                rules.insert(
                    (r.left_table.clone(), r.right_table.clone()),
                    (lk.clone(), rk.clone()),
                );
            }
        }
        Self { rules }
    }

    fn apply_to_join(&self, join: &mut crate::llm::plan_generator::JoinInfo) {
        // Exact direction match
        if let Some((lk, rk)) = self
            .rules
            .get(&(join.left_table.clone(), join.right_table.clone()))
        {
            join.left_key = lk.clone();
            join.right_key = rk.clone();
            return;
        }

        // Reverse direction match
        if let Some((lk, rk)) = self
            .rules
            .get(&(join.right_table.clone(), join.left_table.clone()))
        {
            // Swap keys to match join direction
            join.left_key = rk.clone();
            join.right_key = lk.clone();
        }
    }
}

struct SchemaIndex {
    // table -> columns
    columns_by_table: HashMap<String, HashSet<String>>,
}

impl SchemaIndex {
    fn from_metadata_pack(pack: &MetadataPack) -> Self {
        let mut columns_by_table: HashMap<String, HashSet<String>> = HashMap::new();
        for t in &pack.tables {
            let cols: HashSet<String> = t.columns.iter().map(|c| c.name.clone()).collect();
            columns_by_table.insert(t.name.clone(), cols);
        }
        Self { columns_by_table }
    }

    fn table_has_column(&self, table: &str, column: &str) -> bool {
        self.columns_by_table
            .get(table)
            .map(|cols| cols.contains(column))
            .unwrap_or(false)
    }

    fn columns_for_table(&self, table: &str) -> Vec<String> {
        let mut cols: Vec<String> = self
            .columns_by_table
            .get(table)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();
        cols.sort();
        cols
    }

    fn tables_with_column_in(&self, column: &str, covered_tables: &HashSet<String>) -> HashSet<String> {
        let mut out = HashSet::new();
        for t in covered_tables {
            if self.table_has_column(t, column) {
                out.insert(t.clone());
            }
        }
        out
    }
}

fn covered_tables(plan: &StructuredPlan) -> HashSet<String> {
    let mut tables: HashSet<String> = HashSet::new();
    for d in &plan.datasets {
        tables.insert(d.table.clone());
    }
    for j in &plan.joins {
        tables.insert(j.left_table.clone());
        tables.insert(j.right_table.clone());
    }
    tables
}

fn split_qualified(s: &str) -> Option<(&str, &str)> {
    s.split_once('.')
}

fn pluralize_simple(s: &str) -> String {
    // Very small heuristic: order -> orders, category -> categorys (not perfect, but only used as last-resort rewrite)
    // We prefer rejecting ambiguity over guessing.
    if s.ends_with('s') {
        s.to_string()
    } else {
        format!("{}s", s)
    }
}


