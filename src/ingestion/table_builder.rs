//! Table Builder - Creates/updates tables from inferred schemas

use crate::worldstate::schema::{TableSchema, ColumnInfo, SchemaRegistry};
use crate::worldstate::keys::{KeyRegistry, TableKeys, PrimaryKey, DedupeStrategy};
use crate::ingestion::schema_inference::{InferredSchema, InferredColumn, SchemaEvolution};
use crate::engine::HypergraphSQLEngine;
use anyhow::{Result, Context};
use serde_json::Value;

/// Table Builder - Converts inferred schemas to actual tables
pub struct TableBuilder {
    /// Auto-infer primary keys
    pub auto_infer_keys: bool,
    
    /// Default dedupe strategy
    pub default_dedupe: DedupeStrategy,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            auto_infer_keys: true,
            default_dedupe: DedupeStrategy::AppendOnly,
        }
    }
    
    /// Build table schema from inferred schema
    pub fn build_table_schema(&self, inferred: &InferredSchema) -> TableSchema {
        let mut schema = TableSchema::new(inferred.table_name.clone());
        
        // Convert inferred columns to ColumnInfo
        for inferred_col in &inferred.columns {
            let col = ColumnInfo {
                name: inferred_col.name.clone(),
                data_type: inferred_col.data_type.clone(),
                nullable: inferred_col.nullable,
                semantic_tags: self.infer_semantic_tags(inferred_col),
                description: None,
            };
            schema.add_column(col);
        }
        
        // Add child table references
        for child_table in &inferred.child_tables {
            schema.child_tables.push(child_table.table_name.clone());
        }
        
        schema
    }
    
    /// Infer semantic tags from column name and samples
    fn infer_semantic_tags(&self, col: &InferredColumn) -> Vec<String> {
        let mut tags = Vec::new();
        
        let name_lower = col.name.to_lowercase();
        
        // Time-related tags
        if name_lower.contains("time") || name_lower.contains("date") || name_lower.contains("timestamp") {
            tags.push("time/event".to_string());
        }
        
        // ID-related tags
        if name_lower.ends_with("_id") || name_lower == "id" {
            tags.push("key/natural".to_string());
        }
        
        // Amount/money tags
        if name_lower.contains("amount") || name_lower.contains("price") || name_lower.contains("cost") {
            tags.push("fact/amount".to_string());
        }
        
        // User-related tags
        if name_lower.contains("user") || name_lower.contains("customer") {
            tags.push("dimension/user".to_string());
        }
        
        tags
    }
    
    /// Auto-infer primary key from schema
    pub fn infer_primary_key(&self, schema: &TableSchema) -> Option<PrimaryKey> {
        if !self.auto_infer_keys {
            return None;
        }
        
        // Look for common primary key patterns
        for col in &schema.columns {
            let name_lower = col.name.to_lowercase();
            
            // Exact match: "id"
            if name_lower == "id" {
                return Some(PrimaryKey {
                    columns: vec![col.name.clone()],
                    is_synthetic: false,
                });
            }
            
            // Pattern: "{table}_id"
            if name_lower == format!("{}_id", schema.table_name.to_lowercase()) {
                return Some(PrimaryKey {
                    columns: vec![col.name.clone()],
                    is_synthetic: false,
                });
            }
        }
        
        None
    }
    
    /// Create table in engine from inferred schema
    pub fn create_table_from_schema(
        &self,
        engine: &mut HypergraphSQLEngine,
        inferred: &InferredSchema,
        sample_data: &[Value],
    ) -> Result<()> {
        // Build table schema
        let table_schema = self.build_table_schema(inferred);
        
        // Create table in engine using DDL FIRST (before registering in WorldState)
        // This ensures the table exists in the hypergraph before we try to insert data
        let ddl = self.generate_ddl(&table_schema);
        eprintln!("DEBUG: Creating table {} with DDL: {}", inferred.table_name, ddl);
        let _result = engine.execute_query(&ddl)
            .with_context(|| format!("Failed to create table {} in hypergraph. DDL was: {}", inferred.table_name, ddl))?;
        eprintln!("DEBUG: Table {} created successfully", inferred.table_name);
        
        // Verify table was actually created in hypergraph
        {
            let graph = engine.graph();
            let table_name_lower = inferred.table_name.to_lowercase();
            let table_exists = graph.iter_nodes().any(|(_, node)| {
                matches!(node.node_type, crate::hypergraph::node::NodeType::Table)
                    && node.table_name.as_ref()
                        .map(|name| name.to_lowercase() == table_name_lower)
                        .unwrap_or(false)
            });
            
            if !table_exists {
                let available_tables: Vec<String> = graph.iter_nodes()
                    .filter_map(|(_, node)| {
                        if matches!(node.node_type, crate::hypergraph::node::NodeType::Table) {
                            node.table_name.clone()
                        } else {
                            None
                        }
                    })
                    .collect();
                eprintln!("WARNING: Table '{}' not found in hypergraph after creation. Available tables: {:?}", inferred.table_name, available_tables);
                // Don't fail here - let INSERT handle it, but log the warning
            } else {
                eprintln!("DEBUG: Verified table '{}' exists in hypergraph", inferred.table_name);
            }
        }
        
        // Register in WorldState AFTER table is created in hypergraph
        {
            let mut world_state = engine.world_state_mut();
            world_state.schema_registry.register_table(table_schema.clone());
            
            // Auto-infer primary key
            if let Some(pk) = self.infer_primary_key(&table_schema) {
                let mut table_keys = crate::worldstate::keys::TableKeys::default();
                table_keys.primary_key = Some(pk);
                table_keys.dedupe_strategy = self.default_dedupe.clone();
                world_state.key_registry.register_table_keys(
                    inferred.table_name.clone(),
                    table_keys,
                );
            }
            
            world_state.bump_version();
        }
        
        Ok(())
    }
    
    /// Generate DDL for table creation
    fn generate_ddl(&self, schema: &TableSchema) -> String {
        let mut ddl = format!("CREATE TABLE {} (", schema.table_name);
        
        let mut col_defs = Vec::new();
        for col in &schema.columns {
            let nullable = if col.nullable { "" } else { " NOT NULL" };
            col_defs.push(format!("{} {}{}", col.name, col.data_type, nullable));
        }
        
        ddl.push_str(&col_defs.join(", "));
        ddl.push_str(")");
        
        ddl
    }
    
    /// Apply schema evolution (ALTER TABLE ADD COLUMN)
    pub fn apply_evolution(
        &self,
        engine: &mut HypergraphSQLEngine,
        table_name: &str,
        evolution: &SchemaEvolution,
    ) -> Result<()> {
        match evolution {
            SchemaEvolution::NoChange => {
                // Nothing to do
            }
            SchemaEvolution::AddColumns { columns, new_version } => {
                // Update WorldState schema
                {
                    let mut world_state = engine.world_state_mut();
                    if let Some(table_schema) = world_state.schema_registry.get_table_mut(table_name) {
                        for col in columns {
                            table_schema.add_column(col.clone());
                        }
                    }
                    world_state.bump_version();
                }
                
                // Generate and execute ALTER TABLE statements
                for col in columns {
                    let nullable = if col.nullable { "" } else { " NOT NULL" };
                    let alter_ddl = format!(
                        "ALTER TABLE {} ADD COLUMN {} {}{}",
                        table_name, col.name, col.data_type, nullable
                    );
                    engine.execute_query(&alter_ddl)
                        .with_context(|| format!("Failed to add column {} to {}", col.name, table_name))?;
                }
            }
            SchemaEvolution::BreakingChange { reason, .. } => {
                return Err(anyhow::anyhow!(
                    "Breaking schema change detected for {}: {}",
                    table_name, reason
                ));
            }
        }
        
        Ok(())
    }
}

impl Default for TableBuilder {
    fn default() -> Self {
        Self::new()
    }
}

