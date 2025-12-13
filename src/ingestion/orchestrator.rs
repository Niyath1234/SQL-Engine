//! Ingestion Orchestrator - Main ingestion pipeline

use crate::ingestion::{
    IngestionConnector, ConnectorResult, IngestionResult, IngestionStatus,
    SchemaInference, InferredSchema, SchemaEvolution,
    TableBuilder,
    IngestionLineage,
};
use crate::worldstate::stats_collector::StatsCollector;
use crate::engine::HypergraphSQLEngine;
use crate::worldstate::schema::TableSchema;
use anyhow::{Result, Context};
use serde_json::Value;
use uuid::Uuid;

/// Ingestion Orchestrator - Coordinates the entire ingestion pipeline
pub struct IngestionOrchestrator {
    schema_inference: SchemaInference,
    table_builder: TableBuilder,
    stats_collector: StatsCollector,
}

impl IngestionOrchestrator {
    pub fn new() -> Self {
        Self {
            schema_inference: SchemaInference::new(),
            table_builder: TableBuilder::new(),
            stats_collector: StatsCollector::new(),
        }
    }
    
    /// Ingest data from a connector
    pub fn ingest(
        &self,
        engine: &mut HypergraphSQLEngine,
        mut connector: Box<dyn IngestionConnector>,
        table_name: Option<String>,
    ) -> Result<IngestionResult> {
        let run_id = Uuid::new_v4().to_string();
        let source_id = connector.source_id().to_string();
        
        // Register source if not already registered
        {
            let world_state = engine.world_state();
            if world_state.lineage_registry.get_source(&source_id).is_none() {
                drop(world_state);
                let mut ws = engine.world_state_mut();
                IngestionLineage::register_source(
                    &mut ws,
                    source_id.clone(),
                    connector.source_type().to_string(),
                    connector.source_uri().map(|s| s.to_string()),
                );
                ws.bump_version();
            } else {
                drop(world_state);
            }
        }
        
        // Start ingestion run
        let run = {
            let mut ws = engine.world_state_mut();
            IngestionLineage::start_run(&mut ws, &source_id, run_id.clone())
        };
        
        // Determine table name
        let table_name = table_name.unwrap_or_else(|| format!("table_{}", source_id));
        
        let mut all_payloads = Vec::new();
        let mut checkpoint = None;
        let mut has_more = true;
        
        // Fetch all batches
        while has_more {
            let result = connector.fetch(checkpoint.clone())
                .context("Failed to fetch from connector")?;
            
            all_payloads.extend(result.payloads);
            checkpoint = Some(result.checkpoint);
            has_more = result.has_more;
        }
        
        if all_payloads.is_empty() {
            return Ok(IngestionResult {
                records_ingested: 0,
                tables_affected: Vec::new(),
                schema_versions: Default::default(),
                child_tables_created: Vec::new(),
                run_id: run_id.clone(),
                status: IngestionStatus::Success,
                error: None,
            });
        }
        
        // Infer schema
        eprintln!("DEBUG orchestrator: Inferring schema for table '{}'", table_name);
        let inferred_schema = self.schema_inference.infer_schema(&table_name, &all_payloads);
        eprintln!("DEBUG orchestrator: Inferred {} columns for '{}'", inferred_schema.columns.len(), table_name);
        
        // Check if table exists in WorldState
        let existing_schema = {
            let world_state = engine.world_state();
            world_state.schema_registry.get_table(&table_name).cloned()
        };
        
        // Check if table exists in hypergraph (the actual storage)
        // Collect table names first to avoid lifetime issues
        let table_name_lower = table_name.to_lowercase();
        let hypergraph_table_names: Vec<String> = {
            let graph = engine.graph();
            graph.iter_nodes()
                .filter_map(|(_, node)| {
                    if matches!(node.node_type, crate::hypergraph::node::NodeType::Table) {
                        node.table_name.clone()
                    } else {
                        None
                    }
                })
                .collect()
        };
        let table_exists_in_hypergraph = hypergraph_table_names.iter()
            .any(|name| name.to_lowercase() == table_name_lower);
        
        eprintln!("DEBUG orchestrator: Table '{}' - WorldState: {}, Hypergraph: {}", 
            table_name, 
            if existing_schema.is_some() { "EXISTS" } else { "NONE" },
            if table_exists_in_hypergraph { "EXISTS" } else { "NONE" }
        );
        
        // If table exists in WorldState but NOT in hypergraph, we need to create it
        let should_create_table = existing_schema.is_none() || !table_exists_in_hypergraph;
        
        let evolution = self.schema_inference.compare_schema(&inferred_schema, existing_schema.as_ref());
        eprintln!("DEBUG orchestrator: Schema evolution for '{}': {:?}, should_create: {}", table_name, evolution, should_create_table);
        
        // Create or update table
        let mut tables_affected = vec![table_name.clone()];
        let mut schema_versions = std::collections::HashMap::new();
        let mut child_tables_created = Vec::new();
        
        eprintln!("DEBUG orchestrator: Matching evolution for '{}'", table_name);
        match evolution {
            SchemaEvolution::NoChange if should_create_table => {
                // New table - create it
                eprintln!("DEBUG orchestrator: Creating new table '{}'", table_name);
                match self.table_builder.create_table_from_schema(engine, &inferred_schema, &all_payloads) {
                    Ok(_) => {
                        eprintln!("DEBUG orchestrator: Successfully called create_table_from_schema for '{}'", table_name);
                    }
                    Err(e) => {
                        eprintln!("ERROR orchestrator: Failed to create table '{}': {}", table_name, e);
                        return Err(e).context("Failed to create table");
                    }
                }
                
                let version = {
                    let world_state = engine.world_state();
                    world_state.schema_registry.get_table(&table_name)
                        .map(|s| s.version)
                        .unwrap_or(1)
                };
                schema_versions.insert(table_name.clone(), version);
            }
            SchemaEvolution::AddColumns { new_version, .. } => {
                // Schema evolution - add columns
                self.table_builder.apply_evolution(engine, &table_name, &evolution)
                    .context("Failed to apply schema evolution")?;
                schema_versions.insert(table_name.clone(), new_version);
            }
            SchemaEvolution::NoChange => {
                // Table exists in both WorldState and hypergraph, no schema changes
                if table_exists_in_hypergraph {
                    eprintln!("DEBUG orchestrator: Table '{}' exists in both WorldState and hypergraph - skipping creation", table_name);
                    let version = existing_schema.map(|s| s.version).unwrap_or(1);
                    schema_versions.insert(table_name.clone(), version);
                } else {
                    // Table exists in WorldState but NOT in hypergraph - create it!
                    eprintln!("DEBUG orchestrator: Table '{}' exists in WorldState but NOT in hypergraph - creating it", table_name);
                    self.table_builder.create_table_from_schema(engine, &inferred_schema, &all_payloads)
                        .context("Failed to create table in hypergraph (table exists in WorldState but not hypergraph)")?;
                    let version = existing_schema.map(|s| s.version).unwrap_or(1);
                    schema_versions.insert(table_name.clone(), version);
                }
            }
            SchemaEvolution::BreakingChange { reason, .. } => {
                return Err(anyhow::anyhow!("Breaking schema change: {}", reason));
            }
        }
        
        // Create child tables for arrays
        for child_table in &inferred_schema.child_tables {
            let _child_schema = InferredSchema {
                table_name: child_table.table_name.clone(),
                columns: child_table.columns.clone(),
                child_tables: Vec::new(),
            };
            
            // Extract child data from payloads
            let child_data: Vec<Value> = all_payloads.iter()
                .filter_map(|payload| {
                    // Find array in payload and extract items
                    // This is simplified - in production, would need proper path resolution
                    if let Some(arr) = payload.get("items").and_then(|v| v.as_array()) {
                        Some(arr.clone())
                    } else {
                        None
                    }
                })
                .flatten()
                .map(|v| v.clone())
                .collect();
            
            if !child_data.is_empty() {
                let child_inferred = self.schema_inference.infer_schema(&child_table.table_name, &child_data);
                self.table_builder.create_table_from_schema(engine, &child_inferred, &child_data)
                    .context("Failed to create child table")?;
                child_tables_created.push(child_table.table_name.clone());
                tables_affected.push(child_table.table_name.clone());
            }
        }
        
        // Insert data into tables
        // Note: Table should exist after create_table_from_schema, but we verify anyway
        // The table is created via execute_query which calls execute_create_table which calls load_table
        // So the table should be in the hypergraph. If not, there's a bug in table creation.
        // For now, we'll proceed with insertion and let execute_insert handle the error if table doesn't exist.
        
        eprintln!("DEBUG orchestrator: Inserting {} records into table '{}'", all_payloads.len(), table_name);
        self.insert_data(engine, &table_name, &all_payloads)
            .with_context(|| format!("Failed to insert data into table '{}'", table_name))?;
        eprintln!("DEBUG orchestrator: Successfully inserted {} records into table '{}'", all_payloads.len(), table_name);
        
        // PHASE 4: Collect statistics after ingestion
        {
            let mut world_state = engine.world_state_mut();
            let stats = self.stats_collector.collect_table_stats(
                &table_name,
                &world_state.schema_registry,
                all_payloads.len() as u64,
                None, // No sample data for now
            );
            world_state.stats_registry.register_table_stats(stats);
            world_state.bump_version();
        }
        
        // Update lineage
        {
            let mut ws = engine.world_state_mut();
            let version = schema_versions.get(&table_name).copied().unwrap_or(1);
            IngestionLineage::register_table_lineage(
                &mut ws,
                table_name.clone(),
                &source_id,
                version,
                run_id.clone(),
            );
            IngestionLineage::complete_run(&mut ws, &run_id, all_payloads.len() as u64)
                .context("Failed to complete run")?;
            ws.bump_version();
        }
        
        // Save WorldState
        engine.save_world_state()
            .context("Failed to save WorldState")?;
        
        Ok(IngestionResult {
            records_ingested: all_payloads.len() as u64,
            tables_affected,
            schema_versions,
            child_tables_created,
            run_id,
            status: IngestionStatus::Success,
            error: None,
        })
    }
    
    /// Insert data into a table
    fn insert_data(
        &self,
        engine: &mut HypergraphSQLEngine,
        table_name: &str,
        payloads: &[Value],
    ) -> Result<()> {
        if payloads.is_empty() {
            return Ok(());
        }
        
        // Get table schema to know columns
        let columns: Vec<String> = {
            let world_state = engine.world_state();
            world_state.schema_registry.get_table(table_name)
                .map(|s| s.columns.iter().map(|c| c.name.clone()).collect())
                .unwrap_or_else(|| {
                    // Fallback: infer from first payload
                    self.extract_column_names(&payloads[0])
                })
        };
        
        // Build INSERT statements (batch insert)
        // For simplicity, we'll do one INSERT per record
        // In production, would batch multiple VALUES
        for (idx, payload) in payloads.iter().enumerate() {
            let values: Vec<String> = columns.iter()
                .map(|col| {
                    self.extract_value(payload, col)
                        .unwrap_or_else(|| {
                            eprintln!("WARNING: Column '{}' not found in payload {} for table '{}', using NULL", col, idx, table_name);
                            "NULL".to_string()
                        })
                })
                .collect();
            
            // Verify we have the same number of values as columns
            if values.len() != columns.len() {
                return Err(anyhow::anyhow!(
                    "Mismatch: {} columns but {} values for table '{}' at record {}",
                    columns.len(), values.len(), table_name, idx
                ));
            }
            
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table_name,
                columns.join(", "),
                values.join(", ")
            );
            
            // Add detailed error context
            if let Err(e) = engine.execute_query(&insert_sql) {
                eprintln!("ERROR inserting record {} into {}: {}", idx, table_name, e);
                eprintln!("  SQL: {}", insert_sql);
                eprintln!("  Columns: {:?}", columns);
                eprintln!("  Values: {:?}", values);
                return Err(e).with_context(|| format!(
                    "Failed to insert record {} into {}. SQL: {}. Columns: {:?}. Values: {:?}",
                    idx, table_name, insert_sql, columns, values
                ));
            }
        }
        eprintln!("DEBUG insert_data: Successfully inserted {} records into '{}'", payloads.len(), table_name);
        
        Ok(())
    }
    
    /// Extract column names from a JSON payload
    fn extract_column_names(&self, payload: &Value) -> Vec<String> {
        match payload {
            Value::Object(obj) => {
                obj.keys().cloned().collect()
            }
            _ => Vec::new(),
        }
    }
    
    /// Extract value for a column from JSON payload
    fn extract_value(&self, payload: &Value, column: &str) -> Option<String> {
        // Handle nested columns (with __ separator)
        let parts: Vec<&str> = column.split("__").collect();
        let mut current = payload;
        
        for part in parts {
            current = current.get(part)?;
        }
        
        match current {
            Value::Null => Some("NULL".to_string()),
            Value::Bool(b) => Some(if *b { "TRUE".to_string() } else { "FALSE".to_string() }),
            Value::Number(n) => Some(n.to_string()),
            Value::String(s) => Some(format!("'{}'", s.replace("'", "''"))),
            _ => None,
        }
    }
}

impl Default for IngestionOrchestrator {
    fn default() -> Self {
        Self::new()
    }
}

