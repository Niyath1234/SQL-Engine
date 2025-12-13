//! Ingestion Lineage - Track source → table → schema version

use crate::worldstate::lineage::{LineageRegistry, SourceInfo, IngestionRun, TableLineage};
use crate::worldstate::WorldState;
use anyhow::Result;
use std::time::{SystemTime, UNIX_EPOCH};

/// Ingestion Lineage Manager
pub struct IngestionLineage {
    /// Source ID
    source_id: String,
}

impl IngestionLineage {
    pub fn new(source_id: String) -> Self {
        Self { source_id }
    }
    
    /// Register a data source in WorldState
    pub fn register_source(
        world_state: &mut WorldState,
        source_id: String,
        source_type: String,
        uri: Option<String>,
    ) {
        let source = SourceInfo {
            source_id: source_id.clone(),
            source_type,
            uri,
            created_at: Self::now_timestamp(),
        };
        world_state.lineage_registry.register_source(source);
    }
    
    /// Start an ingestion run
    pub fn start_run(
        world_state: &mut WorldState,
        source_id: &str,
        run_id: String,
    ) -> IngestionRun {
        let run = IngestionRun {
            run_id: run_id.clone(),
            source_id: source_id.to_string(),
            started_at: Self::now_timestamp(),
            ended_at: None,
            records_ingested: 0,
            status: "running".to_string(),
            error: None,
        };
        world_state.lineage_registry.register_run(run.clone());
        run
    }
    
    /// Complete an ingestion run
    pub fn complete_run(
        world_state: &mut WorldState,
        run_id: &str,
        records_ingested: u64,
    ) -> Result<()> {
        if let Some(run) = world_state.lineage_registry.get_run(run_id) {
            let mut updated_run = run.clone();
            updated_run.ended_at = Some(Self::now_timestamp());
            updated_run.records_ingested = records_ingested;
            updated_run.status = "completed".to_string();
            world_state.lineage_registry.register_run(updated_run);
        }
        Ok(())
    }
    
    /// Register table lineage
    pub fn register_table_lineage(
        world_state: &mut WorldState,
        table_name: String,
        source_id: &str,
        schema_version: u64,
        run_id: String,
    ) {
        let existing = world_state.lineage_registry.get_table_lineage(&table_name);
        
        let lineage = if let Some(existing) = existing {
            let mut updated = existing.clone();
            updated.updated_at = Self::now_timestamp();
            updated.ingestion_runs.push(run_id);
            updated
        } else {
            TableLineage {
                table_name: table_name.clone(),
                source_id: source_id.to_string(),
                schema_version,
                ingestion_runs: vec![run_id],
                created_at: Self::now_timestamp(),
                updated_at: Self::now_timestamp(),
            }
        };
        
        world_state.lineage_registry.register_table_lineage(lineage);
    }
    
    fn now_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

