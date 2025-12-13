//! Lineage Registry - Source → Ingestion → Table → Schema Version

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Data source information
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceInfo {
    /// Source ID (unique identifier)
    pub source_id: String,
    
    /// Source type (e.g., "api", "simulator", "file")
    pub source_type: String,
    
    /// Source URI/endpoint
    pub uri: Option<String>,
    
    /// Created timestamp
    pub created_at: u64,
}

impl Hash for SourceInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.source_id.hash(state);
        self.source_type.hash(state);
    }
}

/// Ingestion run information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IngestionRun {
    /// Run ID (unique identifier)
    pub run_id: String,
    
    /// Source ID
    pub source_id: String,
    
    /// Start timestamp
    pub started_at: u64,
    
    /// End timestamp (None if still running)
    pub ended_at: Option<u64>,
    
    /// Number of records ingested
    pub records_ingested: u64,
    
    /// Status (running, completed, failed)
    pub status: String,
    
    /// Error message (if failed)
    pub error: Option<String>,
}

/// Table lineage information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableLineage {
    /// Table name
    pub table_name: String,
    
    /// Source ID that produced this table
    pub source_id: String,
    
    /// Schema version when table was created
    pub schema_version: u64,
    
    /// Ingestion runs that affected this table
    pub ingestion_runs: Vec<String>, // Run IDs
    
    /// Created timestamp
    pub created_at: u64,
    
    /// Last updated timestamp
    pub updated_at: u64,
}

impl Hash for TableLineage {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.source_id.hash(state);
        self.schema_version.hash(state);
    }
}

/// Lineage Registry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LineageRegistry {
    /// Source ID → SourceInfo
    sources: HashMap<String, SourceInfo>,
    
    /// Run ID → IngestionRun
    runs: HashMap<String, IngestionRun>,
    
    /// Table name → TableLineage
    table_lineage: HashMap<String, TableLineage>,
}

impl Hash for LineageRegistry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash sources
        let mut source_ids: Vec<_> = self.sources.keys().collect();
        source_ids.sort();
        for id in source_ids {
            if let Some(source) = self.sources.get(id) {
                source.hash(state);
            }
        }
        
        // Hash table lineage
        let mut table_names: Vec<_> = self.table_lineage.keys().collect();
        table_names.sort();
        for name in table_names {
            if let Some(lineage) = self.table_lineage.get(name) {
                lineage.hash(state);
            }
        }
    }
}

impl LineageRegistry {
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
            runs: HashMap::new(),
            table_lineage: HashMap::new(),
        }
    }
    
    /// Register a data source
    pub fn register_source(&mut self, source: SourceInfo) {
        self.sources.insert(source.source_id.clone(), source);
    }
    
    /// Register an ingestion run
    pub fn register_run(&mut self, run: IngestionRun) {
        self.runs.insert(run.run_id.clone(), run);
    }
    
    /// Register table lineage
    pub fn register_table_lineage(&mut self, lineage: TableLineage) {
        self.table_lineage.insert(lineage.table_name.clone(), lineage);
    }
    
    /// Get source info
    pub fn get_source(&self, source_id: &str) -> Option<&SourceInfo> {
        self.sources.get(source_id)
    }
    
    /// Get ingestion run
    pub fn get_run(&self, run_id: &str) -> Option<&IngestionRun> {
        self.runs.get(run_id)
    }
    
    /// Get table lineage
    pub fn get_table_lineage(&self, table_name: &str) -> Option<&TableLineage> {
        self.table_lineage.get(table_name)
    }
    
    /// List all sources
    pub fn list_sources(&self) -> Vec<&SourceInfo> {
        self.sources.values().collect()
    }
    
    /// List all tables with lineage
    pub fn list_tables(&self) -> Vec<String> {
        self.table_lineage.keys().cloned().collect()
    }
}

impl Default for LineageRegistry {
    fn default() -> Self {
        Self::new()
    }
}

