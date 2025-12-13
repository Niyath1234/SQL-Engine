//! Ingestion Module - Phase 2: "Blood starts flowing"
//! 
//! Handles data ingestion from APIs/simulators into the engine with:
//! - Schema inference
//! - Automatic table creation
//! - Schema evolution
//! - Lineage tracking
//! - Idempotency

pub mod connector;
pub mod schema_inference;
pub mod table_builder;
pub mod lineage;
pub mod simulator;
pub mod json_connector;
pub mod orchestrator;
pub mod join_inference;
pub mod join_validator;

pub use connector::{IngestionConnector, ConnectorResult, Checkpoint};
pub use schema_inference::{SchemaInference, InferredSchema, InferredColumn, SchemaEvolution};
pub use table_builder::TableBuilder;
pub use lineage::IngestionLineage;
pub use simulator::{SimulatorConnector, SimulatorSchema};
pub use json_connector::JsonConnector;
pub use orchestrator::IngestionOrchestrator;
pub use join_inference::{JoinInference, JoinProposal};
pub use join_validator::{JoinValidator, JoinValidationResult};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Ingestion result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IngestionResult {
    /// Number of records ingested
    pub records_ingested: u64,
    
    /// Tables created/updated
    pub tables_affected: Vec<String>,
    
    /// Schema versions created
    pub schema_versions: HashMap<String, u64>,
    
    /// Child tables created (for array columns)
    pub child_tables_created: Vec<String>,
    
    /// Ingestion run ID
    pub run_id: String,
    
    /// Status
    pub status: IngestionStatus,
    
    /// Error message (if failed)
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum IngestionStatus {
    Success,
    Partial,
    Failed,
}

