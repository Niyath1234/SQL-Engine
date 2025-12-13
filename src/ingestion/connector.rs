//! Ingestion Connector Trait - Abstract interface for data sources

use serde::{Deserialize, Serialize};
use serde_json::Value;
use anyhow::Result;

/// Checkpoint for resumable ingestion
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Source-specific cursor/state
    pub cursor: String,
    
    /// Timestamp of last successful fetch
    pub last_fetch_at: u64,
    
    /// Number of records fetched so far
    pub records_fetched: u64,
}

impl Checkpoint {
    pub fn new(cursor: String) -> Self {
        Self {
            cursor,
            last_fetch_at: Self::now_timestamp(),
            records_fetched: 0,
        }
    }
    
    pub fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

/// Result from connector fetch
#[derive(Clone, Debug)]
pub struct ConnectorResult {
    /// Payloads (JSON records)
    pub payloads: Vec<Value>,
    
    /// Updated checkpoint (for next fetch)
    pub checkpoint: Checkpoint,
    
    /// Has more data (false if this was the last batch)
    pub has_more: bool,
}

/// Ingestion Connector Trait
/// 
/// Implementations:
/// - SimulatorConnector: Artificial data for development
/// - ApiConnector: Real API endpoints (future)
/// - FileConnector: File-based ingestion (future)
pub trait IngestionConnector: Send + Sync {
    /// Fetch next batch of data
    fn fetch(&mut self, checkpoint: Option<Checkpoint>) -> Result<ConnectorResult>;
    
    /// Get source ID (unique identifier)
    fn source_id(&self) -> &str;
    
    /// Get source type (e.g., "simulator", "api", "file")
    fn source_type(&self) -> &str;
    
    /// Get source URI/endpoint (if applicable)
    fn source_uri(&self) -> Option<&str>;
}

