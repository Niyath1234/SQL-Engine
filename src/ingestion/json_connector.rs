//! JSON Connector - Accepts raw JSON payloads for ingestion

use crate::ingestion::connector::{IngestionConnector, ConnectorResult, Checkpoint};
use serde_json::Value;
use anyhow::Result;

/// JSON Connector - Wraps provided JSON payloads
pub struct JsonConnector {
    source_id: String,
    payloads: Vec<Value>,
    consumed: bool,
}

impl JsonConnector {
    pub fn new(source_id: String, payloads: Vec<Value>) -> Self {
        Self {
            source_id,
            payloads,
            consumed: false,
        }
    }
}

impl IngestionConnector for JsonConnector {
    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn source_type(&self) -> &str {
        "json"
    }

    fn source_uri(&self) -> Option<&str> {
        None
    }

    fn fetch(&mut self, _checkpoint: Option<Checkpoint>) -> Result<ConnectorResult> {
        if self.consumed {
            return Ok(ConnectorResult {
                payloads: vec![],
                checkpoint: Checkpoint::new("done".to_string()),
                has_more: false,
            });
        }

        self.consumed = true;
        Ok(ConnectorResult {
            payloads: self.payloads.clone(),
            checkpoint: Checkpoint::new("done".to_string()),
            has_more: false,
        })
    }

}

