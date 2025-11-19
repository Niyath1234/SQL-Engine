use crate::storage::fragment::Value;
use serde::{Deserialize, Serialize};

/// Delta operation - represents a change to data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DeltaOperation {
    Insert {
        table: String,
        row: Vec<(String, Value)>,
    },
    Update {
        table: String,
        key: Vec<(String, Value)>,
        updates: Vec<(String, Value)>,
    },
    Delete {
        table: String,
        key: Vec<(String, Value)>,
    },
}

/// Delta store - stores pending changes
pub struct DeltaStore {
    deltas: Vec<DeltaOperation>,
}

impl DeltaStore {
    pub fn new() -> Self {
        Self {
            deltas: vec![],
        }
    }
    
    pub fn add_delta(&mut self, delta: DeltaOperation) {
        self.deltas.push(delta);
    }
    
    pub fn take_deltas(&mut self) -> Vec<DeltaOperation> {
        std::mem::take(&mut self.deltas)
    }
}

impl Default for DeltaStore {
    fn default() -> Self {
        Self::new()
    }
}

