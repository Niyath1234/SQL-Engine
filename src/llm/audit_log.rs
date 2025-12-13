//! Audit Log - Stores structured plans, SQL, hashes, and stats

use crate::llm::plan_generator::StructuredPlan;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Audit log entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditLogEntry {
    /// Entry ID
    pub id: String,
    
    /// User intent
    pub user_intent: String,
    
    /// Structured plan
    pub structured_plan: StructuredPlan,
    
    /// Generated SQL
    pub sql: String,
    
    /// WorldState hash at time of generation
    pub world_hash: u64,
    
    /// Query signature hash
    pub query_hash: u64,
    
    /// Execution stats (if executed)
    pub execution_stats: Option<ExecutionStats>,
    
    /// Timestamp
    pub timestamp: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionStats {
    pub row_count: u64,
    pub execution_time_ms: f64,
    pub cache_hit: bool,
}

/// Audit Log - In-memory audit log (in production, would persist to database)
pub struct AuditLog {
    entries: Arc<RwLock<Vec<AuditLogEntry>>>,
    max_entries: usize,
}

impl AuditLog {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: Arc::new(RwLock::new(Vec::new())),
            max_entries,
        }
    }
    
    /// Log an entry
    pub fn log(
        &self,
        user_intent: String,
        structured_plan: StructuredPlan,
        sql: String,
        world_hash: u64,
        query_hash: u64,
    ) -> String {
        let entry_id = uuid::Uuid::new_v4().to_string();
        
        let entry = AuditLogEntry {
            id: entry_id.clone(),
            user_intent,
            structured_plan,
            sql,
            world_hash,
            query_hash,
            execution_stats: None,
            timestamp: Self::now_timestamp(),
        };
        
        let mut entries = self.entries.write().unwrap();
        entries.push(entry);
        
        // Evict oldest if at capacity
        if entries.len() > self.max_entries {
            entries.remove(0);
        }
        
        entry_id
    }
    
    /// Update entry with execution stats
    pub fn update_execution_stats(
        &self,
        entry_id: &str,
        row_count: u64,
        execution_time_ms: f64,
        cache_hit: bool,
    ) {
        let mut entries = self.entries.write().unwrap();
        if let Some(entry) = entries.iter_mut().find(|e| e.id == entry_id) {
            entry.execution_stats = Some(ExecutionStats {
                row_count,
                execution_time_ms,
                cache_hit,
            });
        }
    }
    
    /// Get all entries
    pub fn get_entries(&self) -> Vec<AuditLogEntry> {
        self.entries.read().unwrap().clone()
    }
    
    /// Get entry by ID
    pub fn get_entry(&self, entry_id: &str) -> Option<AuditLogEntry> {
        self.entries.read().unwrap()
            .iter()
            .find(|e| e.id == entry_id)
            .cloned()
    }
    
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new(1000) // Default: keep last 1000 entries
    }
}

