//! Statistics Registry - Row counts, NDV, null rates, distributions

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Column-level statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Column name
    pub column: String,
    
    /// Number of distinct values (NDV)
    pub ndv: Option<u64>,
    
    /// Null rate (0.0-1.0)
    pub null_rate: Option<f64>,
    
    /// Minimum value (if numeric)
    pub min_value: Option<String>,
    
    /// Maximum value (if numeric)
    pub max_value: Option<String>,
    
    /// Last updated timestamp
    pub updated_at: u64,
}

impl Hash for ColumnStats {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.column.hash(state);
        if let Some(ndv) = self.ndv {
            ndv.hash(state);
        }
        // Note: null_rate, min/max may change frequently
        // For cache keying, we may want to exclude these or use a separate "data hash"
    }
}

/// Table-level statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableStats {
    /// Table name
    pub table_name: String,
    
    /// Row count
    pub row_count: u64,
    
    /// Column statistics
    pub column_stats: HashMap<String, ColumnStats>,
    
    /// Last updated timestamp
    pub updated_at: u64,
}

impl Hash for TableStats {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        // Note: row_count changes frequently, may want to exclude from hash
        // or use separate "schema hash" vs "data hash"
    }
}

impl TableStats {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            row_count: 0,
            column_stats: HashMap::new(),
            updated_at: Self::now_timestamp(),
        }
    }
    
    pub fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

/// Statistics Registry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatsRegistry {
    /// Table name → TableStats
    tables: HashMap<String, TableStats>,
}

impl Hash for StatsRegistry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Stats change frequently, so we may want to exclude from deterministic hashing
        // or use a separate "data hash" that doesn't affect plan cache keys
        // For now, we'll hash table names only
        let mut table_names: Vec<_> = self.tables.keys().collect();
        table_names.sort();
        table_names.hash(state);
    }
}

impl StatsRegistry {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }
    
    /// Register or update table stats
    pub fn register_table_stats(&mut self, stats: TableStats) {
        self.tables.insert(stats.table_name.clone(), stats);
    }
    
    /// Get table stats
    pub fn get_table_stats(&self, table_name: &str) -> Option<&TableStats> {
        self.tables.get(table_name)
    }
    
    /// Get mutable table stats
    pub fn get_table_stats_mut(&mut self, table_name: &str) -> Option<&mut TableStats> {
        self.tables.get_mut(table_name)
    }
    
    /// Update row count for a table
    pub fn update_row_count(&mut self, table_name: &str, row_count: u64) {
        if let Some(stats) = self.tables.get_mut(table_name) {
            stats.row_count = row_count;
            stats.updated_at = Self::now_timestamp();
        } else {
            let mut stats = TableStats::new(table_name.to_string());
            stats.row_count = row_count;
            self.tables.insert(table_name.to_string(), stats);
        }
    }
    
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

impl Default for StatsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

