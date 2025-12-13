//! Key Registry - Primary keys, natural keys, event time columns

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Primary key definition
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrimaryKey {
    /// Column name(s) that form the primary key
    pub columns: Vec<String>,
    
    /// Is synthetic (auto-generated) or natural
    pub is_synthetic: bool,
}

impl Hash for PrimaryKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut cols = self.columns.clone();
        cols.sort();
        cols.hash(state);
        self.is_synthetic.hash(state);
    }
}

/// Natural key (business key that may not be unique)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NaturalKey {
    /// Column name(s)
    pub columns: Vec<String>,
    
    /// Optional uniqueness constraint
    pub is_unique: bool,
}

impl Hash for NaturalKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut cols = self.columns.clone();
        cols.sort();
        cols.hash(state);
        self.is_unique.hash(state);
    }
}

/// Event time column definition
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventTime {
    /// Column name
    pub column: String,
    
    /// Time granularity (e.g., "second", "millisecond", "nanosecond")
    pub granularity: String,
}

impl Hash for EventTime {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.column.hash(state);
        self.granularity.hash(state);
    }
}

/// Deduplication strategy
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DedupeStrategy {
    /// Append-only (no deduplication)
    AppendOnly,
    
    /// Latest wins (keep most recent based on updated_at)
    LatestWins,
    
    /// First wins (keep first occurrence)
    FirstWins,
    
    /// Custom deduplication logic
    Custom(String),
}

/// Key registry for a table
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableKeys {
    /// Primary key (if defined)
    pub primary_key: Option<PrimaryKey>,
    
    /// Natural keys
    pub natural_keys: Vec<NaturalKey>,
    
    /// Event time column (if defined)
    pub event_time: Option<EventTime>,
    
    /// Updated timestamp column (if defined)
    pub updated_at: Option<String>,
    
    /// Deduplication strategy
    pub dedupe_strategy: DedupeStrategy,
}

impl Hash for TableKeys {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let Some(ref pk) = self.primary_key {
            pk.hash(state);
        }
        for nk in &self.natural_keys {
            nk.hash(state);
        }
        if let Some(ref et) = self.event_time {
            et.hash(state);
        }
        if let Some(ref ua) = self.updated_at {
            ua.hash(state);
        }
        self.dedupe_strategy.hash(state);
    }
}

impl Default for TableKeys {
    fn default() -> Self {
        Self {
            primary_key: None,
            natural_keys: Vec::new(),
            event_time: None,
            updated_at: None,
            dedupe_strategy: DedupeStrategy::AppendOnly,
        }
    }
}

/// Key Registry - stores keys for all tables
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyRegistry {
    /// Table name → TableKeys
    tables: HashMap<String, TableKeys>,
}

impl Hash for KeyRegistry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut table_names: Vec<_> = self.tables.keys().collect();
        table_names.sort();
        for name in table_names {
            name.hash(state);
            if let Some(keys) = self.tables.get(name) {
                keys.hash(state);
            }
        }
    }
}

impl KeyRegistry {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }
    
    /// Register keys for a table
    pub fn register_table_keys(&mut self, table_name: String, keys: TableKeys) {
        self.tables.insert(table_name, keys);
    }
    
    /// Get keys for a table
    pub fn get_table_keys(&self, table_name: &str) -> Option<&TableKeys> {
        self.tables.get(table_name)
    }
    
    /// Get mutable keys for a table
    pub fn get_table_keys_mut(&mut self, table_name: &str) -> Option<&mut TableKeys> {
        self.tables.get_mut(table_name)
    }
}

impl Default for KeyRegistry {
    fn default() -> Self {
        Self::new()
    }
}

