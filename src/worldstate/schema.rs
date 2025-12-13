//! Schema Registry - Table schemas with versioning

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Schema version identifier
pub type SchemaVersion = u64;

/// Column information
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    
    /// Data type (Arrow-compatible string representation)
    pub data_type: String,
    
    /// Is nullable
    pub nullable: bool,
    
    /// Semantic tags (e.g., "dimension/user", "fact/amount", "time/event")
    pub semantic_tags: Vec<String>,
    
    /// Optional description
    pub description: Option<String>,
}

impl Hash for ColumnInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.data_type.hash(state);
        self.nullable.hash(state);
        // Semantic tags sorted for deterministic hashing
        let mut tags = self.semantic_tags.clone();
        tags.sort();
        tags.hash(state);
    }
}

/// Table schema with versioning
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableSchema {
    /// Table name
    pub table_name: String,
    
    /// Current schema version
    pub version: SchemaVersion,
    
    /// Columns in this table
    pub columns: Vec<ColumnInfo>,
    
    /// Child tables (for array columns that were split)
    pub child_tables: Vec<String>, // Names of child tables
    
    /// Timestamp when schema was created
    pub created_at: u64,
    
    /// Timestamp when schema was last updated
    pub updated_at: u64,
}

impl Hash for TableSchema {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.version.hash(state);
        // Hash columns deterministically
        for col in &self.columns {
            col.hash(state);
        }
        // Hash child tables
        let mut child_tables = self.child_tables.clone();
        child_tables.sort();
        child_tables.hash(state);
    }
}

impl TableSchema {
    pub fn new(table_name: String) -> Self {
        let now = Self::now_timestamp();
        Self {
            table_name,
            version: 1,
            columns: Vec::new(),
            child_tables: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }
    
    /// Add a column (creates new version)
    pub fn add_column(&mut self, column: ColumnInfo) -> SchemaVersion {
        self.columns.push(column);
        self.version += 1;
        self.updated_at = Self::now_timestamp();
        self.version
    }
    
    /// Get column by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.name == name)
    }
    
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

/// Schema Registry - stores all table schemas
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaRegistry {
    /// Table name → TableSchema
    tables: HashMap<String, TableSchema>,
}

impl Hash for SchemaRegistry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash tables in sorted order for deterministic hashing
        let mut table_names: Vec<_> = self.tables.keys().collect();
        table_names.sort();
        for name in table_names {
            name.hash(state);
            if let Some(schema) = self.tables.get(name) {
                schema.hash(state);
            }
        }
    }
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }
    
    /// Register or update a table schema
    pub fn register_table(&mut self, schema: TableSchema) {
        self.tables.insert(schema.table_name.clone(), schema);
    }
    
    /// Get table schema
    pub fn get_table(&self, table_name: &str) -> Option<&TableSchema> {
        self.tables.get(table_name)
    }
    
    /// Get mutable table schema
    pub fn get_table_mut(&mut self, table_name: &str) -> Option<&mut TableSchema> {
        self.tables.get_mut(table_name)
    }
    
    /// List all table names
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }
    
    /// Check if table exists
    pub fn has_table(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

