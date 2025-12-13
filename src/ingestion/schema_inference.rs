//! Schema Inference - Deterministic schema detection from JSON payloads

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use crate::worldstate::schema::{TableSchema, ColumnInfo, SchemaVersion};

/// Inferred column information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InferredColumn {
    /// Column name (with path prefix for nested objects)
    pub name: String,
    
    /// Inferred data type
    pub data_type: String,
    
    /// Is nullable (true if we've seen null values)
    pub nullable: bool,
    
    /// Is array (true if this is an array column that should be split)
    pub is_array: bool,
    
    /// Sample values seen (for semantic tagging)
    pub sample_values: Vec<String>,
}

/// Inferred schema for a table
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InferredSchema {
    /// Table name
    pub table_name: String,
    
    /// Inferred columns
    pub columns: Vec<InferredColumn>,
    
    /// Child tables (for array columns)
    pub child_tables: Vec<InferredChildTable>,
}

/// Inferred child table (for array columns)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InferredChildTable {
    /// Child table name
    pub table_name: String,
    
    /// Parent table name
    pub parent_table: String,
    
    /// Parent key column (to link back)
    pub parent_key: String,
    
    /// Columns in child table
    pub columns: Vec<InferredColumn>,
}

/// Schema evolution plan
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SchemaEvolution {
    /// No changes needed
    NoChange,
    
    /// Add new columns (ALTER TABLE ADD COLUMN)
    AddColumns {
        columns: Vec<ColumnInfo>,
        new_version: SchemaVersion,
    },
    
    /// Breaking change (requires new table version)
    BreakingChange {
        reason: String,
        suggested_action: String,
    },
}

/// Schema Inference Engine
pub struct SchemaInference {
    /// Minimum samples to infer type (default: 1)
    min_samples: usize,
    
    /// Maximum sample values to store per column
    max_samples: usize,
}

impl SchemaInference {
    pub fn new() -> Self {
        Self {
            min_samples: 1,
            max_samples: 10,
        }
    }
    
    /// Infer schema from a batch of JSON payloads
    pub fn infer_schema(&self, table_name: &str, payloads: &[Value]) -> InferredSchema {
        let mut column_map: HashMap<String, InferredColumn> = HashMap::new();
        let mut child_tables: Vec<InferredChildTable> = Vec::new();
        
        for payload in payloads {
            self.infer_from_value(payload, "", &mut column_map, &mut child_tables, table_name);
        }
        
        // Convert to sorted vector for deterministic output
        let mut columns: Vec<InferredColumn> = column_map.into_values().collect();
        columns.sort_by_key(|c| c.name.clone());
        
        InferredSchema {
            table_name: table_name.to_string(),
            columns,
            child_tables,
        }
    }
    
    /// Recursively infer schema from a JSON value
    fn infer_from_value(
        &self,
        value: &Value,
        prefix: &str,
        column_map: &mut HashMap<String, InferredColumn>,
        child_tables: &mut Vec<InferredChildTable>,
        parent_table: &str,
    ) {
        match value {
            Value::Null => {
                // Null values don't contribute to schema inference
            }
            Value::Bool(_) => {
                self.add_column(column_map, prefix, "INT", true); // Map BOOL to INT (0/1)
            }
            Value::Number(n) => {
                if n.is_f64() {
                    self.add_column(column_map, prefix, "FLOAT", false);
                } else {
                    self.add_column(column_map, prefix, "INT", false);
                }
            }
            Value::String(s) => {
                self.add_column_with_sample(column_map, prefix, "VARCHAR", false, s.clone());
            }
            Value::Array(arr) => {
                // Arrays become child tables
                if !arr.is_empty() {
                    let child_table_name = format!("{}__items", parent_table);
                    let mut child_columns: HashMap<String, InferredColumn> = HashMap::new();
                    
                    // Infer schema from first array element
                    if let Some(first) = arr.first() {
                        self.infer_from_value(first, "", &mut child_columns, child_tables, &child_table_name);
                    }
                    
                    // Check if child table already exists
                    if !child_tables.iter().any(|ct| ct.table_name == child_table_name) {
                        let mut cols: Vec<InferredColumn> = child_columns.into_values().collect();
                        cols.sort_by_key(|c| c.name.clone());
                        
                        child_tables.push(InferredChildTable {
                            table_name: child_table_name.clone(),
                            parent_table: parent_table.to_string(),
                            parent_key: format!("{}_id", parent_table), // Synthetic parent key
                            columns: cols,
                        });
                    }
                }
            }
            Value::Object(obj) => {
                // Nested objects: flatten with prefix
                for (key, val) in obj {
                    let new_prefix = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{}__{}", prefix, key)
                    };
                    self.infer_from_value(val, &new_prefix, column_map, child_tables, parent_table);
                }
            }
        }
    }
    
    /// Add or update a column in the column map
    fn add_column(
        &self,
        column_map: &mut HashMap<String, InferredColumn>,
        name: &str,
        data_type: &str,
        nullable: bool,
    ) {
        column_map
            .entry(name.to_string())
            .and_modify(|col| {
                // Update type if we see a more general type
                col.data_type = self.merge_types(&col.data_type, data_type);
                col.nullable = col.nullable || nullable;
            })
            .or_insert_with(|| InferredColumn {
                name: name.to_string(),
                data_type: data_type.to_string(),
                nullable,
                is_array: false,
                sample_values: Vec::new(),
            });
    }
    
    /// Add column with sample value
    fn add_column_with_sample(
        &self,
        column_map: &mut HashMap<String, InferredColumn>,
        name: &str,
        data_type: &str,
        nullable: bool,
        sample: String,
    ) {
        column_map
            .entry(name.to_string())
            .and_modify(|col| {
                col.nullable = col.nullable || nullable;
                if col.sample_values.len() < self.max_samples {
                    col.sample_values.push(sample.clone());
                }
            })
            .or_insert_with(|| {
                let mut samples = Vec::new();
                samples.push(sample);
                InferredColumn {
                    name: name.to_string(),
                    data_type: data_type.to_string(),
                    nullable,
                    is_array: false,
                    sample_values: samples,
                }
            });
    }
    
    /// Merge two types (return the more general type)
    fn merge_types(&self, type1: &str, type2: &str) -> String {
        // Type hierarchy: INT < FLOAT < VARCHAR
        if type1 == type2 {
            return type1.to_string();
        }
        
        match (type1, type2) {
            ("INT", "FLOAT") | ("FLOAT", "INT") => "FLOAT".to_string(),
            (_, "VARCHAR") | ("VARCHAR", _) => "VARCHAR".to_string(),
            _ => type1.to_string(), // Default to first type
        }
    }
    
    /// Compare inferred schema with existing schema and determine evolution
    pub fn compare_schema(
        &self,
        inferred: &InferredSchema,
        existing: Option<&TableSchema>,
    ) -> SchemaEvolution {
        if let Some(existing) = existing {
            // Check for new columns
            let existing_col_names: HashSet<&str> = existing.columns.iter()
                .map(|c| c.name.as_str())
                .collect();
            
            let new_columns: Vec<_> = inferred.columns.iter()
                .filter(|c| !existing_col_names.contains(c.name.as_str()))
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                    semantic_tags: Vec::new(), // Will be inferred later
                    description: None,
                })
                .collect();
            
            if !new_columns.is_empty() {
                return SchemaEvolution::AddColumns {
                    columns: new_columns,
                    new_version: existing.version + 1,
                };
            }
            
            SchemaEvolution::NoChange
        } else {
            // New table - no evolution needed
            SchemaEvolution::NoChange
        }
    }
}

impl Default for SchemaInference {
    fn default() -> Self {
        Self::new()
    }
}

