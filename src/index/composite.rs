/// Phase 5: Composite Indexes
/// 
/// Multi-column indexes for optimizing queries with multiple predicates
/// Supports:
/// - Multi-column range queries
/// - Multi-column equality queries
/// - Partial index usage (use subset of columns)

use crate::storage::fragment::Value;
use std::collections::HashMap;
use anyhow::Result;

/// Composite index for multiple columns
pub struct CompositeIndex {
    /// Column names in index order
    columns: Vec<String>,
    
    /// Index entries: (column_values...) -> row_ids
    /// Values are stored in column order
    entries: HashMap<Vec<Value>, Vec<usize>>,
    
    /// Total number of rows indexed
    row_count: usize,
    
    /// Table name
    table_name: String,
}

impl CompositeIndex {
    /// Create new composite index
    pub fn new(table_name: String, columns: Vec<String>) -> Self {
        Self {
            columns,
            entries: HashMap::new(),
            row_count: 0,
            table_name,
        }
    }
    
    /// Build index from column data
    pub fn build(
        table_name: String,
        columns: Vec<String>,
        column_data: Vec<Vec<Value>>,
    ) -> Result<Self> {
        if columns.len() != column_data.len() {
            anyhow::bail!("Column count mismatch: {} columns but {} data vectors", columns.len(), column_data.len());
        }
        
        if column_data.is_empty() {
            return Ok(Self::new(table_name, columns));
        }
        
        let row_count = column_data[0].len();
        let mut entries: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();
        
        // Build index: for each row, create key from all column values
        for row_idx in 0..row_count {
            let mut key = Vec::with_capacity(columns.len());
            for col_data in &column_data {
                if row_idx < col_data.len() {
                    key.push(col_data[row_idx].clone());
                } else {
                    key.push(Value::Null);
                }
            }
            
            entries.entry(key).or_insert_with(Vec::new).push(row_idx);
        }
        
        Ok(Self {
            columns,
            entries,
            row_count,
            table_name,
        })
    }
    
    /// Search for rows matching all column values (exact match)
    pub fn search_exact(&self, values: &[Value]) -> Vec<usize> {
        if values.len() != self.columns.len() {
            return vec![];
        }
        
        self.entries.get(values).cloned().unwrap_or_default()
    }
    
    /// Search for rows matching prefix of columns (partial match)
    /// Useful when query only uses first N columns of index
    pub fn search_prefix(&self, prefix_values: &[Value]) -> Vec<usize> {
        if prefix_values.len() > self.columns.len() {
            return vec![];
        }
        
        let mut results = Vec::new();
        for (key, row_ids) in &self.entries {
            if key.len() >= prefix_values.len() {
                let matches = key.iter()
                    .zip(prefix_values.iter())
                    .all(|(k, p)| k == p);
                if matches {
                    results.extend(row_ids);
                }
            }
        }
        
        results.sort();
        results.dedup();
        results
    }
    
    /// Get column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    
    /// Get row count
    pub fn row_count(&self) -> usize {
        self.row_count
    }
    
    /// Check if index can be used for a query
    /// Returns number of matching columns (0 = cannot use)
    pub fn can_use_for_query(&self, query_columns: &[String]) -> usize {
        // Check if query columns are a prefix of index columns
        if query_columns.len() > self.columns.len() {
            return 0;
        }
        
        for (i, query_col) in query_columns.iter().enumerate() {
            if i >= self.columns.len() || self.columns[i] != *query_col {
                return 0;
            }
        }
        
        query_columns.len()
    }
}

/// Composite index manager
/// Manages multiple composite indexes for a table
pub struct CompositeIndexManager {
    /// Table name
    table_name: String,
    
    /// Indexes: index_name -> CompositeIndex
    indexes: HashMap<String, CompositeIndex>,
}

impl CompositeIndexManager {
    /// Create new manager
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            indexes: HashMap::new(),
        }
    }
    
    /// Add an index
    pub fn add_index(&mut self, name: String, index: CompositeIndex) {
        self.indexes.insert(name, index);
    }
    
    /// Find best index for a query
    /// Returns (index_name, matching_columns_count)
    pub fn find_best_index(&self, query_columns: &[String]) -> Option<(String, usize)> {
        let mut best: Option<(String, usize)> = None;
        
        for (name, index) in &self.indexes {
            let match_count = index.can_use_for_query(query_columns);
            if match_count > 0 {
                if let Some((_, best_count)) = &best {
                    if match_count > *best_count {
                        best = Some((name.clone(), match_count));
                    }
                } else {
                    best = Some((name.clone(), match_count));
                }
            }
        }
        
        best
    }
    
    /// Get index by name
    pub fn get_index(&self, name: &str) -> Option<&CompositeIndex> {
        self.indexes.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_composite_index() {
        let columns = vec!["col1".to_string(), "col2".to_string()];
        let col1_data = vec![Value::Int64(1), Value::Int64(1), Value::Int64(2)];
        let col2_data = vec![Value::Int64(10), Value::Int64(20), Value::Int64(10)];
        
        let index = CompositeIndex::build(
            "test_table".to_string(),
            columns.clone(),
            vec![col1_data, col2_data],
        ).unwrap();
        
        // Test exact search
        let results = index.search_exact(&[Value::Int64(1), Value::Int64(10)]);
        assert_eq!(results, vec![0]);
        
        // Test prefix search
        let results = index.search_prefix(&[Value::Int64(1)]);
        assert_eq!(results.len(), 2); // Rows 0 and 1
    }
}

