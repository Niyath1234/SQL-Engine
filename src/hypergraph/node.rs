use crate::storage::fragment::ColumnFragment;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::collections::HashMap;

/// A node in the hypergraph represents a table or column
/// Nodes store references to column fragments
/// Note: Cannot derive Serialize/Deserialize due to ColumnFragment containing Arc<dyn Array>
#[derive(Clone, Debug)]
pub struct HyperNode {
    /// Unique node ID
    pub id: NodeId,
    
    /// Node type (Table or Column)
    pub node_type: NodeType,
    
    /// Table name (if this is a table node)
    pub table_name: Option<String>,
    
    /// Column name (if this is a column node)
    pub column_name: Option<String>,
    
    /// Column fragments stored in this node
    pub fragments: Vec<ColumnFragment>,
    
    /// Statistics about this node
    pub stats: NodeStatistics,
    
    /// Metadata
    pub metadata: HashMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NodeType {
    Table,
    Column,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeStatistics {
    /// Estimated row count
    pub row_count: usize,
    
    /// Estimated cardinality
    pub cardinality: usize,
    
    /// Size in bytes
    pub size_bytes: usize,
    
    /// Last update timestamp
    pub last_updated: u64,
}

impl HyperNode {
    pub fn new_table(id: NodeId, table_name: String) -> Self {
        Self {
            id,
            node_type: NodeType::Table,
            table_name: Some(table_name),
            column_name: None,
            fragments: vec![],
            stats: NodeStatistics {
                row_count: 0,
                cardinality: 0,
                size_bytes: 0,
                last_updated: 0,
            },
            metadata: HashMap::new(),
        }
    }
    
    pub fn new_column(id: NodeId, table_name: String, column_name: String) -> Self {
        Self {
            id,
            node_type: NodeType::Column,
            table_name: Some(table_name),
            column_name: Some(column_name),
            fragments: vec![],
            stats: NodeStatistics {
                row_count: 0,
                cardinality: 0,
                size_bytes: 0,
                last_updated: 0,
            },
            metadata: HashMap::new(),
        }
    }
    
    /// Add a fragment to this node
    pub fn add_fragment(&mut self, fragment: ColumnFragment) {
        self.fragments.push(fragment);
        self.update_stats();
    }
    
    /// Update statistics from fragments
    pub fn update_stats(&mut self) {
        // For table nodes, all column fragments should have the same row_count
        // So we use the first fragment's row_count (or 0 if no fragments)
        // For column nodes, there's typically one fragment, so this works correctly
        self.stats.row_count = self.fragments.first()
            .map(|f| f.len())
            .unwrap_or(0);
        self.stats.size_bytes = self.fragments.iter().map(|f| f.metadata.memory_size).sum();
        // TODO: Update cardinality estimate
    }
    
    /// Get total row count across all fragments
    pub fn total_rows(&self) -> usize {
        self.stats.row_count
    }
}

