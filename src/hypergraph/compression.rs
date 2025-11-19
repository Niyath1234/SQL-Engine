/// Hypergraph Compression - Compress nodes and edges to reduce memory usage
use crate::hypergraph::node::{HyperNode, NodeId};
use crate::hypergraph::edge::{HyperEdge, EdgeId};
use crate::storage::fragment::ColumnFragment;
use std::collections::HashMap;
use zstd::encode_all;
use zstd::decode_all;

/// Compressed representation of a hypergraph
pub struct CompressedHyperGraph {
    /// Compressed node data
    compressed_nodes: Vec<CompressedNode>,
    
    /// Compressed edge data
    compressed_edges: Vec<CompressedEdge>,
    
    /// Compression ratio achieved
    compression_ratio: f64,
}

struct CompressedNode {
    id: NodeId,
    compressed_data: Vec<u8>,
    original_size: usize,
}

struct CompressedEdge {
    id: EdgeId,
    compressed_data: Vec<u8>,
    original_size: usize,
}

/// Compress a hypergraph
pub struct HypergraphCompressor {
    /// Compression level (1-22, higher = better compression but slower)
    compression_level: i32,
}

impl HypergraphCompressor {
    pub fn new(compression_level: i32) -> Self {
        Self {
            compression_level: compression_level.clamp(1, 22),
        }
    }
    
    /// Compress node metadata (excludes actual data arrays)
    pub fn compress_node(&self, node: &HyperNode) -> Result<CompressedNode, Box<dyn std::error::Error>> {
        // Serialize node metadata (excluding fragments which contain Arc<dyn Array>)
        let metadata = NodeMetadata {
            id: node.id,
            node_type: node.node_type.clone(),
            table_name: node.table_name.clone(),
            column_name: node.column_name.clone(),
            stats: node.stats.clone(),
            metadata: node.metadata.clone(),
            fragment_count: node.fragments.len(),
        };
        
        let serialized = bincode::serialize(&metadata)?;
        let original_size = serialized.len();
        
        // Compress using zstd
        let compressed = encode_all(serialized.as_slice(), self.compression_level)?;
        
        Ok(CompressedNode {
            id: node.id,
            compressed_data: compressed,
            original_size,
        })
    }
    
    /// Compress edge metadata
    pub fn compress_edge(&self, edge: &HyperEdge) -> Result<CompressedEdge, Box<dyn std::error::Error>> {
        let metadata = EdgeMetadata {
            id: edge.id,
            source: edge.source,
            target: edge.target,
            join_type: edge.join_type.clone(),
            predicate: edge.predicate.clone(),
            stats: edge.stats.clone(),
            is_materialized: edge.is_materialized,
        };
        
        let serialized = bincode::serialize(&metadata)?;
        let original_size = serialized.len();
        
        let compressed = encode_all(serialized.as_slice(), self.compression_level)?;
        
        Ok(CompressedEdge {
            id: edge.id,
            compressed_data: compressed,
            original_size,
        })
    }
    
    /// Decompress a node
    pub fn decompress_node(&self, compressed: &CompressedNode) -> Result<NodeMetadata, Box<dyn std::error::Error>> {
        let decompressed = decode_all(compressed.compressed_data.as_slice())?;
        let metadata: NodeMetadata = bincode::deserialize(&decompressed)?;
        Ok(metadata)
    }
    
    /// Decompress an edge
    pub fn decompress_edge(&self, compressed: &CompressedEdge) -> Result<EdgeMetadata, Box<dyn std::error::Error>> {
        let decompressed = decode_all(compressed.compressed_data.as_slice())?;
        let metadata: EdgeMetadata = bincode::deserialize(&decompressed)?;
        Ok(metadata)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct NodeMetadata {
    id: NodeId,
    node_type: crate::hypergraph::node::NodeType,
    table_name: Option<String>,
    column_name: Option<String>,
    stats: crate::hypergraph::node::NodeStatistics,
    metadata: HashMap<String, String>,
    fragment_count: usize,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct EdgeMetadata {
    id: EdgeId,
    source: NodeId,
    target: NodeId,
    join_type: crate::hypergraph::edge::JoinType,
    predicate: crate::hypergraph::edge::JoinPredicate,
    stats: crate::hypergraph::edge::EdgeStatistics,
    is_materialized: bool,
}

/// Dictionary compression for repeated strings in hypergraph
pub struct DictionaryCompressor {
    /// Dictionary mapping strings to IDs
    dictionary: HashMap<String, u32>,
    
    /// Reverse mapping
    reverse_dict: HashMap<u32, String>,
    
    /// Next dictionary ID
    next_id: u32,
}

impl DictionaryCompressor {
    pub fn new() -> Self {
        Self {
            dictionary: HashMap::new(),
            reverse_dict: HashMap::new(),
            next_id: 1,
        }
    }
    
    /// Compress a string using dictionary
    pub fn compress_string(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.dictionary.get(s) {
            id
        } else {
            let id = self.next_id;
            self.next_id += 1;
            self.dictionary.insert(s.to_string(), id);
            self.reverse_dict.insert(id, s.to_string());
            id
        }
    }
    
    /// Decompress a string ID
    pub fn decompress_string(&self, id: u32) -> Option<&String> {
        self.reverse_dict.get(&id)
    }
    
    /// Get compression ratio
    pub fn compression_ratio(&self) -> f64 {
        if self.dictionary.is_empty() {
            return 1.0;
        }
        
        let original_size: usize = self.reverse_dict.values().map(|s| s.len()).sum();
        let compressed_size = self.dictionary.len() * 4; // u32 per entry
        
        compressed_size as f64 / original_size as f64
    }
}

/// Fragment compression - compress column fragments
pub struct FragmentCompressor {
    compression_level: i32,
}

impl FragmentCompressor {
    pub fn new(compression_level: i32) -> Self {
        Self {
            compression_level: compression_level.clamp(1, 22),
        }
    }
    
    /// Compress fragment metadata (not the actual data arrays)
    pub fn compress_fragment_metadata(
        &self,
        fragment: &ColumnFragment,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let metadata = fragment.metadata.clone();
        let serialized = bincode::serialize(&metadata)?;
        let compressed = encode_all(serialized.as_slice(), self.compression_level)?;
        Ok(compressed)
    }
}

/// Delta compression for incremental updates
pub struct DeltaCompressor;

impl DeltaCompressor {
    /// Compress differences between two hypergraph states
    pub fn compress_delta(
        &self,
        old_state: &HashMap<NodeId, Vec<u8>>,
        new_state: &HashMap<NodeId, Vec<u8>>,
    ) -> Vec<DeltaEntry> {
        let mut deltas = Vec::new();
        
        // Find added/modified nodes
        for (id, new_data) in new_state {
            if let Some(old_data) = old_state.get(id) {
                if old_data != new_data {
                    // Modified
                    deltas.push(DeltaEntry {
                        node_id: *id,
                        operation: DeltaOperation::Modified,
                        data: new_data.clone(),
                    });
                }
            } else {
                // Added
                deltas.push(DeltaEntry {
                    node_id: *id,
                    operation: DeltaOperation::Added,
                    data: new_data.clone(),
                });
            }
        }
        
        // Find removed nodes
        for id in old_state.keys() {
            if !new_state.contains_key(id) {
                deltas.push(DeltaEntry {
                    node_id: *id,
                    operation: DeltaOperation::Removed,
                    data: vec![],
                });
            }
        }
        
        deltas
    }
}

#[derive(Clone, Debug)]
pub struct DeltaEntry {
    node_id: NodeId,
    operation: DeltaOperation,
    data: Vec<u8>,
}

#[derive(Clone, Debug)]
enum DeltaOperation {
    Added,
    Modified,
    Removed,
}

