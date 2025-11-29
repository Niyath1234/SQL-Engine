//! Level 1: Persistent Hypergraph Storage
//!
//! This is the foundation layer that:
//! - Persists across restarts (never cleared)
//! - Uses 128-bit vectorized encoding for minimal storage
//! - Stores metadata, paths, statistics
//! - Builds up over time (cumulative knowledge)

use crate::hypergraph::HyperGraph;
use crate::hypergraph::path::{HyperPath, PathSignature};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// Compact 128-bit representation of a hypergraph node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactNode {
    /// Node ID (128-bit: high 64 bits = table hash, low 64 bits = column hash)
    pub id: u128,
    
    /// Table name hash (first 64 bits of id)
    pub table_hash: u64,
    
    /// Column name hash (second 64 bits of id, or 0 for table nodes)
    pub column_hash: u64,
    
    /// Compact statistics (packed into minimal bytes)
    pub stats: CompactStats,
    
    /// Vectorized metadata (128-bit aligned)
    pub metadata_bits: u128,
    
    /// Last update timestamp (epoch seconds, packed)
    pub last_updated: u64,
}

/// Compact statistics (optimized for 128-bit alignment)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactStats {
    /// Row count (32-bit, supports up to 4B rows)
    pub row_count: u32,
    
    /// Size in bytes (48-bit, supports up to 256TB)
    pub size_bytes: u64, // Will pack into 48 bits in binary format
    
    /// Cardinality estimate (32-bit)
    pub cardinality: u32,
}

/// Compact path representation (128-bit vectorized)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactPath {
    /// Path signature hash (128-bit)
    pub signature_hash: u128,
    
    /// Node IDs (compressed, up to 16 nodes in 128-bit chunks)
    pub node_ids: Vec<u128>,
    
    /// Edge IDs (compressed, up to 16 edges in 128-bit chunks)
    pub edge_ids: Vec<u128>,
    
    /// Compact statistics
    pub stats: CompactStats,
    
    /// Usage count (for promotion decisions)
    pub usage_count: u32,
    
    /// Last used timestamp
    pub last_used: u64,
    
    /// Whether path is materialized
    pub is_materialized: bool,
}

/// Persistent hypergraph storage (Level 1)
pub struct PersistentHypergraph {
    /// Compact nodes (table and column metadata)
    nodes: HashMap<u128, CompactNode>,
    
    /// Compact paths (join patterns)
    paths: HashMap<u128, CompactPath>,
    
    /// Table name to hash mapping (for lookup)
    table_name_map: HashMap<String, u64>,
    
    /// Storage path
    storage_path: PathBuf,
    
    /// Compression enabled (for binary format)
    compressed: bool,
}

impl PersistentHypergraph {
    /// Load or create persistent hypergraph
    pub fn load_or_create() -> Result<Self, anyhow::Error> {
        let storage_dir = Self::get_storage_dir()?;
        let storage_path = storage_dir.join("hypergraph.db");
        
        if storage_path.exists() {
            Self::load_from_disk(&storage_path)
        } else {
            Ok(Self::new_empty(storage_path))
        }
    }
    
    /// Create new empty persistent hypergraph
    fn new_empty(storage_path: PathBuf) -> Self {
        Self {
            nodes: HashMap::new(),
            paths: HashMap::new(),
            table_name_map: HashMap::new(),
            storage_path,
            compressed: true, // Use compression for storage
        }
    }
    
    /// Load from disk
    fn load_from_disk(path: &PathBuf) -> Result<Self, anyhow::Error> {
        // Try compressed format first, then fallback to JSON
        if path.with_extension("db.zstd").exists() {
            let compressed_data = fs::read(path.with_extension("db.zstd"))?;
            let data = zstd::decode_all(&compressed_data[..])?;
            let (nodes, paths, table_name_map): (HashMap<u128, CompactNode>, HashMap<u128, CompactPath>, HashMap<String, u64>) = 
                bincode::deserialize(&data)?;
            Ok(Self {
                nodes,
                paths,
                table_name_map,
                storage_path: path.clone(),
                compressed: true,
            })
        } else if path.exists() {
            // Fallback to JSON (for compatibility)
            let data = fs::read_to_string(path)?;
            let loaded: PersistentHypergraphData = serde_json::from_str(&data)?;
            Ok(Self {
                nodes: loaded.nodes,
                paths: loaded.paths,
                table_name_map: loaded.table_name_map,
                storage_path: path.clone(),
                compressed: false,
            })
        } else {
            Ok(Self::new_empty(path.clone()))
        }
    }
    
    /// Save to disk
    pub fn save(&self) -> Result<(), anyhow::Error> {
        // Create storage directory if it doesn't exist
        if let Some(parent) = self.storage_path.parent() {
            fs::create_dir_all(parent)?;
        }
        
        if self.compressed {
            // Use compressed binary format (smallest size)
            let data = bincode::serialize(&(&self.nodes, &self.paths, &self.table_name_map))?;
            let compressed = zstd::encode_all(&data[..], 3)?; // Level 3 compression
            fs::write(self.storage_path.with_extension("db.zstd"), compressed)?;
        } else {
            // Fallback to JSON (for debugging/compatibility)
            let data = PersistentHypergraphData {
                nodes: self.nodes.clone(),
                paths: self.paths.clone(),
                table_name_map: self.table_name_map.clone(),
            };
            let json = serde_json::to_string_pretty(&data)?;
            fs::write(&self.storage_path, json)?;
        }
        
        Ok(())
    }
    
    /// Add or update node metadata
    pub fn add_node(&mut self, table_name: &str, column_name: Option<&str>, 
                   row_count: usize, size_bytes: usize, cardinality: usize) {
        let table_hash = Self::hash_table_name(table_name);
        let column_hash = column_name.map(|c| Self::hash_column_name(c)).unwrap_or(0);
        
        let node_id = Self::combine_to_128bit(table_hash, column_hash);
        
        // Store table name mapping
        self.table_name_map.insert(table_name.to_string(), table_hash);
        
        let compact_node = CompactNode {
            id: node_id,
            table_hash,
            column_hash,
            stats: CompactStats {
                row_count: row_count.min(u32::MAX as usize) as u32,
                size_bytes: size_bytes as u64, // Will be compressed in binary format
                cardinality: cardinality.min(u32::MAX as usize) as u32,
            },
            metadata_bits: 0, // TODO: Pack metadata into 128 bits
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        
        self.nodes.insert(node_id, compact_node);
    }
    
    /// Add or update path
    pub fn add_path(&mut self, signature: &PathSignature, path: &HyperPath) {
        let signature_hash = Self::hash_path_signature(signature);
        
        // Compress node IDs into 128-bit chunks
        // Each NodeId is u32 (4 bytes), so we can fit 4 NodeIds per 128-bit chunk
        let node_ids: Vec<u128> = path.nodes.chunks(4)
            .map(|chunk| {
                let mut combined: u128 = 0;
                for (i, node_id) in chunk.iter().enumerate() {
                    if i < 4 {
                        combined |= (node_id.0 as u128) << (i * 32);
                    }
                }
                combined
            })
            .collect();
        
        // Compress edge IDs similarly
        let edge_ids: Vec<u128> = path.edges.chunks(4)
            .map(|chunk| {
                let mut combined: u128 = 0;
                for (i, edge_id) in chunk.iter().enumerate() {
                    if i < 4 {
                        combined |= (edge_id.0 as u128) << (i * 32);
                    }
                }
                combined
            })
            .collect();
        
        let compact_path = CompactPath {
            signature_hash,
            node_ids,
            edge_ids,
            stats: CompactStats {
                row_count: path.stats.cardinality.min(u32::MAX as usize) as u32,
                size_bytes: 0, // Path size not tracked
                cardinality: path.stats.cardinality.min(u32::MAX as usize) as u32,
            },
            usage_count: path.usage_count.min(u32::MAX as u64) as u32,
            last_used: path.last_used,
            is_materialized: path.is_materialized,
        };
        
        self.paths.insert(signature_hash, compact_path);
    }
    
    /// Get node metadata
    pub fn get_node(&self, table_hash: u64, column_hash: u64) -> Option<&CompactNode> {
        let node_id = Self::combine_to_128bit(table_hash, column_hash);
        self.nodes.get(&node_id)
    }
    
    /// Get path by signature
    pub fn get_path(&self, signature: &PathSignature) -> Option<&CompactPath> {
        let signature_hash = Self::hash_path_signature(signature);
        self.paths.get(&signature_hash)
    }
    
    /// Get all paths for a table
    pub fn get_table_paths(&self, table_hash: u64) -> Vec<&CompactPath> {
        // This requires scanning paths - could be optimized with an index
        // For now, return all paths (optimization: add table->path mapping)
        self.paths.values().collect()
    }
    
    /// Merge nodes from in-memory hypergraph (called after table loads)
    pub fn merge_from_graph(&mut self, graph: &HyperGraph) {
        // Iterate through all nodes in the graph and add/update in persistent storage
        for (_node_id, node) in graph.iter_nodes() {
            let table_name = node.table_name.as_deref().unwrap_or("unknown");
            let column_name = node.column_name.as_deref();
            
            let stats = &node.stats;
            self.add_node(
                table_name,
                column_name,
                stats.row_count,
                stats.size_bytes,
                stats.cardinality,
            );
        }
    }
    
    /// Hash table name to 64 bits
    fn hash_table_name(name: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        hasher.finish()
    }
    
    /// Hash column name to 64 bits
    fn hash_column_name(name: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        hasher.finish()
    }
    
    /// Combine two 64-bit values into 128 bits
    fn combine_to_128bit(high: u64, low: u64) -> u128 {
        ((high as u128) << 64) | (low as u128)
    }
    
    /// Hash path signature to 128 bits
    fn hash_path_signature(signature: &PathSignature) -> u128 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        signature.nodes.hash(&mut hasher);
        signature.edges.hash(&mut hasher);
        hasher.finish() as u128
    }
    
    /// Get storage directory
    fn get_storage_dir() -> Result<PathBuf, anyhow::Error> {
        let mut path = dirs::data_dir()
            .ok_or_else(|| anyhow::anyhow!("Cannot find data directory"))?;
        path.push("hypergraph-sql-engine");
        path.push("persistent");
        Ok(path)
    }
    
    /// Get storage size estimate (in bytes)
    pub fn storage_size_estimate(&self) -> usize {
        // Rough estimate: each node ~64 bytes, each path ~128 bytes
        (self.nodes.len() * 64) + (self.paths.len() * 128)
    }
}

#[derive(Serialize, Deserialize)]
struct PersistentHypergraphData {
    nodes: HashMap<u128, CompactNode>,
    paths: HashMap<u128, CompactPath>,
    table_name_map: HashMap<String, u64>,
}

