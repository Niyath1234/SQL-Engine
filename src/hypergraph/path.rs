use crate::hypergraph::node::NodeId;
use crate::hypergraph::edge::EdgeId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A path in the hypergraph represents a precomputed multi-way join
/// Paths are cached and reused for similar queries
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HyperPath {
    /// Unique path ID
    pub id: PathId,
    
    /// Sequence of nodes in this path
    pub nodes: Vec<NodeId>,
    
    /// Sequence of edges connecting the nodes
    pub edges: Vec<EdgeId>,
    
    /// Statistics about this path
    pub stats: PathStatistics,
    
    /// Usage count (for LRU eviction)
    pub usage_count: u64,
    
    /// Last used timestamp
    pub last_used: u64,
    
    /// Whether this path is materialized
    pub is_materialized: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PathId(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PathStatistics {
    /// Estimated result cardinality
    pub cardinality: usize,
    
    /// Estimated cost (in arbitrary units)
    pub cost: f64,
    
    /// Selectivity of this path
    pub selectivity: f64,
}

impl HyperPath {
    pub fn new(id: PathId, nodes: Vec<NodeId>, edges: Vec<EdgeId>) -> Self {
        Self {
            id,
            nodes,
            edges,
            stats: PathStatistics {
                cardinality: 0,
                cost: 0.0,
                selectivity: 1.0,
            },
            usage_count: 0,
            last_used: 0,
            is_materialized: false,
        }
    }
    
    /// Record usage of this path
    pub fn record_usage(&mut self) {
        self.usage_count += 1;
        self.last_used = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
    
    /// Check if this path covers a set of tables
    pub fn covers_tables(&self, tables: &[String]) -> bool {
        // TODO: Check if path includes all required tables
        false
    }
    
    /// Check if this path is similar to another path (for reuse)
    pub fn is_similar(&self, other: &HyperPath) -> bool {
        // TODO: Implement similarity check
        self.nodes == other.nodes && self.edges == other.edges
    }
}

/// Path cache for storing and reusing hypergraph paths
pub struct PathCache {
    /// Map from path signature to path
    paths: HashMap<PathSignature, HyperPath>,
    
    /// Maximum number of paths to cache
    max_size: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PathSignature {
    /// Sorted list of node IDs
    pub nodes: Vec<NodeId>,
    
    /// Sorted list of edge IDs
    pub edges: Vec<EdgeId>,
}

impl PathCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            paths: HashMap::new(),
            max_size,
        }
    }
    
    /// Get a path by signature
    pub fn get(&mut self, signature: &PathSignature) -> Option<&mut HyperPath> {
        self.paths.get_mut(signature)
    }
    
    /// Insert a path
    pub fn insert(&mut self, signature: PathSignature, path: HyperPath) {
        // Evict least recently used if cache is full
        if self.paths.len() >= self.max_size {
            self.evict_lru();
        }
        self.paths.insert(signature, path);
    }
    
    /// Evict least recently used path
    fn evict_lru(&mut self) {
        let lru_key = self.paths
            .iter()
            .min_by_key(|(_, path)| path.last_used)
            .map(|(key, _)| key.clone());
        
        if let Some(key) = lru_key {
            self.paths.remove(&key);
        }
    }
    
    /// Clean up old/unused paths (periodic maintenance)
    /// Removes paths that haven't been used in a long time, even if cache isn't full
    pub fn cleanup_expired(&mut self, max_age_seconds: u64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Collect expired paths (haven't been used in max_age_seconds)
        let expired_keys: Vec<PathSignature> = self.paths
            .iter()
            .filter(|(_, path)| {
                let age = now.saturating_sub(path.last_used);
                age > max_age_seconds
            })
            .map(|(key, _)| key.clone())
            .collect();
        
        // Remove expired paths
        for key in expired_keys {
            self.paths.remove(&key);
        }
    }
    
    /// Get cache size
    pub fn len(&self) -> usize {
        self.paths.len()
    }
    
    /// Find similar paths (for reuse)
    pub fn find_similar(&self, target: &PathSignature) -> Vec<&HyperPath> {
        self.paths
            .iter()
            .filter_map(|(sig, path)| {
                if self.is_similar_signature(sig, target) {
                    Some(path)
                } else {
                    None
                }
            })
            .collect()
    }
    
    fn is_similar_signature(&self, sig1: &PathSignature, sig2: &PathSignature) -> bool {
        // Check if sig1 is a subset of sig2 or vice versa
        sig1.nodes.iter().all(|n| sig2.nodes.contains(n))
            || sig2.nodes.iter().all(|n| sig1.nodes.contains(n))
    }
}

