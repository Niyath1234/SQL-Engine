use crate::hypergraph::node::NodeId;
use crate::hypergraph::edge::EdgeId;
use std::hash::{Hash, Hasher};
use ahash::AHasher;

/// Partitions hypergraph across multiple nodes
pub struct Partitioner {
    num_partitions: usize,
}

impl Partitioner {
    pub fn new(num_partitions: usize) -> Self {
        Self { num_partitions }
    }
    
    /// Partition a node ID
    pub fn partition_node(&self, node_id: NodeId) -> usize {
        self.hash_node(node_id) % self.num_partitions
    }
    
    /// Partition an edge ID
    pub fn partition_edge(&self, edge_id: EdgeId) -> usize {
        self.hash_edge(edge_id) % self.num_partitions
    }
    
    fn hash_node(&self, node_id: NodeId) -> usize {
        let mut hasher = AHasher::default();
        node_id.0.hash(&mut hasher);
        hasher.finish() as usize
    }
    
    fn hash_edge(&self, edge_id: EdgeId) -> usize {
        let mut hasher = AHasher::default();
        edge_id.0.hash(&mut hasher);
        hasher.finish() as usize
    }
}

