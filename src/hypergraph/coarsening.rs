/// Hypergraph Coarsening - Merge nodes and edges to reduce granularity
/// Improves query performance by reducing graph size while preserving semantics
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::{HyperNode, NodeId};
use crate::hypergraph::edge::{HyperEdge, EdgeId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Coarsened hypergraph with merged nodes/edges
pub struct CoarsenedHyperGraph {
    /// Original graph
    original: HyperGraph,
    
    /// Mapping from original node IDs to coarsened node IDs
    node_mapping: HashMap<NodeId, NodeId>,
    
    /// Mapping from original edge IDs to coarsened edge IDs
    edge_mapping: HashMap<EdgeId, EdgeId>,
    
    /// Coarsened nodes (merged nodes)
    coarsened_nodes: HashMap<NodeId, CoarsenedNode>,
    
    /// Coarsened edges (merged edges)
    coarsened_edges: HashMap<EdgeId, CoarsenedEdge>,
}

/// A coarsened node represents multiple merged nodes
struct CoarsenedNode {
    /// Original node IDs that were merged
    original_nodes: Vec<NodeId>,
    
    /// Representative node (one of the original nodes)
    representative: NodeId,
    
    /// Aggregated statistics
    aggregated_stats: NodeStatistics,
}

/// A coarsened edge represents multiple merged edges
struct CoarsenedEdge {
    /// Original edge IDs that were merged
    original_edges: Vec<EdgeId>,
    
    /// Representative edge
    representative: EdgeId,
    
    /// Aggregated statistics
    aggregated_stats: EdgeStatistics,
}

/// Statistics for a coarsened node
#[derive(Clone)]
struct NodeStatistics {
    total_rows: usize,
    total_fragments: usize,
    avg_cardinality: f64,
}

/// Statistics for a coarsened edge
#[derive(Clone)]
struct EdgeStatistics {
    total_joins: usize,
    avg_selectivity: f64,
}

/// Hypergraph coarsening algorithm
pub struct HypergraphCoarsener {
    /// Coarsening level (higher = more aggressive merging)
    coarsening_level: f64,
    
    /// Minimum similarity threshold for merging nodes
    similarity_threshold: f64,
}

impl HypergraphCoarsener {
    pub fn new(coarsening_level: f64, similarity_threshold: f64) -> Self {
        Self {
            coarsening_level: coarsening_level.clamp(0.0, 1.0),
            similarity_threshold: similarity_threshold.clamp(0.0, 1.0),
        }
    }
    
    /// Coarsen a hypergraph by merging similar nodes
    pub fn coarsen(&self, graph: &HyperGraph) -> CoarsenedHyperGraph {
        let mut node_mapping = HashMap::new();
        let mut edge_mapping = HashMap::new();
        let mut coarsened_nodes = HashMap::new();
        let mut coarsened_edges = HashMap::new();
        
        // Step 1: Identify nodes to merge based on similarity
        let node_groups = self.identify_mergeable_nodes(graph);
        
        // Step 2: Create coarsened nodes
        for (group_id, node_ids) in node_groups.iter().enumerate() {
            if node_ids.is_empty() {
                continue;
            }
            
            let coarsened_node_id = NodeId(group_id as u64 + 1000000); // Use high IDs for coarsened nodes
            let representative = node_ids[0];
            
            // Map all original nodes to coarsened node
            for &node_id in node_ids {
                node_mapping.insert(node_id, coarsened_node_id);
            }
            
            // Aggregate statistics
            let aggregated_stats = self.aggregate_node_stats(graph, node_ids);
            
            coarsened_nodes.insert(
                coarsened_node_id,
                CoarsenedNode {
                    original_nodes: node_ids.clone(),
                    representative,
                    aggregated_stats,
                },
            );
        }
        
        // Step 3: Merge edges between coarsened nodes
        let edge_groups = self.identify_mergeable_edges(graph, &node_mapping);
        
        for (group_id, edge_ids) in edge_groups.iter().enumerate() {
            if edge_ids.is_empty() {
                continue;
            }
            
            let coarsened_edge_id = EdgeId(group_id as u64 + 1000000);
            let representative = edge_ids[0];
            
            for &edge_id in edge_ids {
                edge_mapping.insert(edge_id, coarsened_edge_id);
            }
            
            let aggregated_stats = self.aggregate_edge_stats(graph, edge_ids);
            
            coarsened_edges.insert(
                coarsened_edge_id,
                CoarsenedEdge {
                    original_edges: edge_ids.clone(),
                    representative,
                    aggregated_stats,
                },
            );
        }
        
        CoarsenedHyperGraph {
            original: graph.clone(),
            node_mapping,
            edge_mapping,
            coarsened_nodes,
            coarsened_edges,
        }
    }
    
    /// Identify nodes that should be merged
    fn identify_mergeable_nodes(&self, graph: &HyperGraph) -> Vec<Vec<NodeId>> {
        let mut groups = Vec::new();
        let mut processed: HashSet<NodeId> = HashSet::new();
        
        // Group nodes by table (simple strategy)
        let mut table_groups: HashMap<Option<String>, Vec<NodeId>> = HashMap::new();
        
        for (_node_id, node) in graph.iter_nodes() {
            if processed.contains(&node.id) {
                continue;
            }
            
            let table_key = node.table_name.clone();
            table_groups.entry(table_key).or_insert_with(Vec::new).push(node.id);
        }
        
        // Merge nodes within same table if they're similar
        for (_, node_ids) in table_groups {
            if node_ids.len() <= 1 {
                continue;
            }
            
            // Check similarity and merge if above threshold
            let mut current_group = vec![node_ids[0]];
            processed.insert(node_ids[0]);
            
            for &node_id in node_ids.iter().skip(1) {
                if processed.contains(&node_id) {
                    continue;
                }
                
                // Check if similar to any node in current group
                let similar = current_group.iter().any(|&existing_id| {
                    self.nodes_similar(graph, existing_id, node_id)
                });
                
                if similar {
                    current_group.push(node_id);
                    processed.insert(node_id);
                }
            }
            
            if current_group.len() > 1 {
                groups.push(current_group);
            }
        }
        
        groups
    }
    
    
    /// Compute similarity between node statistics
    fn compute_stat_similarity(
        &self,
        stats1: &crate::hypergraph::node::NodeStatistics,
        stats2: &crate::hypergraph::node::NodeStatistics,
    ) -> f64 {
        // Simple similarity: compare row counts and cardinality
        let row_diff = (stats1.row_count as f64 - stats2.row_count as f64).abs();
        let max_rows = stats1.row_count.max(stats2.row_count) as f64;
        let row_similarity = if max_rows > 0.0 {
            1.0 - (row_diff / max_rows).min(1.0)
        } else {
            1.0
        };
        
        let card_diff = (stats1.cardinality as f64 - stats2.cardinality as f64).abs();
        let max_card = stats1.cardinality.max(stats2.cardinality) as f64;
        let card_similarity = if max_card > 0.0 {
            1.0 - (card_diff / max_card).min(1.0)
        } else {
            1.0
        };
        
        (row_similarity + card_similarity) / 2.0
    }
    
    /// Identify edges that should be merged
    fn identify_mergeable_edges(
        &self,
        graph: &HyperGraph,
        node_mapping: &HashMap<NodeId, NodeId>,
    ) -> Vec<Vec<EdgeId>> {
        let mut groups = Vec::new();
        let mut processed: HashSet<EdgeId> = HashSet::new();
        
        // Group edges by their coarsened source/target nodes
        let mut edge_groups: HashMap<(NodeId, NodeId), Vec<EdgeId>> = HashMap::new();
        
        for (_edge_id, edge) in graph.iter_edges() {
            if processed.contains(&edge.id) {
                continue;
            }
            
            let source_coarse = node_mapping.get(&edge.source).copied().unwrap_or(edge.source);
            let target_coarse = node_mapping.get(&edge.target).copied().unwrap_or(edge.target);
            
            edge_groups
                .entry((source_coarse, target_coarse))
                .or_insert_with(Vec::new)
                .push(edge.id);
        }
        
        // Create groups for edges with same coarsened endpoints
        for (_, edge_ids) in edge_groups {
            if edge_ids.len() > 1 {
                groups.push(edge_ids);
            }
        }
        
        groups
    }
    
    /// Aggregate statistics for merged nodes
    fn aggregate_node_stats(
        &self,
        graph: &HyperGraph,
        node_ids: &[NodeId],
    ) -> NodeStatistics {
        let mut total_rows = 0;
        let mut total_fragments = 0;
        let mut total_cardinality = 0.0;
        
        for &node_id in node_ids {
            if let Some(node) = graph.get_node(node_id) {
                total_rows += node.stats.row_count;
                total_fragments += node.fragments.len();
                total_cardinality += node.stats.cardinality as f64;
            }
        }
        
        let count = node_ids.len() as f64;
        
        NodeStatistics {
            total_rows,
            total_fragments,
            avg_cardinality: if count > 0.0 {
                total_cardinality / count
            } else {
                0.0
            },
        }
    }
    
    /// Aggregate statistics for merged edges
    fn aggregate_edge_stats(
        &self,
        graph: &HyperGraph,
        edge_ids: &[EdgeId],
    ) -> EdgeStatistics {
        let mut total_joins = 0;
        let mut total_selectivity = 0.0;
        
        for &edge_id in edge_ids {
            if let Some(edge) = graph.get_edge(edge_id) {
                total_joins += 1;
                total_selectivity += edge.stats.selectivity;
            }
        }
        
        let count = edge_ids.len() as f64;
        
        EdgeStatistics {
            total_joins,
            avg_selectivity: if count > 0.0 {
                total_selectivity / count
            } else {
                0.0
            },
        }
    }
}

impl CoarsenedHyperGraph {
    /// Get coarsened node for an original node
    pub fn get_coarsened_node(&self, original_id: NodeId) -> Option<&CoarsenedNode> {
        let coarsened_id = self.node_mapping.get(&original_id)?;
        self.coarsened_nodes.get(coarsened_id)
    }
    
    /// Get coarsened edge for an original edge
    pub fn get_coarsened_edge(&self, original_id: EdgeId) -> Option<&CoarsenedEdge> {
        let coarsened_id = self.edge_mapping.get(&original_id)?;
        self.coarsened_edges.get(coarsened_id)
    }
    
    /// Get compression ratio (how much smaller the coarsened graph is)
    pub fn compression_ratio(&self) -> f64 {
        // Count nodes using public method
        let original_nodes = self.original.node_count();
        let coarsened_nodes = self.coarsened_nodes.len();
        
        if original_nodes == 0 {
            return 1.0;
        }
        
        coarsened_nodes as f64 / original_nodes as f64
    }
}

// Helper to fix Option return type
impl HypergraphCoarsener {
    fn nodes_similar(&self, graph: &HyperGraph, node1_id: NodeId, node2_id: NodeId) -> bool {
        if let (Some(node1), Some(node2)) = (graph.get_node(node1_id), graph.get_node(node2_id)) {
            // Similarity based on:
            // 1. Same table
            if node1.table_name != node2.table_name {
                return false;
            }
            
            // 2. Similar statistics
            let stat_similarity = self.compute_stat_similarity(&node1.stats, &node2.stats);
            
            stat_similarity >= self.similarity_threshold
        } else {
            false
        }
    }
}

