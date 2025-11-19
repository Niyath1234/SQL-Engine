/// ε-Cost Sharding for distributed hypergraph execution
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::NodeId;
use crate::hypergraph::edge::EdgeId;
use crate::hypergraph::path::HyperPath;
use std::collections::{HashMap, HashSet};
use anyhow::Result;

/// Cost model for sharding
pub struct ShardingCostModel {
    /// Cost per local access
    local_cost: f64,
    /// Cost per remote access
    remote_cost: f64,
    /// Cost per network transfer (per byte)
    network_cost_per_byte: f64,
}

impl Default for ShardingCostModel {
    fn default() -> Self {
        Self {
            local_cost: 1.0,
            remote_cost: 10.0,
            network_cost_per_byte: 0.001,
        }
    }
}

/// ε-Cost sharding algorithm
pub struct EpsilonCostSharding {
    graph: HyperGraph,
    cost_model: ShardingCostModel,
    /// Access frequency per node
    node_frequency: HashMap<NodeId, usize>,
    /// Access frequency per edge
    edge_frequency: HashMap<EdgeId, usize>,
}

impl EpsilonCostSharding {
    pub fn new(graph: HyperGraph) -> Self {
        Self {
            graph,
            cost_model: ShardingCostModel::default(),
            node_frequency: HashMap::new(),
            edge_frequency: HashMap::new(),
        }
    }
    
    /// Compute ε-cost sharding for N partitions
    pub fn compute_sharding(&self, num_partitions: usize) -> Result<ShardingPlan> {
        // Build cost graph
        let cost_graph = self.build_cost_graph()?;
        
        // Use greedy algorithm to assign nodes to partitions
        let mut assignment = HashMap::new();
        let mut partition_loads = vec![0.0; num_partitions];
        
        // Sort nodes by access frequency (descending)
        let mut nodes_by_freq: Vec<(NodeId, usize)> = self.node_frequency
            .iter()
            .map(|(id, freq)| (*id, *freq))
            .collect();
        nodes_by_freq.sort_by_key(|(_, freq)| std::cmp::Reverse(*freq));
        
        // Assign nodes to partitions
        for (node_id, freq) in nodes_by_freq {
            // Find partition with lowest cost
            let best_partition = self.find_best_partition(
                node_id,
                &assignment,
                &partition_loads,
                &cost_graph,
            )?;
            
            assignment.insert(node_id, best_partition);
            partition_loads[best_partition] += freq as f64;
        }
        
        // Assign edges based on node assignments
        let edge_assignment = self.assign_edges(&assignment)?;
        
        Ok(ShardingPlan {
            node_assignment: assignment,
            edge_assignment,
            partition_loads,
        })
    }
    
    /// Build cost graph from hypergraph
    fn build_cost_graph(&self) -> Result<CostGraph> {
        let mut edges = HashMap::new();
        
        // For each edge, compute cost of placing nodes on different partitions
        for edge_id in self.edge_frequency.keys() {
            if let Some(edge) = self.graph.get_edge(*edge_id) {
                let cost = self.compute_edge_cost(edge.source, edge.target, *edge_id)?;
                edges.insert(*edge_id, cost);
            }
        }
        
        Ok(CostGraph { edges })
    }
    
    /// Compute cost of an edge (higher = more expensive to split)
    fn compute_edge_cost(&self, _source: NodeId, _target: NodeId, edge_id: EdgeId) -> Result<f64> {
        let freq = self.edge_frequency.get(&edge_id).copied().unwrap_or(1);
        
        // Cost increases with frequency and data size
        // If nodes are on different partitions, we pay remote cost
        Ok(freq as f64 * self.cost_model.remote_cost)
    }
    
    /// Find best partition for a node
    fn find_best_partition(
        &self,
        node_id: NodeId,
        assignment: &HashMap<NodeId, usize>,
        partition_loads: &[f64],
        cost_graph: &CostGraph,
    ) -> Result<usize> {
        let mut best_partition = 0;
        let mut best_cost = f64::INFINITY;
        
        for partition in 0..partition_loads.len() {
            let cost = self.compute_assignment_cost(
                node_id,
                partition,
                assignment,
                partition_loads,
                cost_graph,
            )?;
            
            if cost < best_cost {
                best_cost = cost;
                best_partition = partition;
            }
        }
        
        Ok(best_partition)
    }
    
    /// Compute cost of assigning node to partition
    fn compute_assignment_cost(
        &self,
        node_id: NodeId,
        partition: usize,
        assignment: &HashMap<NodeId, usize>,
        partition_loads: &[f64],
        cost_graph: &CostGraph,
    ) -> Result<f64> {
        let mut cost = partition_loads[partition];
        
        // Add cost for edges connecting to nodes in other partitions
        let outgoing_edges = self.graph.get_outgoing_edges(node_id);
        for edge in outgoing_edges {
            if let Some(target_partition) = assignment.get(&edge.target) {
                if *target_partition != partition {
                    // Remote edge - add cost
                    if let Some(edge_cost) = cost_graph.edges.get(&edge.id) {
                        cost += edge_cost;
                    }
                }
            }
        }
        
        Ok(cost)
    }
    
    /// Assign edges to partitions based on node assignments
    fn assign_edges(&self, node_assignment: &HashMap<NodeId, usize>) -> Result<HashMap<EdgeId, usize>> {
        let mut edge_assignment = HashMap::new();
        
        for edge_id in self.edge_frequency.keys() {
            if let Some(edge) = self.graph.get_edge(*edge_id) {
                // Assign edge to partition of source node (or target if source not assigned)
                let partition = node_assignment
                    .get(&edge.source)
                    .or_else(|| node_assignment.get(&edge.target))
                    .copied()
                    .unwrap_or(0);
                
                edge_assignment.insert(*edge_id, partition);
            }
        }
        
        Ok(edge_assignment)
    }
    
    /// Update access frequency for a node
    pub fn record_node_access(&mut self, node_id: NodeId) {
        *self.node_frequency.entry(node_id).or_insert(0) += 1;
    }
    
    /// Update access frequency for an edge
    pub fn record_edge_access(&mut self, edge_id: EdgeId) {
        *self.edge_frequency.entry(edge_id).or_insert(0) += 1;
    }
    
    /// Update access frequency from path usage
    pub fn update_from_path(&mut self, path: &HyperPath) {
        for &node_id in &path.nodes {
            self.record_node_access(node_id);
        }
        for &edge_id in &path.edges {
            self.record_edge_access(edge_id);
        }
    }
}

struct CostGraph {
    edges: HashMap<EdgeId, f64>,
}

/// Sharding plan
pub struct ShardingPlan {
    /// Node to partition assignment
    pub node_assignment: HashMap<NodeId, usize>,
    /// Edge to partition assignment
    pub edge_assignment: HashMap<EdgeId, usize>,
    /// Load per partition
    pub partition_loads: Vec<f64>,
}

impl ShardingPlan {
    /// Get partition for a node
    pub fn get_partition(&self, node_id: NodeId) -> usize {
        self.node_assignment.get(&node_id).copied().unwrap_or(0)
    }
    
    /// Check if nodes are co-located
    pub fn are_colocated(&self, node1: NodeId, node2: NodeId) -> bool {
        self.get_partition(node1) == self.get_partition(node2)
    }
    
    /// Get nodes in a partition
    pub fn get_partition_nodes(&self, partition: usize) -> Vec<NodeId> {
        self.node_assignment
            .iter()
            .filter(|(_, &p)| p == partition)
            .map(|(&id, _)| id)
            .collect()
    }
}

