/// Join Graph representation for DuckDB/Presto-grade join engine
/// 
/// The join graph is a logical representation of all joins in a query.
/// It is unordered - the planner determines join order by walking the graph.
use std::collections::{HashMap, HashSet};
use crate::query::parser::JoinEdge;
use anyhow::{Result, Context};

/// Join Graph - logical representation of all joins
/// 
/// The join graph consists of:
/// - nodes: Table aliases (e.g., "c", "o", "p")
/// - edges: Join predicates connecting tables
/// 
/// This is a logical plan structure - the physical plan will determine join order.
#[derive(Debug, Clone)]
pub struct JoinGraph {
    /// Table aliases (nodes in the graph)
    pub nodes: Vec<String>,
    
    /// Join edges (connections between tables)
    pub edges: Vec<JoinEdge>,
    
    /// Node to edges mapping for fast lookup
    /// Maps table alias to indices of edges that connect to it
    node_to_edges: HashMap<String, Vec<usize>>,
}

impl JoinGraph {
    /// Create new empty join graph
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            node_to_edges: HashMap::new(),
        }
    }
    
    /// Create join graph from ParsedQuery
    /// 
    /// Extracts all join edges and table aliases from the parsed query.
    pub fn from_parsed_query(
        tables: &[String],
        join_edges: &[JoinEdge],
        _table_aliases: &HashMap<String, String>,
    ) -> Result<Self> {
        let mut graph = Self::new();
        
        // Add all tables as nodes
        for table in tables {
            graph.add_node(table.clone());
        }
        
        // Add all join edges
        for edge in join_edges {
            graph.add_edge(edge.clone())?;
        }
        
        // Validate graph is connected
        graph.validate_connected()
            .context("Join graph is disconnected - all tables must be connected via join edges")?;
        
        Ok(graph)
    }
    
    /// Add a node (table alias) to the graph
    pub fn add_node(&mut self, node: String) {
        if !self.nodes.contains(&node) {
            let node_clone = node.clone();
            self.nodes.push(node);
            self.node_to_edges.insert(node_clone, Vec::new());
        }
    }
    
    /// Add an edge (join predicate) to the graph
    pub fn add_edge(&mut self, edge: JoinEdge) -> Result<()> {
        // Ensure both nodes exist
        self.add_node(edge.left.table_alias.clone());
        self.add_node(edge.right.table_alias.clone());
        
        let edge_idx = self.edges.len();
        let left_alias = edge.left.table_alias.clone();
        let right_alias = edge.right.table_alias.clone();
        self.edges.push(edge);
        
        // Update node to edges mapping
        self.node_to_edges
            .get_mut(&left_alias)
            .unwrap()
            .push(edge_idx);
        self.node_to_edges
            .get_mut(&right_alias)
            .unwrap()
            .push(edge_idx);
        
        Ok(())
    }
    
    /// Get edges connected to a node
    pub fn edges_for_node(&self, node: &str) -> Vec<&JoinEdge> {
        self.node_to_edges
            .get(node)
            .map(|indices| {
                indices.iter().map(|&idx| &self.edges[idx]).collect()
            })
            .unwrap_or_default()
    }
    
    /// Get edges connecting two nodes
    pub fn edges_between(&self, left: &str, right: &str) -> Vec<&JoinEdge> {
        self.edges
            .iter()
            .filter(|edge| {
                (edge.left.table_alias == left && edge.right.table_alias == right) ||
                (edge.left.table_alias == right && edge.right.table_alias == left)
            })
            .collect()
    }
    
    /// Validate that the graph is connected
    /// 
    /// A connected graph means all tables can be reached from any starting table
    /// via join edges. Disconnected graphs indicate missing join predicates.
    pub fn validate_connected(&self) -> Result<()> {
        if self.nodes.is_empty() {
            return Ok(()); // Empty graph is trivially connected
        }
        
        if self.nodes.len() == 1 {
            return Ok(()); // Single node is trivially connected
        }
        
        // Use DFS to check connectivity
        let mut visited = HashSet::new();
        let start_node = &self.nodes[0];
        self.dfs_visit(start_node, &mut visited);
        
        if visited.len() == self.nodes.len() {
            Ok(())
        } else {
            let missing: Vec<String> = self.nodes
                .iter()
                .filter(|n| !visited.contains(*n))
                .cloned()
                .collect();
            Err(anyhow::anyhow!(
                "Join graph is disconnected. Tables not reachable from '{}': {:?}",
                start_node, missing
            ))
        }
    }
    
    /// DFS visit helper for connectivity check
    fn dfs_visit(&self, node: &str, visited: &mut HashSet<String>) {
        if visited.contains(node) {
            return;
        }
        
        visited.insert(node.to_string());
        
        // Visit all neighbors
        for edge in self.edges_for_node(node) {
            let neighbor = if edge.left.table_alias == node {
                &edge.right.table_alias
            } else {
                &edge.left.table_alias
            };
            
            if !visited.contains(neighbor) {
                self.dfs_visit(neighbor, visited);
            }
        }
    }
    
    /// Get number of nodes
    pub fn num_nodes(&self) -> usize {
        self.nodes.len()
    }
    
    /// Get number of edges
    pub fn num_edges(&self) -> usize {
        self.edges.len()
    }
    
    /// Check if graph is empty
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

impl Default for JoinGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_join_graph_creation() {
        use crate::query::parser::{JoinEdge, QualifiedColumn, JoinType};
        let mut graph = JoinGraph::new();
        graph.add_node("a".to_string());
        graph.add_node("b".to_string());
        
        let edge = JoinEdge {
            left: QualifiedColumn::new("a".to_string(), "id".to_string()),
            right: QualifiedColumn::new("b".to_string(), "a_id".to_string()),
            join_type: JoinType::Inner,
        };
        
        graph.add_edge(edge).unwrap();
        assert_eq!(graph.num_nodes(), 2);
        assert_eq!(graph.num_edges(), 1);
    }
    
    #[test]
    fn test_validate_connected() {
        use crate::query::parser::{JoinEdge, QualifiedColumn, JoinType};
        let mut graph = JoinGraph::new();
        graph.add_node("a".to_string());
        graph.add_node("b".to_string());
        graph.add_node("c".to_string());
        
        // Add edge connecting a and b
        let edge1 = JoinEdge {
            left: QualifiedColumn::new("a".to_string(), "id".to_string()),
            right: QualifiedColumn::new("b".to_string(), "a_id".to_string()),
            join_type: JoinType::Inner,
        };
        graph.add_edge(edge1).unwrap();
        
        // Graph is disconnected (c is not connected)
        assert!(graph.validate_connected().is_err());
        
        // Add edge connecting b and c
        let edge2 = JoinEdge {
            left: QualifiedColumn::new("b".to_string(), "id".to_string()),
            right: QualifiedColumn::new("c".to_string(), "b_id".to_string()),
            join_type: JoinType::Inner,
        };
        graph.add_edge(edge2).unwrap();
        
        // Graph is now connected
        assert!(graph.validate_connected().is_ok());
    }
}

