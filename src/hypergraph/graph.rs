use crate::hypergraph::node::{HyperNode, NodeId};
use crate::hypergraph::edge::{HyperEdge, EdgeId};
use crate::hypergraph::path::{HyperPath, PathId, PathSignature};
use crate::storage::fragment::ColumnFragment;
use dashmap::DashMap;
use std::sync::Arc;
use std::collections::HashMap;

/// The main hypergraph structure
/// Stores all nodes, edges, and paths
pub struct HyperGraph {
    /// Map from node ID to node
    nodes: DashMap<NodeId, Arc<HyperNode>>,
    
    /// Map from edge ID to edge
    edges: DashMap<EdgeId, Arc<HyperEdge>>,
    
    /// Adjacency list: node_id -> list of outgoing edge IDs
    adjacency: DashMap<NodeId, Vec<EdgeId>>,
    
    /// Reverse adjacency: node_id -> list of incoming edge IDs
    reverse_adjacency: DashMap<NodeId, Vec<EdgeId>>,
    
    /// Path cache for reusing paths
    path_cache: Arc<dashmap::DashMap<PathSignature, HyperPath>>,
    
    /// Map from (table, column) to node ID
    table_column_map: DashMap<(String, String), NodeId>,
    
    /// Map from table name to table node ID (for O(1) table lookups)
    pub(crate) table_index: DashMap<String, NodeId>,
    
    /// Hot fragment statistics: (node_id, fragment_idx) -> stats
    fragment_stats: DashMap<(NodeId, usize), FragmentStats>,
    
    /// Next node ID
    next_node_id: std::sync::atomic::AtomicU64,
    
    /// Next edge ID
    next_edge_id: std::sync::atomic::AtomicU64,
    
    /// Next path ID
    next_path_id: std::sync::atomic::AtomicU64,
}

/// Hot fragment statistics
#[derive(Clone, Debug)]
pub struct FragmentStats {
    /// Number of times this fragment has been accessed
    pub access_count: u64,
    /// Last access time (nanoseconds since UNIX epoch)
    pub last_access_ns: u64,
    /// Approximate size in bytes
    pub bytes: usize,
}

impl HyperGraph {
    pub fn new() -> Self {
        Self {
            nodes: DashMap::new(),
            edges: DashMap::new(),
            adjacency: DashMap::new(),
            reverse_adjacency: DashMap::new(),
            path_cache: Arc::new(dashmap::DashMap::new()),
            table_column_map: DashMap::new(),
            table_index: DashMap::new(),
            fragment_stats: DashMap::new(),
            next_node_id: std::sync::atomic::AtomicU64::new(1),
            next_edge_id: std::sync::atomic::AtomicU64::new(1),
            next_path_id: std::sync::atomic::AtomicU64::new(1),
        }
    }
    
    /// Add a node to the graph
    pub fn add_node(&self, node: HyperNode) -> NodeId {
        let id = node.id;
        
        // Index by table name for table nodes (NodeType::Table)
        // Normalize to lowercase for case-insensitive lookups
        if matches!(node.node_type, crate::hypergraph::node::NodeType::Table) {
            if let Some(table) = &node.table_name {
                self.table_index.insert(table.to_lowercase(), id);
            }
        }
        
        // Index by (table, column) for column nodes
        if let (Some(table), Some(col)) = (node.table_name.clone(), node.column_name.clone()) {
            self.table_column_map.insert((table, col), id);
        }
        
        self.nodes.insert(id, Arc::new(node));
        id
    }
    
    /// Get table node by table name (O(1) lookup with fallback to O(n))
    /// Table name is normalized to lowercase for case-insensitive lookups
    pub fn get_table_node(&self, table_name: &str) -> Option<Arc<HyperNode>> {
        let normalized = table_name.to_lowercase();
        
        // Try index lookup first (fast path)
        if let Some(node_id) = self.table_index.get(&normalized) {
            return self.get_node(*node_id.value());
        }
        
        // Fallback: search all nodes (in case index wasn't populated correctly)
        for (_, node) in self.iter_nodes() {
            if matches!(node.node_type, crate::hypergraph::node::NodeType::Table) {
                if let Some(table) = &node.table_name {
                    if table.to_lowercase() == normalized {
                        // Found it! Re-index it for future lookups
                        self.table_index.insert(normalized.clone(), node.id);
                        return Some(node);
                    }
                }
            }
        }
        
        None
    }
    
    /// Rebuild the table_index by scanning all nodes
    /// This is useful after loading nodes from persistent storage
    pub fn rebuild_table_index(&self) {
        self.table_index.clear();
        for (_, node) in self.iter_nodes() {
            if matches!(node.node_type, crate::hypergraph::node::NodeType::Table) {
                if let Some(table) = &node.table_name {
                    self.table_index.insert(table.to_lowercase(), node.id);
                }
            }
        }
    }
    
    /// Get column nodes for a table (O(n) but faster with index)
    pub fn get_column_nodes(&self, table_name: &str) -> Vec<Arc<HyperNode>> {
        let mut result = Vec::new();
        // Normalize table name for case-insensitive lookup
        let normalized_table = table_name.to_lowercase();
        // Use table_column_map to find all columns for this table
        for entry in self.table_column_map.iter() {
            let ((table, _), node_id) = (entry.key(), entry.value());
            // Normalize comparison for case-insensitive matching
            if table.to_lowercase() == normalized_table {
                if let Some(node) = self.get_node(*node_id) {
                    if matches!(node.node_type, crate::hypergraph::node::NodeType::Column) {
                        result.push(node);
                    }
                }
            }
        }
        result
    }
    
    /// Get a node by ID
    pub fn get_node(&self, id: NodeId) -> Option<Arc<HyperNode>> {
        self.nodes.get(&id).map(|entry| entry.clone())
    }
    
    /// Get node by table and column name
    pub fn get_node_by_table_column(&self, table: &str, column: &str) -> Option<Arc<HyperNode>> {
        let id = self.table_column_map.get(&(table.to_string(), column.to_string()))?;
        self.get_node(*id.value())
    }
    
    /// Record access to a fragment (for hot-fragment statistics)
    pub fn record_fragment_access(&self, node_id: NodeId, fragment_idx: usize, bytes: usize) {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        
        let key = (node_id, fragment_idx);
        self.fragment_stats
            .entry(key)
            .and_modify(|stats| {
                stats.access_count += 1;
                stats.last_access_ns = now_ns;
                stats.bytes = bytes;
            })
            .or_insert(FragmentStats {
                access_count: 1,
                last_access_ns: now_ns,
                bytes,
            });
    }
    
    /// Get statistics for a specific fragment
    pub fn get_fragment_stats(&self, node_id: NodeId, fragment_idx: usize) -> Option<FragmentStats> {
        self.fragment_stats.get(&(node_id, fragment_idx)).map(|e| e.clone())
    }
    
    /// Get top-N hottest fragments by access_count
    pub fn hot_fragments(&self, top_n: usize) -> Vec<((NodeId, usize), FragmentStats)> {
        let mut entries: Vec<_> = self.fragment_stats.iter().map(|e| (*e.key(), e.value().clone())).collect();
        entries.sort_by_key(|(_, stats)| std::cmp::Reverse(stats.access_count));
        entries.truncate(top_n);
        entries
    }
    
    /// Add an edge to the graph
    pub fn add_edge(&self, edge: HyperEdge) -> EdgeId {
        let id = edge.id;
        let source = edge.source;
        let target = edge.target;
        
        self.edges.insert(id, Arc::new(edge));
        
        // Update adjacency lists
        self.adjacency
            .entry(source)
            .or_insert_with(Vec::new)
            .push(id);
        
        self.reverse_adjacency
            .entry(target)
            .or_insert_with(Vec::new)
            .push(id);
        
        id
    }
    
    /// Get an edge by ID
    pub fn get_edge(&self, id: EdgeId) -> Option<Arc<HyperEdge>> {
        self.edges.get(&id).map(|entry| entry.clone())
    }
    
    /// Get outgoing edges from a node
    pub fn get_outgoing_edges(&self, node_id: NodeId) -> Vec<Arc<HyperEdge>> {
        self.adjacency
            .get(&node_id)
            .map(|entry| {
                entry
                    .iter()
                    .filter_map(|edge_id| self.get_edge(*edge_id))
                    .collect()
            })
            .unwrap_or_default()
    }
    
    /// Get incoming edges to a node
    pub fn get_incoming_edges(&self, node_id: NodeId) -> Vec<Arc<HyperEdge>> {
        self.reverse_adjacency
            .get(&node_id)
            .map(|entry| {
                entry
                    .iter()
                    .filter_map(|edge_id| self.get_edge(*edge_id))
                    .collect()
            })
            .unwrap_or_default()
    }
    
    /// Find a path between nodes (BFS traversal)
    pub fn find_path(&self, start: NodeId, end: NodeId) -> Option<Vec<EdgeId>> {
        use std::collections::{VecDeque, HashSet};
        
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let mut parent = HashMap::new();
        
        queue.push_back(start);
        visited.insert(start);
        
        while let Some(current) = queue.pop_front() {
            if current == end {
                // Reconstruct path
                let mut path = vec![];
                let mut node = end;
                while let Some((prev_node, edge_id)) = parent.get(&node) {
                    path.push(*edge_id);
                    node = *prev_node;
                    if node == start {
                        break;
                    }
                }
                path.reverse();
                return Some(path);
            }
            
            for edge in self.get_outgoing_edges(current) {
                let next = edge.target;
                if !visited.contains(&next) {
                    visited.insert(next);
                    parent.insert(next, (current, edge.id));
                    queue.push_back(next);
                }
            }
        }
        
        None
    }
    
    /// Add a path to the cache
    pub fn cache_path(&self, signature: PathSignature, path: HyperPath) {
        self.path_cache.insert(signature, path);
    }
    
    /// Get a cached path
    pub fn get_cached_path(&self, signature: &PathSignature) -> Option<HyperPath> {
        self.path_cache.get(signature).map(|entry| entry.clone())
    }
    
    /// Clean up old path cache entries
    /// Removes paths that haven't been used in max_age_seconds
    pub fn cleanup_path_cache(&self, max_age_seconds: u64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Collect expired paths
        let expired_signatures: Vec<PathSignature> = self.path_cache
            .iter()
            .filter(|entry| {
                let path = entry.value();
                let age = now.saturating_sub(path.last_used);
                age > max_age_seconds
            })
            .map(|entry| entry.key().clone())
            .collect();
        
        // Remove expired paths
        for signature in expired_signatures {
            self.path_cache.remove(&signature);
        }
    }
    
    /// Generate next node ID
    pub fn next_node_id(&self) -> NodeId {
        NodeId(self.next_node_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
    
    /// Generate next edge ID
    pub fn next_edge_id(&self) -> EdgeId {
        EdgeId(self.next_edge_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
    
    /// Generate next path ID
    pub fn next_path_id(&self) -> PathId {
        PathId(self.next_path_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
    
    /// Update a node's fragments (for incremental updates)
    pub fn update_node_fragments(&self, node_id: NodeId, fragments: Vec<ColumnFragment>) {
        if let Some(mut node) = self.nodes.get_mut(&node_id) {
            let mut new_node = (**node).clone();
            new_node.fragments = fragments;
            new_node.update_stats();
            *node = Arc::new(new_node);
        }
    }
    
    /// Update a node's metadata (for schema and alias storage)
    pub fn update_node_metadata(&self, node_id: NodeId, metadata_updates: HashMap<String, String>) {
        if let Some(mut node) = self.nodes.get_mut(&node_id) {
            let mut new_node = (**node).clone();
            for (key, value) in metadata_updates {
                new_node.metadata.insert(key, value);
            }
            *node = Arc::new(new_node);
        }
    }
    
    /// Get table aliases from node metadata (alias -> table name)
    pub fn get_table_aliases_from_metadata(&self, table_name: &str) -> HashMap<String, String> {
        if let Some(table_node) = self.get_table_node(table_name) {
            table_node.metadata.get("table_aliases")
                .and_then(|s| serde_json::from_str::<HashMap<String, String>>(s).ok())
                .unwrap_or_default()
        } else {
            HashMap::new()
        }
    }
    
    /// Get alias names for a table from node metadata
    pub fn get_alias_names_from_metadata(&self, table_name: &str) -> Vec<String> {
        if let Some(table_node) = self.get_table_node(table_name) {
            table_node.metadata.get("alias_names")
                .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    }
    
    /// Resolve alias to actual table name using stored metadata
    pub fn resolve_alias_to_table(&self, alias: &str) -> Option<String> {
        // Try to find the table node that has this alias in its metadata
        for node_entry in self.nodes.iter() {
            let node = node_entry.value();
            if matches!(node.node_type, crate::hypergraph::node::NodeType::Table) {
                if let Some(aliases_json) = node.metadata.get("table_aliases") {
                    if let Ok(aliases) = serde_json::from_str::<HashMap<String, String>>(aliases_json) {
                        if aliases.contains_key(alias) {
                            return aliases.get(alias).cloned();
                        }
                    }
                }
            }
        }
        None
    }
    
    /// Remove a node from the graph (for DROP TABLE)
    /// Also removes all related edges and updates indexes
    pub fn remove_node(&self, node_id: NodeId) -> anyhow::Result<()> {
        // Get the node first to extract metadata
        let node = if let Some(n) = self.nodes.get(&node_id) {
            n.clone()
        } else {
            return Ok(()); // Node doesn't exist, nothing to remove
        };
        
        // Remove from table_index if it's a table node
        if matches!(node.node_type, crate::hypergraph::node::NodeType::Table) {
            if let Some(table_name) = &node.table_name {
                self.table_index.remove(&table_name.to_lowercase());
            }
        }
        
        // Remove from table_column_map if it's a column node
        if let (Some(table_name), Some(column_name)) = (&node.table_name, &node.column_name) {
            self.table_column_map.remove(&(table_name.clone(), column_name.clone()));
        }
        
        // Remove all edges connected to this node
        let outgoing_edges: Vec<EdgeId> = self.adjacency.get(&node_id)
            .map(|entry| entry.value().clone())
            .unwrap_or_default();
        let incoming_edges: Vec<EdgeId> = self.reverse_adjacency.get(&node_id)
            .map(|entry| entry.value().clone())
            .unwrap_or_default();
        
        // Remove edges from edge map
        for edge_id in outgoing_edges.iter().chain(incoming_edges.iter()) {
            if let Some(edge) = self.edges.get(edge_id) {
                // Remove from adjacency lists of connected nodes
                if let Some(mut adj_list) = self.adjacency.get_mut(&edge.source) {
                    adj_list.retain(|&id| id != *edge_id);
                }
                if let Some(mut adj_list) = self.reverse_adjacency.get_mut(&edge.target) {
                    adj_list.retain(|&id| id != *edge_id);
                }
            }
            self.edges.remove(edge_id);
        }
        
        // Remove adjacency list entries for this node
        self.adjacency.remove(&node_id);
        self.reverse_adjacency.remove(&node_id);
        
        // Remove the node itself
        self.nodes.remove(&node_id);
        
        // If this is a table node, also remove all column nodes for this table
        if matches!(node.node_type, crate::hypergraph::node::NodeType::Table) {
            if let Some(table_name) = &node.table_name {
                let column_nodes: Vec<NodeId> = self.get_column_nodes(table_name)
                    .iter()
                    .map(|n| n.id)
                    .collect();
                
                for col_node_id in column_nodes {
                    self.remove_node(col_node_id)?;
                }
            }
        }
        
        // Remove fragment statistics
        let fragment_keys: Vec<(NodeId, usize)> = self.fragment_stats.iter()
            .filter(|entry| entry.key().0 == node_id)
            .map(|entry| *entry.key())
            .collect();
        for key in fragment_keys {
            self.fragment_stats.remove(&key);
        }
        
        Ok(())
    }
    
    /// Iterate over all nodes (for DROP TABLE and other operations)
    pub fn iter_nodes(&self) -> impl Iterator<Item = (NodeId, Arc<HyperNode>)> + '_ {
        self.nodes.iter().map(|entry| (*entry.key(), entry.value().clone()))
    }
    
    /// Iterate over all edges (for coarsening/compression)
    pub fn iter_edges(&self) -> impl Iterator<Item = (EdgeId, Arc<HyperEdge>)> + '_ {
        self.edges.iter().map(|entry| (*entry.key(), entry.value().clone()))
    }
    
    /// Get node count
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
    
    /// Get edge count
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }
}

impl Clone for HyperGraph {
    fn clone(&self) -> Self {
        // Create a new graph and copy all nodes and edges
        let mut new_graph = Self::new();
        
        // Copy nodes
        for entry in self.nodes.iter() {
            new_graph.nodes.insert(*entry.key(), entry.value().clone());
        }
        
        // Copy edges
        for entry in self.edges.iter() {
            new_graph.edges.insert(*entry.key(), entry.value().clone());
        }
        
        // Copy adjacency lists
        for entry in self.adjacency.iter() {
            new_graph.adjacency.insert(*entry.key(), entry.value().clone());
        }
        
        // Copy reverse adjacency
        for entry in self.reverse_adjacency.iter() {
            new_graph.reverse_adjacency.insert(*entry.key(), entry.value().clone());
        }
        
        // Copy table column map
        for entry in self.table_column_map.iter() {
            new_graph.table_column_map.insert(entry.key().clone(), *entry.value());
        }
        
        // Share path cache
        new_graph.path_cache = self.path_cache.clone();
        
        // Copy atomic counters
        let node_id = self.next_node_id.load(std::sync::atomic::Ordering::SeqCst);
        new_graph.next_node_id.store(node_id, std::sync::atomic::Ordering::SeqCst);
        
        let edge_id = self.next_edge_id.load(std::sync::atomic::Ordering::SeqCst);
        new_graph.next_edge_id.store(edge_id, std::sync::atomic::Ordering::SeqCst);
        
        let path_id = self.next_path_id.load(std::sync::atomic::Ordering::SeqCst);
        new_graph.next_path_id.store(path_id, std::sync::atomic::Ordering::SeqCst);
        
        new_graph
    }
}

impl Default for HyperGraph {
    fn default() -> Self {
        Self::new()
    }
}

