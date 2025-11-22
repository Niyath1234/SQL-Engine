/// HNSW Graph Implementation
/// Phase 3: Proper hierarchical navigable small world graph for fast ANN search
use std::collections::{HashMap, HashSet, BinaryHeap};
use std::cmp::{Ordering, Reverse};
use ordered_float::OrderedFloat;
use rand::Rng;
use anyhow::Result;

/// HNSW Graph structure
#[derive(Debug)]
pub struct HNSWGraph {
    /// Maximum level in the graph
    pub max_level: usize,
    /// Graph structure: layer -> (node_id -> neighbors)
    pub graph: Vec<HashMap<usize, Vec<usize>>>,
    /// Entry point (top layer node)
    pub entry_point: Option<usize>,
    /// Level for each node (exponential distribution)
    pub node_levels: Vec<usize>,
    /// M: Maximum connections per node per layer
    pub m: usize,
    /// M_max0: Maximum connections for layer 0
    pub m_max0: usize,
}

impl HNSWGraph {
    /// Create a new HNSW graph
    pub fn new(m: usize, m_max0: usize) -> Self {
        Self {
            max_level: 0,
            graph: Vec::new(),
            entry_point: None,
            node_levels: Vec::new(),
            m,
            m_max0,
        }
    }
    
    /// Insert a node into the graph
    pub fn insert(&mut self, node_id: usize, vector: &[f32], dimension: usize, vectors: &[Vec<f32>]) -> Result<()> {
        // Assign level to this node (exponential distribution)
        let level = self.random_level();
        self.node_levels.push(level);
        
        // Ensure we have enough layers
        while self.graph.len() <= level {
            self.graph.push(HashMap::new());
        }
        
        // If this is the first node, it becomes the entry point
        if node_id == 0 {
            self.entry_point = Some(0);
            self.max_level = level;
            for l in 0..=level {
                self.graph[l].insert(node_id, Vec::new());
            }
            return Ok(());
        }
        
        // Find nearest neighbors starting from entry point
        let mut current_closest = self.entry_point;
        
        // Search from top layer down to find good starting point
        for layer in (0..=self.max_level.min(level)).rev() {
            if let Some(start) = current_closest {
                let candidates = self.search_layer_simple(vector, start, vectors, 1, layer)?;
                current_closest = candidates.first().map(|(idx, _)| *idx);
            }
        }
        
        // Insert at each layer up to node's level
        for layer in 0..=level.min(self.max_level) {
            // Find candidates using greedy search
            let candidates = if layer == 0 {
                if let Some(start) = current_closest {
                    self.search_layer_simple(vector, start, vectors, 10, 0)?
                } else {
                    Vec::new()
                }
            } else {
                if let Some(start) = current_closest {
                    self.search_layer_simple(vector, start, vectors, 10, layer)?
                } else {
                    Vec::new()
                }
            };
            
            // Select neighbors (heuristic: take top M)
            let m_actual = if layer == 0 { self.m_max0 } else { self.m };
            let neighbors: Vec<usize> = candidates.iter()
                .take(m_actual)
                .map(|(idx, _)| *idx)
                .collect();
            
            // Initialize node at this layer
            if !self.graph[layer].contains_key(&node_id) {
                self.graph[layer].insert(node_id, Vec::new());
            }
            
            // Add bidirectional links
            for &neighbor_id in &neighbors {
                // Add neighbor -> node link
                if !self.graph[layer].contains_key(&neighbor_id) {
                    self.graph[layer].insert(neighbor_id, Vec::new());
                }
                
                if let Some(neighbor_neighbors) = self.graph[layer].get_mut(&neighbor_id) {
                    if !neighbor_neighbors.contains(&node_id) {
                        neighbor_neighbors.push(node_id);
                        
                        // Prune if exceeds M_max
                        if neighbor_neighbors.len() > self.m_max0 {
                            neighbor_neighbors.truncate(self.m_max0);
                        }
                    }
                }
                
                // Add node -> neighbor link
                if let Some(node_neighbors) = self.graph[layer].get_mut(&node_id) {
                    if !node_neighbors.contains(&neighbor_id) {
                        node_neighbors.push(neighbor_id);
                    }
                }
            }
            
            // Prune node's neighbors if needed
            if let Some(node_neighbors) = self.graph[layer].get_mut(&node_id) {
                let m_limit = if layer == 0 { self.m_max0 } else { self.m };
                if node_neighbors.len() > m_limit {
                    node_neighbors.truncate(m_limit);
                }
            }
        }
        
        // If this node has higher level than max, update entry point
        if level > self.max_level {
            self.max_level = level;
            self.entry_point = Some(node_id);
        }
        
        Ok(())
    }
    
    /// Search for k nearest neighbors using HNSW graph
    pub fn search(
        &self,
        query: &[f32],
        vectors: &[Vec<f32>],
        k: usize,
        ef: usize,
        metric: crate::storage::vector_index::VectorMetric,
    ) -> Vec<(usize, f32)> {
        if vectors.is_empty() || self.entry_point.is_none() {
            return Vec::new();
        }
        
        let entry = self.entry_point.unwrap();
        let mut current_closest = Some(entry);
        
        // Search from top layer down to layer 1
        for layer in (1..=self.max_level).rev() {
            if layer < self.graph.len() {
                if let Some(start) = current_closest {
                    let candidates = self.search_layer_simple(query, start, vectors, 1, layer)
                        .unwrap_or_default();
                    current_closest = candidates.first().map(|(idx, _)| *idx);
                }
            }
        }
        
        // Final search at layer 0
        if let Some(start) = current_closest {
            let results = self.search_layer_simple(query, start, vectors, ef.max(k), 0)
                .unwrap_or_default();
            
            // Convert to similarity and return top k
            let mut results_with_similarity: Vec<(usize, f32)> = results.into_iter()
                .take(k)
                .map(|(idx, distance)| {
                    // Convert distance to similarity based on metric
                    let similarity = match metric {
                        crate::storage::vector_index::VectorMetric::Cosine => 1.0 - distance,
                        crate::storage::vector_index::VectorMetric::L2 => 1.0 / (1.0 + distance),
                        crate::storage::vector_index::VectorMetric::InnerProduct => -distance, // Inner product as distance
                    };
                    (idx, similarity)
                })
                .collect();
            
            // Sort by similarity (descending)
            results_with_similarity.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
            
            results_with_similarity
        } else {
            Vec::new()
        }
    }
    
    /// Search at a specific layer using greedy search
    fn search_layer_simple(
        &self,
        query: &[f32],
        entry: usize,
        vectors: &[Vec<f32>],
        ef: usize,
        layer: usize,
    ) -> Result<Vec<(usize, f32)>> {
        if layer >= self.graph.len() || entry >= vectors.len() {
            return Ok(Vec::new());
        }
        
        if !self.graph[layer].contains_key(&entry) {
            return Ok(Vec::new());
        }
        
        // Greedy search with priority queue
        let mut visited = HashSet::new();
        let mut candidates: BinaryHeap<Reverse<(OrderedFloat<f32>, usize)>> = BinaryHeap::new();
        let mut dynamic_list: BinaryHeap<(OrderedFloat<f32>, usize)> = BinaryHeap::new();
        
        // Start from entry point
        let entry_dist = self.compute_distance(query, &vectors[entry], crate::storage::vector_index::VectorMetric::Cosine);
        candidates.push(Reverse((OrderedFloat(entry_dist), entry)));
        dynamic_list.push((OrderedFloat(entry_dist), entry));
        visited.insert(entry);
        
        let mut current_best = entry;
        let mut current_best_dist = entry_dist;
        
        // Greedy search: keep finding better neighbors
        loop {
            let mut found_better = false;
            
            // Check neighbors of current best
            if let Some(neighbors) = self.graph[layer].get(&current_best) {
                for &neighbor_id in neighbors {
                    if visited.contains(&neighbor_id) || neighbor_id >= vectors.len() {
                        continue;
                    }
                    
                    visited.insert(neighbor_id);
                    let dist = self.compute_distance(query, &vectors[neighbor_id], crate::storage::vector_index::VectorMetric::Cosine);
                    
                    if dist < current_best_dist {
                        current_best = neighbor_id;
                        current_best_dist = dist;
                        found_better = true;
                    }
                    
                    candidates.push(Reverse((OrderedFloat(dist), neighbor_id)));
                    
                    // Add to dynamic list
                    dynamic_list.push((OrderedFloat(dist), neighbor_id));
                    
                    // Keep only ef best candidates
                    if dynamic_list.len() > ef {
                        dynamic_list.pop();
                    }
                }
            }
            
            if !found_better {
                break;
            }
        }
        
        // Extract results from dynamic list
        let mut results: Vec<(usize, f32)> = dynamic_list.into_iter()
            .map(|(dist, idx)| (idx, dist.into_inner()))
            .collect();
        
        // Sort by distance (ascending)
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        
        Ok(results)
    }
    
    /// Compute distance between vectors based on metric
    fn compute_distance(
        &self,
        a: &[f32],
        b: &[f32],
        metric: crate::storage::vector_index::VectorMetric,
    ) -> f32 {
        match metric {
            crate::storage::vector_index::VectorMetric::Cosine => {
                // Cosine distance = 1 - cosine_similarity
                let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm_a == 0.0 || norm_b == 0.0 {
                    return 1.0;
                }
                1.0 - (dot_product / (norm_a * norm_b))
            }
            crate::storage::vector_index::VectorMetric::L2 => {
                // L2 (Euclidean) distance
                a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum::<f32>().sqrt()
            }
            crate::storage::vector_index::VectorMetric::InnerProduct => {
                // Inner product as distance (negated)
                -a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>()
            }
        }
    }
    
    /// Random level assignment (exponential distribution)
    fn random_level(&self) -> usize {
        // Probability decreases by 1/M per level
        let mut rng = rand::thread_rng();
        let mut level = 0;
        while rng.gen::<f64>() < 1.0 / (self.m as f64) && level < 16 {
            level += 1;
        }
        level
    }
}

