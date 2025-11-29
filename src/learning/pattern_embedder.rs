/// Convert query patterns to vector embeddings for similarity search
use crate::learning::pattern_extractor::QueryPattern;
use crate::hypergraph::node::NodeId;
use std::collections::HashMap;
use anyhow::Result;

/// Converts query patterns to vector embeddings
pub struct PatternEmbedder {
    /// Embedding dimension (tunable)
    pub embedding_dim: usize,
    
    /// Learned weights (initialized, then updated over time)
    pub table_weights: HashMap<String, Vec<f32>>,
    pub column_weights: HashMap<String, Vec<f32>>,
    pub join_weights: HashMap<(String, String), Vec<f32>>,
    
    /// Weight initialization seed (for deterministic initial weights)
    weight_seed: u64,
}

impl PatternEmbedder {
    /// Create new embedder with specified dimension
    pub fn new(embedding_dim: usize) -> Self {
        Self {
            embedding_dim,
            table_weights: HashMap::new(),
            column_weights: HashMap::new(),
            join_weights: HashMap::new(),
            weight_seed: 12345, // Deterministic seed
        }
    }
    
    /// Generate vector embedding from query pattern
    pub fn embed_pattern(&mut self, pattern: &QueryPattern) -> Vec<f32> {
        let mut embedding = vec![0.0f32; self.embedding_dim];
        
        // 1. Table presence encoding (sparse -> dense)
        for table in &pattern.tables {
            let weights = self.get_or_init_table_weights(table);
            for i in 0..self.embedding_dim {
                embedding[i] += weights[i];
            }
        }
        
        // 2. Column access frequency encoding
        for (col, freq) in &pattern.column_access_frequency {
            let weights = self.get_or_init_column_weights(col);
            let freq_f32 = *freq as f32;
            for i in 0..self.embedding_dim {
                embedding[i] += weights[i] * freq_f32;
            }
        }
        
        // 3. Join relationship encoding
        for join in &pattern.join_relationships {
            let key = (join.left_table.clone(), join.right_table.clone());
            let weights = self.get_or_init_join_weights(&key);
            let join_weight = (join.frequency as f32) * join.selectivity as f32;
            for i in 0..self.embedding_dim {
                embedding[i] += weights[i] * join_weight;
            }
        }
        
        // 4. Hypergraph path encoding
        let path_encoding = self.encode_hypergraph_path(&pattern.hypergraph_path);
        for i in 0..self.embedding_dim.min(path_encoding.len()) {
            embedding[i] += path_encoding[i];
        }
        
        // 5. Temporal encoding (circular encoding for time)
        let temporal_encoding = self.encode_temporal(pattern.time_of_day as f32);
        for i in 0..self.embedding_dim.min(temporal_encoding.len()) {
            embedding[i] += temporal_encoding[i];
        }
        
        // 6. Filter pattern encoding (simplified)
        let filter_encoding = self.encode_filters(&pattern.filter_patterns);
        for i in 0..self.embedding_dim.min(filter_encoding.len()) {
            embedding[i] += filter_encoding[i];
        }
        
        // Normalize embedding
        self.normalize(&mut embedding);
        embedding
    }
    
    /// Get or initialize table weights
    fn get_or_init_table_weights(&mut self, table: &str) -> Vec<f32> {
        // Check if weights exist, otherwise initialize and store
        if let Some(weights) = self.table_weights.get(table) {
            weights.clone()
        } else {
            let weights = self.init_random_weights(table);
            self.table_weights.insert(table.to_string(), weights.clone());
            weights
        }
    }
    
    /// Get or initialize column weights
    fn get_or_init_column_weights(&mut self, column: &str) -> Vec<f32> {
        if let Some(weights) = self.column_weights.get(column) {
            weights.clone()
        } else {
            let weights = self.init_random_weights(column);
            self.column_weights.insert(column.to_string(), weights.clone());
            weights
        }
    }
    
    /// Get or initialize join weights
    fn get_or_init_join_weights(&mut self, key: &(String, String)) -> Vec<f32> {
        if let Some(weights) = self.join_weights.get(key) {
            weights.clone()
        } else {
            let key_str = format!("{}:{}", key.0, key.1);
            let weights = self.init_random_weights(&key_str);
            self.join_weights.insert(key.clone(), weights.clone());
            weights
        }
    }
    
    /// Initialize random weights (deterministic based on key)
    fn init_random_weights(&self, key: &str) -> Vec<f32> {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        
        // Hash key for deterministic initialization
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        
        // Simple deterministic random initialization
        let mut weights = Vec::with_capacity(self.embedding_dim);
        let mut seed = hash.wrapping_mul(self.weight_seed);
        
        for _ in 0..self.embedding_dim {
            // Simple PRNG: xorshift
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            
            // Convert to float in [-0.1, 0.1]
            let val = ((seed % 2000) as f32 - 1000.0) / 10000.0;
            weights.push(val);
        }
        
        weights
    }
    
    /// Encode hypergraph path as vector
    fn encode_hypergraph_path(&self, path: &[NodeId]) -> Vec<f32> {
        // Simple encoding: path length and node IDs
        let mut encoding = vec![0.0f32; self.embedding_dim.min(16)];
        
        // Path length encoding
        if !encoding.is_empty() {
            encoding[0] = (path.len() as f32) / 100.0; // Normalize
        }
        
        // Node ID encoding (simplified)
        for (i, node_id) in path.iter().take(15).enumerate() {
            if i + 1 < encoding.len() {
                // Simple hash-based encoding
                let node_id_u64 = match *node_id {
                    crate::hypergraph::node::NodeId(id) => id,
                };
                let hash = node_id_u64.wrapping_mul(2654435761);
                encoding[i + 1] = ((hash % 1000) as f32) / 1000.0;
            }
        }
        
        encoding
    }
    
    /// Encode temporal patterns (circular encoding for time)
    fn encode_temporal(&self, time_of_day: f32) -> Vec<f32> {
        // Circular encoding for time (sine/cosine)
        let hour_rad = (time_of_day * 2.0 * std::f32::consts::PI) / 24.0;
        
        vec![
            hour_rad.sin(),
            hour_rad.cos(),
            // Add more harmonics for better encoding
            (hour_rad * 2.0).sin(),
            (hour_rad * 2.0).cos(),
        ]
    }
    
    /// Encode filter patterns
    fn encode_filters(&self, filters: &[crate::learning::optimization_hints::FilterPattern]) -> Vec<f32> {
        // Simple encoding: filter count and column diversity
        let mut encoding = vec![0.0f32; 8];
        
        if !encoding.is_empty() {
            encoding[0] = (filters.len() as f32) / 10.0; // Normalize
        }
        
        // Column diversity
        let unique_columns: std::collections::HashSet<_> = filters
            .iter()
            .map(|f| f.column.clone())
            .collect();
        
        if encoding.len() > 1 {
            encoding[1] = (unique_columns.len() as f32) / 10.0;
        }
        
        encoding
    }
    
    /// Normalize embedding vector
    fn normalize(&self, embedding: &mut [f32]) {
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in embedding.iter_mut() {
                *x /= norm;
            }
        }
    }
}

impl Default for PatternEmbedder {
    fn default() -> Self {
        Self::new(128) // Default: 128 dimensions
    }
}

