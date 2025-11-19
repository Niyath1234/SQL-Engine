use crate::execution::batch::ExecutionBatch;
use crate::query::cache::QuerySignature;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Cache for query results
pub struct ResultCache {
    /// Map from query signature to cached result
    cache: HashMap<QuerySignature, CachedResult>,
    
    /// Maximum number of results to cache
    max_size: usize,
    
    /// TTL in seconds
    ttl_seconds: u64,
}

struct CachedResult {
    batches: Vec<ExecutionBatch>,
    timestamp: u64,
}

impl ResultCache {
    pub fn new(max_size: usize, ttl_seconds: u64) -> Self {
        Self {
            cache: HashMap::new(),
            max_size,
            ttl_seconds,
        }
    }
    
    /// Get cached result
    pub fn get(&self, signature: &QuerySignature) -> Option<Vec<ExecutionBatch>> {
        let result = self.cache.get(signature)?;
        
        // Check TTL
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        if now - result.timestamp > self.ttl_seconds {
            return None;
        }
        
        Some(result.batches.iter().cloned().collect())
    }
    
    /// Insert result
    pub fn insert(&mut self, signature: QuerySignature, batches: Vec<ExecutionBatch>) {
        if self.cache.len() >= self.max_size {
            self.evict_oldest();
        }
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        self.cache.insert(signature, CachedResult { batches, timestamp });
    }
    
    /// Invalidate cache for a table (when table is updated)
    pub fn invalidate_table(&mut self, table: &str) {
        // TODO: Remove entries that reference this table
        self.cache.clear(); // Simple: clear all for now
    }
    
    /// Clear all cached results (for benchmarking)
    pub fn clear(&mut self) {
        self.cache.clear();
    }
    
    /// Get cache size
    pub fn len(&self) -> usize {
        self.cache.len()
    }
    
    fn evict_oldest(&mut self) {
        let oldest_key = self.cache
            .iter()
            .min_by_key(|(_, result)| result.timestamp)
            .map(|(key, _)| key.clone());
        
        if let Some(key) = oldest_key {
            self.cache.remove(&key);
        }
    }
}

