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
    
    /// Maximum memory usage in bytes (approximate)
    max_memory_bytes: usize,
    
    /// Current memory usage in bytes (approximate)
    current_memory_bytes: usize,
    
    /// TTL in seconds
    ttl_seconds: u64,
}

#[derive(Clone)]
struct CachedResult {
    batches: Vec<ExecutionBatch>,
    timestamp: u64,
}

impl ResultCache {
    pub fn new(max_size: usize, ttl_seconds: u64) -> Self {
        Self::new_with_memory_limit(max_size, ttl_seconds, 500 * 1024 * 1024) // Default 500MB
    }
    
    pub fn new_with_memory_limit(max_size: usize, ttl_seconds: u64, max_memory_bytes: usize) -> Self {
        Self {
            cache: HashMap::new(),
            max_size,
            max_memory_bytes,
            current_memory_bytes: 0,
            ttl_seconds,
        }
    }
    
    /// Get cached result
    /// Automatically removes expired entries when accessed
    pub fn get(&mut self, signature: &QuerySignature) -> Option<Vec<ExecutionBatch>> {
        let result = self.cache.get(signature)?;
        
        // Check TTL
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        if now - result.timestamp > self.ttl_seconds {
            // Entry expired - remove it automatically
            if let Some(removed) = self.cache.remove(signature) {
                let memory_size = Self::estimate_memory_size(&removed.batches);
                self.current_memory_bytes = self.current_memory_bytes.saturating_sub(memory_size);
            }
            return None;
        }
        
        Some(result.batches.iter().cloned().collect())
    }
    
    /// Clean up expired entries automatically
    /// Should be called periodically to free memory
    pub fn cleanup_expired(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Collect expired keys
        let expired_keys: Vec<QuerySignature> = self.cache
            .iter()
            .filter(|(_, result)| now - result.timestamp > self.ttl_seconds)
            .map(|(key, _)| key.clone())
            .collect();
        
        // Remove expired entries and update memory count
        for key in expired_keys {
            if let Some(result) = self.cache.remove(&key) {
                let memory_size = Self::estimate_memory_size(&result.batches);
                self.current_memory_bytes = self.current_memory_bytes.saturating_sub(memory_size);
            }
        }
    }
    
    /// Clear all cached results
    pub fn clear(&mut self) {
        self.cache.clear();
        self.current_memory_bytes = 0;
    }
    
    /// Estimate memory size of batches in bytes
    fn estimate_memory_size(batches: &[ExecutionBatch]) -> usize {
        batches.iter().map(|batch| {
            // Rough estimate: schema size + array data size
            let schema_size = batch.batch.schema.fields().len() * 64; // Rough estimate per field
            let data_size: usize = batch.batch.columns.iter().map(|col| {
                // Estimate based on array length and data type
                let len = col.len();
                match col.data_type() {
                    arrow::datatypes::DataType::Int64 => len * 8,
                    arrow::datatypes::DataType::Float64 => len * 8,
                    arrow::datatypes::DataType::Utf8 => {
                        // For strings, estimate average 20 bytes per string
                        len * 20
                    }
                    _ => len * 8, // Default estimate
                }
            }).sum();
            schema_size + data_size
        }).sum()
    }
    
    /// Insert result
    pub fn insert(&mut self, signature: QuerySignature, batches: Vec<ExecutionBatch>) {
        let new_memory_size = Self::estimate_memory_size(&batches);
        
        // Evict entries until we have space (both count and memory)
        while (self.cache.len() >= self.max_size || 
               (self.current_memory_bytes + new_memory_size) > self.max_memory_bytes) &&
              !self.cache.is_empty() {
            self.evict_oldest();
        }
        
        // If we're removing an existing entry, subtract its memory
        if let Some(old_result) = self.cache.get(&signature) {
            let old_memory_size = Self::estimate_memory_size(&old_result.batches);
            self.current_memory_bytes = self.current_memory_bytes.saturating_sub(old_memory_size);
        }
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        self.cache.insert(signature, CachedResult { batches, timestamp });
        self.current_memory_bytes += new_memory_size;
    }
    
    /// Invalidate cache for a table (when table is updated)
    pub fn invalidate_table(&mut self, _table: &str) {
        // TODO: Remove entries that reference this table
        self.clear(); // Use clear() method
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
            if let Some(result) = self.cache.remove(&key) {
                let memory_size = Self::estimate_memory_size(&result.batches);
                self.current_memory_bytes = self.current_memory_bytes.saturating_sub(memory_size);
            }
        }
    }
    
    /// Get current memory usage in bytes
    pub fn memory_usage_bytes(&self) -> usize {
        self.current_memory_bytes
    }
    
    /// Get maximum memory limit in bytes
    pub fn max_memory_bytes(&self) -> usize {
        self.max_memory_bytes
    }
}

