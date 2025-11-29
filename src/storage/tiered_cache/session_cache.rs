//! Level 2: Session Cache
//!
//! Session-scoped cache that:
//! - Holds larger query results and plans
//! - Gets refreshed per session
//! - Promotes frequently-used patterns to Level 1 (persistent)

use crate::execution::batch::ExecutionBatch;
use crate::query::cache::QuerySignature;
use crate::hypergraph::path::{HyperPath, PathSignature};
use crate::storage::tiered_cache::persistent_hypergraph::{PersistentHypergraph, CompactPath};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Session cache entry
struct SessionCacheEntry {
    /// Cached batches (larger data)
    batches: Vec<ExecutionBatch>,
    
    /// Timestamp when cached
    timestamp: u64,
    
    /// Access count (for promotion to Level 1)
    access_count: u32,
    
    /// Last access time
    last_accessed: u64,
}

/// Session-level cache (Level 2)
pub struct SessionCache {
    /// Query result cache
    result_cache: HashMap<QuerySignature, SessionCacheEntry>,
    
    /// Path cache (larger join patterns)
    path_cache: HashMap<PathSignature, (HyperPath, u32)>, // Path + access count
    
    /// Maximum size (entries)
    max_size: usize,
    
    /// Maximum memory (bytes)
    max_memory_bytes: usize,
    
    /// Current memory usage
    current_memory_bytes: usize,
    
    /// Promotion threshold (access count needed to promote to Level 1)
    promotion_threshold: u32,
}

impl SessionCache {
    pub fn new() -> Self {
        Self {
            result_cache: HashMap::new(),
            path_cache: HashMap::new(),
            max_size: 1000, // Max 1000 entries
            max_memory_bytes: 200 * 1024 * 1024, // 200MB max
            current_memory_bytes: 0,
            promotion_threshold: 5, // Promote after 5 accesses
        }
    }
    
    /// Get cached result
    pub fn get_result(&mut self, signature: &QuerySignature) -> Option<Vec<ExecutionBatch>> {
        let entry = self.result_cache.get_mut(signature)?;
        
        // Update access tracking
        entry.access_count += 1;
        entry.last_accessed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        Some(entry.batches.clone())
    }
    
    /// Cache result
    pub fn cache_result(&mut self, signature: QuerySignature, batches: Vec<ExecutionBatch>) {
        let memory_size = Self::estimate_memory_size(&batches);
        
        // Evict if needed
        while (self.result_cache.len() >= self.max_size ||
               (self.current_memory_bytes + memory_size) > self.max_memory_bytes) &&
              !self.result_cache.is_empty() {
            self.evict_oldest_result();
        }
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Remove old entry if exists
        if let Some(old_entry) = self.result_cache.remove(&signature) {
            self.current_memory_bytes -= Self::estimate_memory_size(&old_entry.batches);
        }
        
        self.result_cache.insert(signature, SessionCacheEntry {
            batches,
            timestamp,
            access_count: 1,
            last_accessed: timestamp,
        });
        
        self.current_memory_bytes += memory_size;
    }
    
    /// Get cached path
    pub fn get_path(&mut self, signature: &PathSignature) -> Option<HyperPath> {
        let (path, access_count) = self.path_cache.get_mut(signature)?;
        *access_count += 1;
        Some(path.clone())
    }
    
    /// Cache path
    pub fn cache_path(&mut self, signature: PathSignature, path: HyperPath) {
        let access_count = self.path_cache.get(&signature)
            .map(|(_, count)| *count)
            .unwrap_or(0);
        
        self.path_cache.insert(signature, (path, access_count));
    }
    
    /// Get items ready for promotion (access count >= threshold)
    pub fn get_promotion_candidates(&self) -> Vec<(PathSignature, HyperPath)> {
        self.path_cache.iter()
            .filter(|(_, (_, count))| *count >= self.promotion_threshold)
            .map(|(sig, (path, _))| (sig.clone(), path.clone()))
            .collect()
    }
    
    /// Clear session cache (called on session end)
    pub fn clear(&mut self) {
        self.result_cache.clear();
        self.path_cache.clear();
        self.current_memory_bytes = 0;
    }
    
    /// Evict oldest result entry
    fn evict_oldest_result(&mut self) {
        let oldest_key = self.result_cache.iter()
            .min_by_key(|(_, entry)| entry.last_accessed)
            .map(|(key, _)| key.clone());
        
        if let Some(key) = oldest_key {
            if let Some(entry) = self.result_cache.remove(&key) {
                self.current_memory_bytes -= Self::estimate_memory_size(&entry.batches);
            }
        }
    }
    
    /// Estimate memory size of batches
    fn estimate_memory_size(batches: &[ExecutionBatch]) -> usize {
        batches.iter().map(|batch| {
            let schema_size = batch.batch.schema.fields().len() * 64;
            let data_size: usize = batch.batch.columns.iter().map(|col| {
                let len = col.len();
                match col.data_type() {
                    arrow::datatypes::DataType::Int64 => len * 8,
                    arrow::datatypes::DataType::Float64 => len * 8,
                    arrow::datatypes::DataType::Utf8 => len * 20,
                    _ => len * 8,
                }
            }).sum();
            schema_size + data_size
        }).sum()
    }
}

