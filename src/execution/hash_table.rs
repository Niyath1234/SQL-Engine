/// Unified Hash Table API for DuckDB/Presto-grade join engine
/// 
/// This module provides a hash table structure with partitioning hooks
/// for efficient hash joins, supporting both in-memory and future partitioned modes.
use fxhash::FxHashMap;
use crate::storage::fragment::Value;

/// Hash table bucket - stores row references
/// 
/// For in-memory mode: stores encoded indices (batch_idx << 16 | row_idx)
/// For partitioned mode: stores partition-local indices
#[derive(Debug, Clone)]
pub struct Bucket {
    /// Row references in this bucket
    /// Encoded as: (batch_idx << 16) | row_idx
    pub row_refs: Vec<usize>,
}

impl Bucket {
    pub fn new() -> Self {
        Self {
            row_refs: Vec::new(),
        }
    }
    
    pub fn add(&mut self, row_ref: usize) {
        self.row_refs.push(row_ref);
    }
    
    pub fn len(&self) -> usize {
        self.row_refs.len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.row_refs.is_empty()
    }
}

/// Hash Table for join operations
/// 
/// Supports:
/// - In-memory single-partition mode (current)
/// - Partitioned/grace hash join (future)
/// 
/// Design:
/// - Uses FxHash for fast hashing
/// - Buckets store row references
/// - Partitioning hooks for future spill-to-disk
#[derive(Debug, Clone)]
pub struct HashTable {
    /// Hash buckets: key -> bucket
    buckets: FxHashMap<Value, Bucket>,
    
    /// Number of partitions (1 for in-memory, N for partitioned)
    num_partitions: usize,
    
    /// Total number of rows in hash table
    total_rows: usize,
}

impl HashTable {
    /// Create new in-memory hash table (single partition)
    pub fn new() -> Self {
        Self {
            buckets: FxHashMap::default(),
            num_partitions: 1,
            total_rows: 0,
        }
    }
    
    /// Create partitioned hash table (for future grace hash join)
    pub fn partitioned(num_partitions: usize) -> Self {
        Self {
            buckets: FxHashMap::default(),
            num_partitions,
            total_rows: 0,
        }
    }
    
    /// Insert a key-value pair into the hash table
    /// 
    /// # Arguments
    /// * `key` - Join key value
    /// * `row_ref` - Encoded row reference: (batch_idx << 16) | row_idx
    pub fn insert(&mut self, key: Value, row_ref: usize) {
        self.buckets
            .entry(key)
            .or_insert_with(Bucket::new)
            .add(row_ref);
        self.total_rows += 1;
    }
    
    /// Probe the hash table for a key
    /// 
    /// # Returns
    /// Vector of row references matching the key, or empty vector if not found
    pub fn probe(&self, key: &Value) -> Vec<usize> {
        self.buckets
            .get(key)
            .map(|bucket| bucket.row_refs.clone())
            .unwrap_or_default()
    }
    
    /// Get partition for a key (for partitioned hash join)
    /// 
    /// # Returns
    /// Partition index (0 to num_partitions - 1)
    pub fn partition_for_key(&self, key: &Value) -> usize {
        if self.num_partitions == 1 {
            return 0;
        }
        
        // Use key's hash to determine partition
        let hash = self.hash_key(key);
        (hash % self.num_partitions as u64) as usize
    }
    
    /// Fast hash function for key (using FxHash)
    fn hash_key(&self, key: &Value) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = fxhash::FxHasher::default();
        key.hash(&mut hasher);
        hasher.finish()
    }
    
    /// Get number of buckets
    pub fn num_buckets(&self) -> usize {
        self.buckets.len()
    }
    
    /// Get total number of rows
    pub fn total_rows(&self) -> usize {
        self.total_rows
    }
    
    /// Get number of partitions
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }
    
    /// Check if hash table is empty
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
    
    /// Clear all entries (for reuse)
    pub fn clear(&mut self) {
        self.buckets.clear();
        self.total_rows = 0;
    }
    
    /// Get memory usage estimate (rough)
    pub fn memory_usage_bytes(&self) -> usize {
        // Rough estimate: buckets overhead + row references
        let buckets_overhead = self.buckets.len() * std::mem::size_of::<Bucket>();
        let row_refs_size = self.total_rows * std::mem::size_of::<usize>();
        buckets_overhead + row_refs_size
    }
}

impl Default for HashTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Multi-key hash table support
/// 
/// For joins with multiple key columns, hash the tuple of keys
impl HashTable {
    /// Insert with multi-key (tuple of keys)
    pub fn insert_multi_key(&mut self, keys: &[Value], row_ref: usize) {
        // Create composite key by hashing all keys together
        let composite_key = self.hash_multi_key(keys);
        self.insert(composite_key, row_ref);
    }
    
    /// Probe with multi-key
    pub fn probe_multi_key(&self, keys: &[Value]) -> Vec<usize> {
        let composite_key = self.hash_multi_key(keys);
        self.probe(&composite_key)
    }
    
    /// Hash multiple keys into a single composite key
    fn hash_multi_key(&self, keys: &[Value]) -> Value {
        use std::hash::{Hash, Hasher};
        let mut hasher = fxhash::FxHasher::default();
        
        // Hash all keys together
        for key in keys {
            key.hash(&mut hasher);
        }
        
        // Use hash as composite key (store as Int64 for now)
        // In production, might use a proper composite key type
        Value::Int64(hasher.finish() as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_hash_table_insert_probe() {
        let mut ht = HashTable::new();
        
        ht.insert(Value::Int64(1), 100);
        ht.insert(Value::Int64(1), 200);
        ht.insert(Value::Int64(2), 300);
        
        let results = ht.probe(&Value::Int64(1));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&100));
        assert!(results.contains(&200));
        
        let results = ht.probe(&Value::Int64(2));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 300);
    }
    
    #[test]
    fn test_hash_table_multi_key() {
        let mut ht = HashTable::new();
        
        ht.insert_multi_key(&[Value::Int64(1), Value::Int64(10)], 100);
        ht.insert_multi_key(&[Value::Int64(1), Value::Int64(10)], 200);
        
        let results = ht.probe_multi_key(&[Value::Int64(1), Value::Int64(10)]);
        assert_eq!(results.len(), 2);
    }
}

