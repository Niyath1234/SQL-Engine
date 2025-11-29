/// Runtime Filters (Bloom Filters) for DuckDB/Presto-grade join engine
/// 
/// Bloom filters are constructed from join keys and propagated to earlier scans
/// to drastically improve multi-table join performance (20-100 joins).
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use bitvec::prelude::{bitvec, BitVec, Lsb0};
use crate::storage::fragment::Value;
use anyhow::{Result, anyhow};

/// Bloom filter for runtime filtering
/// 
/// A Bloom filter is a probabilistic data structure that can tell you
/// if an element is definitely not in a set, or possibly in a set.
/// 
/// For joins: After building hash table from right side, construct Bloom filter
/// from join keys. Propagate filter to earlier scans to filter out rows that
/// definitely won't match.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit array for the filter
    bits: BitVec<u8, Lsb0>,
    
    /// Number of hash functions
    num_hashes: usize,
    
    /// Number of bits in the filter
    num_bits: usize,
    
    /// Number of elements inserted
    num_inserted: usize,
}

impl BloomFilter {
    /// Create new Bloom filter
    /// 
    /// # Arguments
    /// * `num_bits` - Number of bits in the filter (should be ~10x expected elements)
    /// * `num_hashes` - Number of hash functions (typically 3-5)
    pub fn new(num_bits: usize, num_hashes: usize) -> Self {
        Self {
            bits: bitvec![u8, Lsb0; 0; num_bits],
            num_hashes,
            num_bits,
            num_inserted: 0,
        }
    }
    
    /// Create Bloom filter with default parameters
    /// 
    /// Uses 10 bits per expected element and 3 hash functions
    pub fn with_expected_size(expected_elements: usize) -> Self {
        let num_bits = (expected_elements * 10).max(1000); // At least 1000 bits
        let num_hashes = 3;
        Self::new(num_bits, num_hashes)
    }
    
    /// Insert a value into the Bloom filter
    pub fn insert(&mut self, value: &Value) {
        for i in 0..self.num_hashes {
            let hash = self.hash(value, i);
            let bit_idx = hash % self.num_bits;
            self.bits.set(bit_idx, true);
        }
        self.num_inserted += 1;
    }
    
    /// Insert multiple values (for batch insertion)
    pub fn insert_batch(&mut self, values: &[Value]) {
        for value in values {
            self.insert(value);
        }
    }
    
    /// Check if a value might be in the filter
    /// 
    /// # Returns
    /// - `true` if value might be in the filter (could be false positive)
    /// - `false` if value is definitely not in the filter (no false negatives)
    pub fn might_contain(&self, value: &Value) -> bool {
        for i in 0..self.num_hashes {
            let hash = self.hash(value, i);
            let bit_idx = hash % self.num_bits;
            if !self.bits[bit_idx] {
                return false; // Definitely not in filter
            }
        }
        true // Might be in filter
    }
    
    /// Hash a value with a specific hash function index
    fn hash(&self, value: &Value, hash_idx: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        (hash_idx as u64).hash(&mut hasher);
        (hasher.finish() % self.num_bits as u64) as usize
    }
    
    /// Get number of bits set
    pub fn bits_set(&self) -> usize {
        self.bits.count_ones()
    }
    
    /// Get false positive rate estimate
    /// 
    /// Formula: (1 - e^(-k*n/m))^k
    /// where k = num_hashes, n = num_inserted, m = num_bits
    pub fn false_positive_rate(&self) -> f64 {
        if self.num_inserted == 0 {
            return 0.0;
        }
        
        let k = self.num_hashes as f64;
        let n = self.num_inserted as f64;
        let m = self.num_bits as f64;
        
        let exponent = -k * n / m;
        (1.0 - exponent.exp()).powf(k)
    }
    
    /// Merge two Bloom filters (union)
    /// 
    /// Used when combining filters from multiple joins
    pub fn merge(&self, other: &Self) -> Result<Self> {
        if self.num_bits != other.num_bits || self.num_hashes != other.num_hashes {
            return Err(anyhow::anyhow!(
                "Cannot merge Bloom filters with different parameters"
            ));
        }
        
        let mut merged = Self::new(self.num_bits, self.num_hashes);
        // Bitwise OR the two bit vectors
        for i in 0..self.num_bits {
            merged.bits.set(i, self.bits[i] || other.bits[i]);
        }
        merged.num_inserted = self.num_inserted + other.num_inserted;
        
        Ok(merged)
    }
    
    /// Get memory usage in bytes
    pub fn memory_usage_bytes(&self) -> usize {
        self.num_bits / 8 + std::mem::size_of::<Self>()
    }
}

/// Multi-key Bloom filter (for joins with multiple key columns)
impl BloomFilter {
    /// Insert multiple keys (tuple of keys)
    pub fn insert_multi_key(&mut self, keys: &[Value]) {
        // Hash all keys together to create composite key
        let composite = self.hash_multi_key(keys);
        self.insert(&composite);
    }
    
    /// Check if multi-key might be in filter
    pub fn might_contain_multi_key(&self, keys: &[Value]) -> bool {
        let composite = self.hash_multi_key(keys);
        self.might_contain(&composite)
    }
    
    /// Hash multiple keys into a single composite value
    fn hash_multi_key(&self, keys: &[Value]) -> Value {
        let mut hasher = DefaultHasher::new();
        for key in keys {
            key.hash(&mut hasher);
        }
        // Use hash as composite key (store as Int64)
        Value::Int64(hasher.finish() as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_bloom_filter_insert_check() {
        let mut filter = BloomFilter::with_expected_size(100);
        
        filter.insert(&Value::Int64(1));
        filter.insert(&Value::Int64(2));
        filter.insert(&Value::Int64(3));
        
        assert!(filter.might_contain(&Value::Int64(1)));
        assert!(filter.might_contain(&Value::Int64(2)));
        assert!(filter.might_contain(&Value::Int64(3)));
        
        // False positives are possible, but false negatives are not
        // (If we didn't insert it, might_contain might still return true due to hash collision)
    }
    
    #[test]
    fn test_bloom_filter_multi_key() {
        let mut filter = BloomFilter::with_expected_size(100);
        
        filter.insert_multi_key(&[Value::Int64(1), Value::Int64(10)]);
        
        assert!(filter.might_contain_multi_key(&[Value::Int64(1), Value::Int64(10)]));
    }
}

