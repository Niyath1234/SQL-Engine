/// Count-Min sketch for approximate frequency estimation
/// Provides fast approximate counts with bounded error
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

/// Count-Min sketch for frequency estimation
pub struct CountMin {
    /// Buckets: k rows × m columns
    pub buckets: Vec<u64>,
    /// Number of hash functions (rows)
    pub k: usize,
    /// Number of buckets per hash function (columns)
    pub m: usize,
}

impl CountMin {
    /// Create a new Count-Min sketch
    /// k: number of hash functions (typically 3-5)
    /// m: number of buckets per hash function (typically 2^16 to 2^20)
    pub fn new(k: usize, m: usize) -> Self {
        Self {
            buckets: vec![0; m * k],
            k,
            m,
        }
    }
    
    /// Add a value to the sketch
    pub fn add(&mut self, value: &str) {
        // Hash the value k times with different seeds
        for i in 0..self.k {
            let hash = self.hash(value, i);
            let idx = i * self.m + (hash % self.m as u64) as usize;
            self.buckets[idx] = self.buckets[idx].saturating_add(1);
        }
    }
    
    /// Add a value with a count
    pub fn add_count(&mut self, value: &str, count: u64) {
        for i in 0..self.k {
            let hash = self.hash(value, i);
            let idx = i * self.m + (hash % self.m as u64) as usize;
            self.buckets[idx] = self.buckets[idx].saturating_add(count);
        }
    }
    
    /// Estimate the frequency of a value
    /// Returns the minimum count across all k hash functions
    pub fn estimate(&self, value: &str) -> u64 {
        let mut min_count = u64::MAX;
        
        for i in 0..self.k {
            let hash = self.hash(value, i);
            let idx = i * self.m + (hash % self.m as u64) as usize;
            min_count = min_count.min(self.buckets[idx]);
        }
        
        min_count
    }
    
    /// Hash function with seed for different hash functions
    fn hash(&self, value: &str, seed: usize) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        value.hash(&mut hasher);
        hasher.finish()
    }
    
    /// Get the number of hash functions
    pub fn k(&self) -> usize {
        self.k
    }
    
    /// Get the number of buckets per hash function
    pub fn m(&self) -> usize {
        self.m
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_count_min_add_estimate() {
        let mut sketch = CountMin::new(3, 1024);
        
        // Add values
        for _ in 0..100 {
            sketch.add("key1");
        }
        for _ in 0..50 {
            sketch.add("key2");
        }
        
        // Estimate frequencies
        let est1 = sketch.estimate("key1");
        let est2 = sketch.estimate("key2");
        
        // Estimates should be close to actual counts
        // Count-Min is an overestimate, so estimates >= actual
        assert!(est1 >= 100, "Count-Min estimate should be >= actual count");
        assert!(est2 >= 50, "Count-Min estimate should be >= actual count");
    }
    
    #[test]
    fn test_count_min_add_count() {
        let mut sketch = CountMin::new(3, 1024);
        
        sketch.add_count("key1", 100);
        sketch.add_count("key2", 50);
        
        let est1 = sketch.estimate("key1");
        let est2 = sketch.estimate("key2");
        
        assert!(est1 >= 100);
        assert!(est2 >= 50);
    }
}

