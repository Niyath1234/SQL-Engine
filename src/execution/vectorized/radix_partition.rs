/// Radix Partitioning for Parallel Hash Join
/// 
/// This module provides radix-based partitioning for efficient parallel hash joins.
/// Radix partitioning uses the high-order bits of hash values to distribute keys
/// across partitions, providing better cache locality and predictable partition sizes.
use crate::storage::fragment::Value;
use crate::error::EngineResult;
use std::hash::{Hash, Hasher};
use fxhash::FxHasher;

/// Partition a value using radix bits
/// 
/// Extracts the high-order `radix_bits` from the hash value to determine partition.
/// This ensures that keys with similar hash values are distributed evenly.
pub fn radix_partition(value: &Value, radix_bits: usize, num_partitions: usize) -> usize {
    let hash = hash_value(value);
    // Extract radix bits from high-order bits of hash
    let partition = (hash >> (64 - radix_bits)) as usize;
    partition % num_partitions
}

/// Hash a value to u64
pub fn hash_value(value: &Value) -> u64 {
    let mut hasher = FxHasher::default();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Partition a batch of values using radix partitioning
pub fn partition_values(values: &[Value], radix_bits: usize, num_partitions: usize) -> Vec<Vec<usize>> {
    let mut partitions: Vec<Vec<usize>> = vec![Vec::new(); num_partitions];
    
    for (idx, value) in values.iter().enumerate() {
        let partition_idx = radix_partition(value, radix_bits, num_partitions);
        partitions[partition_idx].push(idx);
    }
    
    partitions
}

/// Calculate optimal radix bits for a given number of partitions
/// 
/// Returns the number of bits needed to represent `num_partitions` partitions.
/// For example, 16 partitions need 4 bits (2^4 = 16).
pub fn calculate_radix_bits(num_partitions: usize) -> usize {
    if num_partitions == 0 {
        return 0;
    }
    
    // Find the smallest power of 2 >= num_partitions
    let mut bits = 0;
    let mut power = 1;
    while power < num_partitions {
        power <<= 1;
        bits += 1;
    }
    
    bits
}

/// Partition statistics for monitoring partition balance
#[derive(Debug, Clone)]
pub struct PartitionStats {
    pub partition_sizes: Vec<usize>,
    pub min_size: usize,
    pub max_size: usize,
    pub avg_size: f64,
    pub std_dev: f64,
}

impl PartitionStats {
    /// Calculate statistics for partition sizes
    pub fn from_sizes(sizes: Vec<usize>) -> Self {
        if sizes.is_empty() {
            return Self {
                partition_sizes: sizes,
                min_size: 0,
                max_size: 0,
                avg_size: 0.0,
                std_dev: 0.0,
            };
        }
        
        // Safe to unwrap: we return early if sizes.is_empty()
        let min_size = *sizes.iter().min()
            .expect("min() should never return None after empty check");
        let max_size = *sizes.iter().max()
            .expect("max() should never return None after empty check");
        let sum: usize = sizes.iter().sum();
        let avg_size = sum as f64 / sizes.len() as f64;
        
        // Calculate standard deviation
        let variance: f64 = sizes.iter()
            .map(|&size| {
                let diff = size as f64 - avg_size;
                diff * diff
            })
            .sum::<f64>() / sizes.len() as f64;
        let std_dev = variance.sqrt();
        
        Self {
            partition_sizes: sizes,
            min_size,
            max_size,
            avg_size,
            std_dev,
        }
    }
    
    /// Check if partitions are balanced (within threshold)
    pub fn is_balanced(&self, threshold: f64) -> bool {
        if self.avg_size == 0.0 {
            return true;
        }
        let coefficient_of_variation = self.std_dev / self.avg_size;
        coefficient_of_variation < threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_radix_partition() {
        let value = Value::Int64(42);
        let partition = radix_partition(&value, 4, 16);
        assert!(partition < 16);
    }
    
    #[test]
    fn test_calculate_radix_bits() {
        assert_eq!(calculate_radix_bits(16), 4);
        assert_eq!(calculate_radix_bits(8), 3);
        assert_eq!(calculate_radix_bits(4), 2);
        assert_eq!(calculate_radix_bits(1), 0);
    }
    
    #[test]
    fn test_partition_values() {
        let values = vec![
            Value::Int64(1),
            Value::Int64(2),
            Value::Int64(3),
            Value::Int64(4),
        ];
        
        let partitions = partition_values(&values, 2, 4);
        assert_eq!(partitions.len(), 4);
        
        // All indices should be assigned
        let total: usize = partitions.iter().map(|p| p.len()).sum();
        assert_eq!(total, values.len());
    }
    
    #[test]
    fn test_partition_stats() {
        let sizes = vec![10, 12, 11, 13, 10];
        let stats = PartitionStats::from_sizes(sizes);
        
        assert_eq!(stats.min_size, 10);
        assert_eq!(stats.max_size, 13);
        assert!((stats.avg_size - 11.2).abs() < 0.1);
    }
}

