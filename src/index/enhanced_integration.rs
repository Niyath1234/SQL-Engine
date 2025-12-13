/// Phase 5: Enhanced Index Integration
/// 
/// Provides unified interface for all index types:
/// - Learned indexes (range queries)
/// - Sparse indexes (sparse data)
/// - Vector indexes (similarity search)
/// - Composite indexes (multi-column)
/// - Bitmap indexes (equality predicates)

use crate::storage::fragment::Value;
use anyhow::Result;

/// Index type enumeration
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexType {
    /// Learned index (ML-based position prediction)
    Learned,
    
    /// Sparse index (for sparse/time-series data)
    Sparse,
    
    /// Vector index (HNSW for similarity search)
    Vector,
    
    /// Composite index (multi-column)
    Composite,
    
    /// Bitmap index (equality predicates)
    Bitmap,
}

/// Index usage recommendation
#[derive(Clone, Debug)]
pub struct IndexRecommendation {
    /// Index type to use
    pub index_type: IndexType,
    
    /// Index name/identifier
    pub index_name: String,
    
    /// Expected speedup (e.g., 2.0 = 2× faster)
    pub expected_speedup: f64,
    
    /// Confidence (0.0 to 1.0)
    pub confidence: f64,
}

/// Index advisor - recommends best index for a query
pub struct IndexAdvisor;

impl IndexAdvisor {
    /// Recommend best index for a query pattern
    pub fn recommend_index(
        query_type: &str,
        columns: &[String],
        predicate_type: &str,
    ) -> Vec<IndexRecommendation> {
        let mut recommendations = Vec::new();
        
        match (query_type, predicate_type) {
            // Range queries → learned index
            ("range", _) => {
                recommendations.push(IndexRecommendation {
                    index_type: IndexType::Learned,
                    index_name: format!("learned_{}", columns[0]),
                    expected_speedup: 2.0,
                    confidence: 0.8,
                });
            }
            
            // Sparse data → sparse index
            ("sparse", _) => {
                recommendations.push(IndexRecommendation {
                    index_type: IndexType::Sparse,
                    index_name: format!("sparse_{}", columns[0]),
                    expected_speedup: 3.0,
                    confidence: 0.9,
                });
            }
            
            // Similarity search → vector index
            ("similarity", _) => {
                recommendations.push(IndexRecommendation {
                    index_type: IndexType::Vector,
                    index_name: format!("vector_{}", columns[0]),
                    expected_speedup: 10.0,
                    confidence: 0.95,
                });
            }
            
            // Multi-column → composite index
            ("multi_column", _) if columns.len() > 1 => {
                recommendations.push(IndexRecommendation {
                    index_type: IndexType::Composite,
                    index_name: format!("composite_{}", columns.join("_")),
                    expected_speedup: 2.5,
                    confidence: 0.85,
                });
            }
            
            // Equality → bitmap index
            ("equality", _) => {
                recommendations.push(IndexRecommendation {
                    index_type: IndexType::Bitmap,
                    index_name: format!("bitmap_{}", columns[0]),
                    expected_speedup: 5.0,
                    confidence: 0.9,
                });
            }
            
            _ => {}
        }
        
        recommendations
    }
    
    /// Check if index should be used based on selectivity
    pub fn should_use_index(
        index_type: &IndexType,
        selectivity: f64, // Fraction of rows that match (0.0 to 1.0)
    ) -> bool {
        match index_type {
            // Learned indexes: good for low selectivity (few rows match)
            IndexType::Learned => selectivity < 0.1,
            
            // Sparse indexes: good for very sparse data
            IndexType::Sparse => selectivity < 0.01,
            
            // Vector indexes: always use (similarity search)
            IndexType::Vector => true,
            
            // Composite indexes: good for low selectivity
            IndexType::Composite => selectivity < 0.2,
            
            // Bitmap indexes: good for low-medium selectivity
            IndexType::Bitmap => selectivity < 0.3,
        }
    }
}

/// Index statistics for cost estimation
#[derive(Clone, Debug)]
pub struct IndexStats {
    /// Index type
    pub index_type: IndexType,
    
    /// Number of entries
    pub entry_count: usize,
    
    /// Memory size in bytes
    pub memory_size: usize,
    
    /// Average lookup time (nanoseconds)
    pub avg_lookup_time_ns: u64,
    
    /// Cache hit rate (0.0 to 1.0)
    pub cache_hit_rate: f64,
}

impl IndexStats {
    /// Estimate cost of using this index
    pub fn estimate_cost(&self, num_lookups: usize) -> f64 {
        let lookup_cost = self.avg_lookup_time_ns as f64 * num_lookups as f64;
        let cache_benefit = lookup_cost * self.cache_hit_rate * 0.5; // Cache hits are 2× faster
        lookup_cost - cache_benefit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_index_recommendation() {
        let recs = IndexAdvisor::recommend_index("range", &["col1".to_string()], ">");
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].index_type, IndexType::Learned);
    }
    
    #[test]
    fn test_should_use_index() {
        assert!(IndexAdvisor::should_use_index(&IndexType::Learned, 0.05));
        assert!(!IndexAdvisor::should_use_index(&IndexType::Learned, 0.5));
    }
}

