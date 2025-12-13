/// Phase 7.3: Advanced Query Optimization
/// 
/// Enhanced optimization strategies:
/// - Query plan caching with fingerprinting
/// - Runtime query re-optimization
/// - Better statistics collection
/// - Cost-based optimization improvements
/// - Adaptive query optimization (AQO)

use crate::query::plan::{QueryPlan, PlanOperator};
use crate::query::adaptive_optimizer::{AdaptiveOptimizer, RuntimeStatistics};
use crate::query::statistics::StatisticsCatalog;
use crate::query::fingerprint::QueryFingerprint;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use anyhow::Result;

/// Query plan cache with fingerprinting
pub struct QueryPlanCache {
    /// Cached plans: fingerprint -> (plan, hit_count, last_used)
    cache: Arc<RwLock<HashMap<u64, CachedPlan>>>,
    
    /// Maximum cache size
    max_size: usize,
    
    /// Cache hit statistics
    hits: Arc<RwLock<usize>>,
    misses: Arc<RwLock<usize>>,
}

/// Cached plan entry
struct CachedPlan {
    plan: QueryPlan,
    hit_count: usize,
    last_used: std::time::Instant,
}

impl QueryPlanCache {
    /// Create new plan cache
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            max_size,
            hits: Arc::new(RwLock::new(0)),
            misses: Arc::new(RwLock::new(0)),
        }
    }
    
    /// Get plan from cache
    pub fn get(&self, fingerprint: u64) -> Option<QueryPlan> {
        let mut cache = self.cache.write().unwrap();
        
        if let Some(cached) = cache.get_mut(&fingerprint) {
            cached.hit_count += 1;
            cached.last_used = std::time::Instant::now();
            *self.hits.write().unwrap() += 1;
            Some(cached.plan.clone())
        } else {
            *self.misses.write().unwrap() += 1;
            None
        }
    }
    
    /// Store plan in cache
    pub fn put(&self, fingerprint: u64, plan: QueryPlan) {
        let mut cache = self.cache.write().unwrap();
        
        // Evict if cache is full (LRU)
        if cache.len() >= self.max_size && !cache.contains_key(&fingerprint) {
            self.evict_lru(&mut cache);
        }
        
        cache.insert(fingerprint, CachedPlan {
            plan,
            hit_count: 1,
            last_used: std::time::Instant::now(),
        });
    }
    
    /// Evict least recently used entry
    fn evict_lru(&self, cache: &mut HashMap<u64, CachedPlan>) {
        let lru_key = cache.iter()
            .min_by_key(|(_, cached)| cached.last_used)
            .map(|(k, _)| *k);
        
        if let Some(key) = lru_key {
            cache.remove(&key);
        }
    }
    
    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let hits = *self.hits.read().unwrap();
        let misses = *self.misses.read().unwrap();
        let total = hits + misses;
        let hit_rate = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };
        
        CacheStats {
            hits,
            misses,
            hit_rate,
            size: self.cache.read().unwrap().len(),
        }
    }
    
    /// Clear cache
    pub fn clear(&self) {
        self.cache.write().unwrap().clear();
        *self.hits.write().unwrap() = 0;
        *self.misses.write().unwrap() = 0;
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub hits: usize,
    pub misses: usize,
    pub hit_rate: f64,
    pub size: usize,
}

/// Enhanced adaptive query optimizer
pub struct EnhancedAdaptiveOptimizer {
    /// Base adaptive optimizer
    base: Arc<AdaptiveOptimizer>,
    
    /// Plan cache
    plan_cache: Arc<QueryPlanCache>,
    
    /// Statistics catalog
    stats: Arc<StatisticsCatalog>,
    
    /// Reoptimization threshold
    reoptimize_threshold: f64,
}

impl EnhancedAdaptiveOptimizer {
    /// Create new enhanced optimizer
    pub fn new(
        base: Arc<AdaptiveOptimizer>,
        plan_cache: Arc<QueryPlanCache>,
        stats: Arc<StatisticsCatalog>,
    ) -> Self {
        Self {
            base,
            plan_cache,
            stats,
            reoptimize_threshold: 0.3, // 30% error threshold
        }
    }
    
    /// Optimize query with caching and adaptive optimization
    pub fn optimize_with_cache(
        &self,
        query: &str,
        fingerprint: u64,
    ) -> Result<Option<QueryPlan>> {
        // Check cache first
        if let Some(cached_plan) = self.plan_cache.get(fingerprint) {
            return Ok(Some(cached_plan));
        }
        
        Ok(None)
    }
    
    /// Record runtime statistics and check for reoptimization
    pub fn record_and_check_reoptimize(
        &self,
        stats: RuntimeStatistics,
        estimated_cardinality: f64,
    ) -> bool {
        // Record statistics
        self.base.record_runtime_stats(stats.clone());
        
        // Check if reoptimization is needed
        let should_reopt = self.base.should_reoptimize(
            estimated_cardinality,
            stats.actual_cardinality,
        );
        
        if should_reopt {
            // Update statistics catalog with actual values
            // TODO: Update stats catalog
        }
        
        should_reopt
    }
    
    /// Get optimization recommendations
    pub fn get_recommendations(&self) -> Vec<OptimizationRecommendation> {
        let mut recommendations = Vec::new();
        
        // Check cache hit rate
        let cache_stats = self.plan_cache.stats();
        if cache_stats.hit_rate < 0.5 {
            recommendations.push(OptimizationRecommendation::IncreaseCacheSize);
        }
        
        // Check runtime statistics for patterns
        let runtime_stats = self.base.get_runtime_stats();
        if runtime_stats.len() > 100 {
            // Analyze patterns
            let avg_error = self.calculate_avg_estimation_error(&runtime_stats);
            if avg_error > 0.5 {
                recommendations.push(OptimizationRecommendation::ImproveStatistics);
            }
        }
        
        recommendations
    }
    
    /// Calculate average estimation error
    fn calculate_avg_estimation_error(&self, stats: &[RuntimeStatistics]) -> f64 {
        if stats.is_empty() {
            return 0.0;
        }
        
        // TODO: Calculate from runtime stats
        0.0
    }
}

/// Optimization recommendations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizationRecommendation {
    IncreaseCacheSize,
    ImproveStatistics,
    EnableReoptimization,
    UpdateCostModel,
}

/// Runtime query re-optimizer
pub struct RuntimeReoptimizer {
    /// Adaptive optimizer
    adaptive: Arc<EnhancedAdaptiveOptimizer>,
    
    /// Reoptimization enabled
    enabled: bool,
}

impl RuntimeReoptimizer {
    /// Create new runtime reoptimizer
    pub fn new(adaptive: Arc<EnhancedAdaptiveOptimizer>) -> Self {
        Self {
            adaptive,
            enabled: true,
        }
    }
    
    /// Check if query should be reoptimized during execution
    pub fn should_reoptimize(
        &self,
        estimated_cardinality: f64,
        actual_cardinality: usize,
        execution_time_ms: f64,
    ) -> bool {
        if !self.enabled {
            return false;
        }
        
        // Check cardinality error
        if estimated_cardinality > 0.0 {
            let error_ratio = (estimated_cardinality - actual_cardinality as f64).abs() / estimated_cardinality;
            if error_ratio > 0.5 {
                return true;
            }
        }
        
        // Check if execution is taking too long
        if execution_time_ms > 10_000.0 {
            // Query taking > 10 seconds, consider reoptimization
            return true;
        }
        
        false
    }
    
    /// Reoptimize query plan
    pub fn reoptimize(&self, original_plan: &QueryPlan) -> Result<QueryPlan> {
        // TODO: Implement reoptimization logic
        // For now, return original plan
        Ok(original_plan.clone())
    }
}

/// Statistics collector for better optimization
pub struct StatisticsCollector {
    /// Collected statistics
    stats: Arc<RwLock<HashMap<String, ColumnStatistics>>>,
}

/// Column-level statistics
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    /// Distinct count
    pub distinct_count: usize,
    
    /// Null count
    pub null_count: usize,
    
    /// Min value
    pub min_value: Option<crate::storage::fragment::Value>,
    
    /// Max value
    pub max_value: Option<crate::storage::fragment::Value>,
    
    /// Average value (for numeric types)
    pub avg_value: Option<f64>,
}

impl StatisticsCollector {
    /// Create new statistics collector
    pub fn new() -> Self {
        Self {
            stats: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Collect statistics for a column
    pub fn collect_column_stats(
        &self,
        table_name: &str,
        column_name: &str,
        values: &[crate::storage::fragment::Value],
    ) {
        let key = format!("{}.{}", table_name, column_name);
        let mut stats = self.stats.write().unwrap();
        
        let distinct_count = self.count_distinct(values);
        let null_count = values.iter().filter(|v| matches!(v, crate::storage::fragment::Value::Null)).count();
        let (min_val, max_val) = self.find_min_max(values);
        let avg_val = self.calculate_avg(values);
        
        stats.insert(key, ColumnStatistics {
            distinct_count,
            null_count,
            min_value: min_val,
            max_value: max_val,
            avg_value: avg_val,
        });
    }
    
    /// Count distinct values
    fn count_distinct(&self, values: &[crate::storage::fragment::Value]) -> usize {
        use std::collections::HashSet;
        let mut distinct = HashSet::new();
        for val in values {
            if !matches!(val, crate::storage::fragment::Value::Null) {
                distinct.insert(val.clone());
            }
        }
        distinct.len()
    }
    
    /// Find min and max values
    fn find_min_max(&self, values: &[crate::storage::fragment::Value]) -> (Option<crate::storage::fragment::Value>, Option<crate::storage::fragment::Value>) {
        let mut min_val: Option<&crate::storage::fragment::Value> = None;
        let mut max_val: Option<&crate::storage::fragment::Value> = None;
        
        for val in values {
            if matches!(val, crate::storage::fragment::Value::Null) {
                continue;
            }
            
            if min_val.is_none() || self.compare_values(val, min_val.unwrap()) == std::cmp::Ordering::Less {
                min_val = Some(val);
            }
            
            if max_val.is_none() || self.compare_values(val, max_val.unwrap()) == std::cmp::Ordering::Greater {
                max_val = Some(val);
            }
        }
        
        (min_val.cloned(), max_val.cloned())
    }
    
    /// Compare two values
    fn compare_values(&self, a: &crate::storage::fragment::Value, b: &crate::storage::fragment::Value) -> std::cmp::Ordering {
        match (a, b) {
            (crate::storage::fragment::Value::Int64(x), crate::storage::fragment::Value::Int64(y)) => x.cmp(y),
            (crate::storage::fragment::Value::Float64(x), crate::storage::fragment::Value::Float64(y)) => {
                ordered_float::OrderedFloat(*x).cmp(&ordered_float::OrderedFloat(*y))
            }
            (crate::storage::fragment::Value::String(x), crate::storage::fragment::Value::String(y)) => x.cmp(y),
            _ => std::cmp::Ordering::Equal,
        }
    }
    
    /// Calculate average (for numeric types)
    fn calculate_avg(&self, values: &[crate::storage::fragment::Value]) -> Option<f64> {
        let mut sum = 0.0;
        let mut count = 0;
        
        for val in values {
            match val {
                crate::storage::fragment::Value::Int64(x) => {
                    sum += *x as f64;
                    count += 1;
                }
                crate::storage::fragment::Value::Float64(x) => {
                    sum += *x;
                    count += 1;
                }
                _ => {}
            }
        }
        
        if count > 0 {
            Some(sum / count as f64)
        } else {
            None
        }
    }
    
    /// Get statistics for a column
    pub fn get_column_stats(&self, table_name: &str, column_name: &str) -> Option<ColumnStatistics> {
        let key = format!("{}.{}", table_name, column_name);
        let stats = self.stats.read().unwrap();
        stats.get(&key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_plan_cache() {
        let cache = QueryPlanCache::new(100);
        
        let fingerprint = 12345;
        let plan = QueryPlan {
            root: PlanOperator::Scan {
                table: "test".to_string(),
                columns: vec![],
                predicate: None,
            },
        };
        
        cache.put(fingerprint, plan.clone());
        
        let cached = cache.get(fingerprint);
        assert!(cached.is_some());
        
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);
    }
    
    #[test]
    fn test_statistics_collector() {
        let collector = StatisticsCollector::new();
        
        let values = vec![
            crate::storage::fragment::Value::Int64(1),
            crate::storage::fragment::Value::Int64(2),
            crate::storage::fragment::Value::Int64(3),
            crate::storage::fragment::Value::Int64(2),
        ];
        
        collector.collect_column_stats("test", "col1", &values);
        
        let stats = collector.get_column_stats("test", "col1");
        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert_eq!(stats.distinct_count, 3);
        assert_eq!(stats.null_count, 0);
    }
}

