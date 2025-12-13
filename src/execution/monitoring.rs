/// Phase 4: Monitoring & Metrics Collection
/// 
/// Provides production-ready monitoring:
/// - Query performance metrics
/// - Resource usage tracking
/// - Error rate monitoring
/// - Throughput measurement

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::sync::Arc;
use dashmap::DashMap;

/// Query performance metrics
#[derive(Clone, Debug)]
pub struct QueryMetrics {
    pub query_id: u64,
    pub execution_time: Duration,
    pub rows_processed: u64,
    pub memory_used: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub simd_operations: u64,
    pub errors: u64,
}

/// Global metrics collector
pub struct MetricsCollector {
    /// Total queries executed
    total_queries: Arc<AtomicU64>,
    
    /// Total execution time
    total_time: Arc<AtomicU64>, // microseconds
    
    /// Total rows processed
    total_rows: Arc<AtomicU64>,
    
    /// Total memory used
    total_memory: Arc<AtomicU64>,
    
    /// Cache hit rate
    cache_hits: Arc<AtomicU64>,
    cache_misses: Arc<AtomicU64>,
    
    /// SIMD operations count
    simd_operations: Arc<AtomicU64>,
    
    /// Error count
    errors: Arc<AtomicU64>,
    
    /// Per-query metrics
    query_metrics: Arc<DashMap<u64, QueryMetrics>>,
}

impl MetricsCollector {
    /// Create new metrics collector
    pub fn new() -> Self {
        Self {
            total_queries: Arc::new(AtomicU64::new(0)),
            total_time: Arc::new(AtomicU64::new(0)),
            total_rows: Arc::new(AtomicU64::new(0)),
            total_memory: Arc::new(AtomicU64::new(0)),
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
            simd_operations: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
            query_metrics: Arc::new(DashMap::new()),
        }
    }
    
    /// Record query execution
    pub fn record_query(&self, metrics: QueryMetrics) {
        self.total_queries.fetch_add(1, Ordering::Relaxed);
        self.total_time.fetch_add(
            metrics.execution_time.as_micros() as u64,
            Ordering::Relaxed,
        );
        self.total_rows.fetch_add(metrics.rows_processed, Ordering::Relaxed);
        self.total_memory.fetch_add(metrics.memory_used, Ordering::Relaxed);
        self.cache_hits.fetch_add(metrics.cache_hits, Ordering::Relaxed);
        self.cache_misses.fetch_add(metrics.cache_misses, Ordering::Relaxed);
        self.simd_operations.fetch_add(metrics.simd_operations, Ordering::Relaxed);
        self.errors.fetch_add(metrics.errors, Ordering::Relaxed);
        
        self.query_metrics.insert(metrics.query_id, metrics);
    }
    
    /// Get summary statistics
    pub fn get_summary(&self) -> MetricsSummary {
        let total_queries = self.total_queries.load(Ordering::Relaxed);
        let total_time_us = self.total_time.load(Ordering::Relaxed);
        let total_rows = self.total_rows.load(Ordering::Relaxed);
        let total_memory = self.total_memory.load(Ordering::Relaxed);
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);
        let simd_ops = self.simd_operations.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);
        
        let avg_time_ms = if total_queries > 0 {
            (total_time_us as f64 / total_queries as f64) / 1000.0
        } else {
            0.0
        };
        
        let cache_hit_rate = if cache_hits + cache_misses > 0 {
            (cache_hits as f64 / (cache_hits + cache_misses) as f64) * 100.0
        } else {
            0.0
        };
        
        let throughput = if total_time_us > 0 {
            (total_rows as f64 / total_time_us as f64) * 1_000_000.0 // rows per second
        } else {
            0.0
        };
        
        MetricsSummary {
            total_queries,
            avg_time_ms,
            total_rows,
            total_memory,
            cache_hit_rate,
            simd_operations: simd_ops,
            errors,
            throughput,
        }
    }
    
    /// Reset all metrics
    pub fn reset(&self) {
        self.total_queries.store(0, Ordering::Relaxed);
        self.total_time.store(0, Ordering::Relaxed);
        self.total_rows.store(0, Ordering::Relaxed);
        self.total_memory.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.simd_operations.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.query_metrics.clear();
    }
}

/// Metrics summary
#[derive(Debug, Clone)]
pub struct MetricsSummary {
    pub total_queries: u64,
    pub avg_time_ms: f64,
    pub total_rows: u64,
    pub total_memory: u64,
    pub cache_hit_rate: f64,
    pub simd_operations: u64,
    pub errors: u64,
    pub throughput: f64, // rows per second
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Query timer for measuring execution time
pub struct QueryTimer {
    start: Instant,
    query_id: u64,
}

impl QueryTimer {
    /// Start timing a query
    pub fn start(query_id: u64) -> Self {
        Self {
            start: Instant::now(),
            query_id,
        }
    }
    
    /// Stop timing and return duration
    pub fn stop(self) -> Duration {
        self.start.elapsed()
    }
    
    /// Get elapsed time without consuming
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_metrics_collector() {
        let collector = MetricsCollector::new();
        
        let metrics = QueryMetrics {
            query_id: 1,
            execution_time: Duration::from_millis(100),
            rows_processed: 1000,
            memory_used: 1024,
            cache_hits: 5,
            cache_misses: 2,
            simd_operations: 10,
            errors: 0,
        };
        
        collector.record_query(metrics);
        
        let summary = collector.get_summary();
        assert_eq!(summary.total_queries, 1);
        assert_eq!(summary.total_rows, 1000);
    }
}

