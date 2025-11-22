/// Column-level statistics for query optimization
use crate::storage::fragment::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Column-level statistics stored in node metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Number of NULL values in this column
    pub null_count: usize,
    
    /// Number of distinct values (more accurate than cardinality)
    pub distinct_count: usize,
    
    /// Histogram buckets for value distribution
    pub histogram: Option<Histogram>,
    
    /// Percentiles for range query optimization
    pub percentiles: Option<Percentiles>,
    
    /// Data skew indicator (0.0 = uniform, 1.0 = highly skewed)
    pub skew_indicator: f64,
    
    /// Last update timestamp
    pub last_updated: u64,
}

/// Histogram bucket for value distribution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistogramBucket {
    /// Minimum value in bucket
    pub min_value: Option<Value>,
    
    /// Maximum value in bucket
    pub max_value: Option<Value>,
    
    /// Count of values in this bucket
    pub count: usize,
}

/// Histogram for column value distribution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Histogram {
    /// Histogram buckets
    pub buckets: Vec<HistogramBucket>,
    
    /// Total count of values (excluding NULLs)
    pub total_count: usize,
    
    /// Number of buckets
    pub bucket_count: usize,
}

/// Percentiles for range query optimization
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Percentiles {
    pub p25: Option<Value>,
    pub p50: Option<Value>,
    pub p75: Option<Value>,
    pub p95: Option<Value>,
    pub p99: Option<Value>,
}

/// Index metadata for optimization
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexMetadata {
    /// Index type (bitmap, btree, vector, bloom)
    pub index_type: String,
    
    /// Selectivity of this index (0.0 to 1.0, lower = more selective)
    pub selectivity: f64,
    
    /// Size of index in bytes
    pub size_bytes: usize,
    
    /// Number of times this index has been used
    pub usage_count: u64,
    
    /// Timestamp of last usage
    pub last_used: u64,
    
    /// Average query speedup when using this index
    pub avg_speedup: f64,
}

/// Access pattern metadata for adaptive optimization
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccessPatternMetadata {
    /// Access frequency (accesses per second)
    pub access_frequency: f64,
    
    /// Ratio of reads to writes (0.0 = all writes, 1.0 = all reads)
    pub read_write_ratio: f64,
    
    /// List of frequently accessed columns (ordered by frequency)
    pub frequently_accessed_columns: Vec<String>,
    
    /// Common query patterns
    pub query_patterns: QueryPatterns,
    
    /// Temporal access patterns (hot/cold hours)
    pub temporal_pattern: Option<TemporalPattern>,
    
    /// Hotness score (0.0 = cold, 1.0 = hot)
    pub hotness_score: f64,
    
    /// Last access timestamp
    pub last_accessed: u64,
}

/// Query patterns for optimization hints
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryPatterns {
    /// Common filter patterns
    pub filter_patterns: Vec<String>,
    
    /// Common join patterns
    pub join_patterns: Vec<String>,
    
    /// Common aggregation patterns
    pub aggregation_patterns: Vec<String>,
}

/// Temporal access pattern
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemporalPattern {
    /// Hot hours (0-23) when table is frequently accessed
    pub hot_hours: Vec<u8>,
    
    /// Cold hours (0-23) when table is rarely accessed
    pub cold_hours: Vec<u8>,
}

/// Join statistics for join optimization
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinStatistics {
    /// List of join partners (tables frequently joined with this one)
    pub join_partners: Vec<JoinPartner>,
    
    /// Cached optimal join order for common patterns
    pub best_join_order_cache: HashMap<String, JoinOrder>,
    
    /// Average join selectivity across all joins
    pub avg_join_selectivity: f64,
    
    /// Last update timestamp
    pub last_updated: u64,
}

/// Join partner information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinPartner {
    /// Table name
    pub table: String,
    
    /// Join frequency (joins per second)
    pub join_frequency: f64,
    
    /// Join selectivity (0.0 to 1.0, lower = more selective)
    pub join_selectivity: f64,
    
    /// Estimated join cardinality
    pub join_cardinality: usize,
    
    /// Best join order for this pair
    pub best_join_order: Vec<String>,
    
    /// Last join timestamp
    pub last_joined: u64,
}

/// Cached join order
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinOrder {
    /// Optimal join order (table names in order)
    pub optimal_order: Vec<String>,
    
    /// Estimated cost for this join order
    pub estimated_cost: f64,
    
    /// Query pattern that uses this join order
    pub query_pattern: String,
    
    /// Last used timestamp
    pub last_used: u64,
}

/// Query optimization hints
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OptimizationHints {
    /// Optimal scan strategy (sequential, index, vector)
    pub optimal_scan_strategy: String,
    
    /// Cached filter selectivity for common filters
    pub filter_selectivity_cache: HashMap<String, f64>,
    
    /// Materialized view candidates
    pub materialized_view_candidates: Vec<MaterializedViewCandidate>,
    
    /// Cached query signatures that benefit from result caching
    pub cached_query_signatures: Vec<String>,
    
    /// Last update timestamp
    pub last_updated: u64,
}

/// Materialized view candidate
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MaterializedViewCandidate {
    /// Expression to materialize (e.g., "COUNT(*) GROUP BY status")
    pub expression: String,
    
    /// Benefit score (0.0 to 1.0, higher = more beneficial)
    pub benefit_score: f64,
    
    /// Access frequency for this expression
    pub access_frequency: f64,
    
    /// Estimated materialization cost
    pub materialization_cost: f64,
}

impl ColumnStatistics {
    pub fn new(row_count: usize) -> Self {
        Self {
            null_count: 0,
            distinct_count: 0,
            histogram: None,
            percentiles: None,
            skew_indicator: 0.0,
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
    
    /// Estimate selectivity for a filter predicate
    pub fn estimate_selectivity(&self, predicate: &str) -> f64 {
        // Default selectivity based on distinct_count
        if self.distinct_count > 0 {
            // Equality predicate: 1 / distinct_count
            if predicate.contains("=") {
                return (1.0 / self.distinct_count as f64).min(1.0);
            }
            // Range predicate: assume 10% selectivity
            if predicate.contains(">") || predicate.contains("<") {
                return 0.1;
            }
            // LIKE predicate: assume 5% selectivity
            if predicate.contains("LIKE") {
                return 0.05;
            }
        }
        // Default: 10% selectivity
        0.1
    }
}

impl AccessPatternMetadata {
    pub fn new() -> Self {
        Self {
            access_frequency: 0.0,
            read_write_ratio: 1.0, // Default: all reads
            frequently_accessed_columns: Vec::new(),
            query_patterns: QueryPatterns {
                filter_patterns: Vec::new(),
                join_patterns: Vec::new(),
                aggregation_patterns: Vec::new(),
            },
            temporal_pattern: None,
            hotness_score: 0.0,
            last_accessed: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
    
    /// Update access frequency
    pub fn update_access(&mut self, column: Option<&str>) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Update access frequency (simple moving average)
        let elapsed = now.saturating_sub(self.last_accessed).max(1);
        let accesses_per_second = 1.0 / elapsed as f64;
        self.access_frequency = (self.access_frequency * 0.9) + (accesses_per_second * 0.1);
        
        // Update hotness score (0.0 = cold, 1.0 = hot)
        self.hotness_score = (self.access_frequency / 100.0).min(1.0);
        
        // Update frequently accessed columns
        if let Some(col) = column {
            if !self.frequently_accessed_columns.contains(&col.to_string()) {
                self.frequently_accessed_columns.push(col.to_string());
            }
            // Keep top 10 most accessed columns
            if self.frequently_accessed_columns.len() > 10 {
                self.frequently_accessed_columns.remove(0);
            }
        }
        
        self.last_accessed = now;
    }
}

impl JoinStatistics {
    pub fn new() -> Self {
        Self {
            join_partners: Vec::new(),
            best_join_order_cache: HashMap::new(),
            avg_join_selectivity: 1.0,
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
    
    /// Update join statistics for a join partner
    pub fn update_join(&mut self, partner: &str, selectivity: f64, cardinality: usize) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Find or create join partner
        if let Some(existing) = self.join_partners.iter_mut().find(|p| p.table == partner) {
            existing.join_frequency += 1.0;
            existing.join_selectivity = (existing.join_selectivity * 0.9) + (selectivity * 0.1);
            existing.join_cardinality = cardinality;
            existing.last_joined = now;
        } else {
            self.join_partners.push(JoinPartner {
                table: partner.to_string(),
                join_frequency: 1.0,
                join_selectivity: selectivity,
                join_cardinality: cardinality,
                best_join_order: Vec::new(),
                last_joined: now,
            });
        }
        
        // Update average join selectivity
        let total_selectivity: f64 = self.join_partners.iter().map(|p| p.join_selectivity).sum();
        self.avg_join_selectivity = total_selectivity / self.join_partners.len() as f64;
        
        self.last_updated = now;
    }
}

impl OptimizationHints {
    pub fn new() -> Self {
        Self {
            optimal_scan_strategy: "sequential".to_string(),
            filter_selectivity_cache: HashMap::new(),
            materialized_view_candidates: Vec::new(),
            cached_query_signatures: Vec::new(),
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
    
    /// Cache filter selectivity for a filter pattern
    pub fn cache_filter_selectivity(&mut self, filter: &str, selectivity: f64) {
        self.filter_selectivity_cache.insert(filter.to_string(), selectivity);
        // Keep only top 100 most common filters
        if self.filter_selectivity_cache.len() > 100 {
            let mut entries: Vec<_> = self.filter_selectivity_cache.iter().collect();
            entries.sort_by(|a, b| a.1.partial_cmp(b.1).unwrap());
            entries.truncate(100);
            self.filter_selectivity_cache = entries.into_iter().map(|(k, v)| (k.clone(), *v)).collect();
        }
    }
    
    /// Get cached filter selectivity
    pub fn get_filter_selectivity(&self, filter: &str) -> Option<f64> {
        self.filter_selectivity_cache.get(filter).copied()
    }
}

