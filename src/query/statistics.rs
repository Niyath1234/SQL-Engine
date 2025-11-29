/// Statistics catalog for query optimization
/// Provides column-level statistics for cardinality estimation, selectivity, etc.
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Statistics catalog - stores statistics per (table, column)
#[derive(Clone, Debug)]
pub struct StatisticsCatalog {
    /// Statistics per (table, column) key
    pub stats: HashMap<(String, String), ColumnStatistics>,
    
    /// Table-level statistics
    pub table_stats: HashMap<String, TableStatistics>,
}

/// Column-level statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Minimum value
    pub min: Option<StatValue>,
    
    /// Maximum value
    pub max: Option<StatValue>,
    
    /// Fraction of NULL values (0.0 to 1.0)
    pub null_fraction: f64,
    
    /// Approximate number of distinct values (NDV)
    pub distinct_count: f64,
    
    /// Histogram (equi-depth or equi-height)
    pub histogram: Option<Histogram>,
    
    /// Quantile sketch (TDigest or KLL)
    pub quantile_sketch: Option<QuantileSketch>,
    
    /// Top-K most frequent values
    pub top_k: Option<Vec<TopKValue>>,
    
    /// Average width in bytes
    pub avg_width: f64,
}

/// Table-level statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableStatistics {
    /// Total row count
    pub row_count: usize,
    
    /// Total size in bytes
    pub total_size: usize,
    
    /// Number of columns
    pub column_count: usize,
    
    /// Last updated timestamp
    pub last_updated: Option<std::time::SystemTime>,
}

/// Statistical value (can be different types)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, PartialOrd)]
pub enum StatValue {
    Int64(i64),
    Float64(f64),
    String(String),
    Date(i64), // Days since epoch
    Timestamp(i64), // Milliseconds since epoch
}

/// Histogram for value distribution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Histogram {
    /// Histogram type
    pub histogram_type: HistogramType,
    
    /// Buckets
    pub buckets: Vec<HistogramBucket>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HistogramType {
    EquiDepth,   // Equal number of values per bucket
    EquiHeight,  // Equal height (frequency) per bucket
    EquiWidth,   // Equal width per bucket
}

/// A histogram bucket
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistogramBucket {
    /// Lower bound (inclusive)
    pub lower: StatValue,
    
    /// Upper bound (inclusive)
    pub upper: StatValue,
    
    /// Number of distinct values in this bucket
    pub distinct_count: f64,
    
    /// Frequency (number of rows)
    pub frequency: f64,
}

/// Quantile sketch for approximate quantiles
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuantileSketch {
    /// Sketch type
    pub sketch_type: SketchType,
    
    /// Serialized sketch data
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SketchType {
    TDigest,
    KLL,
}

/// Top-K value with frequency
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopKValue {
    /// Value
    pub value: StatValue,
    
    /// Frequency (count)
    pub frequency: usize,
}

impl StatisticsCatalog {
    pub fn new() -> Self {
        Self {
            stats: HashMap::new(),
            table_stats: HashMap::new(),
        }
    }
    
    /// Get column statistics
    pub fn get_column_stats(&self, table: &str, column: &str) -> Option<&ColumnStatistics> {
        let key = (table.to_lowercase(), column.to_lowercase());
        self.stats.get(&key)
    }
    
    /// Get table statistics
    pub fn get_table_stats(&self, table: &str) -> Option<&TableStatistics> {
        self.table_stats.get(&table.to_lowercase())
    }
    
    /// Get mutable table statistics
    pub fn get_table_stats_mut(&mut self, table: &str) -> Option<&mut TableStatistics> {
        self.table_stats.get_mut(&table.to_lowercase())
    }
    
    /// Update column statistics
    pub fn update_column_stats(&mut self, table: &str, column: &str, stats: ColumnStatistics) {
        let key = (table.to_lowercase(), column.to_lowercase());
        self.stats.insert(key, stats);
    }
    
    /// Update table statistics
    pub fn update_table_stats(&mut self, table: &str, stats: TableStatistics) {
        self.table_stats.insert(table.to_lowercase(), stats);
    }
    
    /// Estimate selectivity for a predicate
    /// Returns fraction of rows that match (0.0 to 1.0)
    pub fn estimate_selectivity(
        &self,
        table: &str,
        column: &str,
        predicate: &Predicate,
    ) -> Result<f64, String> {
        let col_stats = self.get_column_stats(table, column)
            .ok_or_else(|| format!("No statistics available for {}.{}", table, column))?;
        
        match predicate {
            Predicate::Equals(value) => {
                // Use histogram or distinct count
                if let Some(ref hist) = col_stats.histogram {
                    self.estimate_equals_from_histogram(hist, value)
                } else if col_stats.distinct_count > 0.0 {
                    // Assume uniform distribution
                    Ok(1.0 / col_stats.distinct_count)
                } else {
                    // Fallback: very low selectivity
                    Ok(0.01)
                }
            }
            Predicate::Range(lower, upper) => {
                if let Some(ref hist) = col_stats.histogram {
                    self.estimate_range_from_histogram(hist, lower, upper)
                } else if col_stats.min.is_some() && col_stats.max.is_some() {
                    // Estimate from min/max (clone to avoid move)
                    let min = col_stats.min.as_ref().unwrap();
                    let max = col_stats.max.as_ref().unwrap();
                    self.estimate_range_from_minmax(min, max, lower, upper)
                } else {
                    // Fallback: assume 10% selectivity
                    Ok(0.1)
                }
            }
            Predicate::IsNull => {
                Ok(col_stats.null_fraction)
            }
            Predicate::IsNotNull => {
                Ok(1.0 - col_stats.null_fraction)
            }
            Predicate::Like(pattern) => {
                // String predicates are hard to estimate
                // Use heuristics based on pattern
                if pattern.ends_with('%') {
                    // Prefix match: assume 1% selectivity
                    Ok(0.01)
                } else if pattern.starts_with('%') && pattern.ends_with('%') {
                    // Contains: assume 0.1% selectivity
                    Ok(0.001)
                } else {
                    // Exact match: use distinct count
                    if col_stats.distinct_count > 0.0 {
                        Ok(1.0 / col_stats.distinct_count)
                    } else {
                        Ok(0.01)
                    }
                }
            }
        }
    }
    
    fn estimate_equals_from_histogram(&self, hist: &Histogram, value: &StatValue) -> Result<f64, String> {
        // Find bucket containing value
        for bucket in &hist.buckets {
            if value >= &bucket.lower && value <= &bucket.upper {
                // Assume uniform distribution within bucket
                let bucket_selectivity = bucket.frequency / bucket.distinct_count;
                return Ok(bucket_selectivity);
            }
        }
        // Value not in any bucket - very low selectivity
        Ok(0.001)
    }
    
    fn estimate_range_from_histogram(&self, hist: &Histogram, lower: &StatValue, upper: &StatValue) -> Result<f64, String> {
        let mut total_frequency = 0.0;
        let mut total_distinct = 0.0;
        
        for bucket in &hist.buckets {
            // Check if bucket overlaps with range
            if bucket.upper >= *lower && bucket.lower <= *upper {
                total_frequency += bucket.frequency;
                total_distinct += bucket.distinct_count;
            }
        }
        
        if total_distinct > 0.0 {
            Ok(total_frequency / total_distinct)
        } else {
            Ok(0.1) // Fallback
        }
    }
    
    fn estimate_range_from_minmax(&self, min: &StatValue, max: &StatValue, lower: &StatValue, upper: &StatValue) -> Result<f64, String> {
        // Simple linear interpolation
        // This is a rough estimate - assumes uniform distribution
        let range_size = match (min, max) {
            (StatValue::Int64(min_val), StatValue::Int64(max_val)) => {
                if let (StatValue::Int64(l), StatValue::Int64(u)) = (lower, upper) {
                    let total_range = (max_val - min_val) as f64;
                    let query_range = (u - l) as f64;
                    if total_range > 0.0 {
                        query_range / total_range
                    } else {
                        0.1
                    }
                } else {
                    0.1
                }
            }
            (StatValue::Float64(min_val), StatValue::Float64(max_val)) => {
                if let (StatValue::Float64(l), StatValue::Float64(u)) = (lower, upper) {
                    let total_range = max_val - min_val;
                    let query_range = u - l;
                    if total_range > 0.0 {
                        query_range / total_range
                    } else {
                        0.1
                    }
                } else {
                    0.1
                }
            }
            _ => 0.1, // Fallback for other types
        };
        
        Ok(range_size.min(1.0).max(0.0))
    }
    
    /// Estimate join cardinality
    pub fn estimate_join_cardinality(
        &self,
        left_table: &str,
        left_column: &str,
        right_table: &str,
        right_column: &str,
        left_cardinality: f64,
        right_cardinality: f64,
    ) -> Result<f64, String> {
        let left_stats = self.get_column_stats(left_table, left_column)
            .ok_or_else(|| format!("No statistics for {}.{}", left_table, left_column))?;
        let right_stats = self.get_column_stats(right_table, right_column)
            .ok_or_else(|| format!("No statistics for {}.{}", right_table, right_column))?;
        
        // Use distinct count for join cardinality estimation
        // Formula: |R ⋈ S| ≈ |R| * |S| / max(NDV(R), NDV(S))
        let max_ndv = left_stats.distinct_count.max(right_stats.distinct_count);
        
        if max_ndv > 0.0 {
            Ok(left_cardinality * right_cardinality / max_ndv)
        } else {
            // Fallback: assume cartesian product (worst case)
            Ok(left_cardinality * right_cardinality)
        }
    }
}

/// Predicate types for selectivity estimation
#[derive(Clone, Debug)]
pub enum Predicate {
    Equals(StatValue),
    Range(StatValue, StatValue), // Lower and upper bounds
    IsNull,
    IsNotNull,
    Like(String), // Pattern
}

impl Default for StatisticsCatalog {
    fn default() -> Self {
        Self::new()
    }
}

