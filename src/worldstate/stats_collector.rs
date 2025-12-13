//! Statistics Collector - Collects stats during ingestion and table operations

use crate::worldstate::stats::{StatsRegistry, TableStats, ColumnStats};
use crate::worldstate::schema::SchemaRegistry;
use anyhow::Result;
use std::collections::HashMap;

/// Statistics Collector - Collects and updates statistics
pub struct StatsCollector;

impl StatsCollector {
    pub fn new() -> Self {
        Self
    }
    
    /// Collect statistics from a table (scan-based)
    /// 
    /// This is a simplified version. In production, would:
    /// - Sample data for large tables
    /// - Use approximate algorithms (HyperLogLog for NDV)
    /// - Update incrementally
    pub fn collect_table_stats(
        &self,
        table_name: &str,
        schema_registry: &SchemaRegistry,
        row_count: u64,
        sample_data: Option<&[Vec<String>]>, // Optional sample rows for column stats
    ) -> TableStats {
        let mut stats = TableStats::new(table_name.to_string());
        stats.row_count = row_count;
        
        // Get table schema
        if let Some(schema) = schema_registry.get_table(table_name) {
            // Collect column stats from sample data if available
            if let Some(samples) = sample_data {
                for col in &schema.columns {
                    let col_stats = self.collect_column_stats(col.name.as_str(), samples);
                    stats.column_stats.insert(col.name.clone(), col_stats);
                }
            } else {
                // No sample data - create placeholder stats
                for col in &schema.columns {
                    let col_stats = ColumnStats {
                        column: col.name.clone(),
                        ndv: None,
                        null_rate: Some(0.0), // Assume no nulls if no data
                        min_value: None,
                        max_value: None,
                        updated_at: crate::worldstate::stats::TableStats::now_timestamp(),
                    };
                    stats.column_stats.insert(col.name.clone(), col_stats);
                }
            }
        }
        
        stats
    }
    
    /// Collect statistics for a single column
    fn collect_column_stats(&self, column_name: &str, samples: &[Vec<String>]) -> ColumnStats {
        // Find column index (simplified - assumes column order matches)
        // In production, would use column name mapping
        
        let mut distinct_values = std::collections::HashSet::new();
        let mut null_count = 0;
        let mut numeric_values: Vec<f64> = Vec::new();
        
        for row in samples {
            // Simplified: assume first column is the one we're analyzing
            // In production, would map by column name
            if row.is_empty() {
                null_count += 1;
                continue;
            }
            
            let value = &row[0];
            if value.is_empty() || value == "NULL" {
                null_count += 1;
            } else {
                distinct_values.insert(value.clone());
                
                // Try to parse as numeric
                if let Ok(num) = value.parse::<f64>() {
                    numeric_values.push(num);
                }
            }
        }
        
        let total = samples.len() as f64;
        let null_rate = if total > 0.0 {
            null_count as f64 / total
        } else {
            0.0
        };
        
        let (min_value, max_value) = if !numeric_values.is_empty() {
            let min = numeric_values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
            let max = numeric_values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
            (Some(min.to_string()), Some(max.to_string()))
        } else {
            (None, None)
        };
        
        ColumnStats {
            column: column_name.to_string(),
            ndv: Some(distinct_values.len() as u64),
            null_rate: Some(null_rate),
            min_value,
            max_value,
            updated_at: crate::worldstate::stats::TableStats::now_timestamp(),
        }
    }
    
    /// Update row count for a table
    pub fn update_row_count(
        &self,
        stats_registry: &mut StatsRegistry,
        table_name: &str,
        row_count: u64,
    ) {
        stats_registry.update_row_count(table_name, row_count);
    }
    
    /// Estimate filter selectivity
    /// 
    /// Returns selectivity (0.0-1.0) based on column stats
    pub fn estimate_filter_selectivity(
        &self,
        stats_registry: &StatsRegistry,
        table_name: &str,
        column_name: &str,
        filter_type: FilterType,
        filter_value: Option<&str>,
    ) -> f64 {
        let stats = match stats_registry.get_table_stats(table_name) {
            Some(s) => s,
            None => return 0.1, // Default selectivity if no stats
        };
        
        let col_stats = match stats.column_stats.get(column_name) {
            Some(s) => s,
            None => return 0.1,
        };
        
        match filter_type {
            FilterType::Equals => {
                // Equals: 1 / NDV (assuming uniform distribution)
                if let Some(ndv) = col_stats.ndv {
                    if ndv > 0 {
                        return 1.0 / ndv as f64;
                    }
                }
                0.1 // Default
            }
            FilterType::Range { .. } => {
                // Range: estimate based on min/max if available
                0.3 // Simplified - would use min/max in production
            }
            FilterType::Like => {
                0.2 // LIKE is typically less selective
            }
            FilterType::IsNull => {
                col_stats.null_rate.unwrap_or(0.0)
            }
            FilterType::IsNotNull => {
                1.0 - col_stats.null_rate.unwrap_or(0.0)
            }
        }
    }
    
    /// Estimate join fanout
    /// 
    /// Returns estimated output rows for a join
    pub fn estimate_join_fanout(
        &self,
        stats_registry: &StatsRegistry,
        left_table: &str,
        right_table: &str,
        cardinality: &str, // "1:1", "1:N", "N:M"
    ) -> f64 {
        let left_stats = stats_registry.get_table_stats(left_table);
        let right_stats = stats_registry.get_table_stats(right_table);
        
        let left_rows = left_stats.map(|s| s.row_count as f64).unwrap_or(1000.0);
        let right_rows = right_stats.map(|s| s.row_count as f64).unwrap_or(1000.0);
        
        match cardinality {
            "1:1" => left_rows.min(right_rows),
            "1:N" => left_rows * right_rows / left_rows.max(1.0), // Simplified
            "N:M" => left_rows * right_rows, // Cartesian product (worst case)
            _ => left_rows * right_rows,
        }
    }
}

#[derive(Clone, Debug)]
pub enum FilterType {
    Equals,
    Range { min: Option<f64>, max: Option<f64> },
    Like,
    IsNull,
    IsNotNull,
}

impl Default for StatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

