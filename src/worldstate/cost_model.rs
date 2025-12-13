//! Cost Model - Uses hypergraph stats for join order and cost estimation

use crate::worldstate::stats::{StatsRegistry, TableStats};
use crate::worldstate::rules::JoinRuleRegistry;
use anyhow::Result;

/// Cost Model - Estimates query execution cost
pub struct CostModel;

impl CostModel {
    pub fn new() -> Self {
        Self
    }
    
    /// Estimate cost of a join operation
    /// 
    /// Returns cost in arbitrary units (lower is better)
    pub fn estimate_join_cost(
        &self,
        stats_registry: &StatsRegistry,
        rule_registry: &JoinRuleRegistry,
        left_table: &str,
        right_table: &str,
        join_type: &str,
    ) -> f64 {
        let left_stats = stats_registry.get_table_stats(left_table);
        let right_stats = stats_registry.get_table_stats(right_table);
        
        let left_rows = left_stats.map(|s| s.row_count as f64).unwrap_or(1000.0);
        let right_rows = right_stats.map(|s| s.row_count as f64).unwrap_or(1000.0);
        
        // Get join rule to determine cardinality
        let cardinality = rule_registry.get_approved_rules(left_table, right_table)
            .first()
            .map(|r| r.cardinality.as_str())
            .unwrap_or("N:M");
        
        // Estimate output rows based on cardinality
        let output_rows = match cardinality {
            "1:1" => left_rows.min(right_rows),
            "1:N" => {
                // Foreign key join: output ≈ right_rows
                right_rows
            }
            "N:1" => {
                // Reverse foreign key: output ≈ left_rows
                left_rows
            }
            _ => {
                // N:M: worst case (but should be avoided)
                left_rows * right_rows
            }
        };
        
        // Cost = input size + output size (simplified)
        // In production, would consider:
        // - Hash table build cost
        // - Probe cost
        // - Memory pressure
        let cost = left_rows + right_rows + output_rows;
        
        // Penalize large joins
        if output_rows > 1_000_000.0 {
            cost * 1.5
        } else {
            cost
        }
    }
    
    /// Estimate cost of a filter operation
    pub fn estimate_filter_cost(
        &self,
        stats_registry: &StatsRegistry,
        table_name: &str,
        selectivity: f64,
    ) -> f64 {
        let stats = stats_registry.get_table_stats(table_name);
        let input_rows = stats.map(|s| s.row_count as f64).unwrap_or(1000.0);
        
        // Cost = scan cost + filter cost
        // Simplified: linear scan + filter application
        input_rows * (1.0 + (1.0 - selectivity))
    }
    
    /// Choose optimal join order
    /// 
    /// Returns tables in optimal join order
    pub fn choose_join_order(
        &self,
        stats_registry: &StatsRegistry,
        rule_registry: &JoinRuleRegistry,
        tables: &[String],
    ) -> Vec<String> {
        // Simple heuristic: start with smallest table
        let mut ordered = tables.to_vec();
        
        // Sort by row count (ascending)
        ordered.sort_by_key(|table| {
            stats_registry.get_table_stats(table)
                .map(|s| s.row_count)
                .unwrap_or(u64::MAX)
        });
        
        ordered
    }
}

impl Default for CostModel {
    fn default() -> Self {
        Self::new()
    }
}

