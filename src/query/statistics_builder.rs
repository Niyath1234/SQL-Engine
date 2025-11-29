/// Build statistics catalog from hypergraph metadata
use crate::query::statistics::*;
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::NodeId;
use anyhow::Result;

/// Build statistics catalog from hypergraph
pub struct StatisticsBuilder;

impl StatisticsBuilder {
    /// Build statistics catalog from hypergraph nodes
    pub fn build_from_hypergraph(graph: &HyperGraph) -> Result<StatisticsCatalog> {
        let mut catalog = StatisticsCatalog::new();
        
        // Iterate through all nodes
        for (_node_id, node_arc) in graph.iter_nodes() {
            let node = node_arc.as_ref();
            if let (Some(table_name), Some(column_name)) = (&node.table_name, &node.column_name) {
                // Build column statistics from node metadata
                let col_stats = Self::build_column_stats_from_node(node)?;
                catalog.update_column_stats(table_name, column_name, col_stats);
            }
            
            // Build table statistics
            if let Some(table_name) = &node.table_name {
                if matches!(node.node_type, crate::hypergraph::node::NodeType::Table) {
                    let table_stats = Self::build_table_stats_from_node(node)?;
                    catalog.update_table_stats(table_name, table_stats);
                }
            }
        }
        
        Ok(catalog)
    }
    
    /// Build column statistics from hypergraph node
    fn build_column_stats_from_node(node: &crate::hypergraph::node::HyperNode) -> Result<ColumnStatistics> {
        // Extract statistics from node fragments
        let mut min: Option<StatValue> = None;
        let mut max: Option<StatValue> = None;
        let mut null_count = 0;
        let mut total_count = 0;
        let mut distinct_values = std::collections::HashSet::new();
        
        // Aggregate statistics from all fragments
        for fragment in &node.fragments {
            total_count += fragment.metadata.cardinality;
            
            // Extract min/max from fragment metadata if available
            if let Some(ref metadata) = fragment.metadata.min_value {
                let stat_val = Self::value_to_stat_value(metadata);
                min = Some(match min {
                    Some(ref existing) => {
                        if &stat_val < existing {
                            stat_val
                        } else {
                            existing.clone()
                        }
                    }
                    None => stat_val,
                });
            }
            
            if let Some(ref metadata) = fragment.metadata.max_value {
                let stat_val = Self::value_to_stat_value(metadata);
                max = Some(match max {
                    Some(ref existing) => {
                        if &stat_val > existing {
                            stat_val
                        } else {
                            existing.clone()
                        }
                    }
                    None => stat_val,
                });
            }
            
            // Estimate null count (simplified - would need actual null tracking)
            // For now, assume no nulls if not explicitly tracked
            // null_count remains 0
            
            // Estimate distinct values from cardinality
            // Use cardinality as a proxy for distinct count
            distinct_values.insert(fragment.metadata.cardinality);
        }
        
        // Compute statistics
        let null_fraction = if total_count > 0 {
            null_count as f64 / total_count as f64
        } else {
            0.0
        };
        
        let distinct_count = if !distinct_values.is_empty() {
            distinct_values.iter().sum::<usize>() as f64 / distinct_values.len() as f64
        } else {
            // Estimate from cardinality (assume 10% distinct)
            (total_count as f64 * 0.1).max(1.0)
        };
        
        // Build histogram if we have enough data
        let histogram = if total_count > 100 {
            Some(Self::build_histogram_from_fragments(&node.fragments))
        } else {
            None
        };
        
        // Estimate average width (rough estimate)
        let avg_width = Self::estimate_avg_width(&node.fragments);
        
        Ok(ColumnStatistics {
            min,
            max,
            null_fraction,
            distinct_count,
            histogram,
            quantile_sketch: None, // TODO: build from fragments
            top_k: None, // TODO: extract from fragments
            avg_width,
        })
    }
    
    /// Build table statistics from hypergraph node
    fn build_table_stats_from_node(node: &crate::hypergraph::node::HyperNode) -> Result<TableStatistics> {
        let mut row_count = 0;
        let mut total_size = 0;
        let mut column_count = 0;
        
        // Aggregate from fragments
        for fragment in &node.fragments {
            row_count += fragment.metadata.cardinality;
            total_size += fragment.metadata.memory_size;
        }
        
        // Count columns (nodes with same table name)
        // This is approximate - would need to iterate all nodes
        column_count = 1; // At least this column
        
        Ok(TableStatistics {
            row_count,
            total_size,
            column_count,
            last_updated: Some(std::time::SystemTime::now()),
        })
    }
    
    /// Convert Value to StatValue
    fn value_to_stat_value(value: &crate::storage::fragment::Value) -> StatValue {
        match value {
            crate::storage::fragment::Value::Int64(v) => StatValue::Int64(*v),
            crate::storage::fragment::Value::Int32(v) => StatValue::Int64(*v as i64),
            crate::storage::fragment::Value::Float64(v) => StatValue::Float64(*v),
            crate::storage::fragment::Value::Float32(v) => StatValue::Float64(*v as f64),
            crate::storage::fragment::Value::String(v) => StatValue::String(v.clone()),
            crate::storage::fragment::Value::Bool(v) => StatValue::Int64(if *v { 1 } else { 0 }),
            crate::storage::fragment::Value::Null => StatValue::Int64(0), // Default for null
            crate::storage::fragment::Value::Vector(_) => StatValue::String("vector".to_string()),
        }
    }
    
    /// Build histogram from fragments
    fn build_histogram_from_fragments(fragments: &[crate::storage::fragment::ColumnFragment]) -> Histogram {
        // Simple equi-depth histogram
        // In full implementation, would use actual data distribution
        
        let mut buckets = Vec::new();
        
        if fragments.is_empty() {
            return Histogram {
                histogram_type: HistogramType::EquiDepth,
                buckets,
            };
        }
        
        // Aggregate min/max across fragments
        let mut global_min: Option<StatValue> = None;
        let mut global_max: Option<StatValue> = None;
        let mut total_frequency = 0.0;
        
        for fragment in fragments {
            if let Some(ref min_val) = fragment.metadata.min_value {
                let stat_min = Self::value_to_stat_value(min_val);
                global_min = Some(match global_min {
                    Some(ref existing) => {
                        if &stat_min < existing {
                            stat_min
                        } else {
                            existing.clone()
                        }
                    }
                    None => stat_min,
                });
            }
            
            if let Some(ref max_val) = fragment.metadata.max_value {
                let stat_max = Self::value_to_stat_value(max_val);
                global_max = Some(match global_max {
                    Some(ref existing) => {
                        if &stat_max > existing {
                            stat_max
                        } else {
                            existing.clone()
                        }
                    }
                    None => stat_max,
                });
            }
            
            total_frequency += fragment.metadata.cardinality as f64;
        }
        
        // Create buckets (simplified - would need actual distribution)
        if let (Some(min), Some(max)) = (global_min, global_max) {
            // Create 10 buckets
            let num_buckets = 10;
            for i in 0..num_buckets {
                // This is a simplified version - would need proper value interpolation
                let bucket = HistogramBucket {
                    lower: min.clone(), // Would interpolate
                    upper: max.clone(), // Would interpolate
                    distinct_count: total_frequency / num_buckets as f64,
                    frequency: total_frequency / num_buckets as f64,
                };
                buckets.push(bucket);
            }
        }
        
        Histogram {
            histogram_type: HistogramType::EquiDepth,
            buckets,
        }
    }
    
    /// Estimate average width in bytes
    fn estimate_avg_width(fragments: &[crate::storage::fragment::ColumnFragment]) -> f64 {
        if fragments.is_empty() {
            return 8.0; // Default
        }
        
        let mut total_size = 0;
        let mut total_rows = 0;
        
        for fragment in fragments {
            total_size += fragment.metadata.memory_size;
            total_rows += fragment.metadata.cardinality;
        }
        
        if total_rows > 0 {
            total_size as f64 / total_rows as f64
        } else {
            8.0 // Default
        }
    }
}

