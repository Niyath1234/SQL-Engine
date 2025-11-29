/// Extract query patterns from executed queries
use crate::query::ParsedQuery;
use crate::query::parser::JoinInfo;
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::NodeId;
use crate::hypergraph::edge::EdgeId;
use crate::execution::engine::QueryResult;
use crate::learning::optimization_hints::{JoinPattern, FilterPattern};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use anyhow::Result;

/// Query pattern extracted from executed query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPattern {
    // Topology patterns
    pub tables: Vec<String>,
    pub columns: Vec<String>,
    pub join_relationships: Vec<JoinPattern>,
    pub filter_patterns: Vec<FilterPattern>,
    
    // Access patterns
    pub column_access_frequency: HashMap<String, f64>,
    pub join_frequency: HashMap<(String, String), f64>,
    
    // Temporal patterns
    pub timestamp: u64,
    pub time_of_day: f64,  // 0.0-24.0 hours
    
    // Performance metrics
    pub execution_time_ms: f64,
    pub rows_returned: usize,
    pub optimal_plan_hash: u64,
    
    // Hypergraph context
    pub hypergraph_path: Vec<NodeId>,
    pub edge_usage: Vec<EdgeId>,
}

/// Extracts patterns from query execution
pub struct PatternExtractor;

impl PatternExtractor {
    pub fn new() -> Self {
        Self
    }
    
    /// Extract pattern from executed query
    pub fn extract_pattern(
        &self,
        parsed_query: &ParsedQuery,
        result: &QueryResult,
        execution_time_ms: f64,
        plan_hash: u64,
        graph: &HyperGraph,
    ) -> Result<QueryPattern> {
        // Extract topology information
        let tables = parsed_query.tables.clone();
        let columns = parsed_query.columns.clone();
        
        // Extract join patterns
        let join_relationships = parsed_query.joins.iter()
            .map(|join| {
                // Look up selectivity from graph statistics
                let selectivity = self.get_join_selectivity(
                    &join.left_table,
                    &join.right_table,
                    graph,
                );
                
                JoinPattern {
                    left_table: join.left_table.clone(),
                    right_table: join.right_table.clone(),
                    join_column: join.left_column.clone(),
                    selectivity,
                    frequency: 1,
                }
            })
            .collect();
        
        // Extract filter patterns
        let filter_patterns = parsed_query.filters.iter()
            .map(|filter| FilterPattern {
                table: filter.table.clone(),
                column: filter.column.clone(),
                operator: format!("{:?}", filter.operator),
                value_distribution: Vec::new(), // Could track common values later
            })
            .collect();
        
        // Extract column access frequencies
        let mut column_access_frequency = HashMap::new();
        for col in &columns {
            *column_access_frequency.entry(col.clone()).or_insert(0.0) += 1.0;
        }
        
        // Normalize frequencies
        let total = column_access_frequency.values().sum::<f64>();
        if total > 0.0 {
            for freq in column_access_frequency.values_mut() {
                *freq /= total;
            }
        }
        
        // Extract join frequencies
        let mut join_frequency = HashMap::new();
        for join in &parsed_query.joins {
            let key = (join.left_table.clone(), join.right_table.clone());
            *join_frequency.entry(key).or_insert(0.0) += 1.0;
        }
        
        // Normalize join frequencies
        let join_total = join_frequency.values().sum::<f64>();
        if join_total > 0.0 {
            for freq in join_frequency.values_mut() {
                *freq /= join_total;
            }
        }
        
        // Extract hypergraph path
        let hypergraph_path = self.extract_hypergraph_path(&tables, graph)?;
        let edge_usage = self.extract_edge_usage(&parsed_query.joins, graph)?;
        
        // Get current time
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let time_of_day = ((timestamp % (24 * 3600)) as f64) / 3600.0; // Hours in day
        
        Ok(QueryPattern {
            tables,
            columns,
            join_relationships,
            filter_patterns,
            column_access_frequency,
            join_frequency,
            timestamp,
            time_of_day,
            execution_time_ms,
            rows_returned: result.row_count,
            optimal_plan_hash: plan_hash,
            hypergraph_path,
            edge_usage,
        })
    }
    
    /// Extract hypergraph path (node sequence) from tables
    pub fn extract_hypergraph_path(&self, tables: &[String], graph: &HyperGraph) -> Result<Vec<NodeId>> {
        let mut path = Vec::new();
        
        for table in tables {
            if let Some(node) = graph.get_table_node(table) {
                path.push(node.id);
            }
        }
        
        Ok(path)
    }
    
    /// Extract edge usage from joins
    pub fn extract_edge_usage(&self, joins: &[JoinInfo], graph: &HyperGraph) -> Result<Vec<EdgeId>> {
        let mut edges = Vec::new();
        
        for join in joins {
            // Find edge between join tables
            if let Some(left_node) = graph.get_table_node(&join.left_table) {
                if let Some(right_node) = graph.get_table_node(&join.right_table) {
                    // Find edges connecting these nodes
                    // Try to find edge in graph
                    // For now, we'll collect node pairs (can be enhanced later)
                    // edges.push(...) when we have edge lookup
                }
            }
        }
        
        Ok(edges)
    }
    
    /// Get join selectivity from graph statistics (public for pattern learner)
    pub fn get_join_selectivity(
        &self,
        left_table: &str,
        right_table: &str,
        graph: &HyperGraph,
    ) -> f64 {
        // Look up from join statistics in node metadata
        if let Some(node) = graph.get_table_node(left_table) {
            if let Some(join_stats_json) = node.metadata.get("join_statistics") {
                if let Ok(join_stats) = serde_json::from_str::<crate::storage::statistics::JoinStatistics>(join_stats_json) {
                    // Try to find selectivity for this join
                    // For now return default
                    return 0.5;
                }
            }
        }
        0.5 // Default selectivity
    }
}

impl Default for PatternExtractor {
    fn default() -> Self {
        Self::new()
    }
}

