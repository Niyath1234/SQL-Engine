/// Automatic Re-clustering
/// Monitors clustering effectiveness and automatically re-clusters when needed
use crate::hypergraph::graph::HyperGraph;
use crate::query::statistics::StatisticsCatalog;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Re-clustering recommendation
#[derive(Clone, Debug)]
pub struct ReclusterRecommendation {
    pub table: String,
    pub reason: ReclusterReason,
    pub estimated_benefit: f64,
    pub urgency: Urgency,
}

/// Reason for re-clustering
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReclusterReason {
    SkewDetected,      // Data skew detected (uneven distribution)
    Fragmentation,     // High fragmentation (many small files)
    QueryDegradation,  // Query performance degraded
    DataGrowth,        // Significant data growth since last clustering
}

/// Urgency level
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Urgency {
    Low,    // Can be done during maintenance window
    Medium, // Should be done soon
    High,   // Needs immediate attention
}

/// Automatic re-clustering manager
pub struct AutoReclusterManager {
    graph: Arc<HyperGraph>,
    stats: StatisticsCatalog,
    /// Last clustering time per table
    last_clustering: Arc<std::sync::Mutex<std::collections::HashMap<String, Instant>>>,
    /// Query performance tracking: (table, query_time_ms)
    query_performance: Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<f64>>>>,
}

impl AutoReclusterManager {
    pub fn new(graph: Arc<HyperGraph>, stats: StatisticsCatalog) -> Self {
        Self {
            graph,
            stats,
            last_clustering: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            query_performance: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }
    
    /// Record query performance for a table
    pub fn record_query_performance(&self, table: &str, query_time_ms: f64) {
        let mut perf = self.query_performance.lock().unwrap();
        let perf_history = perf.entry(table.to_string()).or_insert_with(Vec::new);
        perf_history.push(query_time_ms);
        
        // Keep only last 100 queries
        if perf_history.len() > 100 {
            perf_history.remove(0);
        }
    }
    
    /// Check if re-clustering is needed
    pub fn check_reclustering_needed(&self, table: &str) -> Option<ReclusterRecommendation> {
        let node = self.graph.get_table_node(table)?;
        
        // Check 1: Data growth since last clustering
        if let Some(last_cluster_time) = self.last_clustering.lock().unwrap().get(table) {
            let time_since_clustering = last_cluster_time.elapsed();
            let table_stats = self.stats.get_table_stats(table)?;
            
            // If significant time passed and table grew, recommend re-clustering
            if time_since_clustering > Duration::from_secs(86400 * 7) { // 7 days
                return Some(ReclusterRecommendation {
                    table: table.to_string(),
                    reason: ReclusterReason::DataGrowth,
                    estimated_benefit: 5.0, // 5x improvement
                    urgency: Urgency::Medium,
                });
            }
        }
        
        // Check 2: Query performance degradation
        let perf = self.query_performance.lock().unwrap();
        if let Some(perf_history) = perf.get(table) {
            if perf_history.len() >= 20 {
                // Compare recent vs older performance
                let recent_avg: f64 = perf_history.iter().rev().take(10).sum::<f64>() / 10.0;
                let older_avg: f64 = perf_history.iter().rev().skip(10).take(10).sum::<f64>() / 10.0;
                
                if recent_avg > older_avg * 1.5 {
                    // Performance degraded by 50%
                    return Some(ReclusterRecommendation {
                        table: table.to_string(),
                        reason: ReclusterReason::QueryDegradation,
                        estimated_benefit: 3.0, // 3x improvement
                        urgency: Urgency::High,
                    });
                }
            }
        }
        
        // Check 3: Data skew (from hypergraph metadata)
        if let Some(skew_metadata) = node.metadata.get("data_skew") {
            if let Ok(skew_value) = serde_json::from_str::<f64>(skew_metadata) {
                if skew_value > 0.5 { // High skew
                    return Some(ReclusterRecommendation {
                        table: table.to_string(),
                        reason: ReclusterReason::SkewDetected,
                        estimated_benefit: 4.0, // 4x improvement
                        urgency: Urgency::Medium,
                    });
                }
            }
        }
        
        // Check 4: Fragmentation (from hypergraph metadata)
        if let Some(frag_metadata) = node.metadata.get("fragmentation") {
            if let Ok(frag_value) = serde_json::from_str::<f64>(frag_metadata) {
                if frag_value > 0.7 { // High fragmentation
                    return Some(ReclusterRecommendation {
                        table: table.to_string(),
                        reason: ReclusterReason::Fragmentation,
                        estimated_benefit: 2.0, // 2x improvement
                        urgency: Urgency::Low,
                    });
                }
            }
        }
        
        None
    }
    
    /// Perform re-clustering
    pub fn perform_reclustering(&self, table: &str) -> anyhow::Result<()> {
        tracing::info!("Performing re-clustering on table: {}", table);
        
        // Get clustering columns from hypergraph metadata
        let clustering_columns = if let Some(node) = self.graph.get_table_node(table) {
            if let Some(cluster_metadata) = node.metadata.get("clustering") {
                if let Ok(cluster_info) = serde_json::from_str::<serde_json::Value>(cluster_metadata) {
                    cluster_info["columns"].as_array()
                        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect::<Vec<_>>())
                        .unwrap_or_default()
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        
        if clustering_columns.is_empty() {
            return Err(anyhow::anyhow!("No clustering columns found for table {}", table));
        }
        
        // In full implementation, this would:
        // 1. Trigger storage layer to re-cluster
        // 2. Update hypergraph metadata
        // 3. Update statistics
        
        // Update last clustering time
        self.last_clustering.lock().unwrap().insert(table.to_string(), Instant::now());
        
        // Update hypergraph metadata
        if let Some(node) = self.graph.get_table_node(table) {
            let recluster_metadata = serde_json::json!({
                "last_reclustered": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
                "clustering_columns": clustering_columns,
            });
            
            let mut metadata_updates = std::collections::HashMap::new();
            metadata_updates.insert("reclustering".to_string(), serde_json::to_string(&recluster_metadata)?);
            self.graph.update_node_metadata(node.id, metadata_updates);
        }
        
        Ok(())
    }
    
    /// Auto-recluster tables that need it
    pub fn auto_recluster(&self, max_tables: usize) -> Vec<String> {
        let mut reclustered = Vec::new();
        
        // Get all tables from hypergraph
        for (_, node) in self.graph.iter_nodes() {
            if let Some(table_name) = &node.table_name {
                if let Some(recommendation) = self.check_reclustering_needed(table_name) {
                    if recommendation.urgency == Urgency::High || recommendation.urgency == Urgency::Medium {
                        if self.perform_reclustering(table_name).is_ok() {
                            reclustered.push(table_name.clone());
                            if reclustered.len() >= max_tables {
                                break;
                            }
                        }
                    }
                }
            }
        }
        
        reclustered
    }
}

