/// Automatic Storage Layout Tuning
/// Optimizes storage layout (columnar vs row-oriented, compression) based on access patterns
use crate::hypergraph::graph::HyperGraph;
use crate::query::statistics::StatisticsCatalog;
use std::sync::Arc;

/// Storage layout recommendation
#[derive(Clone, Debug)]
pub struct StorageLayoutRecommendation {
    pub table: String,
    pub layout: StorageLayout,
    pub compression: CompressionStrategy,
    pub estimated_benefit: f64,
}

/// Storage layout type
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageLayout {
    Columnar,   // Column-oriented (for analytical queries)
    RowOriented, // Row-oriented (for OLTP queries)
    Hybrid,     // Hybrid (mix based on access patterns)
}

/// Compression strategy
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompressionStrategy {
    None,
    LZ4,        // Fast compression
    Zstd,       // Balanced compression
    Dictionary, // Dictionary encoding for low-cardinality columns
    Delta,      // Delta encoding for sorted columns
}

/// Automatic storage layout tuner
pub struct AutoStorageTuner {
    graph: Arc<HyperGraph>,
    stats: StatisticsCatalog,
    /// Access patterns: (table, access_type) -> count
    /// access_type: "scan_all", "scan_columns", "point_lookup"
    access_patterns: Arc<std::sync::Mutex<std::collections::HashMap<(String, String), usize>>>,
}

impl AutoStorageTuner {
    pub fn new(graph: Arc<HyperGraph>, stats: StatisticsCatalog) -> Self {
        Self {
            graph,
            stats,
            access_patterns: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }
    
    /// Record access pattern
    pub fn record_access(&self, table: &str, access_type: &str) {
        let mut patterns = self.access_patterns.lock().unwrap();
        *patterns.entry((table.to_string(), access_type.to_string())).or_insert(0) += 1;
    }
    
    /// Recommend storage layout based on access patterns and hypergraph metadata
    pub fn recommend_layout(&self, table: &str) -> Option<StorageLayoutRecommendation> {
        let node = self.graph.get_table_node(table)?;
        let table_stats = self.stats.get_table_stats(table)?;
        
        let access_patterns = self.access_patterns.lock().unwrap();
        
        // Analyze access patterns
        let scan_all_count = access_patterns.get(&(table.to_string(), "scan_all".to_string())).copied().unwrap_or(0);
        let scan_columns_count = access_patterns.get(&(table.to_string(), "scan_columns".to_string())).copied().unwrap_or(0);
        let point_lookup_count = access_patterns.get(&(table.to_string(), "point_lookup".to_string())).copied().unwrap_or(0);
        
        // Determine layout based on access patterns
        let layout = if scan_columns_count > scan_all_count * 2 && scan_columns_count > point_lookup_count {
            // More column scans -> columnar
            StorageLayout::Columnar
        } else if point_lookup_count > scan_columns_count * 2 {
            // More point lookups -> row-oriented
            StorageLayout::RowOriented
        } else {
            // Mixed -> hybrid
            StorageLayout::Hybrid
        };
        
        // Determine compression based on column characteristics from hypergraph
        let compression = self.recommend_compression(table, &node);
        
        // Estimate benefit
        let estimated_benefit = match layout {
            StorageLayout::Columnar => (scan_columns_count as f64) * 0.5, // 50% I/O reduction
            StorageLayout::RowOriented => (point_lookup_count as f64) * 0.3, // 30% latency reduction
            StorageLayout::Hybrid => ((scan_columns_count + point_lookup_count) as f64) * 0.2, // 20% overall
        };
        
        Some(StorageLayoutRecommendation {
            table: table.to_string(),
            layout,
            compression,
            estimated_benefit,
        })
    }
    
    /// Recommend compression strategy based on column characteristics
    fn recommend_compression(&self, table: &str, node: &crate::hypergraph::node::HyperNode) -> CompressionStrategy {
        // Analyze columns from hypergraph metadata
        let mut low_cardinality_count = 0;
        let mut total_columns = 0;
        
        // Get column nodes from hypergraph
        for (_, col_node) in self.graph.iter_nodes() {
            if matches!(col_node.node_type, crate::hypergraph::node::NodeType::Column) {
                if col_node.table_name.as_deref() == Some(table) {
                    total_columns += 1;
                    if let Some(col_name) = &col_node.column_name {
                        if let Some(col_stats) = self.stats.get_column_stats(table, col_name) {
                            if col_stats.distinct_count < 100.0 {
                                low_cardinality_count += 1;
                            }
                        }
                    }
                }
            }
        }
        
        // Use dictionary encoding if many low-cardinality columns
        if low_cardinality_count > total_columns / 2 {
            CompressionStrategy::Dictionary
        } else if total_columns > 10 {
            // Large tables benefit from compression
            CompressionStrategy::Zstd
        } else {
            CompressionStrategy::LZ4 // Fast compression for smaller tables
        }
    }
    
    /// Apply storage layout recommendation
    pub fn apply_layout(&self, recommendation: &StorageLayoutRecommendation) -> anyhow::Result<()> {
        tracing::info!(
            "Applying {:?} layout with {:?} compression on {}: benefit={:.2}",
            recommendation.layout,
            recommendation.compression,
            recommendation.table,
            recommendation.estimated_benefit
        );
        
        // Update hypergraph metadata
        if let Some(node) = self.graph.get_table_node(&recommendation.table) {
            let layout_metadata = serde_json::json!({
                "layout": format!("{:?}", recommendation.layout),
                "compression": format!("{:?}", recommendation.compression),
                "updated_at": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
            });
            
            let mut metadata_updates = std::collections::HashMap::new();
            metadata_updates.insert("storage_layout".to_string(), serde_json::to_string(&layout_metadata)?);
            self.graph.update_node_metadata(node.id, metadata_updates);
        }
        
        Ok(())
    }
}

