/// Automatic Index Recommendation & Creation
/// Uses hypergraph metadata and query patterns to recommend and create optimal indexes
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::NodeId;
use crate::query::statistics::StatisticsCatalog;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Index recommendation based on query patterns and metadata
#[derive(Clone, Debug)]
pub struct IndexRecommendation {
    pub table: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
    pub estimated_benefit: f64, // Expected query speedup (1.0 = no benefit, 10.0 = 10x speedup)
    pub creation_cost: f64, // Estimated creation time in ms
    pub priority: f64, // benefit / cost ratio
}

/// Type of index to create
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexType {
    BTree,      // Standard B-tree for range queries
    Hash,       // Hash index for equality lookups
    Bitmap,     // Bitmap index for low-cardinality columns
    Vector,     // Vector index for range filters (min/max block index)
}

/// Automatic index recommender
pub struct AutoIndexRecommender {
    graph: Arc<HyperGraph>,
    stats: StatisticsCatalog,
    /// Query access patterns: (table, column) -> access_count
    access_patterns: Arc<std::sync::Mutex<HashMap<(String, String), usize>>>,
    /// Join patterns: (left_table, left_col, right_table, right_col) -> join_count
    join_patterns: Arc<std::sync::Mutex<HashMap<(String, String, String, String), usize>>>,
    /// Filter patterns: (table, column, operator) -> filter_count
    filter_patterns: Arc<std::sync::Mutex<HashMap<(String, String, String), usize>>>,
}

impl AutoIndexRecommender {
    pub fn new(graph: Arc<HyperGraph>, stats: StatisticsCatalog) -> Self {
        Self {
            graph,
            stats,
            access_patterns: Arc::new(std::sync::Mutex::new(HashMap::new())),
            join_patterns: Arc::new(std::sync::Mutex::new(HashMap::new())),
            filter_patterns: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }
    
    /// Record query access pattern
    pub fn record_access(&self, table: &str, column: &str) {
        let mut patterns = self.access_patterns.lock().unwrap();
        *patterns.entry((table.to_string(), column.to_string())).or_insert(0) += 1;
    }
    
    /// Record join pattern
    pub fn record_join(&self, left_table: &str, left_col: &str, right_table: &str, right_col: &str) {
        let mut patterns = self.join_patterns.lock().unwrap();
        *patterns.entry((
            left_table.to_string(),
            left_col.to_string(),
            right_table.to_string(),
            right_col.to_string(),
        )).or_insert(0) += 1;
    }
    
    /// Record filter pattern
    pub fn record_filter(&self, table: &str, column: &str, operator: &str) {
        let mut patterns = self.filter_patterns.lock().unwrap();
        *patterns.entry((table.to_string(), column.to_string(), operator.to_string())).or_insert(0) += 1;
    }
    
    /// Generate index recommendations based on patterns and hypergraph metadata
    pub fn recommend_indexes(&self, max_recommendations: usize) -> Vec<IndexRecommendation> {
        let mut recommendations = Vec::new();
        
        // Analyze access patterns
        let access_patterns = self.access_patterns.lock().unwrap();
        let join_patterns = self.join_patterns.lock().unwrap();
        let filter_patterns = self.filter_patterns.lock().unwrap();
        
        // 1. Single-column indexes for frequent filters
        // Aggregate by (table, column) across all operators
        let mut column_counts: std::collections::HashMap<(String, String), usize> = std::collections::HashMap::new();
        for ((table, column, _operator), count) in filter_patterns.iter() {
            *column_counts.entry((table.clone(), column.clone())).or_insert(0) += *count;
        }
        
        for ((table, column), count) in column_counts.iter() {
            if *count > 10 { // Threshold: at least 10 uses
                if let Some(rec) = self.recommend_single_column_index(table, column, *count) {
                    recommendations.push(rec);
                }
            }
        }
        
        // 2. Join key indexes
        for ((left_table, left_col, right_table, right_col), count) in join_patterns.iter() {
            if *count > 5 { // Threshold: at least 5 joins
                // Recommend index on left side
                if let Some(rec) = self.recommend_join_index(left_table, left_col, *count) {
                    recommendations.push(rec);
                }
                // Recommend index on right side
                if let Some(rec) = self.recommend_join_index(right_table, right_col, *count) {
                    recommendations.push(rec);
                }
            }
        }
        
        // 3. Composite indexes for multi-column filters (from hypergraph metadata)
        recommendations.extend(self.recommend_composite_indexes());
        
        // 4. Sort by priority (benefit/cost ratio)
        recommendations.sort_by(|a, b| b.priority.partial_cmp(&a.priority).unwrap_or(std::cmp::Ordering::Equal));
        
        // 5. Remove duplicates and limit
        let mut seen = HashSet::new();
        recommendations.into_iter()
            .filter(|rec| {
                let key = (rec.table.clone(), rec.columns.clone());
                seen.insert(key)
            })
            .take(max_recommendations)
            .collect()
    }
    
    /// Recommend single-column index
    fn recommend_single_column_index(&self, table: &str, column: &str, access_count: usize) -> Option<IndexRecommendation> {
        // Get table statistics
        let _node = self.graph.get_table_node(table)?;
        
        // Determine index type based on column characteristics
        let index_type = if let Some(col_stats) = self.stats.get_column_stats(table, column) {
            // Use bitmap for low-cardinality columns
            if col_stats.distinct_count < 100.0 {
                IndexType::Bitmap
            } else if col_stats.min.is_some() && col_stats.max.is_some() {
                // Use vector index for numeric/timestamp columns with ranges
                IndexType::Vector
            } else {
                IndexType::BTree
            }
        } else {
            IndexType::BTree // Default
        };
        
        // Estimate benefit: more benefit for high-selectivity filters
        let selectivity = if let Some(col_stats) = self.stats.get_column_stats(table, column) {
            col_stats.distinct_count / (self.stats.get_table_stats(table)?.row_count as f64).max(1.0)
        } else {
            0.1 // Default selectivity
        };
        
        let estimated_benefit = (access_count as f64) * selectivity * 10.0; // 10x speedup for selective queries
        let creation_cost = self.estimate_index_creation_cost(table, &[column.to_string()]);
        let priority = estimated_benefit / creation_cost.max(1.0);
        
        Some(IndexRecommendation {
            table: table.to_string(),
            columns: vec![column.to_string()],
            index_type,
            estimated_benefit,
            creation_cost,
            priority,
        })
    }
    
    /// Recommend join index
    fn recommend_join_index(&self, table: &str, column: &str, join_count: usize) -> Option<IndexRecommendation> {
        // Join keys benefit from hash indexes for equality joins
        let index_type = IndexType::Hash;
        
        let estimated_benefit = (join_count as f64) * 5.0; // 5x speedup for hash joins
        let creation_cost = self.estimate_index_creation_cost(table, &[column.to_string()]);
        let priority = estimated_benefit / creation_cost.max(1.0);
        
        Some(IndexRecommendation {
            table: table.to_string(),
            columns: vec![column.to_string()],
            index_type,
            estimated_benefit,
            creation_cost,
            priority,
        })
    }
    
    /// Recommend composite indexes from hypergraph metadata
    fn recommend_composite_indexes(&self) -> Vec<IndexRecommendation> {
        let mut recommendations = Vec::new();
        
        // Analyze hypergraph edges to find common multi-column patterns
        for (edge_id, edge) in self.graph.iter_edges() {
            // Extract columns from join predicate (single column per side for now)
            let left_col = edge.predicate.left.1.clone();
            let right_col = edge.predicate.right.1.clone();
            
            // For now, recommend single-column indexes on join keys
            // In full implementation, would track multi-column join patterns
            if let Some(left_node) = self.graph.get_node(edge.source) {
                if let Some(right_node) = self.graph.get_node(edge.target) {
                    if let (Some(left_table), Some(right_table)) = (&left_node.table_name, &right_node.table_name) {
                        // Recommend index on left join column
                        let estimated_benefit = 5.0; // Join indexes help with joins
                        let creation_cost = self.estimate_index_creation_cost(left_table, &[left_col.clone()]);
                        
                        recommendations.push(IndexRecommendation {
                            table: left_table.clone(),
                            columns: vec![left_col],
                            index_type: IndexType::Hash, // Hash index for join keys
                            estimated_benefit,
                            creation_cost,
                            priority: estimated_benefit / creation_cost.max(1.0),
                        });
                        
                        // Recommend index on right join column
                        let creation_cost = self.estimate_index_creation_cost(right_table, &[right_col.clone()]);
                        recommendations.push(IndexRecommendation {
                            table: right_table.clone(),
                            columns: vec![right_col],
                            index_type: IndexType::Hash,
                            estimated_benefit,
                            creation_cost,
                            priority: estimated_benefit / creation_cost.max(1.0),
                        });
                    }
                }
            }
        }
        
        recommendations
    }
    
    /// Estimate index creation cost
    fn estimate_index_creation_cost(&self, table: &str, columns: &[String]) -> f64 {
        // Cost based on table size and number of columns
        let row_count = self.stats.get_table_stats(table)
            .map(|s| s.row_count)
            .unwrap_or(1000);
        
        // Base cost: 1ms per 1000 rows, multiplied by number of columns
        (row_count as f64 / 1000.0) * (columns.len() as f64) * 1.0
    }
    
    /// Create recommended index (would integrate with storage layer)
    pub fn create_index(&self, recommendation: &IndexRecommendation) -> Result<()> {
        tracing::info!(
            "Creating {} index on {}.{}: estimated_benefit={:.2}, cost={:.2}ms",
            format!("{:?}", recommendation.index_type),
            recommendation.table,
            recommendation.columns.join(","),
            recommendation.estimated_benefit,
            recommendation.creation_cost
        );
        
        // In full implementation, this would:
        // 1. Create index in storage layer
        // 2. Update hypergraph metadata with index information
        // 3. Update statistics catalog
        
        // Update hypergraph metadata
        if let Some(node) = self.graph.get_table_node(&recommendation.table) {
            let index_key = format!("index_{}", recommendation.columns.join("_"));
            let index_metadata = serde_json::json!({
                "type": format!("{:?}", recommendation.index_type),
                "columns": recommendation.columns,
                "created_at": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
            });
            
            let mut metadata_updates = std::collections::HashMap::new();
            metadata_updates.insert(index_key, serde_json::to_string(&index_metadata).unwrap());
            self.graph.update_node_metadata(node.id, metadata_updates);
        }
        
        Ok(())
    }
    
    /// Auto-create indexes based on recommendations
    /// Note: This only updates metadata. Use IndexBuilder to actually build indexes.
    pub fn auto_create_indexes(&self, max_indexes: usize, min_priority: f64) -> Vec<IndexRecommendation> {
        let recommendations = self.recommend_indexes(max_indexes * 2); // Get more, filter by priority
        
        let mut created = Vec::new();
        for rec in recommendations {
            if rec.priority >= min_priority {
                if self.create_index(&rec).is_ok() {
                    created.push(rec);
                    if created.len() >= max_indexes {
                        break;
                    }
                }
            }
        }
        
        created
    }
    
    /// Get recommendations for index builder to actually build
    pub fn get_build_recommendations(&self, max_indexes: usize, min_priority: f64) -> Vec<IndexRecommendation> {
        let recommendations = self.recommend_indexes(max_indexes * 2);
        recommendations.into_iter()
            .filter(|rec| rec.priority >= min_priority)
            .take(max_indexes)
            .collect()
    }
}

use anyhow::Result;

