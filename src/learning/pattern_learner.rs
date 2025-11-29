/// Main interface for query pattern learning
use crate::learning::pattern_extractor::{PatternExtractor, QueryPattern};
use crate::learning::pattern_embedder::PatternEmbedder;
use crate::learning::pattern_vector_db::PatternVectorDB;
use crate::learning::optimization_hints::{OptimizationHints, PredictedQuery, JoinPattern, FilterPattern};
use crate::query::ParsedQuery;
use crate::execution::engine::QueryResult;
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::NodeId;
use std::collections::{VecDeque, HashMap, HashSet};
use std::sync::Arc;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use serde_json;
use anyhow::Result;

/// Main query pattern learner
pub struct QueryPatternLearner {
    pub extractor: PatternExtractor,
    pub vector_db: PatternVectorDB,
    
    // Mutable state for weights (will be stored in file later)
    table_weights_cache: HashMap<String, Vec<f32>>,
    column_weights_cache: HashMap<String, Vec<f32>>,
    join_weights_cache: HashMap<(String, String), Vec<f32>>,
    
    // Recent query history (for context)
    pub recent_queries: VecDeque<QueryPattern>,
    pub max_history: usize,
    
    // Hypergraph reference (for topology)
    pub graph: Arc<HyperGraph>,
}

impl QueryPatternLearner {
    /// Create new pattern learner
    pub fn new(graph: Arc<HyperGraph>, db_dir: impl AsRef<std::path::Path>) -> Result<Self> {
        let vector_db = PatternVectorDB::new(db_dir)?;
        let extractor = PatternExtractor::new();
        
        Ok(Self {
            extractor,
            vector_db,
            recent_queries: VecDeque::with_capacity(100),
            max_history: 100,
            graph,
            table_weights_cache: HashMap::new(),
            column_weights_cache: HashMap::new(),
            join_weights_cache: HashMap::new(),
        })
    }
    
    /// Create dummy learner (for error cases)
    pub fn dummy() -> Self {
        // Create with temporary directory that will be ignored
        let graph = Arc::new(HyperGraph::new());
        Self::new(graph, "/tmp/dummy_patterns").unwrap_or_else(|_| {
            // Fallback if even dummy creation fails - create minimal learner
            Self {
                extractor: PatternExtractor::new(),
                vector_db: PatternVectorDB::new("/tmp/dummy_patterns").unwrap_or_else(|_| {
                    // If file creation fails, we can't continue
                    panic!("Failed to create dummy pattern learner")
                }),
                recent_queries: VecDeque::with_capacity(100),
                max_history: 100,
                graph: Arc::new(HyperGraph::new()),
                table_weights_cache: HashMap::new(),
                column_weights_cache: HashMap::new(),
                join_weights_cache: HashMap::new(),
            }
        })
    }
    
    /// Learn from successful query execution (called after query completes)
    pub fn learn_from_query(
        &mut self,
        parsed_query: &ParsedQuery,
        execution_result: &QueryResult,
        execution_time_ms: f64,
        plan_hash: u64,
    ) -> Result<()> {
        // 1. Extract pattern from query
        let pattern = self.extractor.extract_pattern(
            parsed_query,
            execution_result,
            execution_time_ms,
            plan_hash,
            &self.graph,
        )?;
        
        // 2. Generate vector embedding (update weights in embedder)
        let mut embedder = PatternEmbedder::new(128);
        embedder.table_weights = self.table_weights_cache.clone();
        embedder.column_weights = self.column_weights_cache.clone();
        embedder.join_weights = self.join_weights_cache.clone();
        
        let embedding = embedder.embed_pattern(&pattern);
        
        // Update caches with learned weights
        self.table_weights_cache = embedder.table_weights.clone();
        self.column_weights_cache = embedder.column_weights.clone();
        self.join_weights_cache = embedder.join_weights.clone();
        
        // 3. Store in vector database
        self.vector_db.learn_pattern(pattern.clone(), embedding)?;
        
        // 4. Update node metadata with pattern references (Phase 2: Hybrid approach)
        let pattern_hash = Self::hash_pattern(&pattern);
        if let Err(e) = self.update_node_metadata_with_pattern(&pattern, pattern_hash) {
            eprintln!("Warning: Failed to update node metadata with pattern: {}", e);
        }
        
        // 5. Add to recent history
        self.recent_queries.push_back(pattern);
        if self.recent_queries.len() > self.max_history {
            self.recent_queries.pop_front();
        }
        
        Ok(())
    }
    
    /// Hash a pattern to create a unique pattern ID
    pub fn hash_pattern(pattern: &QueryPattern) -> u64 {
        let mut hasher = DefaultHasher::new();
        
        // Hash stable pattern features (not temporal/metrics)
        pattern.tables.hash(&mut hasher);
        pattern.columns.hash(&mut hasher);
        for join in &pattern.join_relationships {
            join.left_table.hash(&mut hasher);
            join.right_table.hash(&mut hasher);
            join.join_column.hash(&mut hasher);
        }
        for filter in &pattern.filter_patterns {
            filter.table.hash(&mut hasher);
            filter.column.hash(&mut hasher);
            filter.operator.hash(&mut hasher);
        }
        
        hasher.finish()
    }
    
    /// Update node metadata with pattern references for all nodes involved in the pattern
    fn update_node_metadata_with_pattern(&self, pattern: &QueryPattern, pattern_hash: u64) -> Result<()> {
        // Collect all unique tables from pattern
        let mut tables: HashSet<String> = pattern.tables.iter().cloned().collect();
        
        // Also collect tables from join relationships
        for join in &pattern.join_relationships {
            tables.insert(join.left_table.clone());
            tables.insert(join.right_table.clone());
        }
        
        // Also collect tables from filter patterns
        for filter in &pattern.filter_patterns {
            tables.insert(filter.table.clone());
        }
        
        // Update metadata for each table node
        for table_name in tables {
            if let Some(table_node) = self.graph.get_table_node(&table_name) {
                let node_id = table_node.id;
                
                // Get existing pattern references from metadata
                let mut pattern_ids: Vec<u64> = table_node.metadata
                    .get("pattern_ids")
                    .and_then(|s| serde_json::from_str::<Vec<u64>>(s).ok())
                    .unwrap_or_default();
                
                let mut pattern_frequency: HashMap<u64, usize> = table_node.metadata
                    .get("pattern_frequency")
                    .and_then(|s| serde_json::from_str::<HashMap<u64, usize>>(s).ok())
                    .unwrap_or_default();
                
                // Add pattern hash if not already present
                if !pattern_ids.contains(&pattern_hash) {
                    pattern_ids.push(pattern_hash);
                }
                
                // Increment frequency for this pattern
                *pattern_frequency.entry(pattern_hash).or_insert(0) += 1;
                
                // Extract common join partners for this table
                let mut join_partners: Vec<String> = Vec::new();
                for join in &pattern.join_relationships {
                    if join.left_table == table_name {
                        join_partners.push(join.right_table.clone());
                    } else if join.right_table == table_name {
                        join_partners.push(join.left_table.clone());
                    }
                }
                
                // Get existing common join partners
                let mut common_join_partners: Vec<String> = table_node.metadata
                    .get("common_join_partners")
                    .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                    .unwrap_or_default();
                
                // Merge with new join partners (deduplicate)
                for partner in join_partners {
                    if !common_join_partners.contains(&partner) {
                        common_join_partners.push(partner);
                    }
                }
                
                // Keep only top 10 most common join partners (to avoid bloat)
                if common_join_partners.len() > 10 {
                    common_join_partners.truncate(10);
                }
                
                // Prepare metadata updates
                let mut metadata_updates = HashMap::new();
                metadata_updates.insert(
                    "pattern_ids".to_string(),
                    serde_json::to_string(&pattern_ids)?,
                );
                metadata_updates.insert(
                    "pattern_frequency".to_string(),
                    serde_json::to_string(&pattern_frequency)?,
                );
                metadata_updates.insert(
                    "common_join_partners".to_string(),
                    serde_json::to_string(&common_join_partners)?,
                );
                
                // Update node metadata
                self.graph.update_node_metadata(node_id, metadata_updates);
            }
        }
        
        Ok(())
    }
    
    /// Get pattern hashes for a given table node
    pub fn get_pattern_ids_for_node(&self, table_name: &str) -> Vec<u64> {
        if let Some(table_node) = self.graph.get_table_node(table_name) {
            table_node.metadata
                .get("pattern_ids")
                .and_then(|s| serde_json::from_str::<Vec<u64>>(s).ok())
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    }
    
    /// Get pattern frequency for a given table node
    pub fn get_pattern_frequency_for_node(&self, table_name: &str) -> HashMap<u64, usize> {
        if let Some(table_node) = self.graph.get_table_node(table_name) {
            table_node.metadata
                .get("pattern_frequency")
                .and_then(|s| serde_json::from_str::<HashMap<u64, usize>>(s).ok())
                .unwrap_or_default()
        } else {
            HashMap::new()
        }
    }
    
    /// Get common join partners for a given table node
    pub fn get_common_join_partners_for_node(&self, table_name: &str) -> Vec<String> {
        if let Some(table_node) = self.graph.get_table_node(table_name) {
            table_node.metadata
                .get("common_join_partners")
                .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    }
    
    /// Get patterns for a given table node (lookup by pattern hashes)
    pub fn get_patterns_for_node(&self, table_name: &str) -> Vec<&QueryPattern> {
        let pattern_ids = self.get_pattern_ids_for_node(table_name);
        let mut patterns = Vec::new();
        
        // Look up patterns by hash in vector DB
        for pattern_id in pattern_ids {
            if let Some(pattern) = self.vector_db.patterns.iter().find(|p| {
                Self::hash_pattern(p) == pattern_id
            }) {
                patterns.push(pattern);
            }
        }
        
        patterns
    }
    
    /// Predict next query and suggest optimizations
    pub fn predict_and_optimize(
        &mut self,
        current_query: &ParsedQuery,
    ) -> Result<OptimizationHints> {
        // 1. Extract preview pattern from current query
        let preview_pattern = self.extract_pattern_preview(current_query)?;
        
        // Create temporary embedder with cached weights
        let mut embedder = PatternEmbedder::new(128);
        embedder.table_weights = self.table_weights_cache.clone();
        embedder.column_weights = self.column_weights_cache.clone();
        embedder.join_weights = self.join_weights_cache.clone();
        
        let current_embedding = embedder.embed_pattern(&preview_pattern);
        
        // Update caches
        self.table_weights_cache = embedder.table_weights.clone();
        self.column_weights_cache = embedder.column_weights.clone();
        self.join_weights_cache = embedder.join_weights.clone();
        
        // 2. Find similar historical patterns
        let similar = self.vector_db.find_similar_patterns(&current_embedding, 5)?;
        
        // 3. Extract optimization hints from similar patterns
        let mut hints = OptimizationHints::default();
        
        for (pattern_idx, similarity) in similar {
            if pattern_idx >= self.vector_db.patterns.len() {
                continue;
            }
            
            let historical_pattern = &self.vector_db.patterns[pattern_idx];
            
            // Only use patterns with good similarity
            if similarity < 0.3 {
                continue;
            }
            
            // Suggest indexes that worked well
            if let Some(indexes) = self.suggest_indexes(historical_pattern) {
                hints.suggested_indexes.extend(indexes);
            }
            
            // Suggest join order
            if hints.suggested_join_order.is_none() {
                if let Some(join_order) = self.suggest_join_order(historical_pattern) {
                    hints.suggested_join_order = Some(join_order);
                }
            }
            
            // Suggest filters to push down
            if hints.filter_pushdown_hints.is_empty() {
                hints.filter_pushdown_hints = historical_pattern.filter_patterns.clone();
            }
        }
        
        // 4. Predict next query (workflow pattern)
        if let Some(predicted) = self.predict_next_query()? {
            hints.predicted_next_query = Some(predicted);
        }
        
        Ok(hints)
    }
    
    /// Extract preview pattern (without execution result)
    fn extract_pattern_preview(
        &self,
        parsed_query: &ParsedQuery,
    ) -> Result<QueryPattern> {
        // Create minimal pattern for preview (before execution)
        let tables = parsed_query.tables.clone();
        let columns = parsed_query.columns.clone();
        
        let join_relationships = parsed_query.joins.iter()
            .map(|join| {
                let selectivity = self.extractor.get_join_selectivity(
                    &join.left_table,
                    &join.right_table,
                    &self.graph,
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
        
        let filter_patterns = parsed_query.filters.iter()
            .map(|filter| FilterPattern {
                table: filter.table.clone(),
                column: filter.column.clone(),
                operator: format!("{:?}", filter.operator),
                value_distribution: Vec::new(),
            })
            .collect();
        
        let mut column_access_frequency = HashMap::new();
        for col in &columns {
            *column_access_frequency.entry(col.clone()).or_insert(0.0) += 1.0;
        }
        
        let total = column_access_frequency.values().sum::<f64>();
        if total > 0.0 {
            for freq in column_access_frequency.values_mut() {
                *freq /= total;
            }
        }
        
        let mut join_frequency = HashMap::new();
        for join in &parsed_query.joins {
            let key = (join.left_table.clone(), join.right_table.clone());
            *join_frequency.entry(key).or_insert(0.0) += 1.0;
        }
        
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let time_of_day = ((timestamp % (24 * 3600)) as f64) / 3600.0;
        
        let hypergraph_path = self.extractor.extract_hypergraph_path(&tables, &self.graph)?;
        let edge_usage = self.extractor.extract_edge_usage(&parsed_query.joins, &self.graph)?;
        
        Ok(QueryPattern {
            tables,
            columns,
            join_relationships,
            filter_patterns,
            column_access_frequency,
            join_frequency,
            timestamp,
            time_of_day,
            execution_time_ms: 0.0, // Unknown before execution
            rows_returned: 0,        // Unknown before execution
            optimal_plan_hash: 0,    // Will be computed later
            hypergraph_path,
            edge_usage,
        })
    }
    
    /// Suggest indexes based on learned patterns
    fn suggest_indexes(&self, pattern: &QueryPattern) -> Option<Vec<String>> {
        // Analyze which columns are frequently accessed
        let mut column_freq: Vec<(&String, &f64)> = pattern
            .column_access_frequency
            .iter()
            .collect();
        
        // Sort by frequency (highest first)
        column_freq.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        // Take top columns
        let top_columns: Vec<String> = column_freq
            .iter()
            .take(5)
            .map(|(col, _)| (*col).clone())
            .collect();
        
        if top_columns.is_empty() {
            None
        } else {
            Some(top_columns)
        }
    }
    
    /// Suggest join order based on learned patterns
    fn suggest_join_order(&self, pattern: &QueryPattern) -> Option<Vec<String>> {
        // Use hypergraph path from pattern
        if !pattern.tables.is_empty() {
            Some(pattern.tables.clone())
        } else {
            None
        }
    }
    
    /// Predict next query based on recent query history
    fn predict_next_query(&self) -> Result<Option<PredictedQuery>> {
        if self.recent_queries.len() < 2 {
            return Ok(None);
        }
        
        // Analyze recent query sequence
        let recent: Vec<&QueryPattern> = self.recent_queries.iter().collect();
        
        // Simple prediction: find patterns that frequently follow current pattern
        // For Phase 1, we'll use a simple heuristic
        
        // Get the most recent query
        let current = recent.last().unwrap();
        
        // Look for queries that frequently follow similar queries
        let mut follow_table_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        let mut follow_column_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        
        // Find similar queries and what followed them
        for i in 0..recent.len() - 1 {
            let query = &recent[i];
            
            // Simple similarity check (tables overlap)
            let table_overlap = self.table_overlap(current, query);
            if table_overlap > 0.5 {
                // Check what came after this query
                if i + 1 < recent.len() {
                    let next = &recent[i + 1];
                    
                    // Count tables/columns that appear in the next query
                    for table in &next.tables {
                        *follow_table_counts.entry(table.clone()).or_insert(0) += 1;
                    }
                    for column in &next.columns {
                        *follow_column_counts.entry(column.clone()).or_insert(0) += 1;
                    }
                }
            }
        }
        
        // Build prediction from most common follow-ups
        let mut follow_table_vec: Vec<(String, usize)> = follow_table_counts.into_iter().collect();
        follow_table_vec.sort_by_key(|(_, count)| *count);
        follow_table_vec.reverse();
        
        let likely_tables: Vec<String> = follow_table_vec
            .into_iter()
            .take(5)
            .map(|(table, _)| table)
            .collect();
        
        let mut follow_column_vec: Vec<(String, usize)> = follow_column_counts.into_iter().collect();
        follow_column_vec.sort_by_key(|(_, count)| *count);
        follow_column_vec.reverse();
        
        let likely_columns: Vec<String> = follow_column_vec
            .into_iter()
            .take(10)
            .map(|(col, _)| col)
            .collect();
        
        if likely_tables.is_empty() {
            return Ok(None);
        }
        
        let confidence = (likely_tables.len() as f32).min(1.0); // Simple confidence
        
        Ok(Some(PredictedQuery {
            likely_tables,
            likely_columns,
            likely_joins: Vec::new(), // Could be enhanced later
            confidence,
        }))
    }
    
    /// Compute table overlap between two patterns
    fn table_overlap(&self, pattern1: &QueryPattern, pattern2: &QueryPattern) -> f32 {
        let set1: std::collections::HashSet<_> = pattern1.tables.iter().collect();
        let set2: std::collections::HashSet<_> = pattern2.tables.iter().collect();
        
        let intersection: Vec<_> = set1.intersection(&set2).collect();
        let union: Vec<_> = set1.union(&set2).collect();
        
        if union.is_empty() {
            0.0
        } else {
            intersection.len() as f32 / union.len() as f32
        }
    }
}


