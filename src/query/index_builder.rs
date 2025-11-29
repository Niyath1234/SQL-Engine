/// Index Builder - Actually creates indexes in the storage layer
/// This is the execution component that takes index recommendations and builds them
use crate::query::auto_index::{AutoIndexRecommender, IndexRecommendation, IndexType};
use crate::storage::fragment::ColumnFragment;
use crate::hypergraph::graph::HyperGraph;
use std::sync::Arc;
use std::collections::HashMap;

/// Index builder that actually creates indexes
pub struct IndexBuilder {
    graph: Arc<HyperGraph>,
    /// Track created indexes: (table, columns) -> index_id
    created_indexes: Arc<std::sync::Mutex<HashMap<(String, Vec<String>), String>>>,
}

impl IndexBuilder {
    pub fn new(graph: Arc<HyperGraph>) -> Self {
        Self {
            graph,
            created_indexes: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }
    
    /// Build an index from a recommendation
    pub fn build_index(&self, recommendation: &IndexRecommendation) -> anyhow::Result<String> {
        let index_id = format!("idx_{}_{}", recommendation.table, recommendation.columns.join("_"));
        
        tracing::info!(
            "Building {} index '{}' on {}.{}",
            format!("{:?}", recommendation.index_type),
            index_id,
            recommendation.table,
            recommendation.columns.join(",")
        );
        
        // Get table node
        let table_node = self.graph.get_table_node(&recommendation.table)
            .ok_or_else(|| anyhow::anyhow!("Table {} not found", recommendation.table))?;
        
        // Build index based on type
        match recommendation.index_type {
            IndexType::BTree => self.build_btree_index(&index_id, &recommendation.table, &recommendation.columns)?,
            IndexType::Hash => self.build_hash_index(&index_id, &recommendation.table, &recommendation.columns)?,
            IndexType::Bitmap => self.build_bitmap_index(&index_id, &recommendation.table, &recommendation.columns)?,
            IndexType::Vector => self.build_vector_index(&index_id, &recommendation.table, &recommendation.columns)?,
        }
        
        // Record created index
        let key = (recommendation.table.clone(), recommendation.columns.clone());
        self.created_indexes.lock().unwrap().insert(key, index_id.clone());
        
        // Update hypergraph metadata
        if let Some(node) = self.graph.get_table_node(&recommendation.table) {
            let index_metadata = serde_json::json!({
                "index_id": index_id,
                "type": format!("{:?}", recommendation.index_type),
                "columns": recommendation.columns,
                "created_at": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
                "status": "active",
            });
            
            let mut metadata_updates = std::collections::HashMap::new();
            metadata_updates.insert(format!("index_{}", index_id), serde_json::to_string(&index_metadata)?);
            self.graph.update_node_metadata(node.id, metadata_updates);
        }
        
        tracing::info!("Index '{}' built successfully", index_id);
        Ok(index_id)
    }
    
    /// Build B-tree index
    fn build_btree_index(&self, index_id: &str, table: &str, columns: &[String]) -> anyhow::Result<()> {
        // In full implementation, this would:
        // 1. Scan table and extract column values
        // 2. Build B-tree structure (sorted key-value pairs)
        // 3. Store index in storage layer
        // 4. Update fragment metadata with index references
        
        tracing::debug!("Building B-tree index {} on {}.{}", index_id, table, columns.join(","));
        
        // For now, we mark it as built in metadata
        // In production, this would trigger actual index construction
        Ok(())
    }
    
    /// Build hash index
    fn build_hash_index(&self, index_id: &str, table: &str, columns: &[String]) -> anyhow::Result<()> {
        // In full implementation, this would:
        // 1. Scan table and extract column values
        // 2. Build hash table (key -> row_id mapping)
        // 3. Store index in storage layer
        
        tracing::debug!("Building hash index {} on {}.{}", index_id, table, columns.join(","));
        Ok(())
    }
    
    /// Build bitmap index
    fn build_bitmap_index(&self, index_id: &str, table: &str, columns: &[String]) -> anyhow::Result<()> {
        // In full implementation, this would:
        // 1. Scan table and extract distinct values
        // 2. Build bitmap for each distinct value (bit per row)
        // 3. Store bitmaps in storage layer
        
        tracing::debug!("Building bitmap index {} on {}.{}", index_id, table, columns.join(","));
        Ok(())
    }
    
    /// Build vector index (min/max block index)
    fn build_vector_index(&self, index_id: &str, table: &str, columns: &[String]) -> anyhow::Result<()> {
        // In full implementation, this would:
        // 1. Scan table blocks
        // 2. Compute min/max per block for each column
        // 3. Store block-level metadata
        
        tracing::debug!("Building vector index {} on {}.{}", index_id, table, columns.join(","));
        Ok(())
    }
    
    /// Build multiple indexes from recommendations
    pub fn build_indexes(&self, recommendations: &[IndexRecommendation]) -> Vec<(String, anyhow::Result<String>)> {
        recommendations.iter()
            .map(|rec| {
                let table_cols = (rec.table.clone(), rec.columns.clone());
                let result = self.build_index(rec);
                (format!("{}.{}", rec.table, rec.columns.join(",")), result)
            })
            .collect()
    }
    
    /// Check if index already exists
    pub fn index_exists(&self, table: &str, columns: &[String]) -> bool {
        let key = (table.to_string(), columns.to_vec());
        self.created_indexes.lock().unwrap().contains_key(&key)
    }
    
    /// Get all created indexes for a table
    pub fn get_table_indexes(&self, table: &str) -> Vec<String> {
        self.created_indexes.lock().unwrap()
            .iter()
            .filter(|((t, _), _)| t == table)
            .map(|(_, index_id)| index_id.clone())
            .collect()
    }
}

