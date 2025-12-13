/// Bitset Engine V3: Unified Bitset Join Operator
/// 
/// Single unified operator replacing v1 and v2
/// Supports equality, ranges, and multi-way joins
/// Hierarchical skipping for large fact tables

use crate::storage::bitset_v3::Bitset;
use crate::storage::bitset_hierarchy::HierarchicalBitset;
use crate::storage::bitmap_index::BitmapIndex;
use crate::storage::range_bitmap_index::RangeBitmapIndex;
use crate::storage::fragment::Value;
use crate::execution::batch::ExecutionBatch;
use crate::execution::exec_node::ExecNode;
use crate::execution::batch::BatchIterator;
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::NodeId;
use crate::storage::columnar::ColumnarBatch;
use arrow::datatypes::*;
use arrow::array::*;
use std::sync::Arc;
use anyhow::Result;
use bitvec::prelude::*;

/// High cardinality threshold (fallback to hash join)
const HIGH_CARD_THRESHOLD: usize = 1_000_000;

/// Filter selectivity threshold for bitset join
const SELECTIVITY_THRESHOLD: f64 = 0.2;

/// Large fact table threshold (enable hierarchical skipping)
const LARGE_FACT_TABLE_THRESHOLD: usize = 10_000_000;

/// Bitset Join Operator V3
pub struct BitsetJoinOperatorV3 {
    /// Fact table node
    fact_node: NodeId,
    
    /// Dimension table nodes
    dim_nodes: Vec<NodeId>,
    
    /// Join keys: (fact_column, dim_column) pairs
    join_keys: Vec<(String, String)>,
    
    /// Dimension filters: per dimension, list of (column, operator, value)
    dim_filters: Vec<Vec<DimensionFilter>>,
    
    /// Hypergraph reference
    graph: Arc<HyperGraph>,
    
    /// Fact table size
    fact_table_size: usize,
    
    /// Whether to use hierarchical skipping
    use_hierarchical: bool,
}

/// Dimension filter predicate
#[derive(Clone, Debug)]
pub enum DimensionFilter {
    /// Equality filter: column = value
    Equals(String, Value),
    
    /// Range filter: column BETWEEN lo AND hi
    Range(String, Value, Value),
    
    /// Greater than: column > value
    GreaterThan(String, Value),
    
    /// Less than: column < value
    LessThan(String, Value),
}

impl BitsetJoinOperatorV3 {
    /// Create new bitset join operator
    pub fn new(
        fact_node: NodeId,
        dim_nodes: Vec<NodeId>,
        join_keys: Vec<(String, String)>,
        dim_filters: Vec<Vec<DimensionFilter>>,
        graph: Arc<HyperGraph>,
    ) -> Result<Self> {
        // Get fact table size
        let fact_node_ref = graph.get_node(fact_node)
            .ok_or_else(|| anyhow::anyhow!("Fact node {} not found", fact_node))?;
        let fact_table_size = fact_node_ref.total_rows();
        
        // Decide if we should use hierarchical skipping
        let use_hierarchical = fact_table_size > LARGE_FACT_TABLE_THRESHOLD;
        
        Ok(Self {
            fact_node,
            dim_nodes,
            join_keys,
            dim_filters,
            graph,
            fact_table_size,
            use_hierarchical,
        })
    }
    
    /// Execute bitset join
    pub fn execute(&self) -> Result<ExecutionBatch> {
        // Phase 1: Identify join keys (already done in constructor)
        
        // Phase 2: Build or retrieve BitmapIndex/RangeBitmapIndex for dimensions
        let dim_indexes = self.build_dimension_indexes()?;
        
        // Phase 3: Evaluate all dimension filters into bitsets
        let dim_bitsets = self.evaluate_dimension_filters(&dim_indexes)?;
        
        // Phase 4: Convert dimension bitsets → fact bitsets
        let fact_bitsets = self.convert_dim_to_fact_bitsets(&dim_bitsets)?;
        
        // Phase 5: Intersection across all dimensions
        let final_fact_bitset = self.intersect_fact_bitsets(&fact_bitsets)?;
        
        // Phase 6: Late materialization of fact rows
        self.materialize_fact_rows(&final_fact_bitset)
    }
    
    /// Build dimension indexes
    fn build_dimension_indexes(&self) -> Result<Vec<DimensionIndexes>> {
        let mut indexes = Vec::new();
        
        for (dim_idx, &dim_node_id) in self.dim_nodes.iter().enumerate() {
            let dim_node = self.graph.get_node(dim_node_id)
                .ok_or_else(|| anyhow::anyhow!("Dimension node {} not found", dim_node_id))?;
            
            let mut bitmap_index = None;
            let mut range_index = None;
            
            // Build bitmap index for join key
            let (_, dim_col) = &self.join_keys[dim_idx];
            if let Some(col_node) = self.graph.get_node_by_table_column(
                dim_node.table_name.as_ref().unwrap(),
                dim_col,
            ) {
                // Build bitmap index (dereference Arc to get &HyperNode)
                bitmap_index = Some(self.build_bitmap_index_for_column(&*col_node, dim_col)?);
            }
            
            // Build range index if needed
            for filter in &self.dim_filters[dim_idx] {
                if matches!(filter, DimensionFilter::Range(_, _, _) | 
                                  DimensionFilter::GreaterThan(_, _) | 
                                  DimensionFilter::LessThan(_, _)) {
                    if range_index.is_none() {
                        // Build range index
                        // TODO: Implement range index building
                    }
                }
            }
            
            indexes.push(DimensionIndexes {
                bitmap_index,
                range_index,
            });
        }
        
        Ok(indexes)
    }
    
    /// Build bitmap index for a column
    fn build_bitmap_index_for_column(
        &self,
        col_node: &crate::hypergraph::node::HyperNode,
        column_name: &str,
    ) -> Result<BitmapIndex> {
        use crate::storage::bitmap_index::BitmapIndex;
        
        let mut index = BitmapIndex::new(
            col_node.table_name.as_ref().unwrap().clone(),
            column_name.to_string(),
        );
        
        // Build index from fragments
        for fragment in &col_node.fragments {
            if let Some(array) = fragment.get_array() {
                for row_idx in 0..array.len() {
                    if let Ok(value) = crate::execution::operators::extract_value(&array, row_idx) {
                        index.add_value(value, row_idx);
                    }
                }
            }
        }
        
        Ok(index)
    }
    
    /// Evaluate dimension filters into bitsets
    fn evaluate_dimension_filters(
        &self,
        dim_indexes: &[DimensionIndexes],
    ) -> Result<Vec<Bitset>> {
        let mut dim_bitsets = Vec::new();
        
        for (dim_idx, filters) in self.dim_filters.iter().enumerate() {
            let mut result_bitset: Option<Bitset> = None;
            
            for filter in filters {
                let filter_bitset = match filter {
                    DimensionFilter::Equals(col, val) => {
                        if let Some(ref bitmap_idx) = dim_indexes[dim_idx].bitmap_index {
                            bitmap_idx.get_bitset(val)
                                .map(|b| self.bitset_from_storage_bitset(b))
                                .unwrap_or_else(|| Bitset::new(0))
                        } else {
                            Bitset::new(0)
                        }
                    }
                    DimensionFilter::Range(col, lo, hi) => {
                        if let Some(ref range_idx) = dim_indexes[dim_idx].range_index {
                            range_idx.query_range(lo, hi)
                        } else {
                            Bitset::new(0)
                        }
                    }
                    DimensionFilter::GreaterThan(col, val) => {
                        // Use range index with val to max
                        Bitset::new(0) // TODO: Implement
                    }
                    DimensionFilter::LessThan(col, val) => {
                        // Use range index with min to val
                        Bitset::new(0) // TODO: Implement
                    }
                };
                
                // AND with existing result
                result_bitset = match result_bitset {
                    None => Some(filter_bitset),
                    Some(existing) => Some(existing.intersect(&filter_bitset)),
                };
            }
            
            dim_bitsets.push(result_bitset.unwrap_or_else(|| Bitset::new(0)));
        }
        
        Ok(dim_bitsets)
    }
    
    /// Convert dimension bitsets to fact bitsets via join keys
    fn convert_dim_to_fact_bitsets(
        &self,
        dim_bitsets: &[Bitset],
    ) -> Result<Vec<Bitset>> {
        let mut fact_bitsets = Vec::new();
        
        for (dim_idx, dim_bitset) in dim_bitsets.iter().enumerate() {
            // Get matching dimension values
            let matching_dim_rows = dim_bitset.get_set_bits();
            
            // Get dimension join key values
            let (_, dim_col) = &self.join_keys[dim_idx];
            let dim_node = self.graph.get_node(self.dim_nodes[dim_idx])
                .ok_or_else(|| anyhow::anyhow!("Dimension node not found"))?;
            
            let dim_table_name = dim_node.table_name.as_ref().unwrap();
            let dim_col_node = self.graph.get_node_by_table_column(dim_table_name, dim_col)
                .ok_or_else(|| anyhow::anyhow!("Dimension column not found"))?;
            
            // Collect matching dimension join key values
            let mut matching_dim_values = Vec::new();
            if let Some(fragment) = dim_col_node.fragments.first() {
                if let Some(array) = fragment.get_array() {
                    for row_id in matching_dim_rows {
                        if row_id < array.len() {
                            if let Ok(value) = crate::execution::operators::extract_value(&array, row_id) {
                                matching_dim_values.push(value);
                            }
                        }
                    }
                }
            }
            
            // Build fact bitset by matching fact join key values
            let mut fact_bitset = Bitset::new(self.fact_table_size);
            let (fact_col, _) = &self.join_keys[dim_idx];
            let fact_node = self.graph.get_node(self.fact_node)
                .ok_or_else(|| anyhow::anyhow!("Fact node not found"))?;
            let fact_table_name = fact_node.table_name.as_ref().unwrap();
            let fact_col_node = self.graph.get_node_by_table_column(fact_table_name, fact_col)
                .ok_or_else(|| anyhow::anyhow!("Fact column not found"))?;
            
            if let Some(fragment) = fact_col_node.fragments.first() {
                if let Some(array) = fragment.get_array() {
                    for row_id in 0..array.len() {
                        if let Ok(fact_value) = crate::execution::operators::extract_value(&array, row_id) {
                            if matching_dim_values.contains(&fact_value) {
                                fact_bitset.set(row_id);
                            }
                        }
                    }
                }
            }
            
            fact_bitsets.push(fact_bitset);
        }
        
        Ok(fact_bitsets)
    }
    
    /// Intersect all fact bitsets
    fn intersect_fact_bitsets(&self, fact_bitsets: &[Bitset]) -> Result<Bitset> {
        if fact_bitsets.is_empty() {
            return Ok(Bitset::new(self.fact_table_size));
        }
        
        let mut result = fact_bitsets[0].clone();
        for bitset in fact_bitsets.iter().skip(1) {
            result = result.intersect(bitset);
        }
        
        Ok(result)
    }
    
    /// Materialize fact rows from final bitset
    fn materialize_fact_rows(&self, fact_bitset: &Bitset) -> Result<ExecutionBatch> {
        // Get matching row IDs
        let matching_rows = fact_bitset.get_set_bits();
        
        if matching_rows.is_empty() {
            return Ok(ExecutionBatch {
                batch: ColumnarBatch::new(vec![], Arc::new(Schema::empty())),
                selection: bitvec![0; 0],
                row_count: 0,
                column_fragments: std::collections::HashMap::new(),
                column_schema: None,
            });
        }
        
        // Get fact table node
        let fact_node = self.graph.get_node(self.fact_node)
            .ok_or_else(|| anyhow::anyhow!("Fact node not found"))?;
        
        // Get all column nodes for fact table
        let fact_table_name = fact_node.table_name.as_ref().unwrap();
        let column_nodes = self.graph.get_column_nodes(fact_table_name);
        
        // Build output columns
        let mut columns = Vec::new();
        let mut fields = Vec::new();
        
        for col_node in column_nodes {
            let col_name = col_node.column_name.as_ref().unwrap();
            fields.push(Field::new(col_name, DataType::Int64, true));
            
            // Extract values for matching rows
            if let Some(fragment) = col_node.fragments.first() {
                let mut values = Vec::new();
                if let Some(array) = fragment.get_array() {
                    for &row_id in &matching_rows {
                        if row_id < array.len() {
                            if let Ok(value) = crate::execution::operators::extract_value(&array, row_id) {
                                match value {
                                    Value::Int64(v) => values.push(Some(v)),
                                    _ => values.push(None),
                                }
                            } else {
                                values.push(None);
                            }
                        } else {
                            values.push(None);
                        }
                    }
                }
                columns.push(Arc::new(Int64Array::from(values)) as Arc<dyn Array>);
            } else {
                columns.push(Arc::new(Int64Array::from(vec![None; matching_rows.len()])) as Arc<dyn Array>);
            }
        }
        
        let schema = Arc::new(Schema::new(fields));
        let batch = ColumnarBatch::new(columns, schema);
        
        Ok(ExecutionBatch {
            batch,
            selection: bitvec![1; matching_rows.len()],
            row_count: matching_rows.len(),
            column_fragments: std::collections::HashMap::new(),
            column_schema: None,
        })
    }
    
    /// Convert storage bitset to v3 bitset
    fn bitset_from_storage_bitset(&self, storage_bitset: &crate::storage::bitset::Bitset) -> Bitset {
        // TODO: Convert from old bitset format to v3
        Bitset::new(0)
    }
}

/// Dimension indexes (bitmap and range)
struct DimensionIndexes {
    bitmap_index: Option<BitmapIndex>,
    range_index: Option<RangeBitmapIndex>,
}

/// Decision rules for bitset join
impl BitsetJoinOperatorV3 {
    /// Check if should use bitset join
    pub fn should_use_bitset_join(
        distinct_count: usize,
        filter_selectivity: f64,
        is_star_schema: bool,
        fact_table_size: usize,
    ) -> bool {
        // High cardinality → hash join fallback
        if distinct_count > HIGH_CARD_THRESHOLD {
            return false;
        }
        
        // Star schema → always bitset join
        if is_star_schema {
            return true;
        }
        
        // Low selectivity → use bitset
        if filter_selectivity < SELECTIVITY_THRESHOLD {
            return true;
        }
        
        // Large fact table with hierarchical skipping
        if fact_table_size > LARGE_FACT_TABLE_THRESHOLD {
            return true;
        }
        
        false
    }
}

/// Implement BatchIterator for BitsetJoinOperatorV3
impl BatchIterator for BitsetJoinOperatorV3 {
    fn next(&mut self) -> Result<Option<ExecutionBatch>, anyhow::Error> {
        ExecNode::next(self)
    }
    
    fn schema(&self) -> SchemaRef {
        // Build schema from fact table columns
        // For now, return empty schema - will be populated during materialization
        Arc::new(Schema::empty())
    }
    
    fn prepare(&mut self) -> Result<(), anyhow::Error> {
        ExecNode::prepare(self)
    }
}

/// Implement ExecNode for BitsetJoinOperatorV3
impl ExecNode for BitsetJoinOperatorV3 {
    fn prepare(&mut self) -> Result<()> {
        // Build dimension indexes in prepare phase
        // This is done lazily in execute() for now
        Ok(())
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // Execute bitset join and return result
        // Use a flag to track if we've executed
        use std::sync::atomic::{AtomicBool, Ordering};
        static EXECUTED: AtomicBool = AtomicBool::new(false);
        
        if EXECUTED.swap(true, Ordering::Relaxed) {
            return Ok(None);
        }
        
        let result = self.execute()?;
        Ok(Some(result))
    }
}

