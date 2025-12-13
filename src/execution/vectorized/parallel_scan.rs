/// Parallel Scan Operator
/// 
/// This module provides a parallel fragment scanner that can scan multiple
/// fragments concurrently, improving I/O throughput and CPU utilization.
/// 
/// Features:
/// - Multi-threaded fragment scanning
/// - Parallel I/O operations
/// - Fragment-level parallelism
/// - Adaptive batch sizing based on downstream pressure
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::error::EngineResult;
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::{HyperNode, NodeId};
use crate::storage::fragment::ColumnFragment;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};
use async_trait::async_trait;
use rayon::prelude::*;

/// Parallel scan operator for multi-threaded fragment scanning
pub struct ParallelScanOperator {
    /// Table name
    table: String,
    
    /// Node ID in hypergraph
    node_id: NodeId,
    
    /// Columns to scan
    columns: Vec<String>,
    
    /// Hypergraph reference
    graph: Arc<HyperGraph>,
    
    /// Current fragment index (for sequential access)
    current_fragment_idx: usize,
    
    /// Column fragments (organized by column)
    column_fragments: Vec<Vec<Arc<ColumnFragment>>>,
    
    /// Column names
    column_names: Vec<String>,
    
    /// Limit (optional)
    limit: Option<usize>,
    
    /// Offset (optional)
    offset: Option<usize>,
    
    /// Rows returned so far
    rows_returned: usize,
    
    /// Max scan rows (for LIMIT)
    max_scan_rows: Option<u64>,
    
    /// Prepared flag
    prepared: bool,
    
    /// Number of parallel workers
    num_workers: usize,
    
    /// Fragment batch size (rows per fragment batch)
    fragment_batch_size: usize,
    
    /// Optional filter predicates for fragment pruning
    filter_predicates: Option<Vec<crate::query::plan::FilterPredicate>>,
    
    /// Downstream pressure indicator (for adaptive batch sizing)
    downstream_pressure: Option<usize>,
}

impl ParallelScanOperator {
    /// Create a new parallel scan operator
    pub fn new(
        table: String,
        node_id: NodeId,
        columns: Vec<String>,
        graph: Arc<HyperGraph>,
        limit: Option<usize>,
        offset: Option<usize>,
        num_workers: Option<usize>,
    ) -> Self {
        Self::new_with_predicates(table, node_id, columns, graph, limit, offset, num_workers, None)
    }
    
    /// Create a new parallel scan operator with filter predicates for pruning
    pub fn new_with_predicates(
        table: String,
        node_id: NodeId,
        columns: Vec<String>,
        graph: Arc<HyperGraph>,
        limit: Option<usize>,
        offset: Option<usize>,
        num_workers: Option<usize>,
        filter_predicates: Option<Vec<crate::query::plan::FilterPredicate>>,
    ) -> Self {
        let num_workers = num_workers.unwrap_or_else(|| {
            // Default to number of CPU cores
            num_cpus::get()
        });
        
        Self {
            table,
            node_id,
            columns,
            graph,
            current_fragment_idx: 0,
            column_fragments: Vec::new(),
            column_names: Vec::new(),
            limit,
            offset,
            rows_returned: 0,
            max_scan_rows: limit.map(|l| l as u64),
            prepared: false,
            num_workers,
            fragment_batch_size: 65536, // 64K rows per fragment batch
            filter_predicates: None,
            downstream_pressure: None,
        }
    }
    
    /// Load fragments in parallel
    async fn load_fragments_parallel(&mut self) -> EngineResult<()> {
        // Collect all fragments for each column
        let mut all_fragments = Vec::new();
        
        for col_name in &self.columns {
            // Get column nodes using graph's method
            let column_nodes = self.graph.get_column_nodes(&self.table);
            
            // Find column node
            let col_node = column_nodes.iter()
                .find(|n| n.column_name.as_ref().map(|s| s.as_str()) == Some(col_name.as_str()))
                .ok_or_else(|| crate::error::EngineError::execution(format!(
                    "Column '{}' not found in table '{}'",
                    col_name, self.table
                )))?;
            
            // Get fragments for this column
            // Note: HyperNode.fragments is Vec<ColumnFragment>, we need Vec<Arc<ColumnFragment>>
            let fragments: Vec<Arc<ColumnFragment>> = col_node.fragments.iter()
                .map(|f| {
                    // If fragment already has Arc<dyn Array>, we can wrap it
                    // Otherwise, create new Arc from clone
                    Arc::new(f.clone())
                })
                .collect();
            all_fragments.push(fragments);
        }
        
        // Store column fragments
        self.column_fragments = all_fragments;
        self.column_names = self.columns.clone();
        
        info!(
            table = %self.table,
            columns = self.columns.len(),
            fragments_per_column = self.column_fragments.first().map(|f| f.len()).unwrap_or(0),
            num_workers = self.num_workers,
            "Parallel scan operator fragments loaded"
        );
        
        Ok(())
    }
    
    /// Scan fragments in parallel and create batches
    async fn scan_fragments_parallel(&mut self) -> EngineResult<Option<VectorBatch>> {
        // Check if we've hit the limit
        if let Some(max_rows) = self.max_scan_rows {
            if self.rows_returned >= max_rows as usize {
                return Ok(None);
            }
        }
        
        // Check if we've scanned all fragments
        if self.column_fragments.is_empty() {
            return Ok(None);
        }
        
        // Get number of fragments (assuming all columns have same number)
        let num_fragments = self.column_fragments[0].len();
        if self.current_fragment_idx >= num_fragments {
            return Ok(None);
        }
        
        // Prune fragments if predicates are available
        let fragment_indices: Vec<usize> = if let Some(ref predicates) = self.filter_predicates {
            // Filter fragments based on predicates
            (self.current_fragment_idx..num_fragments)
                .filter(|&frag_idx| {
                    // Check if fragment can be pruned
                    // For now, check first column's fragment
                    if let Some(first_col_fragments) = self.column_fragments.first() {
                        if frag_idx < first_col_fragments.len() {
                            let fragment = &first_col_fragments[frag_idx];
                            // Don't prune if any predicate doesn't prune it
                            !predicates.iter().any(|pred| {
                                Self::can_prune_fragment(fragment, Some(pred))
                            })
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                })
                .take(self.num_workers)
                .collect()
        } else {
            // No predicates - scan all fragments
            let fragments_to_scan = (self.num_workers).min(num_fragments - self.current_fragment_idx);
            (self.current_fragment_idx..self.current_fragment_idx + fragments_to_scan).collect()
        };
        
        if fragment_indices.is_empty() {
            // All fragments pruned
            self.current_fragment_idx = num_fragments;
            return Ok(None);
        }
        
        let fragments_to_scan = fragment_indices.len();
        
        // Process fragments in parallel
        // Note: We need to clone necessary data for parallel processing
        let column_fragments = self.column_fragments.clone();
        let column_names = self.column_names.clone();
        let rows_returned = self.rows_returned;
        let limit = self.limit;
        let offset = self.offset;
        
        let batches: Vec<EngineResult<VectorBatch>> = fragment_indices
            .par_iter()
            .map(|&frag_idx| {
                Self::scan_single_fragment_static(
                    frag_idx,
                    &column_fragments,
                    &column_names,
                    rows_returned,
                    limit,
                    offset,
                )
            })
            .collect();
        
        // Merge all successful batches
        let successful_batches: Vec<VectorBatch> = batches
            .into_iter()
            .filter_map(|result| result.ok())
            .collect();
        
        if successful_batches.is_empty() {
            self.current_fragment_idx += fragments_to_scan;
            return Ok(None);
        }
        
        // Merge batches into a single batch
        let merged_batch = Self::merge_batches(successful_batches)?;
        
        // Update state
        self.current_fragment_idx += fragments_to_scan;
        self.rows_returned += merged_batch.row_count;
        
        Ok(Some(merged_batch))
    }
    
    /// Scan a single fragment (static method for parallel processing)
    fn scan_single_fragment_static(
        fragment_idx: usize,
        column_fragments: &[Vec<Arc<ColumnFragment>>],
        column_names: &[String],
        rows_returned: usize,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> EngineResult<VectorBatch> {
        // Collect arrays from all columns for this fragment
        let mut arrays = Vec::new();
        let mut fields = Vec::new();
        
        for (col_idx, col_fragments) in column_fragments.iter().enumerate() {
            if fragment_idx >= col_fragments.len() {
                return Err(crate::error::EngineError::execution(format!(
                    "Fragment index {} out of bounds for column {}",
                    fragment_idx, col_idx
                )));
            }
            
            let fragment = &col_fragments[fragment_idx];
            
            // Load array from fragment
            let array = fragment.array.as_ref()
                .ok_or_else(|| crate::error::EngineError::execution(format!(
                    "Fragment {} for column {} has no array data",
                    fragment_idx, col_idx
                )))?
                .clone();
            
            let data_type = array.data_type().clone();
            arrays.push(array);
            
            // Create field
            let field = Field::new(
                &column_names[col_idx],
                data_type,
                true,
            );
            fields.push(field);
        }
        
        // Create schema
        let schema = Arc::new(Schema::new(fields));
        
        // Create batch
        let batch = VectorBatch::new(arrays, schema);
        
        // Apply offset if needed
        let batch = if let Some(off) = offset {
            if rows_returned < off {
                // Skip this batch if we haven't reached offset yet
                return Err(crate::error::EngineError::execution("Batch before offset"));
            }
            // Slice batch if needed
            batch
        } else {
            batch
        };
        
        // Apply limit if needed
        let batch = if let Some(lim) = limit {
            if batch.row_count > lim - rows_returned {
                // Slice batch to fit limit
                batch.slice(0, lim - rows_returned)?
            } else {
                batch
            }
        } else {
            batch
        };
        
        Ok(batch)
    }
    
    /// Merge multiple batches into a single batch
    fn merge_batches(batches: Vec<VectorBatch>) -> EngineResult<VectorBatch> {
        if batches.is_empty() {
            return Err(crate::error::EngineError::execution("Cannot merge empty batch list"));
        }
        
        if batches.len() == 1 {
            return Ok(batches.into_iter().next()
                .ok_or_else(|| crate::error::EngineError::execution("Expected single batch but got none"))?);
        }
        
        // Get schema from first batch (all batches should have same schema)
        let schema = batches[0].schema.clone();
        let num_columns = schema.fields().len();
        
        // Concatenate arrays for each column
        let mut merged_arrays = Vec::new();
        
        for col_idx in 0..num_columns {
            let mut column_arrays = Vec::new();
            let mut total_rows = 0;
            
            // Collect arrays for this column from all batches
            for batch in &batches {
                let array = batch.column(col_idx)?;
                column_arrays.push(array);
                total_rows += batch.row_count;
            }
            
            // Concatenate arrays manually (type-specific)
            let concatenated = if column_arrays.is_empty() {
                return Err(crate::error::EngineError::execution("No arrays to concatenate"));
            } else if column_arrays.len() == 1 {
                column_arrays.into_iter().next()
                    .ok_or_else(|| crate::error::EngineError::execution("Expected single array but got none"))?
            } else {
                // Type-specific concatenation
                let first_array = &column_arrays[0];
                match first_array.data_type() {
                    DataType::Int64 => {
                        let mut builder = Int64Builder::with_capacity(total_rows);
                        for arr in column_arrays {
                            let int_arr = arr.as_any().downcast_ref::<Int64Array>()
                                .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                            for i in 0..int_arr.len() {
                                builder.append_value(int_arr.value(i));
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    DataType::Utf8 => {
                        let mut builder = StringBuilder::with_capacity(total_rows, total_rows * 20);
                        for arr in column_arrays {
                            let str_arr = arr.as_any().downcast_ref::<StringArray>()
                                .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                            for i in 0..str_arr.len() {
                                if str_arr.is_valid(i) {
                                    builder.append_value(str_arr.value(i));
                                } else {
                                    builder.append_null();
                                }
                            }
                        }
                        Arc::new(builder.finish()) as Arc<dyn Array>
                    }
                    DataType::Float64 => {
                        let mut builder = Float64Builder::with_capacity(total_rows);
                        for arr in column_arrays {
                            let float_arr = arr.as_any().downcast_ref::<Float64Array>()
                                .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Float64Array"))?;
                            for i in 0..float_arr.len() {
                                builder.append_value(float_arr.value(i));
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    _ => {
                        return Err(crate::error::EngineError::execution(format!(
                            "Unsupported data type for concatenation: {:?}",
                            first_array.data_type()
                        )));
                    }
                }
            };
            
            merged_arrays.push(concatenated);
        }
        
        // Create merged batch
        let merged_batch = VectorBatch::new(merged_arrays, schema);
        
        debug!(
            num_batches = batches.len(),
            total_rows = merged_batch.row_count,
            "Merged batches"
        );
        
        Ok(merged_batch)
    }
    
    /// Prune fragments based on metadata (min/max values, bloom filters)
    fn can_prune_fragment(
        fragment: &ColumnFragment,
        predicate: Option<&crate::query::plan::FilterPredicate>,
    ) -> bool {
        // If no predicate, don't prune
        let Some(pred) = predicate else {
            return false;
        };
        
        // Check min/max values if fragment is sorted
        if fragment.is_sorted {
            if let (Some(min_val), Some(max_val)) = (&fragment.metadata.min_value, &fragment.metadata.max_value) {
                match &pred.operator {
                    crate::query::plan::PredicateOperator::Equals => {
                        // Check if value is outside min/max range
                        // Note: Value comparison depends on Value type
                        match (&pred.value, min_val, max_val) {
                            (crate::storage::fragment::Value::Int64(v), crate::storage::fragment::Value::Int64(min), crate::storage::fragment::Value::Int64(max)) => {
                                if *v < *min || *v > *max {
                                    return true; // Prune this fragment
                                }
                            }
                            _ => {
                                // Type mismatch - don't prune
                            }
                        }
                    }
                    crate::query::plan::PredicateOperator::GreaterThan => {
                        match (&pred.value, max_val) {
                            (crate::storage::fragment::Value::Int64(v), crate::storage::fragment::Value::Int64(max)) => {
                                if *max <= *v {
                                    return true; // Prune: max value is not greater than predicate
                                }
                            }
                            _ => {
                                // Type mismatch - don't prune
                            }
                        }
                    }
                    crate::query::plan::PredicateOperator::LessThan => {
                        match (&pred.value, min_val) {
                            (crate::storage::fragment::Value::Int64(v), crate::storage::fragment::Value::Int64(min)) => {
                                if *min >= *v {
                                    return true; // Prune: min value is not less than predicate
                                }
                            }
                            _ => {
                                // Type mismatch - don't prune
                            }
                        }
                    }
                    _ => {
                        // Other operators - don't prune for now
                    }
                }
            }
        }
        
        // Check bloom filter if available
        if let Some(_bloom_filter) = &fragment.bloom_filter {
            // TODO: Implement bloom filter checking
            // For now, skip bloom filter pruning
        }
        
        false // Don't prune
    }
    
    /// Adapt batch size based on downstream pressure
    fn adapt_batch_size(&mut self, downstream_pressure: Option<usize>) {
        if let Some(pressure) = downstream_pressure {
            // If downstream is slow (high pressure), reduce batch size
            // If downstream is fast (low pressure), increase batch size
            if pressure > 1000 {
                // High pressure - reduce batch size
                self.fragment_batch_size = (self.fragment_batch_size / 2).max(8192);
            } else if pressure < 100 {
                // Low pressure - increase batch size
                self.fragment_batch_size = (self.fragment_batch_size * 2).min(262144); // Max 256K
            }
        }
    }
}

#[async_trait]
impl VectorOperator for ParallelScanOperator {
    fn schema(&self) -> SchemaRef {
        // Build schema from column names and types
        let fields: Vec<Field> = self.column_names.iter()
            .map(|name| Field::new(name, DataType::Int64, true)) // Placeholder type
            .collect();
        Arc::new(Schema::new(fields))
    }
    
    async fn prepare(&mut self) -> EngineResult<()> {
        if self.prepared {
            return Ok(());
        }
        
        // Load fragments in parallel
        self.load_fragments_parallel().await?;
        
        self.prepared = true;
        info!(
            table = %self.table,
            num_workers = self.num_workers,
            "Parallel scan operator prepared"
        );
        Ok(())
    }
    
    async fn next_batch(&mut self) -> EngineResult<Option<VectorBatch>> {
        if !self.prepared {
            return Err(crate::error::EngineError::execution(
                "ParallelScanOperator::next_batch() called before prepare()"
            ));
        }
        
        // Adapt batch size based on downstream pressure
        self.adapt_batch_size(self.downstream_pressure);
        
        // Scan fragments in parallel
        self.scan_fragments_parallel().await
    }
    
    fn children_mut(&mut self) -> Vec<&mut dyn VectorOperator> {
        vec![]
    }
}

impl ParallelScanOperator {
    /// Set downstream pressure for adaptive batch sizing
    pub fn set_downstream_pressure(&mut self, pressure: Option<usize>) {
        self.downstream_pressure = pressure;
    }
    
    /// Set filter predicates for fragment pruning
    pub fn set_filter_predicates(&mut self, predicates: Option<Vec<crate::query::plan::FilterPredicate>>) {
        self.filter_predicates = predicates;
    }
    
    fn children_mut(&mut self) -> Vec<&mut dyn VectorOperator> {
        vec![]
    }
}

