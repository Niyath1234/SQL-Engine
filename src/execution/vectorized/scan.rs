/// VectorScanOperator: Async, vectorized scan operator
/// 
/// This is the Phase 1 replacement for ScanOperator, designed for:
/// - Async I/O (non-blocking fragment reads)
/// - Parallel fragment scanning
/// - Zero-copy batch creation
/// - Fragment-level pruning
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::error::EngineResult;
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::NodeId;
use crate::storage::fragment::ColumnFragment;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use bitvec::prelude::*;
use tracing::{debug, info, warn};

/// Vector scan operator - async, vectorized table scanning
pub struct VectorScanOperator {
    /// Table name
    table: String,
    
    /// Node ID in hypergraph
    node_id: NodeId,
    
    /// Columns to scan
    columns: Vec<String>,
    
    /// Hypergraph reference
    graph: Arc<HyperGraph>,
    
    /// Current fragment index (across all columns)
    current_fragment_idx: usize,
    
    /// Column fragments (per column)
    column_fragments: Vec<Vec<ColumnFragment>>,
    
    /// Column names
    column_names: Vec<String>,
    
    /// LIMIT (if pushed down)
    limit: Option<usize>,
    
    /// OFFSET (if pushed down)
    offset: Option<usize>,
    
    /// Rows returned so far (for LIMIT/OFFSET)
    rows_returned: usize,
    
    /// Max rows to scan (LLM protocol)
    max_scan_rows: Option<u64>,
    
    /// Rows scanned so far
    rows_scanned: u64,
    
    /// Prepared flag
    prepared: bool,
}

impl VectorScanOperator {
    /// Create a new vector scan operator
    pub fn new(
        table: String,
        node_id: NodeId,
        columns: Vec<String>,
        graph: Arc<HyperGraph>,
        limit: Option<usize>,
        offset: Option<usize>,
        max_scan_rows: Option<u64>,
    ) -> Self {
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
            max_scan_rows,
            rows_scanned: 0,
            prepared: false,
        }
    }
    
    /// Load fragments for all columns (async)
    async fn load_fragments(&mut self) -> EngineResult<()> {
        if !self.column_fragments.is_empty() {
            return Ok(()); // Already loaded
        }
        
        info!(
            table = %self.table,
            columns = ?self.columns,
            "Loading fragments for vector scan"
        );
        
        // Get node from hypergraph
        let node = self.graph.get_node(self.node_id)
            .ok_or_else(|| crate::error::EngineError::execution(format!(
                "Node {} not found in hypergraph",
                self.node_id
            )))?;
        
        // Load fragments for each column using graph's column lookup
        for column_name in &self.columns {
            let col_node = self.graph.get_node_by_table_column(&self.table, column_name)
                .ok_or_else(|| crate::error::EngineError::execution(format!(
                    "Column '{}' not found in table '{}'",
                    column_name, self.table
                )))?;
            
            self.column_fragments.push(col_node.fragments.clone());
            self.column_names.push(column_name.clone());
        }
        
        debug!(
            fragment_count = self.column_fragments.first().map(|f| f.len()).unwrap_or(0),
            "Fragments loaded"
        );
        
        Ok(())
    }
    
    /// Create a batch from fragments (async, with potential I/O)
    fn create_batch_from_fragments(&mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = EngineResult<Option<VectorBatch>>> + Send + '_>> {
        Box::pin(async move {
        // Check LIMIT
        if let Some(limit) = self.limit {
            if self.rows_returned >= limit {
                return Ok(None);
            }
        }
        
        // Check max_scan_rows (LLM protocol)
        if let Some(max_rows) = self.max_scan_rows {
            if self.rows_scanned >= max_rows {
                return Ok(None);
            }
        }
        
        // Find the fragment index that has data
        let max_fragments = self.column_fragments.iter()
            .map(|frags| frags.len())
            .max()
            .unwrap_or(0);
        
        if self.current_fragment_idx >= max_fragments {
            return Ok(None); // All fragments exhausted
        }
        
        // Collect arrays from current fragment across all columns
        let mut arrays = Vec::new();
        let mut batch_row_count = None;
        
        for (col_idx, fragments) in self.column_fragments.iter().enumerate() {
            if self.current_fragment_idx >= fragments.len() {
                // This column has fewer fragments - skip it or use empty array
                warn!(
                    column = %self.column_names[col_idx],
                    fragment_idx = self.current_fragment_idx,
                    "Column has fewer fragments than others"
                );
                continue;
            }
            
            let fragment = fragments[self.current_fragment_idx].clone();
            
            // Get array from fragment (may involve async I/O if lazy-loaded)
            let array = if let Some(ref arr) = fragment.array {
                arr.clone()
            } else {
                // Fragment is lazy-loaded - trigger load
                // For now, we'll use blocking I/O wrapped in spawn_blocking
                // TODO: Make fragment loading truly async
                tokio::task::spawn_blocking(move || {
                    fragment.get_array()
                }).await
                    .map_err(|e| crate::error::EngineError::execution(format!("Task join error: {}", e)))?
                    .ok_or_else(|| crate::error::EngineError::execution(format!(
                        "Fragment array not available"
                    )))?
            };
            
            if batch_row_count.is_none() {
                batch_row_count = Some(array.len());
            } else if batch_row_count != Some(array.len()) {
                warn!(
                    column = %self.column_names[col_idx],
                    expected_rows = batch_row_count.unwrap(),
                    actual_rows = array.len(),
                    "Column array length mismatch"
                );
            }
            
            arrays.push(array);
        }
        
        let row_count = batch_row_count.unwrap_or(0);
        
        if row_count == 0 {
            // Empty fragment - move to next
            self.current_fragment_idx += 1;
            // Recursive call - but we're already in a boxed future, so this is fine
            return self.create_batch_from_fragments().await;
        }
        
        // Apply OFFSET if needed
        let start_offset = if self.rows_returned == 0 {
            self.offset.unwrap_or(0)
        } else {
            0
        };
        
        // Apply LIMIT
        let remaining_limit = self.limit.map(|l| l - self.rows_returned);
        let take_rows = remaining_limit
            .map(|l| l.min(row_count - start_offset))
            .unwrap_or(row_count - start_offset);
        
        if take_rows == 0 {
            return Ok(None);
        }
        
        // Create schema
        let fields: Vec<Field> = self.column_names.iter()
            .zip(arrays.iter())
            .map(|(name, arr)| Field::new(name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        
        // Slice arrays if needed (zero-copy)
        let sliced_arrays: Vec<Arc<dyn Array>> = if start_offset > 0 || take_rows < row_count {
            arrays.iter()
                .map(|arr| arr.slice(start_offset, take_rows))
                .collect()
        } else {
            arrays
        };
        
        // Update counters
        self.rows_returned += take_rows;
        self.rows_scanned += take_rows as u64;
        self.current_fragment_idx += 1;
        
        // Create VectorBatch
        let batch = VectorBatch::new(sliced_arrays, schema);
        
        debug!(
            rows = take_rows,
            total_returned = self.rows_returned,
            "Created vector batch from fragments"
        );
        
        Ok(Some(batch))
        })
    }
}

#[async_trait::async_trait]
impl VectorOperator for VectorScanOperator {
    fn schema(&self) -> SchemaRef {
        // Build schema from column names and fragments
        // For now, use a placeholder - will be set during prepare()
        let fields: Vec<Field> = self.column_names.iter()
            .map(|name| Field::new(name, DataType::Utf8, true)) // Placeholder type
            .collect();
        Arc::new(Schema::new(fields))
    }
    
    async fn prepare(&mut self) -> EngineResult<()> {
        if self.prepared {
            return Ok(());
        }
        
        info!(
            table = %self.table,
            "Preparing vector scan operator"
        );
        
        // Load fragments
        self.load_fragments().await?;
        
        // Build actual schema from first fragment
        if let Some(fragments) = self.column_fragments.first() {
            if let Some(first_fragment) = fragments.first() {
                if let Some(ref array) = first_fragment.array {
                    // Use actual data type from first fragment
                    // TODO: Handle multiple fragments with different types
                }
            }
        }
        
        self.prepared = true;
        
        debug!("Vector scan operator prepared");
        Ok(())
    }
    
    async fn next_batch(&mut self) -> EngineResult<Option<VectorBatch>> {
        if !self.prepared {
            return Err(crate::error::EngineError::execution(
                "VectorScanOperator::next_batch() called before prepare()"
            ));
        }
        
        // Call the boxed future
        self.create_batch_from_fragments().await
    }
}

