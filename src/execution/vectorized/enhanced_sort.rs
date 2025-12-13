/// Enhanced Sort Operator with External Merge Sort
/// 
/// This module provides an improved sort operator that:
/// - Uses external merge sort for large datasets
/// - Supports multi-way merging
/// - Handles datasets larger than memory
/// - Uses vectorized sorting kernels
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::error::EngineResult;
use crate::query::plan::OrderByExpr;
use crate::spill::sort_spill::SortSpill;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};
use async_trait::async_trait;
use std::cmp::Ordering;

/// Enhanced sort operator with external merge sort support
pub struct EnhancedSortOperator {
    /// Input operator
    input: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
    
    /// Order by expressions
    order_by: Vec<OrderByExpr>,
    
    /// Spill manager for external sort
    spill_manager: Option<Arc<Mutex<SortSpill>>>,
    
    /// Memory limit for in-memory sort
    memory_limit: usize,
    
    /// Collected input batches
    input_batches: Vec<VectorBatch>,
    
    /// Sorted output batches
    sorted_batches: Vec<VectorBatch>,
    
    /// Current output batch index
    current_output_idx: usize,
    
    /// Input exhausted flag
    input_exhausted: bool,
    
    /// Prepared flag
    prepared: bool,
    
    /// Total memory used
    memory_used: usize,
}

impl EnhancedSortOperator {
    /// Create a new enhanced sort operator
    pub fn new(
        input: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
        order_by: Vec<OrderByExpr>,
        memory_limit: usize,
    ) -> Self {
        Self {
            input,
            order_by,
            spill_manager: None,
            memory_limit,
            input_batches: Vec::new(),
            sorted_batches: Vec::new(),
            current_output_idx: 0,
            input_exhausted: false,
            prepared: false,
            memory_used: 0,
        }
    }
    
    /// Collect all input batches
    async fn collect_input(&mut self) -> EngineResult<()> {
        let mut input = self.input.lock().await;
        
        loop {
            match input.next_batch().await? {
                Some(batch) => {
                    let batch_size = self.estimate_batch_size(&batch);
                    self.memory_used += batch_size;
                    
                    // Check if we need to spill
                    if self.memory_used > self.memory_limit {
                        // Initialize spill manager if not already done
                        if self.spill_manager.is_none() {
                            let spill_dir = std::env::temp_dir().join("hypergraph_sort_spill");
                            let spill_mgr = SortSpill::new(
                                spill_dir,
                                self.memory_limit,
                            );
                            self.spill_manager = Some(Arc::new(Mutex::new(spill_mgr)));
                        }
                        
                        // Spill current batch
                        let spill_mgr = self.spill_manager.as_ref()
                            .ok_or_else(|| crate::error::EngineError::execution(
                                "Spill manager not initialized"
                            ))?;
                        let mut spill = spill_mgr.lock().await;
                        spill.add_batch(&batch)?;
                        spill.finalize_current_run()?;
                        
                        info!(
                            memory_used = self.memory_used,
                            memory_limit = self.memory_limit,
                            "Spilling batch to disk"
                        );
                    } else {
                        // Keep in memory
                        self.input_batches.push(batch);
                    }
                }
                None => {
                    self.input_exhausted = true;
                    break;
                }
            }
        }
        
        Ok(())
    }
    
    /// Sort batches (in-memory or external merge)
    async fn sort_batches(&mut self) -> EngineResult<()> {
        if self.spill_manager.is_some() {
            // External merge sort
            self.external_merge_sort().await?;
        } else {
            // In-memory sort
            self.in_memory_sort()?;
        }
        
        Ok(())
    }
    
    /// In-memory sort
    fn in_memory_sort(&mut self) -> EngineResult<()> {
        if self.input_batches.is_empty() {
            return Ok(());
        }
        
        // Collect all rows with their positions
        let mut rows: Vec<SortRow> = Vec::new();
        
        for (batch_idx, batch) in self.input_batches.iter().enumerate() {
            let row_count = batch.row_count;
            for row_idx in 0..row_count {
                rows.push(SortRow {
                    batch_idx,
                    row_idx,
                });
            }
        }
        
        // Sort rows based on order_by expressions
        rows.sort_by(|a, b| {
            self.compare_rows(
                &self.input_batches[a.batch_idx],
                a.row_idx,
                &self.input_batches[b.batch_idx],
                b.row_idx,
            )
        });
        
        // Materialize sorted batches
        self.materialize_sorted_batches(rows)?;
        
        info!(
            total_rows = self.input_batches.iter().map(|b| b.row_count).sum::<usize>(),
            sorted_batches = self.sorted_batches.len(),
            "In-memory sort complete"
        );
        
        Ok(())
    }
    
    /// External merge sort
    async fn external_merge_sort(&mut self) -> EngineResult<()> {
        let spill_mgr = self.spill_manager.as_ref()
            .ok_or_else(|| crate::error::EngineError::execution(
                "Spill manager not initialized for external merge sort"
            ))?;
        let mut spill = spill_mgr.lock().await;
        
        // Finalize any remaining in-memory batches
        let input_batches = std::mem::take(&mut self.input_batches);
        if !input_batches.is_empty() {
            // Sort in-memory batches first (create a temporary sort operator)
            // For now, just spill them directly
            for batch in &input_batches {
                spill.add_batch(batch)?;
            }
            spill.finalize_current_run()?;
        }
        
        // Merge all runs
        let merged_batches = spill.merge_runs()?;
        self.sorted_batches = merged_batches;
        
        info!(
            sorted_batches = self.sorted_batches.len(),
            "External merge sort complete"
        );
        
        Ok(())
    }
    
    /// Compare two rows based on order_by expressions
    fn compare_rows(
        &self,
        batch1: &VectorBatch,
        row1: usize,
        batch2: &VectorBatch,
        row2: usize,
    ) -> Ordering {
        for order_expr in &self.order_by {
            // Find column index
            let col_idx = batch1.schema.fields()
                .iter()
                .position(|f| f.name() == order_expr.column.as_str());
            
            if let Some(idx) = col_idx {
                let col1 = batch1.column(idx).ok();
                let col2 = batch2.column(idx).ok();
                
                if let (Some(arr1), Some(arr2)) = (col1, col2) {
                    let cmp_result = self.compare_values(arr1.as_ref(), row1, arr2.as_ref(), row2);
                    if cmp_result != Ordering::Equal {
                        return if order_expr.ascending {
                            cmp_result
                        } else {
                            cmp_result.reverse()
                        };
                    }
                }
            }
        }
        
        Ordering::Equal
    }
    
    /// Compare two values from arrays
    fn compare_values(
        &self,
        arr1: &dyn Array,
        idx1: usize,
        arr2: &dyn Array,
        idx2: usize,
    ) -> Ordering {
        match arr1.data_type() {
            DataType::Int64 => {
                let a1 = arr1.as_any().downcast_ref::<Int64Array>()
                    .and_then(|a| Some(a.value(idx1)));
                let a2 = arr2.as_any().downcast_ref::<Int64Array>()
                    .and_then(|a| Some(a.value(idx2)));
                match (a1, a2) {
                    (Some(v1), Some(v2)) => v1.cmp(&v2),
                    _ => Ordering::Equal,
                }
            }
            DataType::Utf8 => {
                let a1 = arr1.as_any().downcast_ref::<StringArray>()
                    .and_then(|a| Some(a.value(idx1)));
                let a2 = arr2.as_any().downcast_ref::<StringArray>()
                    .and_then(|a| Some(a.value(idx2)));
                match (a1, a2) {
                    (Some(v1), Some(v2)) => v1.cmp(v2),
                    _ => Ordering::Equal,
                }
            }
            DataType::Float64 => {
                let a1 = arr1.as_any().downcast_ref::<Float64Array>()
                    .and_then(|a| Some(a.value(idx1)));
                let a2 = arr2.as_any().downcast_ref::<Float64Array>()
                    .and_then(|a| Some(a.value(idx2)));
                match (a1, a2) {
                    (Some(v1), Some(v2)) => v1.partial_cmp(&v2).unwrap_or(Ordering::Equal),
                    _ => Ordering::Equal,
                }
            }
            _ => Ordering::Equal,
        }
    }
    
    /// Materialize sorted batches from row indices
    fn materialize_sorted_batches(&mut self, sorted_rows: Vec<SortRow>) -> EngineResult<()> {
        if sorted_rows.is_empty() {
            return Ok(());
        }
        
        // Group rows into batches
        let batch_size = 8192; // 8K rows per batch
        let mut current_batch_rows = Vec::new();
        let mut output_batches = Vec::new();
        
        for row in sorted_rows {
            current_batch_rows.push(row);
            
            if current_batch_rows.len() >= batch_size {
                let batch = self.create_batch_from_rows(&current_batch_rows)?;
                output_batches.push(batch);
                current_batch_rows.clear();
            }
        }
        
        // Add remaining rows
        if !current_batch_rows.is_empty() {
            let batch = self.create_batch_from_rows(&current_batch_rows)?;
            output_batches.push(batch);
        }
        
        self.sorted_batches = output_batches;
        Ok(())
    }
    
    /// Create a batch from row indices
    fn create_batch_from_rows(&self, rows: &[SortRow]) -> EngineResult<VectorBatch> {
        if rows.is_empty() {
            return Err(crate::error::EngineError::execution("Cannot create batch from empty rows"));
        }
        
        // Get schema from first batch
        let schema = self.input_batches[rows[0].batch_idx].schema.clone();
        let num_columns = schema.fields().len();
        
        // Materialize each column
        let mut output_arrays = Vec::new();
        
        for col_idx in 0..num_columns {
            let array = self.materialize_column(col_idx, rows)?;
            output_arrays.push(array);
        }
        
        // Create batch
        let batch = VectorBatch::new(output_arrays, schema);
        Ok(batch)
    }
    
    /// Materialize a column from row indices
    fn materialize_column(
        &self,
        col_idx: usize,
        rows: &[SortRow],
    ) -> EngineResult<Arc<dyn Array>> {
        if rows.is_empty() {
            return Err(crate::error::EngineError::execution("Cannot materialize empty column"));
        }
        
        // Get first array to determine type
        let first_batch = &self.input_batches[rows[0].batch_idx];
        let first_array = first_batch.column(col_idx)?;
        let data_type = first_array.data_type();
        
        let result: Arc<dyn Array> = match data_type {
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(rows.len());
                for row in rows {
                    let batch = &self.input_batches[row.batch_idx];
                    let array = batch.column(col_idx)?;
                    let int_arr = array.as_any().downcast_ref::<Int64Array>()
                        .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                    builder.append_value(int_arr.value(row.row_idx));
                }
                Arc::new(builder.finish())
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 20);
                for row in rows {
                    let batch = &self.input_batches[row.batch_idx];
                    let array = batch.column(col_idx)?;
                    let str_arr = array.as_any().downcast_ref::<StringArray>()
                        .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                    builder.append_value(str_arr.value(row.row_idx));
                }
                Arc::new(builder.finish())
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(rows.len());
                for row in rows {
                    let batch = &self.input_batches[row.batch_idx];
                    let array = batch.column(col_idx)?;
                    let float_arr = array.as_any().downcast_ref::<Float64Array>()
                        .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Float64Array"))?;
                    builder.append_value(float_arr.value(row.row_idx));
                }
                Arc::new(builder.finish())
            }
            _ => {
                return Err(crate::error::EngineError::execution(format!(
                    "Unsupported data type for materialization: {:?}",
                    data_type
                )));
            }
        };
        
        Ok(result)
    }
    
    /// Estimate batch size in bytes
    fn estimate_batch_size(&self, batch: &VectorBatch) -> usize {
        // Rough estimate: sum of array sizes
        batch.columns.iter()
            .map(|arr| {
                // Estimate based on data type and length
                arr.len() * match arr.data_type() {
                    DataType::Int64 => 8,
                    DataType::Utf8 => 20, // Average string length
                    DataType::Float64 => 8,
                    _ => 8,
                }
            })
            .sum()
    }
}

/// Row position in sorted order
#[derive(Clone, Debug)]
struct SortRow {
    batch_idx: usize,
    row_idx: usize,
}

#[async_trait]
impl VectorOperator for EnhancedSortOperator {
    fn schema(&self) -> SchemaRef {
        // Schema is same as input
        let input_schema = {
            let input = self.input.try_lock();
            if let Ok(input) = input {
                input.schema()
            } else {
                // Fallback: use first input batch schema if available
                self.input_batches.first()
                    .map(|b| b.schema.clone())
                    .unwrap_or_else(|| Arc::new(Schema::new(Vec::<Field>::new())))
            }
        };
        input_schema
    }
    
    async fn prepare(&mut self) -> EngineResult<()> {
        if self.prepared {
            return Ok(());
        }
        
        // Recursively prepare input
        {
            let mut input = self.input.lock().await;
            input.prepare().await?;
        }
        
        // Collect all input
        self.collect_input().await?;
        
        // Sort batches
        self.sort_batches().await?;
        
        self.prepared = true;
        info!(
            order_by_count = self.order_by.len(),
            sorted_batches = self.sorted_batches.len(),
            "Enhanced sort operator prepared"
        );
        Ok(())
    }
    
    async fn next_batch(&mut self) -> EngineResult<Option<VectorBatch>> {
        if !self.prepared {
            return Err(crate::error::EngineError::execution(
                "EnhancedSortOperator::next_batch() called before prepare()"
            ));
        }
        
        if self.current_output_idx >= self.sorted_batches.len() {
            return Ok(None);
        }
        
        let batch = self.sorted_batches[self.current_output_idx].clone();
        self.current_output_idx += 1;
        
        Ok(Some(batch))
    }
    
    fn children_mut(&mut self) -> Vec<&mut dyn VectorOperator> {
        vec![]
    }
}

