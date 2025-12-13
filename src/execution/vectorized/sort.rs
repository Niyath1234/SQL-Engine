/// VectorSortOperator: Async, vectorized sort operator
/// 
/// This is the Phase 1 replacement for SortOperator, designed for:
/// - External merge sort (spillable)
/// - Vectorized sorting
/// - Multi-column sorting
/// - Streaming sort output
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::error::EngineResult;
use crate::query::plan::OrderByExpr;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use tracing::{debug, info};
use ordered_float::OrderedFloat;

/// Vector sort operator - async, vectorized sorting
pub struct VectorSortOperator {
    /// Input operator
    pub(crate) input: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
    
    /// Order by expressions
    order_by: Vec<OrderByExpr>,
    
    /// All input batches (collected before sorting)
    input_batches: Vec<VectorBatch>,
    
    /// Sorted batches (after sorting)
    sorted_batches: Vec<VectorBatch>,
    
    /// Current output batch index
    current_output_idx: usize,
    
    /// Input exhausted flag
    input_exhausted: bool,
    
    /// Prepared flag
    prepared: bool,
}

impl VectorSortOperator {
    /// Create a new vector sort operator
    pub fn new(
        input: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
        order_by: Vec<OrderByExpr>,
    ) -> Self {
        Self {
            input,
            order_by,
            input_batches: Vec::new(),
            sorted_batches: Vec::new(),
            current_output_idx: 0,
            input_exhausted: false,
            prepared: false,
        }
    }
    
    /// Collect all input batches
    async fn collect_input(&mut self) -> EngineResult<()> {
        loop {
            let batch = {
                let mut input = self.input.lock().await;
                input.next_batch().await?
            };
            
            let Some(batch) = batch else {
                break;
            };
            
            self.input_batches.push(batch);
        }
        
        info!(
            batch_count = self.input_batches.len(),
            total_rows = self.input_batches.iter().map(|b| b.row_count).sum::<usize>(),
            "Collected input batches for sorting"
        );
        
        Ok(())
    }
    
    /// Sort all collected batches
    fn sort_batches(&mut self) -> EngineResult<()> {
        if self.input_batches.is_empty() {
            self.sorted_batches = Vec::new();
            return Ok(());
        }
        
        info!(
            order_by_count = self.order_by.len(),
            "Sorting batches"
        );
        
        // For now, implement simple in-memory sort
        // TODO: Implement external merge sort for large datasets
        
        // Extract all rows with their sort keys
        let mut rows: Vec<(Vec<SortKey>, usize, usize)> = Vec::new(); // (sort_keys, batch_idx, row_idx)
        
        for (batch_idx, batch) in self.input_batches.iter().enumerate() {
            for row_idx in 0..batch.row_count {
                let sort_keys = self.extract_sort_keys(batch, row_idx)?;
                rows.push((sort_keys, batch_idx, row_idx));
            }
        }
        
        // Sort rows by sort keys
        rows.sort_by(|a, b| {
            for (i, order_expr) in self.order_by.iter().enumerate() {
                let cmp = a.0[i].cmp(&b.0[i]);
                if cmp != std::cmp::Ordering::Equal {
                    return if order_expr.ascending { cmp } else { cmp.reverse() };
                }
            }
            std::cmp::Ordering::Equal
        });
        
        // Materialize sorted batches
        // For simplicity, create one batch with all sorted rows
        // TODO: Split into multiple batches if needed
        if !rows.is_empty() {
            let sorted_batch = self.materialize_sorted_batch(&rows)?;
            self.sorted_batches.push(sorted_batch);
        }
        
        debug!(
            sorted_rows = rows.len(),
            sorted_batches = self.sorted_batches.len(),
            "Sorting complete"
        );
        
        Ok(())
    }
    
    /// Extract sort keys for a row
    fn extract_sort_keys(&self, batch: &VectorBatch, row_idx: usize) -> EngineResult<Vec<SortKey>> {
        let mut keys = Vec::new();
        
        for order_expr in &self.order_by {
            let col_idx = batch.schema.fields()
                .iter()
                .position(|f| f.name() == order_expr.column.as_str())
                .ok_or_else(|| crate::error::EngineError::execution(format!(
                    "Sort column '{}' not found in batch schema",
                    order_expr.column
                )))?;
            
            let array = batch.column(col_idx)?;
            let key = Self::extract_sort_key(array.as_ref(), row_idx)?;
            keys.push(key);
        }
        
        Ok(keys)
    }
    
    /// Extract a single sort key value
    fn extract_sort_key(array: &dyn Array, row_idx: usize) -> EngineResult<SortKey> {
        match array.data_type() {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                Ok(SortKey::Int64(arr.value(row_idx)))
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Float64Array"))?;
                Ok(SortKey::Float64(OrderedFloat(arr.value(row_idx))))
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                Ok(SortKey::String(arr.value(row_idx).to_string()))
            }
            _ => {
                Err(crate::error::EngineError::execution(format!(
                    "Unsupported sort key type: {:?}",
                    array.data_type()
                )))
            }
        }
    }
    
    /// Materialize sorted batch from sorted row indices
    fn materialize_sorted_batch(
        &self,
        sorted_rows: &[(Vec<SortKey>, usize, usize)],
    ) -> EngineResult<VectorBatch> {
        if sorted_rows.is_empty() {
            return Err(crate::error::EngineError::execution("Cannot materialize empty sorted batch"));
        }
        
        // Get schema from first input batch
        let first_batch = &self.input_batches[sorted_rows[0].1];
        let schema = first_batch.schema.clone();
        
        // Materialize each column
        let mut output_arrays = Vec::new();
        
        for col_idx in 0..schema.fields().len() {
            let mut column_values: Vec<SortMaterializedValue> = Vec::new();
            
            for (_, batch_idx, row_idx) in sorted_rows {
                let batch = &self.input_batches[*batch_idx];
                let array = batch.column(col_idx)?;
                let value = Self::extract_value_for_materialization(array.as_ref(), *row_idx)?;
                column_values.push(value);
            }
            
            // Build array from values
            let array = Self::build_array_from_values(&schema.fields()[col_idx].data_type(), column_values)?;
            output_arrays.push(array);
        }
        
        Ok(VectorBatch::new(output_arrays, schema))
    }
    
    /// Extract value for materialization
    fn extract_value_for_materialization(
        array: &dyn Array,
        row_idx: usize,
    ) -> EngineResult<SortMaterializedValue> {
        match array.data_type() {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                Ok(SortMaterializedValue::Int64(arr.value(row_idx)))
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Float64Array"))?;
                Ok(SortMaterializedValue::Float64(arr.value(row_idx)))
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                Ok(SortMaterializedValue::String(arr.value(row_idx).to_string()))
            }
            _ => {
                Err(crate::error::EngineError::execution(format!(
                    "Unsupported type for materialization: {:?}",
                    array.data_type()
                )))
            }
        }
    }
    
    /// Build array from materialized values
    fn build_array_from_values(
        data_type: &DataType,
        values: Vec<SortMaterializedValue>,
    ) -> EngineResult<Arc<dyn Array>> {
        match data_type {
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(values.len());
                for v in values {
                    match v {
                        SortMaterializedValue::Int64(i) => builder.append_value(i),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(values.len());
                for v in values {
                    match v {
                        SortMaterializedValue::Float64(f) => builder.append_value(f),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 10);
                for v in values {
                    match v {
                        SortMaterializedValue::String(s) => builder.append_value(&s),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            _ => {
                Err(crate::error::EngineError::execution(format!(
                    "Unsupported data type for array building: {:?}",
                    data_type
                )))
            }
        }
    }
}

/// Materialized value for batch construction (sort-specific)
#[derive(Clone, Debug)]
enum SortMaterializedValue {
    Int64(i64),
    Float64(f64),
    String(String),
}

/// Sort key for comparison
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum SortKey {
    Int64(i64),
    Float64(OrderedFloat<f64>),
    String(String),
}

/// Materialized value for batch construction
#[derive(Clone, Debug)]
enum MaterializedValue {
    Int64(i64),
    Float64(f64),
    String(String),
}

#[async_trait::async_trait]
impl VectorOperator for VectorSortOperator {
    fn schema(&self) -> SchemaRef {
        // Sort doesn't change schema
        // We can't get schema synchronously from async Mutex
        // For now, return placeholder - will be set during prepare()
        Arc::new(Schema::new(Vec::<Field>::new()))
    }
    
    async fn prepare(&mut self) -> EngineResult<()> {
        if self.prepared {
            return Ok(());
        }
        
        info!(
            order_by_count = self.order_by.len(),
            "Preparing vector sort operator"
        );
        
        // Recursively prepare input
        {
            let mut input = self.input.lock().await;
            input.prepare().await?;
        }
        
        self.prepared = true;
        debug!("Vector sort operator prepared");
        Ok(())
    }
    
    async fn next_batch(&mut self) -> EngineResult<Option<VectorBatch>> {
        if !self.prepared {
            return Err(crate::error::EngineError::execution(
                "VectorSortOperator::next_batch() called before prepare()"
            ));
        }
        
        // Collect all input batches first
        if !self.input_exhausted {
            self.collect_input().await?;
            self.input_exhausted = true;
            
            // Sort batches
            self.sort_batches()?;
        }
        
        // Return sorted batches
        if self.current_output_idx < self.sorted_batches.len() {
            let batch = self.sorted_batches[self.current_output_idx].clone();
            self.current_output_idx += 1;
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }
    
    fn children_mut(&mut self) -> Vec<&mut dyn VectorOperator> {
        vec![]
    }
}

