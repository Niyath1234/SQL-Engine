/// VectorDistinctOperator: Async, vectorized distinct operator
/// 
/// This is the Phase 1 replacement for DistinctOperator, designed for:
/// - Hash-based deduplication
/// - Vectorized comparison
/// - Streaming distinct output
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::error::EngineResult;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use tracing::{debug, info};
use fxhash::FxHashSet;

/// Vector distinct operator - async, vectorized deduplication
pub struct VectorDistinctOperator {
    /// Input operator
    input: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
    
    /// Set of seen row keys (for deduplication)
    seen_keys: FxHashSet<Vec<u8>>,
    
    /// Distinct batches (materialized after all input processed)
    distinct_batches: Vec<VectorBatch>,
    
    /// Current output batch index
    current_output_idx: usize,
    
    /// Input exhausted flag
    input_exhausted: bool,
    
    /// Prepared flag
    prepared: bool,
}

impl VectorDistinctOperator {
    /// Create a new vector distinct operator
    pub fn new(
        input: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
    ) -> Self {
        Self {
            input,
            seen_keys: FxHashSet::default(),
            distinct_batches: Vec::new(),
            current_output_idx: 0,
            input_exhausted: false,
            prepared: false,
        }
    }
    
    /// Process all input batches and deduplicate
    async fn process_input(&mut self) -> EngineResult<()> {
        if self.input_exhausted {
            return Ok(()); // Already processed
        }
        
        let mut distinct_rows: Vec<(usize, usize)> = Vec::new(); // (batch_idx, row_idx)
        
        // Collect all input batches
        let mut input_batches = Vec::new();
        loop {
            let batch = {
                let mut input = self.input.lock().await;
                input.next_batch().await?
            };
            
            let Some(batch) = batch else {
                break;
            };
            
            input_batches.push(batch);
        }
        
        // Deduplicate rows
        for (batch_idx, batch) in input_batches.iter().enumerate() {
            for row_idx in 0..batch.row_count {
                let row_key = self.compute_row_key(batch, row_idx)?;
                
                if !self.seen_keys.contains(&row_key) {
                    self.seen_keys.insert(row_key);
                    distinct_rows.push((batch_idx, row_idx));
                }
            }
        }
        
        info!(
            total_rows = distinct_rows.len(),
            input_batches = input_batches.len(),
            "Deduplication complete"
        );
        
        // Materialize distinct batches
        if !distinct_rows.is_empty() && !input_batches.is_empty() {
            let distinct_batch = self.materialize_distinct_batch(&input_batches, &distinct_rows)?;
            self.distinct_batches.push(distinct_batch);
        }
        
        self.input_exhausted = true;
        Ok(())
    }
    
    /// Compute a key for a row (for deduplication)
    fn compute_row_key(&self, batch: &VectorBatch, row_idx: usize) -> EngineResult<Vec<u8>> {
        use std::io::Write;
        
        let mut key = Vec::new();
        
        // Hash all column values
        for col_idx in 0..batch.schema.fields().len() {
            let array = batch.column(col_idx)?;
            let value_bytes = Self::value_to_bytes(array.as_ref(), row_idx)?;
            key.write_all(&value_bytes).unwrap();
        }
        
        Ok(key)
    }
    
    /// Convert a value to bytes for hashing
    fn value_to_bytes(array: &dyn Array, row_idx: usize) -> EngineResult<Vec<u8>> {
        use std::io::Write;
        
        let mut bytes = Vec::new();
        
        match array.data_type() {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                bytes.write_all(&arr.value(row_idx).to_le_bytes()).unwrap();
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Float64Array"))?;
                bytes.write_all(&arr.value(row_idx).to_le_bytes()).unwrap();
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                bytes.write_all(arr.value(row_idx).as_bytes()).unwrap();
            }
            _ => {
                return Err(crate::error::EngineError::execution(format!(
                    "Unsupported type for distinct: {:?}",
                    array.data_type()
                )));
            }
        }
        
        Ok(bytes)
    }
    
    /// Materialize distinct batch from distinct row indices
    fn materialize_distinct_batch(
        &self,
        input_batches: &[VectorBatch],
        distinct_rows: &[(usize, usize)],
    ) -> EngineResult<VectorBatch> {
        if distinct_rows.is_empty() || input_batches.is_empty() {
            return Err(crate::error::EngineError::execution("Cannot materialize empty distinct batch"));
        }
        
        // Get schema from first input batch
        let first_batch = &input_batches[distinct_rows[0].0];
        let schema = first_batch.schema.clone();
        
        // Materialize each column
        let mut output_arrays = Vec::new();
        
        for col_idx in 0..schema.fields().len() {
            let mut column_values = Vec::new();
            
            for (batch_idx, row_idx) in distinct_rows {
                let batch = &input_batches[*batch_idx];
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
    ) -> EngineResult<MaterializedValue> {
        match array.data_type() {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                Ok(MaterializedValue::Int64(arr.value(row_idx)))
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Float64Array"))?;
                Ok(MaterializedValue::Float64(arr.value(row_idx)))
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                Ok(MaterializedValue::String(arr.value(row_idx).to_string()))
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
        values: Vec<MaterializedValue>,
    ) -> EngineResult<Arc<dyn Array>> {
        match data_type {
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(values.len());
                for v in values {
                    match v {
                        MaterializedValue::Int64(i) => builder.append_value(i),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(values.len());
                for v in values {
                    match v {
                        MaterializedValue::Float64(f) => builder.append_value(f),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 10);
                for v in values {
                    match v {
                        MaterializedValue::String(s) => builder.append_value(&s),
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

/// Materialized value for batch construction
#[derive(Clone, Debug)]
enum MaterializedValue {
    Int64(i64),
    Float64(f64),
    String(String),
}

#[async_trait::async_trait]
impl VectorOperator for VectorDistinctOperator {
    fn schema(&self) -> SchemaRef {
        // Distinct doesn't change schema
        // We can't get schema synchronously from async Mutex
        // For now, return placeholder - will be set during prepare()
        Arc::new(Schema::new(Vec::<Field>::new()))
    }
    
    async fn prepare(&mut self) -> EngineResult<()> {
        if self.prepared {
            return Ok(());
        }
        
        info!("Preparing vector distinct operator");
        
        // Recursively prepare input
        {
            let mut input = self.input.lock().await;
            input.prepare().await?;
        }
        
        self.prepared = true;
        debug!("Vector distinct operator prepared");
        Ok(())
    }
    
    async fn next_batch(&mut self) -> EngineResult<Option<VectorBatch>> {
        if !self.prepared {
            return Err(crate::error::EngineError::execution(
                "VectorDistinctOperator::next_batch() called before prepare()"
            ));
        }
        
        // Process all input batches first
        if !self.input_exhausted {
            self.process_input().await?;
        }
        
        // Return distinct batches
        if self.current_output_idx < self.distinct_batches.len() {
            let batch = self.distinct_batches[self.current_output_idx].clone();
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

