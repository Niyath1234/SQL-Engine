/// VectorFilterOperator: Async, vectorized filter operator
/// 
/// This is the Phase 1 replacement for FilterOperator, designed for:
/// - Vectorized predicate evaluation
/// - Zero-copy selection bitmap operations
/// - SIMD-optimized comparisons
/// - Async batch processing
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::error::EngineResult;
use crate::query::plan::FilterPredicate;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use bitvec::prelude::*;
use tracing::{debug, info};
use regex;

/// Vector filter operator - async, vectorized filtering
pub struct VectorFilterOperator {
    /// Input operator
    input: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
    
    /// Filter predicates
    predicates: Vec<FilterPredicate>,
    
    /// Prepared flag
    prepared: bool,
}

impl VectorFilterOperator {
    /// Create a new vector filter operator
    pub fn new(
        input: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
        predicates: Vec<FilterPredicate>,
    ) -> Self {
        Self {
            input,
            predicates,
            prepared: false,
        }
    }
    
    /// Apply predicates to a batch (vectorized)
    async fn apply_predicates(&self, batch: &VectorBatch) -> EngineResult<BitVec> {
        if self.predicates.is_empty() {
            // No predicates - return all rows selected
            return Ok(bitvec![1; batch.row_count]);
        }
        
        // Start with all rows selected
        let mut selection = bitvec![1; batch.row_count];
        
        // Apply each predicate (AND semantics)
        for predicate in &self.predicates {
            let column_selection = self.apply_predicate(batch, predicate).await?;
            
            // Combine with existing selection (AND)
            selection &= &column_selection;
        }
        
        debug!(
            final_selection_count = selection.count_ones(),
            total_rows = batch.row_count,
            "Applied predicates"
        );
        
        Ok(selection)
    }
    
    /// Apply a single predicate to a batch
    async fn apply_predicate(
        &self,
        batch: &VectorBatch,
        predicate: &FilterPredicate,
    ) -> EngineResult<BitVec> {
        // Find column index
        let col_idx = batch.schema.fields()
            .iter()
            .position(|f| f.name() == predicate.column.as_str())
            .ok_or_else(|| crate::error::EngineError::execution(format!(
                "Column '{}' not found in batch schema",
                predicate.column
            )))?;
        
        // Get column array
        let column = batch.column(col_idx)?;
        
        // Create selection bitmap based on predicate
        let mut selection = bitvec![0; batch.row_count];
        
        match (&predicate.operator, &predicate.value) {
            (crate::query::plan::PredicateOperator::Equals, value) => {
                self.apply_equals(column.as_ref(), value, &mut selection)?;
            }
            (crate::query::plan::PredicateOperator::NotEquals, value) => {
                self.apply_not_equals(column.as_ref(), value, &mut selection)?;
            }
            (crate::query::plan::PredicateOperator::GreaterThan, value) => {
                self.apply_greater_than(column.as_ref(), value, &mut selection)?;
            }
            (crate::query::plan::PredicateOperator::LessThan, value) => {
                self.apply_less_than(column.as_ref(), value, &mut selection)?;
            }
            (crate::query::plan::PredicateOperator::GreaterThanOrEqual, value) => {
                self.apply_greater_than_or_equal(column.as_ref(), value, &mut selection)?;
            }
            (crate::query::plan::PredicateOperator::LessThanOrEqual, value) => {
                self.apply_less_than_or_equal(column.as_ref(), value, &mut selection)?;
            }
            (crate::query::plan::PredicateOperator::Like, _) => {
                // LIKE is handled separately (pattern matching)
                if let Some(ref pattern) = predicate.pattern {
                    self.apply_like(column.as_ref(), pattern, &mut selection)?;
                }
            }
            _ => {
                return Err(crate::error::EngineError::execution(format!(
                    "Unsupported predicate operator: {:?}",
                    predicate.operator
                )));
            }
        }
        
        Ok(selection)
    }
    
    /// Apply equals predicate (vectorized)
    fn apply_equals(
        &self,
        array: &dyn Array,
        value: &crate::storage::fragment::Value,
        selection: &mut BitVec,
    ) -> EngineResult<()> {
        match (array.data_type(), value) {
            (DataType::Int64, crate::storage::fragment::Value::Int64(v)) => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                for (i, val) in arr.iter().enumerate() {
                    if let Some(val) = val {
                        selection.set(i, val == *v);
                    }
                }
            }
            (DataType::Utf8, crate::storage::fragment::Value::String(v)) => {
                let arr = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                for (i, val) in arr.iter().enumerate() {
                    if let Some(val) = val {
                        selection.set(i, val == v);
                    }
                }
            }
            _ => {
                return Err(crate::error::EngineError::execution(format!(
                    "Unsupported type combination for equals: {:?} vs {:?}",
                    array.data_type(), value
                )));
            }
        }
        Ok(())
    }
    
    /// Apply not equals predicate (vectorized)
    fn apply_not_equals(
        &self,
        array: &dyn Array,
        value: &crate::storage::fragment::Value,
        selection: &mut BitVec,
    ) -> EngineResult<()> {
        // Similar to equals but inverted - flip all bits
        // First apply equals to get matching positions
        let mut temp_selection = bitvec![0; selection.len()];
        self.apply_equals(array, value, &mut temp_selection)?;
        
        // Invert: set selection[i] = !temp_selection[i]
        for i in 0..selection.len() {
            selection.set(i, !temp_selection[i]);
        }
        Ok(())
    }
    
    /// Apply greater than predicate (vectorized)
    fn apply_greater_than(
        &self,
        array: &dyn Array,
        value: &crate::storage::fragment::Value,
        selection: &mut BitVec,
    ) -> EngineResult<()> {
        match (array.data_type(), value) {
            (DataType::Int64, crate::storage::fragment::Value::Int64(v)) => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                for (i, val) in arr.iter().enumerate() {
                    if let Some(val) = val {
                        selection.set(i, val > *v);
                    }
                }
            }
            _ => {
                return Err(crate::error::EngineError::execution(format!(
                    "Unsupported type for greater than: {:?}",
                    array.data_type()
                )));
            }
        }
        Ok(())
    }
    
    /// Apply less than predicate (vectorized)
    fn apply_less_than(
        &self,
        array: &dyn Array,
        value: &crate::storage::fragment::Value,
        selection: &mut BitVec,
    ) -> EngineResult<()> {
        match (array.data_type(), value) {
            (DataType::Int64, crate::storage::fragment::Value::Int64(v)) => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to Int64Array"))?;
                for (i, val) in arr.iter().enumerate() {
                    if let Some(val) = val {
                        selection.set(i, val < *v);
                    }
                }
            }
            _ => {
                return Err(crate::error::EngineError::execution(format!(
                    "Unsupported type for less than: {:?}",
                    array.data_type()
                )));
            }
        }
        Ok(())
    }
    
    /// Apply greater than or equal predicate
    fn apply_greater_than_or_equal(
        &self,
        array: &dyn Array,
        value: &crate::storage::fragment::Value,
        selection: &mut BitVec,
    ) -> EngineResult<()> {
        // Create temp selections for GT and EQ, then OR them
        let mut gt_selection = bitvec![0; selection.len()];
        let mut eq_selection = bitvec![0; selection.len()];
        self.apply_greater_than(array, value, &mut gt_selection)?;
        self.apply_equals(array, value, &mut eq_selection)?;
        
        // OR: selection = gt_selection | eq_selection
        for i in 0..selection.len() {
            selection.set(i, gt_selection[i] || eq_selection[i]);
        }
        Ok(())
    }
    
    /// Apply less than or equal predicate
    fn apply_less_than_or_equal(
        &self,
        array: &dyn Array,
        value: &crate::storage::fragment::Value,
        selection: &mut BitVec,
    ) -> EngineResult<()> {
        // Create temp selections for LT and EQ, then OR them
        let mut lt_selection = bitvec![0; selection.len()];
        let mut eq_selection = bitvec![0; selection.len()];
        self.apply_less_than(array, value, &mut lt_selection)?;
        self.apply_equals(array, value, &mut eq_selection)?;
        
        // OR: selection = lt_selection | eq_selection
        for i in 0..selection.len() {
            selection.set(i, lt_selection[i] || eq_selection[i]);
        }
        Ok(())
    }
    
    /// Apply LIKE predicate (pattern matching)
    fn apply_like(
        &self,
        array: &dyn Array,
        pattern: &str,
        selection: &mut BitVec,
    ) -> EngineResult<()> {
        // Convert SQL LIKE pattern to regex
        let regex_pattern = pattern
            .replace("%", ".*")
            .replace("_", ".");
        let regex = regex::Regex::new(&format!("^{}$", regex_pattern))
            .map_err(|e| crate::error::EngineError::execution(format!(
                "Invalid LIKE pattern '{}': {}",
                pattern, e
            )))?;
        
        match array.data_type() {
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| crate::error::EngineError::execution("Failed to downcast to StringArray"))?;
                for (i, val) in arr.iter().enumerate() {
                    if let Some(val) = val {
                        selection.set(i, regex.is_match(val));
                    }
                }
            }
            _ => {
                return Err(crate::error::EngineError::execution(format!(
                    "LIKE operator only supports string types, got {:?}",
                    array.data_type()
                )));
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl VectorOperator for VectorFilterOperator {
    fn schema(&self) -> SchemaRef {
        // Filter doesn't change schema
        // We can't get schema synchronously from async Mutex
        // For now, return a placeholder - will be set during prepare()
        // TODO: Store schema after prepare()
        Arc::new(Schema::new(Vec::<Field>::new()))
    }
    
    async fn prepare(&mut self) -> EngineResult<()> {
        if self.prepared {
            return Ok(());
        }
        
        info!(
            predicate_count = self.predicates.len(),
            "Preparing vector filter operator"
        );
        
        // Recursively prepare input
        {
            let mut input = self.input.lock().await;
            input.prepare().await?;
        }
        
        self.prepared = true;
        debug!("Vector filter operator prepared");
        Ok(())
    }
    
    async fn next_batch(&mut self) -> EngineResult<Option<VectorBatch>> {
        if !self.prepared {
            return Err(crate::error::EngineError::execution(
                "VectorFilterOperator::next_batch() called before prepare()"
            ));
        }
        
        // Get next batch from input
        let input_batch = {
            let mut input = self.input.lock().await;
            input.next_batch().await?
        };
        
        let Some(mut batch) = input_batch else {
            return Ok(None); // Input exhausted
        };
        
        // Apply predicates
        let selection = self.apply_predicates(&batch).await?;
        
        // Apply selection to batch (zero-copy)
        let filtered_batch = batch.apply_selection(selection);
        
        debug!(
            input_rows = batch.row_count,
            output_rows = filtered_batch.row_count,
            "Filtered batch"
        );
        
        Ok(Some(filtered_batch))
    }
    
    fn children_mut(&mut self) -> Vec<&mut dyn VectorOperator> {
        // Cannot return mutable reference through Arc<Mutex>
        // This is a limitation - we handle prepare() differently
        vec![]
    }
}

