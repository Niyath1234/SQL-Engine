/// UNION, INTERSECT, EXCEPT Operations
/// Set operations for combining query results
use crate::execution::batch::{ExecutionBatch, BatchIterator};
use crate::storage::fragment::Value;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use anyhow::Result;
use bitvec::prelude::*;
use std::collections::HashSet;

/// Set operation type
#[derive(Clone, Debug)]
pub enum SetOperation {
    Union,
    UnionAll,
    Intersect,
    Except,
}

/// Set operation operator
pub struct SetOperator {
    left: Box<dyn BatchIterator>,
    right: Box<dyn BatchIterator>,
    operation: SetOperation,
    left_exhausted: bool,
    right_exhausted: bool,
    seen_rows: HashSet<Vec<Value>>, // For deduplication in UNION (not UNION ALL)
}

impl SetOperator {
    pub fn new(
        left: Box<dyn BatchIterator>,
        right: Box<dyn BatchIterator>,
        operation: SetOperation,
    ) -> Self {
        Self {
            left,
            right,
            operation,
            left_exhausted: false,
            right_exhausted: false,
            seen_rows: HashSet::new(),
        }
    }
}

impl BatchIterator for SetOperator {
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        match self.operation {
            SetOperation::Union => self.execute_union(),
            SetOperation::UnionAll => self.execute_union_all(),
            SetOperation::Intersect => self.execute_intersect(),
            SetOperation::Except => self.execute_except(),
        }
    }
    
    fn schema(&self) -> SchemaRef {
        // Both sides must have compatible schemas
        self.left.schema()
    }
}

impl SetOperator {
    fn execute_union(&mut self) -> Result<Option<ExecutionBatch>> {
        // UNION: distinct rows from both sides
        let mut output_rows = vec![];
        let schema = self.schema();
        
        // Process left side
        if !self.left_exhausted {
            while let Some(batch) = self.left.next()? {
                for row_idx in 0..batch.row_count {
                    if batch.selection[row_idx] {
                        let row = extract_row(&batch, row_idx, &schema)?;
                        let row_key = row_to_key(&row);
                        if !self.seen_rows.contains(&row_key) {
                            self.seen_rows.insert(row_key);
                            output_rows.push(row);
                        }
                    }
                }
            }
            self.left_exhausted = true;
        }
        
        // Process right side
        if !self.right_exhausted {
            while let Some(batch) = self.right.next()? {
                for row_idx in 0..batch.row_count {
                    if batch.selection[row_idx] {
                        let row = extract_row(&batch, row_idx, &schema)?;
                        let row_key = row_to_key(&row);
                        if !self.seen_rows.contains(&row_key) {
                            self.seen_rows.insert(row_key);
                            output_rows.push(row);
                        }
                    }
                }
            }
            self.right_exhausted = true;
        }
        
        if output_rows.is_empty() {
            return Ok(None);
        }
        
        // Convert to ExecutionBatch
        let row_count = output_rows.len();
        let mut output_arrays = vec![];
        
        if row_count > 0 {
            let col_count = output_rows[0].len();
            for col_idx in 0..col_count {
                let col_values: Vec<Value> = output_rows.iter()
                    .map(|row| row[col_idx].clone())
                    .collect();
                
                // Convert to Arrow array
                let field = schema.field(col_idx);
                let array = values_to_array(&col_values, field.data_type())?;
                output_arrays.push(array);
            }
        }
        
        let output_batch = crate::storage::columnar::ColumnarBatch::new(output_arrays, schema);
        let selection = bitvec![1; row_count];
        
        let mut exec_batch = ExecutionBatch::new(output_batch);
        exec_batch.selection = selection;
        exec_batch.row_count = row_count;
        Ok(Some(exec_batch))
    }
    
    fn execute_union_all(&mut self) -> Result<Option<ExecutionBatch>> {
        // UNION ALL: all rows from both sides (no deduplication)
        // First try left side
        if !self.left_exhausted {
            if let Some(batch) = self.left.next()? {
                return Ok(Some(batch));
            }
            self.left_exhausted = true;
        }
        
        // Then right side
        if !self.right_exhausted {
            if let Some(batch) = self.right.next()? {
                return Ok(Some(batch));
            }
            self.right_exhausted = true;
        }
        
        Ok(None)
    }
    
    fn execute_intersect(&mut self) -> Result<Option<ExecutionBatch>> {
        // INTERSECT: rows that appear in both sides
        // Build set from left side
        if !self.left_exhausted {
            while let Some(batch) = self.left.next()? {
                for row_idx in 0..batch.row_count {
                    if batch.selection[row_idx] {
                        let row = extract_row(&batch, row_idx, &self.schema())?;
                        let row_key = row_to_key(&row);
                        self.seen_rows.insert(row_key);
                    }
                }
            }
            self.left_exhausted = true;
        }
        
        // Find matching rows from right side
        let mut output_rows = vec![];
        let mut matched_keys = HashSet::new();
        
        if !self.right_exhausted {
            while let Some(batch) = self.right.next()? {
                for row_idx in 0..batch.row_count {
                    if batch.selection[row_idx] {
                        let row = extract_row(&batch, row_idx, &self.schema())?;
                        let row_key = row_to_key(&row);
                        if self.seen_rows.contains(&row_key) && !matched_keys.contains(&row_key) {
                            matched_keys.insert(row_key);
                            output_rows.push(row);
                        }
                    }
                }
            }
            self.right_exhausted = true;
        }
        
        if output_rows.is_empty() {
            return Ok(None);
        }
        
        // Convert to ExecutionBatch
        let row_count = output_rows.len();
        let mut output_arrays = vec![];
        let schema = self.schema();
        
        if row_count > 0 {
            let col_count = output_rows[0].len();
            for col_idx in 0..col_count {
                let col_values: Vec<Value> = output_rows.iter()
                    .map(|row| row[col_idx].clone())
                    .collect();
                
                let field = schema.field(col_idx);
                let array = values_to_array(&col_values, field.data_type())?;
                output_arrays.push(array);
            }
        }
        
        let output_batch = crate::storage::columnar::ColumnarBatch::new(output_arrays, schema);
        let selection = bitvec![1; row_count];
        
        let mut exec_batch = ExecutionBatch::new(output_batch);
        exec_batch.selection = selection;
        exec_batch.row_count = row_count;
        Ok(Some(exec_batch))
    }
    
    fn execute_except(&mut self) -> Result<Option<ExecutionBatch>> {
        // EXCEPT: rows from left that are not in right
        // Build set from right side
        if !self.right_exhausted {
            while let Some(batch) = self.right.next()? {
                for row_idx in 0..batch.row_count {
                    if batch.selection[row_idx] {
                        let row = extract_row(&batch, row_idx, &self.schema())?;
                        let row_key = row_to_key(&row);
                        self.seen_rows.insert(row_key);
                    }
                }
            }
            self.right_exhausted = true;
        }
        
        // Find rows from left that are not in right
        let mut output_rows = vec![];
        
        if !self.left_exhausted {
            while let Some(batch) = self.left.next()? {
                for row_idx in 0..batch.row_count {
                    if batch.selection[row_idx] {
                        let row = extract_row(&batch, row_idx, &self.schema())?;
                        let row_key = row_to_key(&row);
                        if !self.seen_rows.contains(&row_key) {
                            self.seen_rows.insert(row_key); // Track to avoid duplicates
                            output_rows.push(row);
                        }
                    }
                }
            }
            self.left_exhausted = true;
        }
        
        if output_rows.is_empty() {
            return Ok(None);
        }
        
        // Convert to ExecutionBatch
        let row_count = output_rows.len();
        let mut output_arrays = vec![];
        let schema = self.schema();
        
        if row_count > 0 {
            let col_count = output_rows[0].len();
            for col_idx in 0..col_count {
                let col_values: Vec<Value> = output_rows.iter()
                    .map(|row| row[col_idx].clone())
                    .collect();
                
                let field = schema.field(col_idx);
                let array = values_to_array(&col_values, field.data_type())?;
                output_arrays.push(array);
            }
        }
        
        let output_batch = crate::storage::columnar::ColumnarBatch::new(output_arrays, schema);
        let selection = bitvec![1; row_count];
        
        let mut exec_batch = ExecutionBatch::new(output_batch);
        exec_batch.selection = selection;
        exec_batch.row_count = row_count;
        Ok(Some(exec_batch))
    }
}

fn extract_row(batch: &ExecutionBatch, row_idx: usize, schema: &SchemaRef) -> Result<Vec<Value>> {
    let mut row = vec![];
    for col_idx in 0..schema.fields().len() {
        if let Some(array) = batch.batch.column(col_idx) {
            if array.is_null(row_idx) {
                row.push(Value::Null);
            } else {
                match array.data_type() {
                    DataType::Int64 => {
                        let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                        row.push(Value::Int64(arr.value(row_idx)));
                    }
                    DataType::Float64 => {
                        let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                        row.push(Value::Float64(arr.value(row_idx)));
                    }
                    DataType::Utf8 => {
                        let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                        row.push(Value::String(arr.value(row_idx).to_string()));
                    }
                    _ => row.push(Value::Null),
                }
            }
        } else {
            row.push(Value::Null);
        }
    }
    Ok(row)
}

fn row_to_key(row: &[Value]) -> Vec<Value> {
    row.to_vec()
}

fn values_to_array(values: &[Value], data_type: &DataType) -> Result<Arc<dyn Array>> {
    match data_type {
        DataType::Int64 => {
            let arr: Vec<Option<i64>> = values.iter().map(|val| match val {
                Value::Int64(i) => Some(*i),
                Value::Null => None,
                _ => None,
            }).collect();
            Ok(Arc::new(Int64Array::from(arr)))
        }
        DataType::Float64 => {
            let arr: Vec<Option<f64>> = values.iter().map(|val| match val {
                Value::Float64(f) => Some(*f),
                Value::Null => None,
                _ => None,
            }).collect();
            Ok(Arc::new(Float64Array::from(arr)))
        }
        DataType::Utf8 => {
            let arr: Vec<Option<String>> = values.iter().map(|val| match val {
                Value::String(s) => Some(s.clone()),
                Value::Null => None,
                _ => None,
            }).collect();
            Ok(Arc::new(StringArray::from(arr)))
        }
        _ => anyhow::bail!("Unsupported data type: {:?}", data_type)
    }
}

