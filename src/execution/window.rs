/// Window Functions Execution
/// Implements window functions: ROW_NUMBER, RANK, SUM OVER, etc.
use crate::execution::batch::{ExecutionBatch, BatchIterator};
use crate::query::plan::{WindowFunctionExpr, WindowFunction, OrderByExpr};
use crate::storage::fragment::Value;
use arrow::datatypes::*;
use arrow::array::*;
use std::sync::Arc;
use anyhow::Result;
use bitvec::prelude::*;
use std::collections::{HashMap, HashSet};
use fxhash::FxHashMap;

/// Window operator - computes window functions
pub struct WindowOperator {
    input: Box<dyn BatchIterator>,
    window_functions: Vec<WindowFunctionExpr>,
    schema: SchemaRef,
    /// Buffered input batches (need all rows for window computation)
    buffered_batches: Vec<ExecutionBatch>,
    buffered: bool,
    current_batch_idx: usize,
    /// Partition grouping (for PARTITION BY)
    partition_groups: Vec<Vec<usize>>, // Each partition group contains row indices
    /// Sorted indices within partitions (for ORDER BY)
    sorted_indices: Vec<usize>,
}

impl WindowOperator {
    pub fn new(
        input: Box<dyn BatchIterator>,
        window_functions: Vec<WindowFunctionExpr>,
    ) -> Self {
        let schema = input.schema();
        Self {
            input,
            window_functions,
            schema,
            buffered_batches: vec![],
            buffered: false,
            current_batch_idx: 0,
            partition_groups: vec![],
            sorted_indices: vec![],
        }
    }
    
    /// Buffer all input batches (window functions need to see all rows)
    fn buffer_all_input(&mut self) -> Result<()> {
        if self.buffered {
            return Ok(());
        }
        
        // Collect all batches
        while let Some(batch) = self.input.next()? {
            self.buffered_batches.push(batch);
        }
        
        self.buffered = true;
        Ok(())
    }
    
    /// Build partition groups based on PARTITION BY columns
    fn build_partitions(&mut self, partition_by: &[String]) -> Result<()> {
        if partition_by.is_empty() {
            // No PARTITION BY - all rows in single partition
            let total_rows = self.buffered_batches.iter().map(|b| b.row_count).sum();
            self.partition_groups = vec![(0..total_rows).collect()];
            return Ok(());
        }
        
        // Group rows by partition key values
        let mut partition_map: FxHashMap<Vec<Value>, Vec<usize>> = FxHashMap::default();
        let mut global_row_idx = 0;
        
        for batch in &self.buffered_batches {
            for row_idx in 0..batch.row_count {
                if !batch.selection[row_idx] {
                    global_row_idx += 1;
                    continue;
                }
                
                // Extract partition key values
                let mut partition_key = Vec::new();
                for col_name in partition_by {
                    let col_idx = self.schema.index_of(col_name)
                        .map_err(|_| anyhow::anyhow!("Partition column '{}' not found", col_name))?;
                    let column = batch.batch.columns.get(col_idx)
                        .ok_or_else(|| anyhow::anyhow!("Column index {} out of range", col_idx))?;
                    
                    let value = Self::extract_value(column, row_idx);
                    partition_key.push(value);
                }
                
                partition_map.entry(partition_key).or_insert_with(Vec::new).push(global_row_idx);
                global_row_idx += 1;
            }
        }
        
        // Convert map to vector of groups
        self.partition_groups = partition_map.into_values().collect();
        
        Ok(())
    }
    
    /// Sort rows within partitions based on ORDER BY
    fn sort_partitions(&mut self, order_by: &[OrderByExpr]) -> Result<()> {
        if order_by.is_empty() {
            // No ORDER BY - use natural order
            let total_rows = self.buffered_batches.iter().map(|b| b.row_count).sum();
            self.sorted_indices = (0..total_rows).collect();
            return Ok(());
        }
        
        // Sort rows within each partition
        let mut all_indices = Vec::new();
        
        for partition_group in &self.partition_groups {
            let mut partition_indices = partition_group.clone();
            
            // Sort indices by ORDER BY columns
            partition_indices.sort_by(|&a_idx, &b_idx| {
                for order_col in order_by {
                    let col_idx = match self.schema.index_of(&order_col.column) {
                        Ok(idx) => idx,
                        Err(_) => continue,
                    };
                    
                    // Get batch and row index for global index a_idx
                    let (batch_a, row_a) = self.global_index_to_batch_row(a_idx);
                    let (batch_b, row_b) = self.global_index_to_batch_row(b_idx);
                    
                    if let (Some(batch_a_ref), Some(batch_b_ref)) = 
                        (self.buffered_batches.get(batch_a), self.buffered_batches.get(batch_b)) {
                        
                        let col_a = batch_a_ref.batch.columns.get(col_idx);
                        let col_b = batch_b_ref.batch.columns.get(col_idx);
                        
                        if let (Some(col_a), Some(col_b)) = (col_a, col_b) {
                            let val_a = Self::extract_value(col_a, row_a);
                            let val_b = Self::extract_value(col_b, row_b);
                            
                            let cmp = Self::compare_values(&val_a, &val_b);
                            if cmp != std::cmp::Ordering::Equal {
                                return if order_col.ascending { cmp } else { cmp.reverse() };
                            }
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
            
            all_indices.extend(partition_indices);
        }
        
        self.sorted_indices = all_indices;
        Ok(())
    }
    
    /// Convert global row index to (batch_index, row_index)
    fn global_index_to_batch_row(&self, global_idx: usize) -> (usize, usize) {
        let mut remaining = global_idx;
        for (batch_idx, batch) in self.buffered_batches.iter().enumerate() {
            if remaining < batch.row_count {
                return (batch_idx, remaining);
            }
            remaining -= batch.row_count;
        }
        (self.buffered_batches.len() - 1, 0) // Fallback
    }
    
    /// Extract value from array at index
    fn extract_value(array: &Arc<dyn Array>, idx: usize) -> Value {
        if array.is_null(idx) {
            return Value::Null;
        }
        
        match array.data_type() {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Value::Int64(arr.value(idx))
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Value::Float64(arr.value(idx))
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                Value::String(arr.value(idx).to_string())
            }
            DataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Value::Bool(arr.value(idx))
            }
            _ => Value::String(format!("{:?}", array))
        }
    }
    
    /// Compare two values for sorting
    fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Int64(i1), Value::Int64(i2)) => i1.cmp(i2),
            (Value::Float64(f1), Value::Float64(f2)) => {
                ordered_float::OrderedFloat(*f1).cmp(&ordered_float::OrderedFloat(*f2))
            }
            (Value::String(s1), Value::String(s2)) => s1.cmp(s2),
            (Value::Bool(b1), Value::Bool(b2)) => b1.cmp(b2),
            (Value::Int64(i), Value::Float64(f)) => {
                (*i as f64).partial_cmp(f).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::Float64(f), Value::Int64(i)) => {
                f.partial_cmp(&(*i as f64)).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        }
    }
    
    /// Compute window function for a row
    fn compute_window_function(
        &self,
        win_func: &WindowFunctionExpr,
        global_row_idx: usize,
        partition_start: usize,
        partition_end: usize,
    ) -> Result<Value> {
        match &win_func.function {
            WindowFunction::RowNumber => {
                // Simple ROW_NUMBER: count rows in sorted partition up to current row
                // Find position of global_row_idx in sorted_indices
                if let Some(pos_in_sorted) = self.sorted_indices.iter().position(|&idx| idx == global_row_idx) {
                    // Count rows in sorted_indices from partition_start to current position
                    let mut row_num = 1;
                    for i in 0..pos_in_sorted {
                        let idx = self.sorted_indices[i];
                        if idx >= partition_start && idx <= partition_end {
                            row_num += 1;
                        }
                    }
                    Ok(Value::Int64(row_num as i64))
                } else {
                    // Row not found in sorted_indices - fallback to 1
                    Ok(Value::Int64(1))
                }
            }
            WindowFunction::Rank => {
                // Rank - same rank for ties, gaps in sequence
                // Find the rank by counting distinct values before current row
                if let Some(ref order_by) = win_func.order_by.first() {
                    let col_name = &order_by.column;
                    let col_idx = self.schema.index_of(col_name)
                        .map_err(|_| anyhow::anyhow!("Order column '{}' not found", col_name))?;
                    
                    // Count rows with distinct values before current row
                    // We iterate through all rows before current row and count distinct values
                    let mut rank = 1;
                    let mut seen_values = std::collections::HashSet::new();
                    
                    for idx in partition_start..global_row_idx {
                        let (b_idx, r_idx) = self.global_index_to_batch_row(idx);
                        if let Some(batch) = self.buffered_batches.get(b_idx) {
                            if batch.selection[r_idx] {
                                if let Some(col) = batch.batch.columns.get(col_idx) {
                                    let val = Self::extract_value(col, r_idx);
                                    if !seen_values.contains(&val) {
                                        seen_values.insert(val);
                                        rank += 1;
                                    }
                                }
                            }
                        }
                    }
                    
                    Ok(Value::Int64(rank as i64))
                } else {
                    // No ORDER BY - use row number
                    let row_num = global_row_idx - partition_start + 1;
                    Ok(Value::Int64(row_num as i64))
                }
            }
            WindowFunction::DenseRank => {
                // DenseRank - same rank for ties, no gaps
                // Similar to Rank but without gaps
                if let Some(ref order_by) = win_func.order_by.first() {
                    let col_name = &order_by.column;
                    let col_idx = self.schema.index_of(col_name)
                        .map_err(|_| anyhow::anyhow!("Order column '{}' not found", col_name))?;
                    
                    // Count distinct values up to and including current row
                    // Note: We count all distinct values in the partition up to current row
                    let mut seen_values = std::collections::HashSet::new();
                    
                    for idx in partition_start..=global_row_idx {
                        let (b_idx, r_idx) = self.global_index_to_batch_row(idx);
                        if let Some(batch) = self.buffered_batches.get(b_idx) {
                            if batch.selection[r_idx] {
                                if let Some(col) = batch.batch.columns.get(col_idx) {
                                    let val = Self::extract_value(col, r_idx);
                                    seen_values.insert(val);
                                }
                            }
                        }
                    }
                    
                    Ok(Value::Int64(seen_values.len() as i64))
                } else {
                    // No ORDER BY - use row number
                    let row_num = global_row_idx - partition_start + 1;
                    Ok(Value::Int64(row_num as i64))
                }
            }
            WindowFunction::SumOver => {
                // SUM() OVER - sum of column values in partition up to current row
                if let Some(ref col_name) = win_func.column {
                    let col_idx = self.schema.index_of(col_name)
                        .map_err(|_| anyhow::anyhow!("Column '{}' not found", col_name))?;
                    
                    let mut sum = 0.0;
                    for idx in partition_start..=global_row_idx {
                        let (batch_idx, row_idx) = self.global_index_to_batch_row(idx);
                        if let Some(batch) = self.buffered_batches.get(batch_idx) {
                            if batch.selection[row_idx] {
                                if let Some(col) = batch.batch.columns.get(col_idx) {
                                    let val = Self::extract_value(col, row_idx);
                                    if let Value::Int64(i) = val {
                                        sum += i as f64;
                                    } else if let Value::Float64(f) = val {
                                        sum += f;
                                    }
                                }
                            }
                        }
                    }
                    Ok(Value::Float64(sum))
                } else {
                    Ok(Value::Float64(0.0))
                }
            }
            WindowFunction::AvgOver => {
                // AVG() OVER - average of column values in partition up to current row
                if let Some(ref col_name) = win_func.column {
                    let col_idx = self.schema.index_of(col_name)
                        .map_err(|_| anyhow::anyhow!("Column '{}' not found", col_name))?;
                    
                    let mut sum = 0.0;
                    let mut count = 0;
                    for idx in partition_start..=global_row_idx {
                        let (batch_idx, row_idx) = self.global_index_to_batch_row(idx);
                        if let Some(batch) = self.buffered_batches.get(batch_idx) {
                            if batch.selection[row_idx] {
                                if let Some(col) = batch.batch.columns.get(col_idx) {
                                    let val = Self::extract_value(col, row_idx);
                                    if let Value::Int64(i) = val {
                                        sum += i as f64;
                                        count += 1;
                                    } else if let Value::Float64(f) = val {
                                        sum += f;
                                        count += 1;
                                    }
                                }
                            }
                        }
                    }
                    if count > 0 {
                        Ok(Value::Float64(sum / count as f64))
                    } else {
                        Ok(Value::Float64(0.0))
                    }
                } else {
                    Ok(Value::Float64(0.0))
                }
            }
            _ => {
                // TODO: Implement other window functions
                Ok(Value::Int64(0))
            }
        }
    }
}

impl BatchIterator for WindowOperator {
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // Buffer all input first (window functions need all rows)
        if !self.buffered {
            self.buffer_all_input()?;
            
            // Build partitions if any window function has PARTITION BY
            if let Some(first_win) = self.window_functions.first() {
                // Clone partition_by and order_by to avoid borrowing conflicts
                let partition_by = first_win.partition_by.clone();
                let order_by = first_win.order_by.clone();
                
                if !partition_by.is_empty() {
                    self.build_partitions(&partition_by)?;
                } else {
                    // No PARTITION BY - single partition with all rows
                    let total_rows: usize = self.buffered_batches.iter().map(|b| b.row_count).sum();
                    self.partition_groups = vec![(0..total_rows).collect()];
                }
                
                // Sort partitions if ORDER BY specified
                if !order_by.is_empty() {
                    self.sort_partitions(&order_by)?;
                } else {
                    let total_rows: usize = self.buffered_batches.iter().map(|b| b.row_count).sum();
                    self.sorted_indices = (0..total_rows).collect();
                }
            }
        }
        
        // Return batches one at a time with window function columns added
        if self.current_batch_idx >= self.buffered_batches.len() {
            return Ok(None);
        }
        
        // Clone batch reference to avoid borrowing issues
        let batch = self.buffered_batches[self.current_batch_idx].clone();
        let batch_start_global: usize = self.buffered_batches[..self.current_batch_idx].iter()
            .map(|b| b.row_count).sum();
        
        // Add window function columns
        let mut new_columns = batch.batch.columns.clone();
        let mut new_field_names = self.schema.fields().iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        
        for win_func in &self.window_functions {
            let mut window_values: Vec<Option<Value>> = Vec::new();
            
            for row_idx in 0..batch.row_count {
                if !batch.selection[row_idx] {
                    window_values.push(Some(Value::Null));
                    continue;
                }
                
                let global_row_idx = batch_start_global + row_idx;
                
                // Find which partition this row belongs to
                // Partition groups contain global row indices
                let (partition_start_idx, partition_end_idx) = if !self.partition_groups.is_empty() {
                    // Find partition containing this row by checking which partition group contains global_row_idx
                    let mut found_partition: Option<&Vec<usize>> = None;
                    for partition in &self.partition_groups {
                        if partition.contains(&global_row_idx) {
                            found_partition = Some(partition);
                            break;
                        }
                    }
                    
                    if let Some(partition) = found_partition {
                        // Get the first and last indices in this partition
                        let part_start = *partition.first().unwrap_or(&0);
                        let part_end = *partition.last().unwrap_or(&0);
                        (part_start, part_end)
                    } else {
                        // Row not found in any partition - use first partition as fallback
                        if let Some(first_partition) = self.partition_groups.first() {
                            let part_start = *first_partition.first().unwrap_or(&0);
                            let part_end = *first_partition.last().unwrap_or(&0);
                            (part_start, part_end)
                        } else {
                            let total: usize = self.buffered_batches.iter().map(|b| b.row_count).sum();
                            (0, total.saturating_sub(1))
                        }
                    }
                } else {
                    let total: usize = self.buffered_batches.iter().map(|b| b.row_count).sum();
                    (0, total.saturating_sub(1))
                };
                
                // Compute window function value
                match self.compute_window_function(win_func, global_row_idx, partition_start_idx, partition_end_idx) {
                    Ok(val) => {
                        window_values.push(Some(val));
                    }
                    Err(_) => window_values.push(Some(Value::Null)),
                }
            }
            
            // Create array for window function column based on window function type
            let window_array: Arc<dyn Array> = match win_func.function {
                WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => {
                    // Return Int64 for ranking functions
                    let int_values: Vec<Option<i64>> = window_values.iter().map(|v| {
                        match v {
                            Some(Value::Int64(i)) => Some(*i),
                            Some(Value::Float64(f)) => Some(*f as i64),
                            _ => None,
                        }
                    }).collect();
                    Arc::new(Int64Array::from(int_values))
                }
                WindowFunction::SumOver | WindowFunction::AvgOver => {
                    // Return Float64 for aggregate windows
                    let float_values: Vec<Option<f64>> = window_values.iter().map(|v| {
                        match v {
                            Some(Value::Int64(i)) => Some(*i as f64),
                            Some(Value::Float64(f)) => Some(*f),
                            _ => None,
                        }
                    }).collect();
                    Arc::new(Float64Array::from(float_values))
                }
                _ => {
                    // Default to Int64
                    let int_values: Vec<Option<i64>> = window_values.iter().map(|v| {
                        match v {
                            Some(Value::Int64(i)) => Some(*i),
                            Some(Value::Float64(f)) => Some(*f as i64),
                            _ => None,
                        }
                    }).collect();
                    Arc::new(Int64Array::from(int_values))
                }
            };
            
            new_columns.push(window_array);
            new_field_names.push(win_func.alias.as_ref().unwrap_or(&"window_result".to_string()).clone());
        }
        
        // Create new schema with window function columns
        let mut new_fields = self.schema.fields().to_vec();
        for win_func in &self.window_functions {
            let field_name = win_func.alias.as_ref().unwrap_or(&"window_result".to_string()).clone();
            // Window functions typically return numeric types
            let data_type = match win_func.function {
                WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => DataType::Int64,
                WindowFunction::SumOver | WindowFunction::AvgOver => DataType::Float64,
                _ => DataType::Int64,
            };
            new_fields.push(Arc::new(Field::new(field_name.clone(), data_type, true)));
        }
        let new_schema = Arc::new(Schema::new(new_fields));
        
        // Create new batch with window function columns
        use crate::storage::columnar::ColumnarBatch;
        let new_batch = ColumnarBatch::new(new_columns, new_schema.clone());
        
        let mut result_batch = ExecutionBatch::new(new_batch);
        result_batch.selection = batch.selection.clone(); // Preserve selection vector
        result_batch.row_count = batch.row_count;
        
        self.current_batch_idx += 1;
        Ok(Some(result_batch))
    }
    
    fn schema(&self) -> SchemaRef {
        // Return schema with input columns + window function columns
        let mut fields = self.schema.fields().to_vec();
        
        // Add window function columns
        for win_func in &self.window_functions {
            let field_name = win_func.alias.as_ref().unwrap_or(&"window_result".to_string()).clone();
            let data_type = match win_func.function {
                WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => DataType::Int64,
                WindowFunction::SumOver | WindowFunction::AvgOver => DataType::Float64,
                _ => DataType::Int64,
            };
            fields.push(Arc::new(Field::new(&field_name, data_type, true)));
        }
        
        Arc::new(Schema::new(fields))
    }
}

