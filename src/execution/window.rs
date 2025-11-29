/// Window Functions Execution
/// Implements window functions: ROW_NUMBER, RANK, SUM OVER, etc.
use crate::execution::batch::{ExecutionBatch, BatchIterator};
use crate::query::plan::{WindowFunctionExpr, WindowFunction, OrderByExpr};
use crate::query::column_resolver::ColumnResolver;
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
    /// Table alias mapping: alias -> actual table name (e.g., "e" -> "employees")
    table_aliases: HashMap<String, String>,
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
        // NO schema snapshot - compute dynamically from input when needed
        Self {
            input,
            window_functions,
            table_aliases: HashMap::new(),
            buffered_batches: vec![],
            buffered: false,
            current_batch_idx: 0,
            partition_groups: vec![],
            sorted_indices: vec![],
        }
    }
    
    /// Create WindowOperator with table aliases
    pub fn with_table_aliases(
        input: Box<dyn BatchIterator>,
        window_functions: Vec<WindowFunctionExpr>,
        table_aliases: HashMap<String, String>,
    ) -> Self {
        // NO schema snapshot - compute dynamically from input when needed
        Self {
            input,
            window_functions,
            table_aliases,
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
    
    /// COLID: Helper to resolve column index using ColumnSchema → ColId when available
    /// Falls back to string-based resolution for backward compatibility
    fn resolve_column_index(&self, batch: &ExecutionBatch, col_name: &str) -> Option<usize> {
        // COLID: Use ColumnSchema-based resolution if available
        if let Some(ref column_schema) = batch.column_schema {
            // Resolve column name to ColId, then get physical index
            if let Some(col_id) = column_schema.resolve(col_name) {
                return column_schema.physical_index(col_id);
            }
        }
        
        // Fallback: Use ColumnResolver for string-based resolution (backward compatibility)
        let resolver = ColumnResolver::new(batch.batch.schema.clone(), self.table_aliases.clone());
        resolver.try_resolve(col_name)
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
            // SAFETY: Iterate only up to batch.batch.row_count to avoid array bounds violations
            let max_rows = batch.batch.row_count;
            for row_idx in 0..max_rows {
                // Check if row is selected (and within selection bitmap bounds)
                if row_idx >= batch.selection.len() || !batch.selection[row_idx] {
                    // Skip unselected rows but still count them for global index
                    global_row_idx += 1;
                    continue;
                }
                
                // COLID: Extract partition key values using ColumnSchema → ColId resolution
                let mut partition_key = Vec::new();
                for col_name in partition_by {
                    // COLID: Use ColumnSchema-based resolution if available
                    let col_idx = self.resolve_column_index(batch, col_name)
                        .ok_or_else(|| anyhow::anyhow!("Partition column '{}' not found (available columns: {:?})", 
                            col_name,
                            batch.batch.schema.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>()))?;
                    let column = batch.batch.columns.get(col_idx)
                        .ok_or_else(|| anyhow::anyhow!("Column index {} out of range", col_idx))?;
                    
                    // Ensure row_idx is within column bounds
                    if row_idx < column.len() {
                        let value = Self::extract_value(column, row_idx);
                        partition_key.push(value);
                    }
                }
                
                if !partition_key.is_empty() {
                    partition_map.entry(partition_key).or_insert_with(Vec::new).push(global_row_idx);
                }
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
                    // Get batch and row index for global index a_idx
                    let (batch_a_idx, row_a) = self.global_index_to_batch_row(a_idx);
                    let (batch_b_idx, row_b) = self.global_index_to_batch_row(b_idx);
                    
                    // Use batch schema for column resolution - all batches should have same schema
                    // After buffering, batches should always be available - use buffered batch schema
                    // In sort_partitions closure, we can't use ? operator, so use unwrap_or_else with panic
                    // COLID: Use batch for ColumnSchema-based resolution
                    let batch_a_for_resolution = self.buffered_batches.get(batch_a_idx)
                        .or_else(|| self.buffered_batches.first())
                        .expect("No buffered batches available for schema resolution in sort_partitions");
                    
                    let col_idx = match self.resolve_column_index(batch_a_for_resolution, &order_col.column) {
                        Some(idx) => idx,
                        None => continue,
                    };
                    
                    // Get batches for comparison
                    let batch_a = match self.buffered_batches.get(batch_a_idx) {
                        Some(b) => b,
                        None => return std::cmp::Ordering::Equal,
                    };
                    let batch_b = match self.buffered_batches.get(batch_b_idx) {
                        Some(b) => b,
                        None => return std::cmp::Ordering::Equal,
                    };
                    
                    // Use the batches we already retrieved
                    let col_a = batch_a.batch.columns.get(col_idx);
                    let col_b = batch_b.batch.columns.get(col_idx);
                    
                    if let (Some(col_a), Some(col_b)) = (col_a, col_b) {
                        let val_a = Self::extract_value(col_a, row_a);
                        let val_b = Self::extract_value(col_b, row_b);
                        
                        let cmp = Self::compare_values(&val_a, &val_b);
                        if cmp != std::cmp::Ordering::Equal {
                            return if order_col.ascending { cmp } else { cmp.reverse() };
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
                    // COLID: Use batch for ColumnSchema-based resolution
                    let batch = self.buffered_batches.first()
                        .ok_or_else(|| anyhow::anyhow!("No buffered batches available for schema resolution"))?;
                    let col_idx = self.resolve_column_index(batch, col_name)
                        .ok_or_else(|| anyhow::anyhow!("Order column '{}' not found (available columns: {:?})", 
                            col_name,
                            batch.batch.schema.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>()))?;
                    
                    // Count rows with distinct values before current row
                    // We iterate through all rows before current row and count distinct values
                    let mut rank = 1;
                    let mut seen_values = std::collections::HashSet::new();
                    
                    for idx in partition_start..global_row_idx {
                        let (b_idx, r_idx) = self.global_index_to_batch_row(idx);
                        if let Some(batch) = self.buffered_batches.get(b_idx) {
                            if batch.selection[r_idx] {
                                // COLID: Resolve column index using ColumnSchema if available
                                if let Some(col_idx_batch) = self.resolve_column_index(batch, col_name) {
                                    if let Some(col) = batch.batch.columns.get(col_idx_batch) {
                                        let val = Self::extract_value(col, r_idx);
                                        if !seen_values.contains(&val) {
                                            seen_values.insert(val);
                                            rank += 1;
                                        }
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
                    // COLID: Use batch for ColumnSchema-based resolution
                    let batch = self.buffered_batches.first()
                        .ok_or_else(|| anyhow::anyhow!("No buffered batches available for schema resolution"))?;
                    let col_idx = self.resolve_column_index(batch, col_name)
                        .ok_or_else(|| anyhow::anyhow!("Order column '{}' not found (available columns: {:?})", 
                            col_name,
                            batch.batch.schema.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>()))?;
                    
                    // Count distinct values up to and including current row
                    // Note: We count all distinct values in the partition up to current row
                    let mut seen_values = std::collections::HashSet::new();
                    
                    for idx in partition_start..=global_row_idx {
                        let (b_idx, r_idx) = self.global_index_to_batch_row(idx);
                        if let Some(batch) = self.buffered_batches.get(b_idx) {
                            if batch.selection[r_idx] {
                                // COLID: Resolve column using ColumnSchema if available
                                if let Some(col_idx_batch) = self.resolve_column_index(batch, col_name) {
                                    if let Some(col) = batch.batch.columns.get(col_idx_batch) {
                                        let val = Self::extract_value(col, r_idx);
                                        seen_values.insert(val);
                                    }
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
                    // COLID: Use batch for ColumnSchema-based resolution
                    let batch = self.buffered_batches.first()
                        .ok_or_else(|| anyhow::anyhow!("No buffered batches available for schema resolution"))?;
                    let col_idx = self.resolve_column_index(batch, col_name)
                        .ok_or_else(|| anyhow::anyhow!("Column '{}' not found (available columns: {:?})", 
                            col_name,
                            batch.batch.schema.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>()))?;
                    
                    let mut sum = 0.0;
                    for idx in partition_start..=global_row_idx {
                        let (batch_idx, row_idx) = self.global_index_to_batch_row(idx);
                        if let Some(batch) = self.buffered_batches.get(batch_idx) {
                            if batch.selection[row_idx] {
                                // COLID: Resolve column using ColumnSchema if available
                                if let Some(col_idx_batch) = self.resolve_column_index(batch, col_name) {
                                    if let Some(col) = batch.batch.columns.get(col_idx_batch) {
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
                    }
                    Ok(Value::Float64(sum))
                } else {
                    Ok(Value::Float64(0.0))
                }
            }
            WindowFunction::AvgOver => {
                // AVG() OVER - average of column values in partition up to current row
                if let Some(ref col_name) = win_func.column {
                    // COLID: Use batch for ColumnSchema-based resolution
                    let batch = self.buffered_batches.first()
                        .ok_or_else(|| anyhow::anyhow!("No buffered batches available for schema resolution"))?;
                    let col_idx = self.resolve_column_index(batch, col_name)
                        .ok_or_else(|| anyhow::anyhow!("Column '{}' not found (available columns: {:?})", 
                            col_name,
                            batch.batch.schema.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>()))?;
                    
                    let mut sum = 0.0;
                    let mut count = 0;
                    for idx in partition_start..=global_row_idx {
                        let (batch_idx, row_idx) = self.global_index_to_batch_row(idx);
                        if let Some(batch) = self.buffered_batches.get(batch_idx) {
                            if batch.selection[row_idx] {
                                // COLID: Resolve column using ColumnSchema if available
                                if let Some(col_idx_batch) = self.resolve_column_index(batch, col_name) {
                                    if let Some(col) = batch.batch.columns.get(col_idx_batch) {
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
    fn prepare(&mut self) -> Result<(), anyhow::Error> {
        Ok(()) // WindowOperator doesn't need prepare() yet
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // CRITICAL FIX: If no window functions, just pass through input without modification
        // This prevents WindowOperator from being called when it shouldn't be
        if self.window_functions.is_empty() {
            return self.input.next();
        }
        
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
        // IMPORTANT: Preserve ALL input columns from the batch, not just self.schema
        // Use batch.batch.schema to get the actual columns present in this batch
        let mut new_columns = batch.batch.columns.clone();
        
        // Track existing field names to prevent duplicates
        let existing_field_names: std::collections::HashSet<String> = batch.batch.schema.fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        
        for win_func in &self.window_functions {
            let mut window_values: Vec<Option<Value>> = Vec::new();
            
            // Iterate through ALL rows in the batch (respect selection bitmap)
            // SAFETY: Iterate only up to batch.batch.row_count to avoid array bounds violations
            let max_rows = batch.batch.row_count;
            for row_idx in 0..max_rows {
                // Check if row is selected (and within selection bitmap bounds)
                if row_idx >= batch.selection.len() || !batch.selection[row_idx] {
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
            
            // Check if this window function alias conflicts with existing columns
            // If it does, skip adding both the array and the field to prevent duplicates
            let field_name = win_func.alias.as_ref().unwrap_or(&"window_result".to_string()).clone();
            if existing_field_names.contains(&field_name) {
                eprintln!("WARNING WindowOperator: Window function alias '{}' conflicts with existing column. Skipping window function entirely.", field_name);
                // Skip this window function - don't add array or field
                continue;
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
        }
        
        // Create new schema with window function columns
        // IMPORTANT: Start with batch's actual schema fields, not self.schema
        // This ensures we preserve all input columns, not just what was captured at init time
        // SCHEMA-FLOW: Validate that output schema matches schema() method output
        let mut new_fields = batch.batch.schema.fields().to_vec();
        let mut seen_field_names: std::collections::HashSet<String> = existing_field_names.clone();
        
        for win_func in &self.window_functions {
            let mut field_name = win_func.alias.as_ref().unwrap_or(&"window_result".to_string()).clone();
            
            // CRITICAL FIX: Check if this field name matches an aggregate function alias
            // If it does, this is likely a bug - window functions shouldn't have the same aliases as aggregates
            // Skip adding this window function column if it conflicts with an existing aggregate column
            // This prevents the _1 suffix from being added to aggregate columns
            // NOTE: We already checked this earlier when creating the arrays, so this should never happen
            // But we keep the check here for safety
            if seen_field_names.contains(&field_name) {
                eprintln!("WARNING WindowOperator: Window function alias '{}' conflicts with existing column. Skipping field (array was already skipped).", field_name);
                // Skip this window function field - array was already skipped earlier
                continue;
            }
            seen_field_names.insert(field_name.clone());
            
            // Window functions typically return numeric types
            let data_type = match win_func.function {
                WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => DataType::Int64,
                WindowFunction::SumOver | WindowFunction::AvgOver => DataType::Float64,
                _ => DataType::Int64,
            };
            new_fields.push(Arc::new(Field::new(field_name.clone(), data_type, true)));
        }
        let new_schema = Arc::new(Schema::new(new_fields));
        
        // SCHEMA-FLOW: Validate output column order = input + computed windows
        // Ensure number of columns matches number of fields
        debug_assert_eq!(new_columns.len(), new_schema.fields().len(), 
            "WindowOperator: Column count ({}) != schema field count ({})", 
            new_columns.len(), new_schema.fields().len());
        
        // Ensure input columns are preserved (first N columns should match input schema)
        let input_field_count = batch.batch.schema.fields().len();
        debug_assert_eq!(batch.batch.columns.len(), input_field_count,
            "WindowOperator: Input batch column count ({}) != input schema field count ({})",
            batch.batch.columns.len(), input_field_count);
        
        // Ensure output has input columns + window columns (accounting for skipped ones)
        // Count how many window functions were actually added (not skipped due to conflicts)
        let added_window_count = new_columns.len() - input_field_count;
        let expected_window_count = self.window_functions.len();
        // Only assert if we added more windows than expected (shouldn't happen)
        // If we added fewer, that's expected when some are skipped due to conflicts
        if added_window_count > expected_window_count {
            debug_assert_eq!(new_columns.len(), input_field_count + self.window_functions.len(),
                "WindowOperator: Output column count ({}) > input columns ({}) + window functions ({})",
                new_columns.len(), input_field_count, self.window_functions.len());
        }
        
        // SCHEMA-FLOW DEBUG: Log output schema
        eprintln!("DEBUG WindowOperator::next() - Output schema has {} fields: {:?}", 
            new_schema.fields().len(),
            new_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        
        // Create new batch with window function columns
        use crate::storage::columnar::ColumnarBatch;
        let new_batch = ColumnarBatch::new(new_columns, new_schema.clone());
        
        // COLID: Preserve ColumnSchema from input and add new window function result columns
        let output_column_schema = if let Some(ref input_schema) = batch.column_schema {
            use crate::execution::column_identity::{ColumnSchema, generate_col_id};
            let mut col_schema = input_schema.clone();
            
            // Add new ColIds for window function result columns
            // Window functions are appended after input columns
            let input_col_count = input_schema.column_ids.len();
            for (idx, win_func) in self.window_functions.iter().enumerate() {
                let field_idx = input_col_count + idx;
                if field_idx < new_schema.fields().len() {
                    let field = new_schema.field(field_idx);
                    let col_id = generate_col_id();
                    let alias = if let Some(ref alias) = win_func.alias {
                        alias.clone()
                    } else {
                        field.name().to_string()
                    };
                    col_schema.add_column(col_id, alias.clone(), alias, field.data_type().clone());
                }
            }
            
            Some(col_schema)
        } else {
            None // No input ColumnSchema - backward compatibility
        };
        
        let mut result_batch = if let Some(ref column_schema) = output_column_schema {
            ExecutionBatch::with_column_schema(new_batch, column_schema.clone())
        } else {
            ExecutionBatch::new(new_batch)
        };
        result_batch.selection = batch.selection.clone(); // Preserve selection vector
        result_batch.row_count = batch.row_count;
        
        self.current_batch_idx += 1;
        Ok(Some(result_batch))
    }
    
    fn schema(&self) -> SchemaRef {
        // Return schema with input columns + window function columns
        // SCHEMA-FLOW: Always compute input schema dynamically from input operator (never use stale snapshot)
        // This ensures schema is always current and reflects actual input, not a snapshot from initialization
        
        // Get input schema dynamically (always current)
        let input_schema = self.input.schema();
        
        // SCHEMA-FLOW DEBUG: Log schema propagation
        eprintln!("DEBUG WindowOperator::schema() - Input schema has {} fields: {:?}", 
            input_schema.fields().len(),
            input_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        
        let mut fields = input_schema.fields().to_vec();
        
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
        
        let output_schema = Arc::new(Schema::new(fields));
        
        // SCHEMA-FLOW DEBUG: Log output schema
        eprintln!("DEBUG WindowOperator::schema() - Output schema has {} fields: {:?}", 
            output_schema.fields().len(),
            output_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        
        output_schema
    }
}

