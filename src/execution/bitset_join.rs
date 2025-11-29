/// Bitset join operator: performs multiway joins using bitmap intersection
use crate::storage::bitmap_index::{BitmapIndex, InvertedList};
use crate::storage::bitset::Bitset;
use crate::execution::batch::{ExecutionBatch, BatchIterator};
use crate::storage::fragment::Value;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use anyhow::Result;
use std::collections::HashMap;

/// Bitset join operator for multiway joins
pub struct BitsetJoinOperator {
    /// Left side bitmap index
    pub left_index: BitmapIndex,
    /// Right side bitmap index
    pub right_index: BitmapIndex,
    /// Join key column name on left side
    pub join_key_left: String,
    /// Join key column name on right side
    pub join_key_right: String,
    /// Left table data (column-major: Vec<column> where each column is Vec<Value>)
    pub left_table: Vec<Vec<Value>>,
    /// Right table data (column-major)
    pub right_table: Vec<Vec<Value>>,
    /// Output schema (column names)
    pub output_schema: Vec<String>,
    /// Left table column names
    pub left_columns: Vec<String>,
    /// Right table column names
    pub right_columns: Vec<String>,
    /// Execution state: current value being processed
    current_value: Option<Value>,
    /// Execution state: matching row pairs for current value
    current_matches: Vec<(usize, usize)>,
    /// Execution state: index into current_matches
    match_index: usize,
    /// Whether execution is finished
    finished: bool,
    /// Batch size for output
    batch_size: usize,
}

impl BitsetJoinOperator {
    /// Create a new bitset join operator
    pub fn new(
        left_index: BitmapIndex,
        right_index: BitmapIndex,
        join_key_left: String,
        join_key_right: String,
        left_table: Vec<Vec<Value>>,
        right_table: Vec<Vec<Value>>,
        left_columns: Vec<String>,
        right_columns: Vec<String>,
        output_schema: Vec<String>,
    ) -> Self {
        Self {
            left_index,
            right_index,
            join_key_left,
            join_key_right,
            left_table,
            right_table,
            left_columns,
            right_columns,
            output_schema,
            current_value: None,
            current_matches: Vec::new(),
            match_index: 0,
            finished: false,
            batch_size: 8192, // Process 8K rows at a time
        }
    }

    /// Execute the join and return the first batch
    fn execute_internal(&mut self) -> Result<Option<ExecutionBatch>> {
        if self.finished {
            return Ok(None);
        }

        // Find all matching values
        let intersections = self.left_index.intersect_with(&self.right_index);
        
        if intersections.is_empty() {
            self.finished = true;
            return Ok(None);
        }

        // Collect all matching row pairs
        let mut all_matches = Vec::new();
        
        for (value, intersection_bitset) in intersections {
            // Get bitsets for both sides
            if let (Some(left_bs), Some(right_bs)) = (
                self.left_index.get_bitset(&value),
                self.right_index.get_bitset(&value),
            ) {
                // For each row in left that matches
                for left_row in left_bs.iter_set_bits() {
                    // For each row in right that matches
                    for right_row in right_bs.iter_set_bits() {
                        all_matches.push((left_row, right_row));
                    }
                }
            }
        }

        if all_matches.is_empty() {
            self.finished = true;
            return Ok(None);
        }

        // Build output batch
        let num_cols = self.output_schema.len();
        let mut output_arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(num_cols);
        
        // Initialize arrays based on schema
        for _ in 0..num_cols {
            // We'll build these dynamically based on the data
        }

        // Build columns: left columns first, then right columns
        let left_col_count = self.left_columns.len();
        let right_col_count = self.right_columns.len();
        
        // Collect values for each column
        let mut column_values: Vec<Vec<Value>> = vec![Vec::new(); num_cols];
        
        for (left_row, right_row) in &all_matches {
            // Add left columns
            for col_idx in 0..left_col_count {
                if col_idx < self.left_table.len() && *left_row < self.left_table[col_idx].len() {
                    column_values[col_idx].push(self.left_table[col_idx][*left_row].clone());
                } else {
                    column_values[col_idx].push(Value::Int64(0)); // Default
                }
            }
            
            // Add right columns
            for col_idx in 0..right_col_count {
                let output_col_idx = left_col_count + col_idx;
                if col_idx < self.right_table.len() && *right_row < self.right_table[col_idx].len() {
                    column_values[output_col_idx].push(self.right_table[col_idx][*right_row].clone());
                } else {
                    column_values[output_col_idx].push(Value::Int64(0)); // Default
                }
            }
        }

        // Convert to Arrow arrays
        for col_values in column_values {
            if col_values.is_empty() {
                continue;
            }
            
            // Determine type from first value
            let array: Arc<dyn Array> = match &col_values[0] {
                Value::Int64(_) => {
                    let values: Vec<Option<i64>> = col_values.iter()
                        .map(|v| match v {
                            Value::Int64(x) => Some(*x),
                            _ => None,
                        })
                        .collect();
                    Arc::new(Int64Array::from(values))
                }
                Value::Int32(_) => {
                    let values: Vec<Option<i32>> = col_values.iter()
                        .map(|v| match v {
                            Value::Int32(x) => Some(*x),
                            _ => None,
                        })
                        .collect();
                    Arc::new(Int32Array::from(values))
                }
                Value::Float64(_) => {
                    let values: Vec<Option<f64>> = col_values.iter()
                        .map(|v| match v {
                            Value::Float64(x) => Some(*x),
                            _ => None,
                        })
                        .collect();
                    Arc::new(Float64Array::from(values))
                }
                Value::String(s) => {
                    let values: Vec<Option<String>> = col_values.iter()
                        .map(|v| match v {
                            Value::String(x) => Some(x.clone()),
                            _ => None,
                        })
                        .collect();
                    Arc::new(StringArray::from(values))
                }
                _ => {
                    // Default to string
                    let values: Vec<Option<String>> = col_values.iter()
                        .map(|v| Some(format!("{:?}", v)))
                        .collect();
                    Arc::new(StringArray::from(values))
                }
            };
            output_arrays.push(array);
        }

        // Create schema
        let fields: Vec<Field> = self.output_schema.iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        let schema = Schema::new(fields);

        // Create ColumnarBatch from arrays
        use crate::storage::columnar::ColumnarBatch;
        let columnar_batch = ColumnarBatch {
            columns: output_arrays,
            schema: Arc::new(schema),
            row_count: all_matches.len(),
        };

        let exec_batch = ExecutionBatch {
            batch: columnar_batch,
            row_count: all_matches.len(),
            selection: bitvec::bitvec![1; all_matches.len()], // All rows selected
            column_fragments: std::collections::HashMap::new(),
            column_schema: None,
        };

        self.finished = true;
        Ok(Some(exec_batch))
    }
}

impl BatchIterator for BitsetJoinOperator {
    fn prepare(&mut self) -> Result<(), anyhow::Error> {
        Ok(()) // BitsetJoinOperator doesn't need prepare() yet
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        self.execute_internal()
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        let fields: Vec<Field> = self.output_schema.iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        Arc::new(Schema::new(fields))
    }
}

/// Multiway bitset join: joins 3+ tables using bitset intersection
pub struct MultiwayBitsetJoinOperator {
    /// Bitmap indexes for each table
    pub indexes: Vec<BitmapIndex>,
    /// Join keys for each table (column name)
    pub join_keys: Vec<String>,
    /// Table data (one per table, column-major)
    pub tables: Vec<Vec<Vec<Value>>>,
    /// Column names for each table
    pub table_columns: Vec<Vec<String>>,
    /// Output schema
    pub output_schema: Vec<String>,
    /// Execution state
    finished: bool,
}

impl MultiwayBitsetJoinOperator {
    /// Create a new multiway bitset join operator
    pub fn new(
        indexes: Vec<BitmapIndex>,
        join_keys: Vec<String>,
        tables: Vec<Vec<Vec<Value>>>,
        table_columns: Vec<Vec<String>>,
        output_schema: Vec<String>,
    ) -> Self {
        Self {
            indexes,
            join_keys,
            tables,
            table_columns,
            output_schema,
            finished: false,
        }
    }

    /// Execute multiway join
    pub fn execute(&mut self) -> Result<Option<ExecutionBatch>> {
        if self.finished {
            return Ok(None);
        }

        if self.indexes.len() < 2 {
            self.finished = true;
            return Ok(None);
        }

        // Start with first index
        let mut current_bitset: Option<Bitset> = None;
        let mut matching_values: Vec<Value> = Vec::new();

        // Intersect all indexes
        for (idx, index) in self.indexes.iter().enumerate() {
            if idx == 0 {
                // Initialize with all values from first index
                matching_values = index.distinct_values().iter().map(|v| (*v).clone()).collect();
            } else {
                // Intersect with previous
                let mut new_matching_values = Vec::new();
                for value in &matching_values {
                    if index.get_bitset(value).is_some() {
                        new_matching_values.push(value.clone());
                    }
                }
                matching_values = new_matching_values;
            }
        }

        if matching_values.is_empty() {
            self.finished = true;
            return Ok(None);
        }

        // For each matching value, find all row combinations
        // This is a simplified version - full implementation would be more efficient
        let mut all_row_combinations = Vec::new();
        
        for value in &matching_values {
            // Get bitsets for all tables
            let mut table_bitsets: Vec<&Bitset> = Vec::new();
            for index in &self.indexes {
                if let Some(bs) = index.get_bitset(value) {
                    table_bitsets.push(bs);
                } else {
                    break; // Skip this value if any table doesn't have it
                }
            }
            
            if table_bitsets.len() != self.indexes.len() {
                continue;
            }

            // Generate cartesian product of matching rows
            // This is simplified - in practice, we'd use a more efficient algorithm
            let mut row_sets: Vec<Vec<usize>> = Vec::new();
            for bs in table_bitsets {
                row_sets.push(bs.iter_set_bits().collect());
            }

            // Generate combinations (simplified - only for small sets)
            if row_sets.len() == 2 {
                for &r1 in &row_sets[0] {
                    for &r2 in &row_sets[1] {
                        all_row_combinations.push(vec![r1, r2]);
                    }
                }
            } else if row_sets.len() == 3 {
                for &r1 in &row_sets[0] {
                    for &r2 in &row_sets[1] {
                        for &r3 in &row_sets[2] {
                            all_row_combinations.push(vec![r1, r2, r3]);
                        }
                    }
                }
            }
        }

        // Build output batch (similar to BitsetJoinOperator)
        // ... (implementation similar to above)
        
        self.finished = true;
        Ok(None) // Placeholder - full implementation would build and return batch
    }
}

