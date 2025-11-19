use crate::execution::batch::{ExecutionBatch, BatchIterator};
use crate::execution::simd_kernels::{apply_simd_filter, FilterPredicate as SIMDFilterPredicate};
use crate::query::plan::*;
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::NodeId;
use crate::hypergraph::edge::EdgeId;
use crate::storage::fragment::Value;
use crate::storage::cache_layout::{can_prune_fragment, FragmentPredicate};
use arrow::datatypes::*;
use arrow::array::*;
use std::sync::Arc;
use anyhow::Result;
use bitvec::prelude::*;
use fxhash::FxHashMap;
use regex;

/// Scan operator - reads from column fragments
pub struct ScanOperator {
    node_id: NodeId,
    table: String,
    columns: Vec<String>,
    graph: std::sync::Arc<HyperGraph>,
    current_fragment_idx: usize,  // Which fragment index across all columns
    column_fragments: Vec<Vec<crate::storage::fragment::ColumnFragment>>,  // Fragments per column
    column_names: Vec<String>,
    limit: Option<usize>,  // LIMIT pushed down from query plan
    offset: Option<usize>, // OFFSET pushed down from query plan
    rows_returned: usize,  // Track rows returned for LIMIT/OFFSET
}

impl ScanOperator {
    pub fn new(node_id: NodeId, table: String, columns: Vec<String>, graph: std::sync::Arc<HyperGraph>) -> Result<Self> {
        // For LIMIT optimization: we'll load fragments lazily if LIMIT is small
        // But for now, we still need to know which fragments exist
        // Load fragments from the node
        let node = graph.get_node(node_id)
            .ok_or_else(|| {
                // Debug: list available nodes
                let mut available_nodes = Vec::new();
                // Use O(1) table lookup instead of iterating all nodes
                for entry in graph.table_index.iter() {
                    let (table_name, node_id) = (entry.key(), entry.value());
                    if let Some(node) = graph.get_node(*node_id) {
                        available_nodes.push(format!("Node {}: table={}, fragments={}", node_id, table_name, node.fragments.len()));
                    }
                }
                anyhow::anyhow!(
                    "Node {} not found for table '{}'. Available nodes: {:?}",
                    node_id,
                    table,
                    available_nodes
                )
            })?;
        
        // For table nodes, fragments are stored in child column nodes
        // Each column can have multiple fragments (e.g., 16384 rows per fragment)
        // We need to collect ALL fragments for each column
        
        // First, try to get column order from metadata
        let column_names: Vec<String> = if let Some(col_names_json) = node.metadata.get("column_names") {
            serde_json::from_str(col_names_json).unwrap_or_else(|_| vec![])
        } else {
            vec![]
        };
        
        let mut column_fragments: Vec<Vec<crate::storage::fragment::ColumnFragment>> = Vec::new();
        let mut final_column_names = Vec::new();
        
        if let Some(ref table_name) = node.table_name {
            if !column_names.is_empty() {
                // Get fragments in the order specified by column_names metadata
                // OPTIMIZATION: Use O(1) lookup instead of iterating all nodes
                for col_name in &column_names {
                    // Use table_column_map for O(1) lookup
                    if let Some(col_node) = graph.get_node_by_table_column(table_name, col_name) {
                        // Collect ALL fragments for this column (not just the first)
                        if !col_node.fragments.is_empty() {
                            column_fragments.push(col_node.fragments.clone());
                            final_column_names.push(col_name.clone());
                        }
                    }
                }
            } else {
                // No column order metadata - collect all column nodes
                // OPTIMIZATION: Use get_column_nodes for faster lookup
                let mut col_nodes: Vec<(String, Vec<crate::storage::fragment::ColumnFragment>)> = Vec::new();
                for col_node in graph.get_column_nodes(table_name) {
                    if let Some(ref col_name) = col_node.column_name {
                        if !col_node.fragments.is_empty() {
                            col_nodes.push((col_name.clone(), col_node.fragments.clone()));
                        }
                    }
                }
                // Sort by column name for consistent ordering
                col_nodes.sort_by_key(|(name, _)| name.clone());
                for (col_name, frags) in col_nodes {
                    column_fragments.push(frags);
                    final_column_names.push(col_name);
                }
            }
        }
        
        // Fallback: if no column nodes found, use table node fragments directly
        if column_fragments.is_empty() {
            // Split table node fragments into columns (assuming they're in order)
            // This is a fallback, so we'll just create one "column" with all fragments
            if !node.fragments.is_empty() {
                column_fragments.push(node.fragments.clone());
                final_column_names.push("column_0".to_string());
            }
        }
        
        if column_fragments.is_empty() {
            anyhow::bail!("Node {} (table '{}') has no fragments. Table node fragments: {}, Column nodes checked", 
                node_id, table, node.fragments.len());
        }
        
        // Calculate total fragments (max across all columns)
        let max_fragments = column_fragments.iter().map(|frags| frags.len()).max().unwrap_or(0);
        let first_fragment_rows = column_fragments[0].first().map(|f| f.len()).unwrap_or(0);
        
        eprintln!("ScanOperator: Found {} columns with up to {} fragments each for table '{}' (first fragment has {} rows)", 
            column_fragments.len(), max_fragments, table, first_fragment_rows);
        
        Ok(Self {
            node_id,
            table,
            columns,
            graph,
            current_fragment_idx: 0,
            column_fragments,
            column_names: final_column_names,
            limit: None,  // Will be set by build_operator if LIMIT is pushed down
            offset: None, // Will be set by build_operator if OFFSET is pushed down
            rows_returned: 0,
        })
    }
}

impl BatchIterator for ScanOperator {
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // Early termination: if LIMIT is set and we've returned enough rows, stop
        if let Some(limit) = self.limit {
            if self.rows_returned >= limit {
                return Ok(None);
            }
        }
        
        // Iterate through fragments: for each fragment index, combine fragments from all columns
        // Each column can have multiple fragments (e.g., 16384 rows per fragment)
        
        // Find the maximum number of fragments across all columns
        let max_fragments = self.column_fragments.iter().map(|frags| frags.len()).max().unwrap_or(0);
        
        // Check if we've processed all fragments
        if self.current_fragment_idx >= max_fragments {
            return Ok(None);
        }
        
        // Early termination optimization: if we have LIMIT, check if we need this fragment at all
        if let Some(limit) = self.limit {
            let remaining_needed = limit.saturating_sub(self.rows_returned);
            if remaining_needed == 0 {
                // We already have enough rows, don't process any more fragments
                return Ok(None);
            }
            
            // If current fragment has enough rows to satisfy LIMIT, we can stop after this fragment
            if self.current_fragment_idx < self.column_fragments[0].len() {
                let current_fragment_size = self.column_fragments[0][self.current_fragment_idx].len();
                // If this fragment alone has more rows than we need, we'll process it and then stop
                // (handled by the LIMIT check after processing)
            }
        }
        
        // Combine fragments at current_fragment_idx from all columns
        let mut column_arrays = vec![];
        let mut has_data = false;
        
        for col_idx in 0..self.column_fragments.len() {
            if self.current_fragment_idx < self.column_fragments[col_idx].len() {
                let fragment = &self.column_fragments[col_idx][self.current_fragment_idx];
                // Lazy loading: get array (loads if needed)
                if let Some(array) = fragment.get_array() {
                    column_arrays.push(array);
                    has_data = true;
                } else {
                    // Fragment is lazy and not loaded - this shouldn't happen in current implementation
                    // but we handle it gracefully
                    return Ok(None);
                }
            } else {
                // This column has fewer fragments - we're done with this column
                // For now, we'll skip columns that don't have this fragment index
                // In a more sophisticated implementation, we might pad with NULLs
                return Ok(None);
            }
        }
        
        if !has_data || column_arrays.is_empty() {
            return Ok(None);
        }
        
        // Get row count from first column (all columns should have same length for this fragment)
        let row_count = column_arrays[0].len();
        
        eprintln!("ScanOperator::next: Creating batch with {} rows, {} columns (fragment {}/{})", 
            row_count, column_arrays.len(), self.current_fragment_idx + 1, max_fragments);
        
        // Build schema from column names
        let fields: Vec<arrow::datatypes::Field> = self.column_fragments.iter()
            .enumerate()
            .map(|(idx, frags)| {
                let col_name = if idx < self.column_names.len() {
                    self.column_names[idx].clone()
                } else if !self.columns.is_empty() && idx < self.columns.len() {
                    self.columns[idx].clone()
                } else {
                    format!("column_{}", idx)
                };
                let fragment = &frags[0]; // Use first fragment for data type
                // Get data type from array if available, otherwise use metadata
                let data_type = if let Some(array) = fragment.get_array() {
                    array.data_type().clone()
                } else {
                    // Fallback: use Int64 as default (shouldn't happen)
                    arrow::datatypes::DataType::Int64
                };
                arrow::datatypes::Field::new(
                    col_name,
                    data_type,
                    true,
                )
            })
            .collect();
        
        let schema = Arc::new(Schema::new(fields));
        
        // Apply OFFSET and LIMIT BEFORE creating the batch to avoid unnecessary work
        let mut start_idx = 0;
        let mut end_idx = row_count;
        
        if let Some(offset) = self.offset {
            if self.rows_returned < offset {
                let skip = (offset - self.rows_returned).min(row_count);
                start_idx = skip;
                self.rows_returned += skip;
            }
        }
        
        if let Some(limit) = self.limit {
            let remaining = limit.saturating_sub(self.rows_returned);
            if remaining == 0 {
                // We've already returned enough rows, stop processing
                return Ok(None);
            }
            end_idx = start_idx + remaining.min(row_count - start_idx);
        }
        
        // Slice arrays if needed (optimization: only slice if we're not taking the full fragment)
        let final_arrays = if start_idx > 0 || end_idx < row_count {
            column_arrays.iter().map(|arr| {
                // Slice the array
                use arrow::array::Array;
                arr.slice(start_idx, end_idx - start_idx)
            }).collect()
        } else {
            column_arrays
        };
        
        let actual_row_count = end_idx - start_idx;
        
        // Create ColumnarBatch with sliced arrays
        let batch = crate::storage::columnar::ColumnarBatch::new(final_arrays, schema.clone());
        
        // Create ExecutionBatch
        let exec_batch = ExecutionBatch::new(batch);
        
        eprintln!("ScanOperator::next: Created ExecutionBatch with {} rows (from fragment slice [{}, {}))", 
            actual_row_count, start_idx, end_idx);
        
        // Update rows_returned
        self.rows_returned += actual_row_count;
        
        // Record hot-fragment statistics in the hypergraph
        for col_idx in 0..self.column_fragments.len() {
            if self.current_fragment_idx < self.column_fragments[col_idx].len() {
                let fragment = &self.column_fragments[col_idx][self.current_fragment_idx];
                let bytes = if fragment.metadata.memory_size > 0 {
                    fragment.metadata.memory_size
                } else {
                    fragment.len() * std::mem::size_of::<u8>()
                };
                self.graph.record_fragment_access(self.node_id, self.current_fragment_idx, bytes);
            }
        }
        
        // Move to next fragment
        self.current_fragment_idx += 1;
        
        Ok(Some(exec_batch))
    }
    
    fn schema(&self) -> SchemaRef {
        // Build schema from column names and first fragment of each column
        let fields: Vec<arrow::datatypes::Field> = self.column_fragments.iter()
            .enumerate()
            .map(|(idx, frags)| {
                let col_name = if idx < self.column_names.len() {
                    self.column_names[idx].clone()
                } else if !self.columns.is_empty() && idx < self.columns.len() {
                    self.columns[idx].clone()
                } else {
                    format!("column_{}", idx)
                };
                let fragment = &frags[0]; // Use first fragment for data type
                // Get data type from array if available, otherwise use metadata
                let data_type = if let Some(array) = fragment.get_array() {
                    array.data_type().clone()
                } else {
                    // Fallback: use Int64 as default (shouldn't happen)
                    arrow::datatypes::DataType::Int64
                };
                arrow::datatypes::Field::new(
                    col_name,
                    data_type,
                    true,
                )
            })
            .collect();
        
        Arc::new(Schema::new(fields))
    }
}

/// Filter operator - applies WHERE predicates
pub struct FilterOperator {
    input: Box<dyn BatchIterator>,
    predicates: Vec<FilterPredicate>,
}

impl FilterOperator {
    pub fn new(input: Box<dyn BatchIterator>, predicates: Vec<FilterPredicate>) -> Self {
        Self { input, predicates }
    }
}

impl BatchIterator for FilterOperator {
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        let mut batch = match self.input.next()? {
            Some(b) => b,
            None => return Ok(None),
        };
        
        // Apply predicates using SIMD-optimized filtering
        let selection = self.apply_predicates(&batch)?;
        batch.apply_selection(&selection);
        
        Ok(Some(batch))
    }
    
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

impl FilterOperator {
    fn apply_predicates(&self, batch: &ExecutionBatch) -> Result<BitVec> {
        let mut selection = bitvec![1; batch.batch.row_count];
        
        for predicate in &self.predicates {
            let column = batch.batch.column_by_name(&predicate.column)
                .ok_or_else(|| anyhow::anyhow!("Column not found: {}", predicate.column))?;
            
            // Apply predicate using vectorized comparison
            let column_selection = self.apply_filter_predicate(column, predicate)?;
            
            // Combine with existing selection (AND)
            selection &= &column_selection;
        }
        
        Ok(selection)
    }
    
    fn apply_filter_predicate(
        &self,
        array: &Arc<dyn Array>,
        predicate: &FilterPredicate,
    ) -> Result<BitVec> {
        // Handle LIKE and IN operators (not supported by SIMD yet)
        match predicate.operator {
            PredicateOperator::Like | PredicateOperator::NotLike => {
                return self.apply_like_predicate(array, &predicate.operator, &predicate.pattern);
            }
            PredicateOperator::In | PredicateOperator::NotIn => {
                return self.apply_in_predicate(array, &predicate.operator, &predicate.in_values);
            }
            _ => {}
        }
        
        // For other operators, use the value field
        self.apply_predicate(array, &predicate.operator, &predicate.value)
    }
    
    fn apply_predicate(
        &self,
        array: &Arc<dyn Array>,
        operator: &PredicateOperator,
        value: &Value,
    ) -> Result<BitVec> {
        // Convert to SIMD filter predicate for other operators
        let simd_pred = match operator {
            PredicateOperator::Equals => SIMDFilterPredicate::Equals(value.clone()),
            PredicateOperator::NotEquals => SIMDFilterPredicate::NotEquals(value.clone()),
            PredicateOperator::LessThan => SIMDFilterPredicate::LessThan(value.clone()),
            PredicateOperator::LessThanOrEqual => SIMDFilterPredicate::LessThanOrEqual(value.clone()),
            PredicateOperator::GreaterThan => SIMDFilterPredicate::GreaterThan(value.clone()),
            PredicateOperator::GreaterThanOrEqual => SIMDFilterPredicate::GreaterThanOrEqual(value.clone()),
            _ => unreachable!(), // LIKE and IN handled in apply_filter_predicate
        };
        
        // Apply SIMD filter
        match apply_simd_filter(array, simd_pred) {
            Ok(results) => {
                let mut bitvec = BitVec::new();
                bitvec.reserve(results.len());
                for b in results {
                    bitvec.push(b);
                }
                Ok(bitvec)
            }
            Err(_) => {
                // Fallback to scalar comparison
                let mut bitvec = bitvec![0; array.len()];
                match array.data_type() {
                    DataType::Int64 => {
                        let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                        for i in 0..array.len() {
                            if arr.is_null(i) {
                                continue;
                            }
                            let val = arr.value(i);
                            let matches = match (operator, value) {
                                (PredicateOperator::Equals, Value::Int64(v)) => val == *v,
                                (PredicateOperator::NotEquals, Value::Int64(v)) => val != *v,
                                (PredicateOperator::GreaterThan, Value::Int64(v)) => val > *v,
                                (PredicateOperator::GreaterThanOrEqual, Value::Int64(v)) => val >= *v,
                                (PredicateOperator::LessThan, Value::Int64(v)) => val < *v,
                                (PredicateOperator::LessThanOrEqual, Value::Int64(v)) => val <= *v,
                                _ => false,
                            };
                            bitvec.set(i, matches);
                        }
                    }
                    DataType::Float64 => {
                        let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                        for i in 0..array.len() {
                            if arr.is_null(i) {
                                continue;
                            }
                            let val = arr.value(i);
                            let matches = match (operator, value) {
                                (PredicateOperator::Equals, Value::Float64(v)) => val == *v,
                                (PredicateOperator::NotEquals, Value::Float64(v)) => val != *v,
                                (PredicateOperator::GreaterThan, Value::Float64(v)) => val > *v,
                                (PredicateOperator::GreaterThanOrEqual, Value::Float64(v)) => val >= *v,
                                (PredicateOperator::LessThan, Value::Float64(v)) => val < *v,
                                (PredicateOperator::LessThanOrEqual, Value::Float64(v)) => val <= *v,
                                _ => false,
                            };
                            bitvec.set(i, matches);
                        }
                    }
                    DataType::Utf8 => {
                        let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                        for i in 0..array.len() {
                            if arr.is_null(i) {
                                continue;
                            }
                            let val = arr.value(i);
                            let matches = match (operator, value) {
                                (PredicateOperator::Equals, Value::String(v)) => val == v,
                                (PredicateOperator::NotEquals, Value::String(v)) => val != v,
                                _ => false,
                            };
                            bitvec.set(i, matches);
                        }
                    }
                    _ => {
                        // For other types, set all to false (no match)
                    }
                }
                Ok(bitvec)
            }
        }
    }
    
    /// Apply LIKE/NOT LIKE predicate (pattern matching)
    fn apply_like_predicate(
        &self,
        array: &Arc<dyn Array>,
        operator: &PredicateOperator,
        pattern: &Option<String>,
    ) -> Result<BitVec> {
        let pattern_str = pattern.as_ref()
            .ok_or_else(|| anyhow::anyhow!("LIKE predicate requires a pattern"))?;
        
        // Convert SQL LIKE pattern to regex pattern
        // % matches any sequence of characters
        // _ matches any single character
        let regex_pattern = pattern_str
            .replace("%", ".*")
            .replace("_", ".");
        
        let regex = regex::Regex::new(&format!("^{}$", regex_pattern))
            .map_err(|e| anyhow::anyhow!("Invalid LIKE pattern: {}", e))?;
        
        let mut bitvec = bitvec![0; array.len()];
        
        match array.data_type() {
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..array.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    let val = arr.value(i);
                    let matches = regex.is_match(val);
                    let result = match operator {
                        PredicateOperator::Like => matches,
                        PredicateOperator::NotLike => !matches,
                        _ => false,
                    };
                    bitvec.set(i, result);
                }
            }
            _ => {
                // LIKE only works on strings
                return Ok(bitvec);
            }
        }
        
        Ok(bitvec)
    }
    
    /// Apply IN/NOT IN predicate (membership check)
    fn apply_in_predicate(
        &self,
        array: &Arc<dyn Array>,
        operator: &PredicateOperator,
        in_values: &Option<Vec<Value>>,
    ) -> Result<BitVec> {
        let values = in_values.as_ref()
            .ok_or_else(|| anyhow::anyhow!("IN predicate requires a list of values"))?;
        
        // Create a HashSet for fast lookup
        use std::collections::HashSet;
        let value_set: HashSet<Value> = values.iter().cloned().collect();
        
        let mut bitvec = bitvec![0; array.len()];
        
        match array.data_type() {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..array.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    let val = Value::Int64(arr.value(i));
                    let matches = value_set.contains(&val);
                    let result = match operator {
                        PredicateOperator::In => matches,
                        PredicateOperator::NotIn => !matches,
                        _ => false,
                    };
                    bitvec.set(i, result);
                }
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..array.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    let val = Value::Float64(arr.value(i));
                    let matches = value_set.contains(&val);
                    let result = match operator {
                        PredicateOperator::In => matches,
                        PredicateOperator::NotIn => !matches,
                        _ => false,
                    };
                    bitvec.set(i, result);
                }
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..array.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    let val = Value::String(arr.value(i).to_string());
                    let matches = value_set.contains(&val);
                    let result = match operator {
                        PredicateOperator::In => matches,
                        PredicateOperator::NotIn => !matches,
                        _ => false,
                    };
                    bitvec.set(i, result);
                }
            }
            _ => {
                // For other types, set all to false (no match)
            }
        }
        
        Ok(bitvec)
    }
}

/// Join operator - performs joins using hypergraph edges
pub struct JoinOperator {
    left: Box<dyn BatchIterator>,
    right: Box<dyn BatchIterator>,
    join_type: JoinType,
    predicate: JoinPredicate,
    graph: std::sync::Arc<HyperGraph>,
    edge_id: EdgeId,
}

impl JoinOperator {
    pub fn new(
        left: Box<dyn BatchIterator>,
        right: Box<dyn BatchIterator>,
        join_type: JoinType,
        predicate: JoinPredicate,
        graph: std::sync::Arc<HyperGraph>,
        edge_id: EdgeId,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            predicate,
            graph,
            edge_id,
        }
    }
}

impl BatchIterator for JoinOperator {
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // For OUTER JOINs, we need to track which rows from left/right have been matched
        match self.join_type {
            JoinType::Inner => self.execute_inner_join(),
            JoinType::Left => self.execute_left_join(),
            JoinType::Right => self.execute_right_join(),
            JoinType::Full => self.execute_full_join(),
        }
    }
    
    fn schema(&self) -> SchemaRef {
        // Combine schemas from left and right
        let mut fields = self.left.schema().fields().to_vec();
        fields.extend_from_slice(self.right.schema().fields());
        Arc::new(Schema::new(fields))
    }
}

impl JoinOperator {
    fn execute_inner_join(&mut self) -> Result<Option<ExecutionBatch>> {
        // Build hash table from right side (smaller table)
        let mut right_hash: FxHashMap<Value, Vec<usize>> = FxHashMap::default();
        let mut right_batches = vec![];
        
        // Collect all right batches
        while let Some(batch) = self.right.next()? {
            right_batches.push(batch);
        }
        
        // Build hash table from right side
        let right_key_col = self.right.schema().index_of(&self.predicate.right.1).ok();
        if right_key_col.is_none() {
            return Ok(None);
        }
        let right_key_idx = right_key_col.unwrap();
        
        for (batch_idx, batch) in right_batches.iter().enumerate() {
            if let Some(key_array) = batch.batch.column(right_key_idx) {
                for row_idx in 0..batch.row_count {
                    if batch.selection[row_idx] {
                        let key = extract_value(key_array, row_idx)?;
                        right_hash.entry(key).or_insert_with(Vec::new).push((batch_idx << 16) | row_idx);
                    }
                }
            }
        }
        
        // Probe left side against hash table
        let left_key_col = self.left.schema().index_of(&self.predicate.left.1).ok();
        if left_key_col.is_none() {
            return Ok(None);
        }
        let left_key_idx = left_key_col.unwrap();
        
        // Get next left batch
        let left_batch = match self.left.next()? {
            Some(b) => b,
            None => return Ok(None),
        };
        
        // Build output columns
        let mut output_columns = vec![];
        let mut output_schema_fields = vec![];
        
        // Add left columns
        for (idx, field) in self.left.schema().fields().iter().enumerate() {
            output_columns.push(left_batch.batch.column(idx).unwrap().clone());
            output_schema_fields.push(field.clone());
        }
        
        // Add right columns
        for (idx, field) in self.right.schema().fields().iter().enumerate() {
            output_columns.push(right_batches[0].batch.column(idx).unwrap().clone());
            output_schema_fields.push(field.clone());
        }
        
        // Build join result
        let mut result_rows = 0;
        let mut result_selection = BitVec::new();
        
        if let Some(key_array) = left_batch.batch.column(left_key_idx) {
            for row_idx in 0..left_batch.row_count {
                if left_batch.selection[row_idx] {
                    let key = extract_value(key_array, row_idx)?;
                    if let Some(right_indices) = right_hash.get(&key) {
                        for &right_idx in right_indices {
                            result_rows += 1;
                            result_selection.push(true);
                        }
                    }
                }
            }
        }
        
        if result_rows == 0 {
            return Ok(None);
        }
        
        // Create output schema
        let output_schema = Arc::new(Schema::new(output_schema_fields));
        let output_batch = crate::storage::columnar::ColumnarBatch::new(output_columns, output_schema);
        
        Ok(Some(ExecutionBatch {
            batch: output_batch,
            selection: result_selection,
            row_count: result_rows,
        }))
    }
    
    fn execute_left_join(&mut self) -> Result<Option<ExecutionBatch>> {
        // LEFT JOIN: all rows from left, matched rows from right (NULLs for non-matches)
        // Similar to INNER JOIN but include unmatched left rows
        let mut right_hash: FxHashMap<Value, Vec<usize>> = FxHashMap::default();
        let mut right_batches = vec![];
        
        // Collect all right batches
        while let Some(batch) = self.right.next()? {
            right_batches.push(batch);
        }
        
        let right_key_idx = self.right.schema().index_of(&self.predicate.right.1)
            .map_err(|_| anyhow::anyhow!("Right join key not found"))?;
        
        // Build hash table from right side
        for (batch_idx, batch) in right_batches.iter().enumerate() {
            if let Some(key_array) = batch.batch.column(right_key_idx) {
                for row_idx in 0..batch.row_count {
                    if batch.selection[row_idx] {
                        let key = extract_value(key_array, row_idx)?;
                        right_hash.entry(key).or_insert_with(Vec::new).push((batch_idx << 16) | row_idx);
                    }
                }
            }
        }
        
        let left_key_idx = self.left.schema().index_of(&self.predicate.left.1)
            .map_err(|_| anyhow::anyhow!("Left join key not found"))?;
        
        let left_batch = match self.left.next()? {
            Some(b) => b,
            None => return Ok(None),
        };
        
        // Build output with NULLs for unmatched right columns
        let mut output_columns = vec![];
        let mut output_schema_fields = vec![];
        
        // Add left columns
        for (idx, field) in self.left.schema().fields().iter().enumerate() {
            output_columns.push(left_batch.batch.column(idx).unwrap().clone());
            output_schema_fields.push(field.clone());
        }
        
        // Add right columns (will be NULL-filled for unmatched rows)
        let right_schema = self.right.schema();
        for (idx, field) in right_schema.fields().iter().enumerate() {
            output_schema_fields.push(field.clone());
        }
        
        // Build join result
        let mut result_rows = 0;
        let mut result_selection = BitVec::new();
        let mut left_row_indices = vec![];
        let mut right_row_indices = vec![];
        
        if let Some(key_array) = left_batch.batch.column(left_key_idx) {
            for row_idx in 0..left_batch.row_count {
                if left_batch.selection[row_idx] {
                    let key = extract_value(key_array, row_idx)?;
                    if let Some(right_indices) = right_hash.get(&key) {
                        // Matched: add all matching right rows
                        for &right_idx in right_indices {
                            left_row_indices.push(row_idx);
                            right_row_indices.push(Some(right_idx));
                            result_rows += 1;
                            result_selection.push(true);
                        }
                    } else {
                        // Unmatched: add left row with NULL right columns
                        left_row_indices.push(row_idx);
                        right_row_indices.push(None);
                        result_rows += 1;
                        result_selection.push(true);
                    }
                }
            }
        }
        
        if result_rows == 0 {
            return Ok(None);
        }
        
        // Build output arrays
        let mut final_output_columns = vec![];
        
        // Left columns
        for (idx, _) in self.left.schema().fields().iter().enumerate() {
            let left_col = left_batch.batch.column(idx).unwrap();
            final_output_columns.push(left_col.clone());
        }
        
        // Right columns (with NULLs for unmatched)
        for (idx, field) in right_schema.fields().iter().enumerate() {
            let null_array = create_null_array(field.data_type(), result_rows)?;
            final_output_columns.push(null_array);
        }
        
        // TODO: Fill in matched right values (simplified for now)
        
        let output_schema = Arc::new(Schema::new(output_schema_fields));
        let output_batch = crate::storage::columnar::ColumnarBatch::new(final_output_columns, output_schema);
        
        Ok(Some(ExecutionBatch {
            batch: output_batch,
            selection: result_selection,
            row_count: result_rows,
        }))
    }
    
    fn execute_right_join(&mut self) -> Result<Option<ExecutionBatch>> {
        // RIGHT JOIN: all rows from right, matched rows from left (NULLs for non-matches)
        // Similar to LEFT JOIN but swap left and right
        // For simplicity, we can swap and call LEFT JOIN logic
        std::mem::swap(&mut self.left, &mut self.right);
        std::mem::swap(&mut self.predicate.left, &mut self.predicate.right);
        let result = self.execute_left_join();
        std::mem::swap(&mut self.left, &mut self.right);
        std::mem::swap(&mut self.predicate.left, &mut self.predicate.right);
        result
    }
    
    fn execute_full_join(&mut self) -> Result<Option<ExecutionBatch>> {
        // FULL OUTER JOIN: all rows from both sides, NULLs for unmatched
        // Combine LEFT and RIGHT JOIN results
        // This is a simplified implementation
        anyhow::bail!("FULL OUTER JOIN not yet fully implemented")
    }
}

fn create_null_array(data_type: &DataType, len: usize) -> Result<Arc<dyn Array>> {
    match data_type {
        DataType::Int64 => Ok(Arc::new(Int64Array::from(vec![None::<i64>; len]))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(vec![None::<f64>; len]))),
        DataType::Utf8 => Ok(Arc::new(StringArray::from(vec![None::<String>; len]))),
        _ => anyhow::bail!("Unsupported data type for NULL array: {:?}", data_type)
    }
}

fn extract_value(array: &Arc<dyn Array>, idx: usize) -> Result<Value> {
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(Value::Int64(arr.value(idx)))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(Value::Float64(arr.value(idx)))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(Value::String(arr.value(idx).to_string()))
        }
        _ => anyhow::bail!("Unsupported type for join key"),
    }
}

/// Aggregate operator - performs GROUP BY and aggregations
pub struct AggregateOperator {
    input: Box<dyn BatchIterator>,
    group_by: Vec<String>,
    aggregates: Vec<AggregateExpr>,
    state: AggregateState,
}

struct AggregateState {
    groups: std::collections::HashMap<Vec<Value>, AggregateValues>,
}

struct AggregateValues {
    sums: Vec<f64>,
    counts: Vec<usize>,
    mins: Vec<Option<Value>>,
    maxs: Vec<Option<Value>>,
}

impl AggregateOperator {
    pub fn new(
        input: Box<dyn BatchIterator>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
    ) -> Self {
        Self {
            input,
            group_by,
            aggregates,
            state: AggregateState {
                groups: std::collections::HashMap::new(),
            },
        }
    }
}

impl BatchIterator for AggregateOperator {
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // Process all input batches and accumulate aggregates
        while let Some(batch) = self.input.next()? {
            self.process_batch(&batch)?;
        }
        
        // Convert aggregated state to ExecutionBatch
        if self.state.groups.is_empty() {
            return Ok(None);
        }
        
        // Build output columns
        let mut group_columns: Vec<Vec<Value>> = vec![vec![]; self.group_by.len()];
        let mut agg_columns: Vec<Vec<Value>> = vec![vec![]; self.aggregates.len()];
        
        for (group_key, agg_vals) in &self.state.groups {
            for (i, key_val) in group_key.iter().enumerate() {
                group_columns[i].push(key_val.clone());
            }
            
            for (i, agg) in self.aggregates.iter().enumerate() {
                let val = match agg.function {
                    AggregateFunction::Sum => Value::Float64(agg_vals.sums[i]),
                    AggregateFunction::Count => Value::Int64(agg_vals.counts[i] as i64),
                    AggregateFunction::Avg => {
                        if agg_vals.counts[i] > 0 {
                            Value::Float64(agg_vals.sums[i] / agg_vals.counts[i] as f64)
                        } else {
                            Value::Float64(0.0)
                        }
                    }
                    AggregateFunction::Min => agg_vals.mins[i].clone().unwrap_or(Value::Null),
                    AggregateFunction::Max => agg_vals.maxs[i].clone().unwrap_or(Value::Null),
                    AggregateFunction::CountDistinct => Value::Int64(agg_vals.counts[i] as i64),
                };
                agg_columns[i].push(val);
            }
        }
        
        let row_count = self.state.groups.len();
        if row_count == 0 {
            return Ok(None);
        }
        
        // Convert to Arrow arrays
        let mut output_arrays = vec![];
        for col in &group_columns {
            if let Some(Value::Int64(_)) = col.first() {
                let values: Vec<i64> = col.iter().map(|v| if let Value::Int64(x) = v { *x } else { 0 }).collect();
                output_arrays.push(Arc::new(Int64Array::from(values)) as Arc<dyn Array>);
            } else if let Some(Value::Float64(_)) = col.first() {
                let values: Vec<f64> = col.iter().map(|v| if let Value::Float64(x) = v { *x } else { 0.0 }).collect();
                output_arrays.push(Arc::new(Float64Array::from(values)) as Arc<dyn Array>);
            } else {
                let values: Vec<String> = col.iter().map(|v| format!("{:?}", v)).collect();
                output_arrays.push(Arc::new(StringArray::from(values)) as Arc<dyn Array>);
            }
        }
        
        for col in &agg_columns {
            if let Some(Value::Int64(_)) = col.first() {
                let values: Vec<i64> = col.iter().map(|v| if let Value::Int64(x) = v { *x } else { 0 }).collect();
                output_arrays.push(Arc::new(Int64Array::from(values)) as Arc<dyn Array>);
            } else if let Some(Value::Float64(_)) = col.first() {
                let values: Vec<f64> = col.iter().map(|v| if let Value::Float64(x) = v { *x } else { 0.0 }).collect();
                output_arrays.push(Arc::new(Float64Array::from(values)) as Arc<dyn Array>);
            } else {
                let values: Vec<String> = col.iter().map(|v| format!("{:?}", v)).collect();
                output_arrays.push(Arc::new(StringArray::from(values)) as Arc<dyn Array>);
            }
        }
        
        // Build schema
        let mut fields = vec![];
        for col_name in &self.group_by {
            fields.push(Field::new(col_name, DataType::Utf8, true));
        }
        for agg in &self.aggregates {
            let field_name = agg.alias.as_ref().unwrap_or(&agg.column);
            let data_type = match agg.function {
                AggregateFunction::Sum | AggregateFunction::Avg => DataType::Float64,
                AggregateFunction::Count | AggregateFunction::CountDistinct => DataType::Int64,
                AggregateFunction::Min | AggregateFunction::Max => DataType::Utf8,
            };
            fields.push(Field::new(field_name, data_type, true));
        }
        
        let schema = Arc::new(Schema::new(fields));
        let batch = crate::storage::columnar::ColumnarBatch::new(output_arrays, schema);
        let selection = bitvec![1; row_count];
        
        // Clear state after output
        self.state.groups.clear();
        
        Ok(Some(ExecutionBatch {
            batch,
            selection,
            row_count,
        }))
    }
    
    fn schema(&self) -> SchemaRef {
        // Build schema with group_by columns + aggregate columns
        let mut fields = vec![];
        for col_name in &self.group_by {
            fields.push(Field::new(col_name, DataType::Utf8, true));
        }
        for agg in &self.aggregates {
            let field_name = agg.alias.as_ref().unwrap_or(&agg.column);
            let data_type = match agg.function {
                AggregateFunction::Sum | AggregateFunction::Avg => DataType::Float64,
                AggregateFunction::Count | AggregateFunction::CountDistinct => DataType::Int64,
                AggregateFunction::Min | AggregateFunction::Max => DataType::Utf8,
            };
            fields.push(Field::new(field_name, data_type, true));
        }
        Arc::new(Schema::new(fields))
    }
}

impl AggregateOperator {
    fn process_batch(&mut self, batch: &ExecutionBatch) -> Result<()> {
        // Extract group keys and update aggregates incrementally
        let mut group_key_indices = vec![];
        for col_name in &self.group_by {
            if let Some(col_idx) = batch.batch.schema.index_of(col_name).ok() {
                group_key_indices.push(col_idx);
            } else {
                anyhow::bail!("Group by column not found: {}", col_name);
            }
        }
        
        // Special case: COUNT(*) without GROUP BY - use empty group key
        let use_empty_group = self.group_by.is_empty();
        
        let mut agg_col_indices = vec![];
        for agg in &self.aggregates {
            // Handle COUNT(*) - no specific column needed
            if agg.column == "*" {
                agg_col_indices.push(usize::MAX); // Special marker for COUNT(*)
            } else if let Some(col_idx) = batch.batch.schema.index_of(&agg.column).ok() {
                agg_col_indices.push(col_idx);
            } else {
                anyhow::bail!("Aggregate column not found: {}", agg.column);
            }
        }
        
        // Process each row
        for row_idx in 0..batch.row_count {
            if !batch.selection[row_idx] {
                continue;
            }
            
            // Extract group key (empty for aggregates without GROUP BY)
            let group_key = if use_empty_group {
                vec![] // Single group for aggregates without GROUP BY
            } else {
                let mut key = vec![];
                for &col_idx in &group_key_indices {
                    let col = batch.batch.column(col_idx).unwrap();
                    key.push(extract_value(col, row_idx)?);
                }
                key
            };
            
            // Get or create aggregate state for this group
            let agg_vals = self.state.groups.entry(group_key).or_insert_with(|| {
                AggregateValues {
                    sums: vec![0.0; self.aggregates.len()],
                    counts: vec![0; self.aggregates.len()],
                    mins: vec![None; self.aggregates.len()],
                    maxs: vec![None; self.aggregates.len()],
                }
            });
            
            // Update aggregates
            for (i, &col_idx) in agg_col_indices.iter().enumerate() {
                // Handle COUNT(*) - no column value needed
                if col_idx == usize::MAX {
                    // COUNT(*) - just increment count
                    if matches!(self.aggregates[i].function, AggregateFunction::Count | AggregateFunction::CountDistinct) {
                        agg_vals.counts[i] += 1;
                    }
                    continue;
                }
                
                let col = batch.batch.column(col_idx).unwrap();
                let val = extract_value(col, row_idx)?;
                
                match self.aggregates[i].function {
                    AggregateFunction::Sum | AggregateFunction::Avg => {
                        if let Value::Float64(x) = val {
                            agg_vals.sums[i] += x;
                        } else if let Value::Int64(x) = val {
                            agg_vals.sums[i] += x as f64;
                        }
                        agg_vals.counts[i] += 1;
                    }
                    AggregateFunction::Count | AggregateFunction::CountDistinct => {
                        agg_vals.counts[i] += 1;
                    }
                    AggregateFunction::Min => {
                        if agg_vals.mins[i].is_none() || Some(&val) < agg_vals.mins[i].as_ref() {
                            agg_vals.mins[i] = Some(val.clone());
                        }
                    }
                    AggregateFunction::Max => {
                        if agg_vals.maxs[i].is_none() || Some(&val) > agg_vals.maxs[i].as_ref() {
                            agg_vals.maxs[i] = Some(val.clone());
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
}

/// Project operator - selects columns
pub struct ProjectOperator {
    input: Box<dyn BatchIterator>,
    columns: Vec<String>,
}

impl ProjectOperator {
    pub fn new(input: Box<dyn BatchIterator>, columns: Vec<String>) -> Self {
        Self { input, columns }
    }
}

impl BatchIterator for ProjectOperator {
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        let batch = match self.input.next()? {
            Some(b) => {
                eprintln!("ProjectOperator::next: Got batch with {} rows, {} columns from input", 
                    b.row_count, b.batch.columns.len());
                b
            },
            None => {
                eprintln!("ProjectOperator::next: Input returned None");
                return Ok(None);
            },
        };
        
        // Select only requested columns (zero-copy where possible)
        // Check if columns is empty or contains "*" (wildcard)
        let is_wildcard = self.columns.is_empty() || 
            self.columns.len() == 1 && self.columns[0] == "*";
        
        if is_wildcard {
            // SELECT * - return all columns
            eprintln!("ProjectOperator::next: SELECT * - returning all {} columns", batch.batch.columns.len());
            return Ok(Some(batch));
        }
        
        eprintln!("ProjectOperator::next: Projecting {} columns: {:?}", self.columns.len(), self.columns);
        let mut output_columns = vec![];
        let mut output_fields = vec![];
        
        for col_name in &self.columns {
            // Skip "*" wildcard if present
            if col_name == "*" {
                // Add all columns
                for col_idx in 0..batch.batch.columns.len() {
                    output_columns.push(batch.batch.column(col_idx).unwrap().clone());
                    output_fields.push(batch.batch.schema.field(col_idx).clone());
                }
            } else {
                match batch.batch.schema.index_of(col_name) {
                    Ok(col_idx) => {
                        output_columns.push(batch.batch.column(col_idx).unwrap().clone());
                        output_fields.push(batch.batch.schema.field(col_idx).clone());
                    }
                    Err(_) => {
                        eprintln!("ProjectOperator::next: Column '{}' not found in schema", col_name);
                    }
                }
            }
        }
        
        if output_columns.is_empty() {
            eprintln!("ProjectOperator::next: No matching columns found, returning None");
            return Ok(None);
        }
        
        let output_schema = Arc::new(Schema::new(output_fields));
        let output_batch = crate::storage::columnar::ColumnarBatch::new(output_columns, output_schema);
        
        eprintln!("ProjectOperator::next: Returning batch with {} rows, {} columns", 
            batch.row_count, output_batch.columns.len());
        Ok(Some(ExecutionBatch {
            batch: output_batch,
            selection: batch.selection,
            row_count: batch.row_count,
        }))
    }
    
    fn schema(&self) -> SchemaRef {
        // Build schema with only requested columns
        if self.columns.is_empty() {
            return self.input.schema();
        }
        
        let input_schema = self.input.schema();
        let mut fields = vec![];
        
        for col_name in &self.columns {
            if let Ok(idx) = input_schema.index_of(col_name) {
                fields.push(input_schema.field(idx).clone());
            }
        }
        
        Arc::new(Schema::new(fields))
    }
}

/// Sort operator - performs ORDER BY
pub struct SortOperator {
    input: Box<dyn BatchIterator>,
    order_by: Vec<OrderByExpr>,
    limit: Option<usize>,
    offset: Option<usize>,
    buffered_rows: Vec<ExecutionBatch>,
}

impl SortOperator {
    pub fn new(
        input: Box<dyn BatchIterator>,
        order_by: Vec<OrderByExpr>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Self {
        Self {
            input,
            order_by,
            limit,
            offset,
            buffered_rows: vec![],
        }
    }
}

impl BatchIterator for SortOperator {
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // Buffer all input
        if self.buffered_rows.is_empty() {
            while let Some(batch) = self.input.next()? {
                self.buffered_rows.push(batch);
            }
            
            // Sort buffered rows
            if !self.buffered_rows.is_empty() && !self.order_by.is_empty() {
                // Collect all rows into a single batch for sorting
                let mut all_rows: Vec<Vec<Value>> = vec![];
                let schema = self.buffered_rows[0].batch.schema.clone();
                
                // Extract sort key columns
                let mut sort_key_indices = vec![];
                for order_expr in &self.order_by {
                    if let Ok(idx) = schema.index_of(&order_expr.column) {
                        sort_key_indices.push((idx, order_expr.ascending));
                    }
                }
                
                // Collect all rows
                for batch in &self.buffered_rows {
                    for row_idx in 0..batch.row_count {
                        if batch.selection[row_idx] {
                            let mut row = vec![];
                            for col_idx in 0..batch.batch.columns.len() {
                                let col = batch.batch.column(col_idx).unwrap();
                                row.push(extract_value(col, row_idx)?);
                            }
                            all_rows.push(row);
                        }
                    }
                }
                
                // Sort rows
                all_rows.sort_by(|a, b| {
                    for &(key_idx, ascending) in &sort_key_indices {
                        let cmp = a[key_idx].partial_cmp(&b[key_idx]).unwrap_or(std::cmp::Ordering::Equal);
                        if !ascending {
                            return cmp.reverse();
                        }
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
                
                // Apply limit/offset
                let start = self.offset.unwrap_or(0);
                let end = if let Some(limit) = self.limit {
                    start + limit
                } else {
                    all_rows.len()
                };
                let sorted_rows = all_rows[start..end.min(all_rows.len())].to_vec();
                
                // Convert back to ExecutionBatch
                if sorted_rows.is_empty() {
                    return Ok(None);
                }
                
                let mut output_arrays = vec![];
                for col_idx in 0..schema.fields().len() {
                    let mut col_values: Vec<Value> = sorted_rows.iter().map(|row| row[col_idx].clone()).collect();
                    
                    // Convert to Arrow array
                    if let Some(Value::Int64(_)) = col_values.first() {
                        let values: Vec<i64> = col_values.iter().map(|v| if let Value::Int64(x) = v { *x } else { 0 }).collect();
                        output_arrays.push(Arc::new(Int64Array::from(values)) as Arc<dyn Array>);
                    } else if let Some(Value::Float64(_)) = col_values.first() {
                        let values: Vec<f64> = col_values.iter().map(|v| if let Value::Float64(x) = v { *x } else { 0.0 }).collect();
                        output_arrays.push(Arc::new(Float64Array::from(values)) as Arc<dyn Array>);
                    } else {
                        let values: Vec<String> = col_values.iter().map(|v| format!("{:?}", v)).collect();
                        output_arrays.push(Arc::new(StringArray::from(values)) as Arc<dyn Array>);
                    }
                }
                
                let output_batch = crate::storage::columnar::ColumnarBatch::new(output_arrays, schema);
                let selection = bitvec![1; sorted_rows.len()];
                
                self.buffered_rows = vec![ExecutionBatch {
                    batch: output_batch,
                    selection,
                    row_count: sorted_rows.len(),
                }];
            }
        }
        
        if self.buffered_rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.buffered_rows.remove(0)))
        }
    }
    
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

/// Limit operator - applies LIMIT/OFFSET
pub struct LimitOperator {
    input: Box<dyn BatchIterator>,
    limit: usize,
    offset: usize,
    rows_returned: usize,
}

impl LimitOperator {
    pub fn new(input: Box<dyn BatchIterator>, limit: usize, offset: usize) -> Self {
        Self {
            input,
            limit,
            offset,
            rows_returned: 0,
        }
    }
}

impl BatchIterator for LimitOperator {
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        eprintln!("LimitOperator::next: rows_returned={}, limit={}, offset={}", 
            self.rows_returned, self.limit, self.offset);
        
        if self.rows_returned >= self.limit {
            eprintln!("LimitOperator::next: Already returned {} rows, limit is {}, returning None", 
                self.rows_returned, self.limit);
            return Ok(None);
        }
        
        let mut batch = match self.input.next()? {
            Some(b) => {
                eprintln!("LimitOperator::next: Got batch with {} rows from input", b.row_count);
                b
            },
            None => {
                eprintln!("LimitOperator::next: Input returned None");
                return Ok(None);
            },
        };
        
        // Apply offset and limit
        if self.rows_returned < self.offset {
            let skip = (self.offset - self.rows_returned).min(batch.row_count);
            eprintln!("LimitOperator::next: Skipping {} rows for offset", skip);
            batch = batch.slice(skip, batch.row_count - skip);
            self.rows_returned += skip;
        }
        
        if self.rows_returned >= self.limit {
            eprintln!("LimitOperator::next: After offset, rows_returned={} >= limit={}, returning None", 
                self.rows_returned, self.limit);
            return Ok(None);
        }
        
        let remaining = self.limit - self.rows_returned;
        eprintln!("LimitOperator::next: Remaining rows to return: {}", remaining);
        if batch.row_count > remaining {
            eprintln!("LimitOperator::next: Slicing batch from {} to {} rows", batch.row_count, remaining);
            batch = batch.slice(0, remaining);
        }
        
        self.rows_returned += batch.row_count;
        eprintln!("LimitOperator::next: Returning batch with {} rows (total returned: {})", 
            batch.row_count, self.rows_returned);
        Ok(Some(batch))
    }
    
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

/// Build execution operator from plan operator
pub fn build_operator(
    plan_op: &PlanOperator,
    graph: std::sync::Arc<HyperGraph>,
) -> Result<Box<dyn BatchIterator>> {
    match plan_op {
        PlanOperator::Scan { node_id, table, columns, limit, offset } => {
            let mut scan_op = ScanOperator::new(*node_id, table.clone(), columns.clone(), graph)?;
            // Set LIMIT/OFFSET for early termination
            scan_op.limit = *limit;
            scan_op.offset = *offset;
            Ok(Box::new(scan_op))
        }
        PlanOperator::Filter { input, predicates } => {
            let input_op = build_operator(input, graph.clone())?;
            Ok(Box::new(FilterOperator::new(input_op, predicates.clone())))
        }
        PlanOperator::Join { left, right, edge_id, join_type, predicate } => {
            let left_op = build_operator(left, graph.clone())?;
            let right_op = build_operator(right, graph.clone())?;
            Ok(Box::new(JoinOperator::new(
                left_op,
                right_op,
                join_type.clone(),
                predicate.clone(),
                graph.clone(),
                *edge_id,
            )))
        }
        PlanOperator::Aggregate { input, group_by, aggregates, having: _having } => {
            let input_op = build_operator(input, graph.clone())?;
            let agg_op = AggregateOperator::new(input_op, group_by.clone(), aggregates.clone());
            // HAVING is handled separately if needed - for now, AggregateOperator doesn't support it directly
            Ok(Box::new(agg_op))
        }
        PlanOperator::Project { input, columns } => {
            let input_op = build_operator(input, graph.clone())?;
            Ok(Box::new(ProjectOperator::new(input_op, columns.clone())))
        }
        PlanOperator::Sort { input, order_by, limit, offset } => {
            let input_op = build_operator(input, graph.clone())?;
            Ok(Box::new(SortOperator::new(
                input_op,
                order_by.clone(),
                *limit,
                *offset,
            )))
        }
        PlanOperator::Limit { input, limit, offset } => {
            let input_op = build_operator(input, graph.clone())?;
            Ok(Box::new(LimitOperator::new(input_op, *limit, *offset)))
        }
    }
}

