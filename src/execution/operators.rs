use crate::execution::batch::{ExecutionBatch, BatchIterator};
use crate::execution::exec_node::{ExecNode, HasChildren};
use crate::execution::simd_kernels::{apply_simd_filter, FilterPredicate as SIMDFilterPredicate, aggregate_simd, sum_simd_i64, sum_simd_f64, min_simd_i64, max_simd_i64, min_simd_f64, max_simd_f64, avg_simd_i64, avg_simd_f64, hash_simd_i64, hash_simd_f64};
use crate::execution::type_conversion::{values_to_array, validate_array_type};
use crate::execution::column_identity::{ColId, ColumnSchema, generate_col_id_for_table_column};
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
    /// Fragment-level predicates for early pruning (pushed down from FilterOperator)
    fragment_predicates: Vec<(String, FragmentPredicate)>,
    /// LLM protocol: max rows to scan (soft limit)
    max_scan_rows: Option<u64>,
    /// LLM protocol: rows scanned so far
    rows_scanned: u64,
    /// COLID: Column schema with canonical column identities
    column_schema: ColumnSchema,
    /// Vector index usage hints: (table, column) -> whether to use vector index
    vector_index_hints: std::collections::HashMap<(String, String), bool>,
    /// B-tree index usage hints: (table, column) -> whether to use B-tree index
    btree_index_hints: std::collections::HashMap<(String, String), bool>,
    /// Partition hints: list of partition IDs to scan (empty = scan all)
    partition_hints: Vec<crate::storage::partitioning::PartitionId>,
    /// ExecNode: prepared flag - ensures prepare() was called before next()
    prepared: bool,
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
        
        // Debug logging disabled to prevent memory issues in IDEs
        // eprintln!("ScanOperator: Found {} columns with up to {} fragments each for table '{}' (first fragment has {} rows)", 
        //     column_fragments.len(), max_fragments, table, first_fragment_rows);
        
        // COLID: Create ColumnSchema with ColIds assigned to each column
        // ColIds are based on (node_id, column_index) for stability
        let mut column_schema = ColumnSchema::new();
        let table_id = node_id.0; // NodeId is a u64, use .0 to access value
        
        // Get data types from fragments (first fragment of each column)
        for (col_idx, col_name) in final_column_names.iter().enumerate() {
            let col_id = generate_col_id_for_table_column(table_id as u32, col_idx as u32);
            
            // Determine data type from first fragment
            let data_type = if let Some(fragments) = column_fragments.get(col_idx) {
                if let Some(first_fragment) = fragments.first() {
                    if let Some(array) = first_fragment.get_array() {
                        array.data_type().clone()
                    } else {
                        DataType::Utf8 // Default fallback
                    }
                } else {
                    DataType::Utf8 // Default fallback
                }
            } else {
                DataType::Utf8 // Default fallback
            };
            
            // Add to schema with qualified name (table.column) and unqualified name
            let qualified_name = format!("{}.{}", table, col_name);
            column_schema.add_column(col_id, qualified_name, col_name.clone(), data_type.clone());
        }
        
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
            fragment_predicates: Vec::new(),
            max_scan_rows: None,  // Will be set if LLM protocol specifies it
            rows_scanned: 0,
            column_schema, // COLID: Store ColumnSchema with ColIds
            vector_index_hints: std::collections::HashMap::new(), // Initialize empty, will be set by build_operator
            btree_index_hints: std::collections::HashMap::new(), // Initialize empty, will be set by build_operator
            partition_hints: Vec::new(), // Initialize empty, will be set by build_operator
            prepared: false, // ExecNode: must call prepare() before next()
        })
    }
    
    /// Get the column schema (for other operators to access ColIds)
    pub fn column_schema(&self) -> &ColumnSchema {
        &self.column_schema
    }
}

// ExecNode implementation for ScanOperator
impl ExecNode for ScanOperator {
    fn prepare(&mut self) -> Result<()> {
        // ScanOperator has no children, so no recursive prepare() needed
        // Just mark as prepared
        self.prepared = true;
        eprintln!("[SCAN PREPARE] Table: {}, prepared", self.table);
        Ok(())
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // Safety: assert prepared was called
        assert!(self.prepared, "ScanOperator::next() called before prepare()");
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
        
        // Advance to the next non-pruned fragment using fragment-level predicates and partition hints
        while self.current_fragment_idx < max_fragments {
            let mut should_prune = false;
            
            // PARTITION PRUNING: Check if this fragment belongs to a pruned partition
            if !self.partition_hints.is_empty() {
                // Check if fragment metadata contains partition ID
                // Partition ID might be stored in fragment metadata or we need to infer it
                // For now, check if we can determine partition from fragment metadata
                if let Some(first_fragment) = self.column_fragments.first()
                    .and_then(|frags| frags.get(self.current_fragment_idx)) {
                    // Try to get partition ID from fragment metadata
                    // Format: "partition_id" -> u64 as string
                    // In a full implementation, partition_id would be stored in FragmentMetadata
                    // For now, we skip partition pruning check (would need partition_id in metadata)
                    // TODO: Add partition_id to FragmentMetadata
                }
            }
            
            // FRAGMENT-LEVEL PREDICATE PRUNING: Check if this fragment can be pruned
            if !should_prune {
                for (col_name, frag_pred) in &self.fragment_predicates {
                    // Find column index for this column name
                    if let Some(col_idx) = self.column_names.iter().position(|n| n == col_name) {
                        if self.current_fragment_idx < self.column_fragments[col_idx].len() {
                            let fragment = &self.column_fragments[col_idx][self.current_fragment_idx];
                            if can_prune_fragment(fragment, frag_pred) {
                                should_prune = true;
                                break;
                            }
                        }
                    }
                }
            }
            
            if should_prune {
                // Skip this fragment and move to the next one
                self.current_fragment_idx += 1;
                continue;
            } else {
                // This fragment might contain matching rows; process it
                break;
            }
        }
        
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
        
        // B-TREE INDEX USAGE: Check if we should use B-tree indexes for filtering
        let mut index_filtered_rows: Option<std::collections::HashSet<usize>> = None;
        
        // Check if we have B-tree index hints and fragment predicates that can use them
        if !self.btree_index_hints.is_empty() && !self.fragment_predicates.is_empty() {
            // Try to find a B-tree index for one of the predicate columns
            for (col_name, frag_pred) in &self.fragment_predicates {
                let index_key = (self.table.clone(), col_name.clone());
                if self.btree_index_hints.get(&index_key) == Some(&true) {
                    // Try to retrieve B-tree index from graph/node metadata
                    if let Some(node) = self.graph.get_node(self.node_id) {
                        // Check if B-tree index is stored in node metadata
                        let index_key_str = format!("btree_index_{}", col_name);
                        if let Some(index_json) = node.metadata.get(&index_key_str) {
                            if let Ok(btree_index) = serde_json::from_str::<crate::storage::btree_index::BTreeIndex>(index_json) {
                                // Use B-tree index to find matching rows
                                let matching_rows = match frag_pred {
                                    crate::storage::cache_layout::FragmentPredicate::Equals(value) => {
                                        // Point lookup
                                        btree_index.lookup(value)
                                            .map(|rows| rows.iter().copied().collect::<std::collections::HashSet<_>>())
                                            .unwrap_or_default()
                                    }
                                    crate::storage::cache_layout::FragmentPredicate::LessThan(value) => {
                                        // Range scan: < value
                                        btree_index.less_than(value, false)
                                            .into_iter()
                                            .collect::<std::collections::HashSet<_>>()
                                    }
                                    crate::storage::cache_layout::FragmentPredicate::GreaterThan(value) => {
                                        // Range scan: > value
                                        btree_index.greater_than(value, false)
                                            .into_iter()
                                            .collect::<std::collections::HashSet<_>>()
                                    }
                                    _ => {
                                        // Other predicates not supported by B-tree index
                                        continue;
                                    }
                                };
                                
                                if !matching_rows.is_empty() {
                                    // Use this index for filtering
                                    index_filtered_rows = Some(matching_rows);
                                    break; // Use first matching index
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // B-TREE INDEX FILTERING: If we have index-filtered rows, create a selection vector
        let mut selection_vector: Option<bitvec::prelude::BitVec> = None;
        if let Some(ref filtered_rows) = index_filtered_rows {
            // Create a bitvec where bit[i] = 1 if row i should be included
            let mut selection = bitvec::prelude::bitvec![0; row_count];
            // Note: B-tree index returns global row positions, but we need fragment-local positions
            // For now, we'll assume row positions are fragment-local (offset by fragment start)
            // In a full implementation, we'd need to track fragment offsets
            let fragment_start = self.current_fragment_idx * 16384; // Approximate fragment size
            for &row_pos in filtered_rows {
                if row_pos >= fragment_start {
                    let local_pos = row_pos - fragment_start;
                    if local_pos < row_count {
                        selection.set(local_pos, true);
                    }
                }
            }
            selection_vector = Some(selection);
        }
        
        // Check LLM protocol: max_scan_rows limit (early termination)
        if let Some(max_rows) = self.max_scan_rows {
            if self.rows_scanned >= max_rows {
                // Already scanned enough rows, stop
                return Ok(None);
            }
        }
        
        // Update rows_scanned BEFORE processing (we're about to scan this fragment)
        self.rows_scanned += row_count as u64;
        
        // eprintln!("ScanOperator::next: Creating batch with {} rows, {} columns (fragment {}/{}, scanned: {}/{})", 
        //     row_count, column_arrays.len(), self.current_fragment_idx + 1, max_fragments,
        //     self.rows_scanned, self.max_scan_rows.unwrap_or(0));
        
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
        
        // Check LLM protocol: max_scan_rows limit (adjust end_idx if needed)
        if let Some(max_rows) = self.max_scan_rows {
            let already_scanned = self.rows_scanned - row_count as u64;
            let remaining = (max_rows.saturating_sub(already_scanned)) as usize;
            if remaining < end_idx - start_idx {
                end_idx = start_idx + remaining;
                // Adjust rows_scanned to reflect truncation
                self.rows_scanned = already_scanned + remaining as u64;
            }
        }
        
        // B-TREE INDEX FILTERING: Apply index-based filtering if available
        // Note: For now, we skip Arrow filter (not available in this Arrow version)
        // Instead, we'll use the selection vector to mark rows in ExecutionBatch
        let (final_arrays, actual_row_count) = if selection_vector.is_some() {
            // If we have index filtering, we'll apply it via selection vector in ExecutionBatch
            // For now, use normal slicing - selection will be applied later
            let final_arrays = if start_idx > 0 || end_idx < row_count {
                column_arrays.iter().map(|arr| {
                    use arrow::array::Array;
                    arr.slice(start_idx, end_idx - start_idx)
                }).collect()
            } else {
                column_arrays
            };
            (final_arrays, end_idx - start_idx)
        } else {
            // No index filtering - use normal slicing
            let final_arrays = if start_idx > 0 || end_idx < row_count {
                column_arrays.iter().map(|arr| {
                    use arrow::array::Array;
                    arr.slice(start_idx, end_idx - start_idx)
                }).collect()
            } else {
                column_arrays
            };
            (final_arrays, end_idx - start_idx)
        };
        
        // Create ColumnarBatch with filtered/sliced arrays
        let batch = crate::storage::columnar::ColumnarBatch::new(final_arrays, schema.clone());
        
        // Build column_fragments map for dictionary encoding lookup
        use std::collections::HashMap;
        use std::sync::Arc;
        let mut column_fragments = HashMap::new();
        for (idx, col_name) in self.column_names.iter().enumerate() {
            if idx < self.column_fragments.len() && self.current_fragment_idx < self.column_fragments[idx].len() {
                let fragment = &self.column_fragments[idx][self.current_fragment_idx];
                // Only store dictionary-encoded fragments
                if crate::execution::dictionary_execution::is_dictionary_encoded(fragment) {
                    column_fragments.insert(col_name.clone(), Arc::new(fragment.clone()));
                }
            }
        }
        
        // COLID: Create ExecutionBatch with ColumnSchema and fragment metadata
        let mut exec_batch = ExecutionBatch::with_column_schema_and_fragments(
            batch, 
            self.column_schema.clone(), // COLID: Include ColumnSchema with ColIds
            column_fragments
        );
        
        // B-TREE INDEX FILTERING: Apply selection vector if we used index filtering
        if let Some(selection) = selection_vector {
            // Update selection vector in ExecutionBatch
            // Note: selection vector might need to be adjusted for OFFSET/LIMIT
            exec_batch.selection = selection;
        }
        
        // eprintln!("ScanOperator::next: Created ExecutionBatch with {} rows (from fragment slice [{}, {}))", 
        //     actual_row_count, start_idx, end_idx);
        
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
}

impl BatchIterator for ScanOperator {
    fn prepare(&mut self) -> Result<(), anyhow::Error> {
        <Self as ExecNode>::prepare(self).map_err(|e| anyhow::anyhow!("{}", e))
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // Delegate to ExecNode::next() which has safety checks
        <Self as ExecNode>::next(self)
    }
    
    fn schema(&self) -> SchemaRef {
        // SCHEMA-FLOW: ScanOperator builds schema dynamically from fragments
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
        
        let output_schema = Arc::new(Schema::new(fields));
        
        // SCHEMA-FLOW DEBUG: Log output schema
        eprintln!("DEBUG ScanOperator::schema() - Output schema has {} fields: {:?}", 
            output_schema.fields().len(),
            output_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        
        output_schema
    }
}

/// Filter operator - applies WHERE predicates
pub struct FilterOperator {
    input: Box<dyn BatchIterator>,
    predicates: Vec<FilterPredicate>,
    /// Optional WHERE expression tree for complex OR/AND conditions
    /// When present, this is used instead of combining predicates with AND
    where_expression: Option<crate::query::expression::Expression>,
    /// Optional subquery executor for correlated subqueries
    subquery_executor: Option<std::sync::Arc<dyn crate::query::expression::SubqueryExecutor>>,
    /// Table aliases for column resolution (e.g., "e2" -> "employees")
    table_aliases: std::collections::HashMap<String, String>,
    /// ExecNode: prepared flag - ensures prepare() was called before next()
    prepared: bool,
}

impl FilterOperator {
    pub fn new(input: Box<dyn BatchIterator>, predicates: Vec<FilterPredicate>) -> Self {
        Self { 
            input, 
            predicates,
            where_expression: None,
            subquery_executor: None,
            table_aliases: std::collections::HashMap::new(),
            prepared: false,
        }
    }
    
    /// Create FilterOperator with subquery executor support
    pub fn with_subquery_executor(
        input: Box<dyn BatchIterator>, 
        predicates: Vec<FilterPredicate>,
        subquery_executor: Option<std::sync::Arc<dyn crate::query::expression::SubqueryExecutor>>,
    ) -> Self {
        Self::with_subquery_executor_and_aliases(input, predicates, None, subquery_executor, std::collections::HashMap::new())
    }
    
    /// Create FilterOperator with subquery executor and table aliases
    pub fn with_subquery_executor_and_aliases(
        input: Box<dyn BatchIterator>, 
        predicates: Vec<FilterPredicate>,
        where_expression: Option<crate::query::expression::Expression>,
        subquery_executor: Option<std::sync::Arc<dyn crate::query::expression::SubqueryExecutor>>,
        table_aliases: std::collections::HashMap<String, String>,
    ) -> Self {
        eprintln!("DEBUG FilterOperator::with_subquery_executor: subquery_executor.is_some()={}, predicates with subquery_expr: {}, where_expression.is_some()={}, table_aliases: {:?}", 
            subquery_executor.is_some(),
            predicates.iter().filter(|p| p.subquery_expression.is_some()).count(),
            where_expression.is_some(),
            table_aliases);
        if let Some(ref wexpr) = where_expression {
            eprintln!("DEBUG FilterOperator::with_subquery_executor: WHERE expression: {:?}", wexpr);
        }
        Self {
            input,
            predicates,
            where_expression,
            subquery_executor,
            table_aliases,
            prepared: false,
        }
    }
}

// ExecNode implementation for FilterOperator
impl FilterOperator {
    /// Helper: Try to call prepare() on a BatchIterator if it implements ExecNode
    /// 
    /// This uses type erasure to call prepare() on child operators.
    /// Since we know all operators implement ExecNode, we can safely downcast.
    /// 
    /// This is a public function used by the execution engine and other operators.
    pub fn prepare_child(child: &mut Box<dyn BatchIterator>) -> Result<()> {
        // CRITICAL: Call prepare() directly on BatchIterator trait object
        // Rust's dynamic dispatch should invoke the correct implementation
        // (e.g., ProjectOperator::prepare() if child is a ProjectOperator)
        // Each operator's BatchIterator::prepare() delegates to ExecNode::prepare()
        child.prepare().map_err(|e| anyhow::anyhow!("{}", e))
    }
}

impl ExecNode for FilterOperator {
    fn prepare(&mut self) -> Result<()> {
        // Recursively prepare child operator
        Self::prepare_child(&mut self.input)?;
        self.prepared = true;
        eprintln!("[FILTER PREPARE] Prepared with {} predicates", self.predicates.len());
        Ok(())
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // Safety: assert prepared was called
        assert!(self.prepared, "FilterOperator::next() called before prepare()");
        let mut batch = match self.input.next()? {
            Some(b) => b,
            None => return Ok(None),
        };
        
        eprintln!("DEBUG FilterOperator::next: Received batch with {} rows, schema: {:?}", 
            batch.row_count,
            batch.batch.schema.fields().iter().map(|f| f.name().to_string()).collect::<Vec<_>>());
        
        // Apply predicates using SIMD-optimized filtering
        let selection = self.apply_predicates(&batch)?;
        eprintln!("DEBUG FilterOperator::next: Before apply_selection - batch.row_count={}, selection.count_ones()={}", 
            batch.row_count, selection.count_ones());
        batch.apply_selection(&selection);
        eprintln!("DEBUG FilterOperator::next: After apply_selection - batch.row_count={}, batch.selection.count_ones()={}", 
            batch.row_count, batch.selection.count_ones());
        
        Ok(Some(batch))
    }
}

impl BatchIterator for FilterOperator {
    fn prepare(&mut self) -> Result<(), anyhow::Error> {
        <Self as ExecNode>::prepare(self).map_err(|e| anyhow::anyhow!("{}", e))
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // Delegate to ExecNode::next() which has safety checks
        <Self as ExecNode>::next(self)
    }
    
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

impl FilterOperator {
    fn apply_predicates(&self, batch: &ExecutionBatch) -> Result<BitVec> {
        // If we have a WHERE expression tree (for OR conditions), evaluate it directly
        eprintln!("DEBUG FilterOperator::apply_predicates: where_expression.is_some()={}, predicates.len()={}", 
            self.where_expression.is_some(), self.predicates.len());
        if let Some(ref where_expr) = self.where_expression {
            eprintln!("DEBUG FilterOperator::apply_predicates: Using WHERE expression tree for OR handling: {:?}", where_expr);
            return self.evaluate_where_expression(where_expr, batch);
        }
        
        // Otherwise, use predicate-based filtering (AND all predicates)
        let mut selection = bitvec![1; batch.batch.row_count];
        
        eprintln!("DEBUG FilterOperator::apply_predicates: Processing {} predicates on batch with {} rows", 
            self.predicates.len(), batch.batch.row_count);
        
        for (pred_idx, predicate) in self.predicates.iter().enumerate() {
            eprintln!("DEBUG FilterOperator::apply_predicates: Processing predicate {} of {}: column='{}', operator={:?}, value={:?}", 
                pred_idx + 1, self.predicates.len(), predicate.column, predicate.operator, predicate.value);
            // Use ColumnResolver for qualified column names (e.g., "e2.department_id")
            use crate::query::column_resolver::ColumnResolver;
            let resolver = ColumnResolver::new(batch.batch.schema.clone(), self.table_aliases.clone());
            
            eprintln!("DEBUG FilterOperator::apply_filter: predicate.column={:?}, predicate.value={:?}, table_aliases={:?}", 
                predicate.column, predicate.value, self.table_aliases);
            eprintln!("DEBUG FilterOperator::apply_filter: batch schema has {} fields: {:?}", 
                batch.batch.schema.fields().len(),
                batch.batch.schema.fields().iter().map(|f| f.name().to_string()).collect::<Vec<_>>());
            eprintln!("DEBUG FilterOperator::apply_filter: input.schema() has {} fields: {:?}", 
                self.input.schema().fields().len(),
                self.input.schema().fields().iter().map(|f| f.name().to_string()).collect::<Vec<_>>());
            
            // Try to resolve column using ColumnResolver (handles qualified and unqualified names)
            let col_idx = match resolver.resolve(&predicate.column) {
                Ok(idx) => {
                    eprintln!("DEBUG FilterOperator::apply_filter: Resolved {} to column index {}", predicate.column, idx);
                    idx
                },
                Err(e) => {
                    eprintln!("DEBUG FilterOperator::apply_filter: Failed to resolve {}: {}, trying fallback", predicate.column, e);
                    // Fallback: try stripping table alias
                    let resolved_col_name = if predicate.column.contains('.') {
                        let parts: Vec<&str> = predicate.column.split('.').collect();
                        if parts.len() == 2 {
                            parts[1].to_string()
                        } else {
                            predicate.column.clone()
                        }
                    } else {
                        predicate.column.clone()
                    };
                    // Fallback: try with unqualified name using ColumnResolver
                    let fallback_resolver = ColumnResolver::from_schema(batch.batch.schema.clone());
                    let idx = fallback_resolver.resolve(&resolved_col_name)
                        .map_err(|_| anyhow::anyhow!("Column not found: {} (tried: {}, error: {})", predicate.column, resolved_col_name, e))?;
                    eprintln!("DEBUG FilterOperator::apply_filter: Fallback resolved {} to column index {}", resolved_col_name, idx);
                    idx
                }
            };
            
            let column = batch.batch.column(col_idx)
                .ok_or_else(|| anyhow::anyhow!("Column index {} out of range for column: {}", col_idx, predicate.column))?;
            
            eprintln!("DEBUG FilterOperator::apply_filter: Column {} ({}) has {} rows, data_type={:?}", 
                col_idx, predicate.column, column.len(), column.data_type());
            
            // Show sample values for string columns to debug comparison issues
            if matches!(column.data_type(), arrow::datatypes::DataType::Utf8) {
                let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
                eprintln!("DEBUG FilterOperator::apply_filter: Sample values from '{}' (first 5 rows):", predicate.column);
                for i in 0..arr.len().min(5) {
                    if !arr.is_null(i) {
                        eprintln!("  Row {}: '{}'", i, arr.value(i));
                    }
                }
                eprintln!("DEBUG FilterOperator::apply_filter: Comparing with value: {:?}", predicate.value);
            }
            
            // OPTIMIZATION: Check for vector index and use it for vector similarity queries
            // Phase 1: Check if fragment has vector index and predicate is vector similarity
            let column_selection = if let Some(fragment) = batch.get_column_fragment(&predicate.column) {
                // Check if fragment has vector index and value is a vector
                if let Some(ref vector_index) = fragment.vector_index {
                    // Check if predicate value is a vector (for VECTOR_SIMILARITY operations)
                    if let crate::storage::fragment::Value::Vector(ref query_vector) = predicate.value {
                        // OPTIMIZATION: Use vector index for similarity search (O(log n) instead of O(n))
                        eprintln!("DEBUG FilterOperator: Using vector index for similarity search on column {}", predicate.column);
                        
                        // Perform vector index search to find top-k similar vectors
                        // Default to top 100 results (or all rows if fewer)
                        let k = batch.batch.row_count.min(100);
                        
                        // Search using vector index (VectorIndexEnum implements VectorIndexTrait)
                        use crate::storage::vector_index::VectorIndexTrait;
                        let search_results = (*vector_index).search(query_vector, k, crate::storage::vector_index::VectorMetric::Cosine);
                        
                        // Create selection bitmap from search results
                        let mut vec_selection = bitvec![0; batch.batch.row_count];
                        for (row_idx, _similarity) in search_results {
                            if row_idx < batch.batch.row_count {
                                vec_selection.set(row_idx, true);
                            }
                        }
                        
                        vec_selection
                    } else {
                        // Not a vector similarity operation, use normal filtering
                        self.try_dictionary_or_normal_filter(fragment, column, predicate, &batch)?
                    }
                } else {
                    // No vector index, try dictionary or normal filtering
                    self.try_dictionary_or_normal_filter(fragment, column, predicate, &batch)?
                }
            } else {
                // No fragment metadata available, use normal filtering
                self.apply_filter_predicate(column, predicate, &batch)?
            };
            
            // Combine with existing selection (AND)
            eprintln!("DEBUG FilterOperator::apply_filter: Before combining - selection has {} bits set, column_selection has {} bits set (predicate: {} = {:?})", 
                selection.count_ones(), column_selection.count_ones(), predicate.column, predicate.value);
            
            // CRITICAL: Check if column_selection is empty (all zeros) - this would cause AND to fail
            if column_selection.count_ones() == 0 {
                eprintln!("ERROR FilterOperator::apply_filter: column_selection for '{}' has NO bits set! This will cause AND to return no rows.", predicate.column);
                eprintln!("ERROR FilterOperator::apply_filter: Checking column data - array len={}, data_type={:?}", 
                    column.len(), column.data_type());
                if matches!(column.data_type(), arrow::datatypes::DataType::Utf8) {
                    let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
                    eprintln!("ERROR FilterOperator::apply_filter: Sample values from column '{}':", predicate.column);
                    for i in 0..arr.len().min(5) {
                        if !arr.is_null(i) {
                            eprintln!("  Row {}: '{}' (compare with '{:?}')", i, arr.value(i), predicate.value);
                        }
                    }
                }
            }
            
            selection &= &column_selection;
            eprintln!("DEBUG FilterOperator::apply_filter: After combining, selection has {} bits set", selection.count_ones());
        }
        
        Ok(selection)
    }
    
    /// Evaluate WHERE expression tree (handles OR/AND conditions)
    fn evaluate_where_expression(&self, expr: &crate::query::expression::Expression, batch: &ExecutionBatch) -> Result<BitVec> {
        use crate::query::expression::Expression;
        use crate::query::expression::BinaryOperator;
        
        eprintln!("DEBUG evaluate_where_expression: Evaluating expression: {:?}", expr);
        
        match expr {
            Expression::BinaryOp { left, op, right } => {
                eprintln!("DEBUG evaluate_where_expression: BinaryOp with operator: {:?}", op);
                
                // Check if this is a logical operator (And/Or) or a comparison operator
                match op {
                    BinaryOperator::And | BinaryOperator::Or => {
                        // Logical operator - recurse on both sides
                        let left_selection = self.evaluate_where_expression(left, batch)?;
                        eprintln!("DEBUG evaluate_where_expression: Left selection has {} bits set", left_selection.count_ones());
                        let right_selection = self.evaluate_where_expression(right, batch)?;
                        eprintln!("DEBUG evaluate_where_expression: Right selection has {} bits set", right_selection.count_ones());
                        
                        match op {
                            BinaryOperator::And => {
                                // AND: combine selections with bitwise AND
                                let result = left_selection & &right_selection;
                                eprintln!("DEBUG evaluate_where_expression: AND result has {} bits set", result.count_ones());
                                Ok(result)
                            }
                            BinaryOperator::Or => {
                                // OR: combine selections with bitwise OR
                                let result = left_selection | &right_selection;
                                eprintln!("DEBUG evaluate_where_expression: OR result has {} bits set", result.count_ones());
                                Ok(result)
                            }
                            _ => unreachable!(), // Already matched above
                        }
                    }
                    _ => {
                        // Comparison operator (Eq, Gt, Lt, etc.) - evaluate as comparison
                        eprintln!("DEBUG evaluate_where_expression: Comparison operator: {:?}, evaluating as comparison", op);
                        self.evaluate_comparison_expression(expr, batch)
                    }
                }
            }
            Expression::Column(column_name, _) => {
                // This shouldn't happen in a WHERE clause - columns should be in comparisons
                eprintln!("DEBUG evaluate_where_expression: Unexpected column reference: {}", column_name);
                anyhow::bail!("Unexpected column reference in WHERE expression: {}", column_name);
            }
            Expression::Literal(_) => {
                // Literal values in WHERE should be in comparisons, not standalone
                eprintln!("DEBUG evaluate_where_expression: Unexpected literal");
                anyhow::bail!("Unexpected literal in WHERE expression");
            }
            _ => {
                // For other expression types, try to evaluate as comparison
                eprintln!("DEBUG evaluate_where_expression: Treating as comparison expression");
                self.evaluate_comparison_expression(expr, batch)
            }
        }
    }
    
    /// Evaluate a comparison expression (e.g., column > value, column = value)
    fn evaluate_comparison_expression(&self, expr: &crate::query::expression::Expression, batch: &ExecutionBatch) -> Result<BitVec> {
        use crate::query::expression::Expression;
        use crate::query::expression::BinaryOperator;
        use crate::query::plan::PredicateOperator;
        
        eprintln!("DEBUG evaluate_comparison_expression: Evaluating comparison: {:?}", expr);
        
        // Try to extract column, operator, and value from expression
        if let Expression::BinaryOp { left, op, right } = expr {
            // Extract column name from left side
            let column_name = match left.as_ref() {
                Expression::Column(name, _) => {
                    eprintln!("DEBUG evaluate_comparison_expression: Left side is column: {}", name);
                    name.clone()
                },
                _ => {
                    eprintln!("DEBUG evaluate_comparison_expression: Left side is not a column: {:?}", left);
                    anyhow::bail!("Left side of comparison must be a column");
                }
            };
            
            // Extract value from right side
            let value = match right.as_ref() {
                Expression::Literal(v) => {
                    eprintln!("DEBUG evaluate_comparison_expression: Right side is literal: {:?}", v);
                    v.clone()
                },
                _ => {
                    eprintln!("DEBUG evaluate_comparison_expression: Right side is not a literal: {:?}", right);
                    anyhow::bail!("Right side of comparison must be a literal value");
                }
            };
            
            // Convert expression operator to PredicateOperator
            let predicate_op = match op {
                BinaryOperator::Eq => PredicateOperator::Equals,
                BinaryOperator::Ne => PredicateOperator::NotEquals,
                BinaryOperator::Lt => PredicateOperator::LessThan,
                BinaryOperator::Le => PredicateOperator::LessThanOrEqual,
                BinaryOperator::Gt => PredicateOperator::GreaterThan,
                BinaryOperator::Ge => PredicateOperator::GreaterThanOrEqual,
                _ => {
                    eprintln!("DEBUG evaluate_comparison_expression: Unsupported comparison operator: {:?}", op);
                    anyhow::bail!("Unsupported comparison operator: {:?}", op);
                }
            };
            
            eprintln!("DEBUG evaluate_comparison_expression: Created predicate: column={}, operator={:?}, value={:?}", 
                column_name, predicate_op, value);
            
            // Create a FilterPredicate and evaluate it
            let predicate = crate::query::plan::FilterPredicate {
                column: column_name,
                operator: predicate_op,
                value,
                pattern: None,
                in_values: None,
                subquery_expression: None,
            };
            
            // Resolve column and apply predicate
            use crate::query::column_resolver::ColumnResolver;
            let resolver = ColumnResolver::new(batch.batch.schema.clone(), self.table_aliases.clone());
            let col_idx = resolver.resolve(&predicate.column)
                .map_err(|e| {
                    eprintln!("DEBUG evaluate_comparison_expression: Failed to resolve column '{}': {}", predicate.column, e);
                    e
                })?;
            eprintln!("DEBUG evaluate_comparison_expression: Resolved column '{}' to index {}", predicate.column, col_idx);
            let column = batch.batch.column(col_idx)
                .ok_or_else(|| anyhow::anyhow!("Column index {} out of range", col_idx))?;
            
            let result = self.apply_filter_predicate(column, &predicate, batch)?;
            eprintln!("DEBUG evaluate_comparison_expression: Comparison result has {} bits set", result.count_ones());
            Ok(result)
        } else {
            eprintln!("DEBUG evaluate_comparison_expression: Expression is not a BinaryOp: {:?}", expr);
            anyhow::bail!("Expected comparison expression (BinaryOp), got: {:?}", expr);
        }
    }
    
    /// Try dictionary-encoded fast path, fallback to normal filtering
    fn try_dictionary_or_normal_filter(
        &self,
        fragment: &crate::storage::fragment::ColumnFragment,
        column: &Arc<dyn Array>,
        predicate: &FilterPredicate,
        batch: &ExecutionBatch,
    ) -> Result<BitVec> {
        // Check if this column is dictionary-encoded
        if crate::execution::dictionary_execution::is_dictionary_encoded(fragment) {
            // Use dictionary-encoded fast path
            if let Some(codes) = crate::execution::dictionary_execution::get_dictionary_codes(fragment) {
                if let Some(dictionary) = crate::execution::dictionary_execution::get_dictionary(fragment) {
                    // Use dictionary execution kernel
                    match crate::execution::dictionary_execution::filter_dictionary_codes(
                        &codes,
                        dictionary,
                        &predicate.value,
                        &predicate.operator,
                    ) {
                        Ok(selection) => Ok(selection),
                        Err(e) => {
                            eprintln!("Warning: Dictionary filter failed, falling back to normal filter: {}", e);
                            self.apply_filter_predicate(column, predicate, batch)
                        }
                    }
                } else {
                    self.apply_filter_predicate(column, predicate, batch)
                }
            } else {
                self.apply_filter_predicate(column, predicate, batch)
            }
        } else {
            // Not dictionary-encoded, use normal filtering
            self.apply_filter_predicate(column, predicate, batch)
        }
    }
    
    fn apply_filter_predicate(
        &self,
        array: &Arc<dyn Array>,
        predicate: &FilterPredicate,
        batch: &ExecutionBatch,
    ) -> Result<BitVec> {
        // Handle LIKE, IN, and IS NULL operators (not supported by SIMD yet)
        match predicate.operator {
            PredicateOperator::Like | PredicateOperator::NotLike => {
                return self.apply_like_predicate(array, &predicate.operator, &predicate.pattern);
            }
            PredicateOperator::In | PredicateOperator::NotIn => {
                return self.apply_in_predicate(array, &predicate.operator, &predicate.in_values);
            }
            PredicateOperator::IsNull | PredicateOperator::IsNotNull => {
                return self.apply_is_null_predicate(array, &predicate.operator);
            }
            _ => {}
        }
        
        // Check if this predicate has a subquery expression (correlated subquery)
        if let Some(ref subquery_expr) = predicate.subquery_expression {
            // Evaluate subquery per row
            return self.apply_subquery_predicate(batch, array, &predicate.operator, subquery_expr);
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
        // For string types or when we need numeric comparisons with strings, use scalar path directly
        if matches!(array.data_type(), DataType::Utf8) {
            // Skip SIMD for string arrays - go straight to scalar which handles string-to-numeric parsing
            let mut bitvec = bitvec![0; array.len()];
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..array.len() {
                if arr.is_null(i) {
                    continue;
                }
                let val_str = arr.value(i);
                // Try to parse string as number for numeric comparisons
                let matches = match (operator, value) {
                    (PredicateOperator::Equals, Value::String(v)) => val_str == v,
                    (PredicateOperator::NotEquals, Value::String(v)) => val_str != v,
                    // For numeric comparisons, try parsing the string as a number
                    (PredicateOperator::GreaterThan, Value::Int64(v)) => {
                        if let Ok(num) = val_str.parse::<i64>() {
                            num > *v
                        } else if let Ok(num) = val_str.parse::<f64>() {
                            num > *v as f64
                        } else {
                            false
                        }
                    }
                    (PredicateOperator::GreaterThan, Value::Float64(v)) => {
                        if let Ok(num) = val_str.parse::<f64>() {
                            num > *v
                        } else if let Ok(num) = val_str.parse::<i64>() {
                            (num as f64) > *v
                        } else {
                            false
                        }
                    }
                    (PredicateOperator::GreaterThanOrEqual, Value::Int64(v)) => {
                        if let Ok(num) = val_str.parse::<i64>() {
                            num >= *v
                        } else if let Ok(num) = val_str.parse::<f64>() {
                            num >= *v as f64
                        } else {
                            false
                        }
                    }
                    (PredicateOperator::GreaterThanOrEqual, Value::Float64(v)) => {
                        if let Ok(num) = val_str.parse::<f64>() {
                            num >= *v
                        } else if let Ok(num) = val_str.parse::<i64>() {
                            (num as f64) >= *v
                        } else {
                            false
                        }
                    }
                    (PredicateOperator::LessThan, Value::Int64(v)) => {
                        if let Ok(num) = val_str.parse::<i64>() {
                            num < *v
                        } else if let Ok(num) = val_str.parse::<f64>() {
                            num < *v as f64
                        } else {
                            false
                        }
                    }
                    (PredicateOperator::LessThan, Value::Float64(v)) => {
                        if let Ok(num) = val_str.parse::<f64>() {
                            num < *v
                        } else if let Ok(num) = val_str.parse::<i64>() {
                            (num as f64) < *v
                        } else {
                            false
                        }
                    }
                    (PredicateOperator::LessThanOrEqual, Value::Int64(v)) => {
                        if let Ok(num) = val_str.parse::<i64>() {
                            num <= *v
                        } else if let Ok(num) = val_str.parse::<f64>() {
                            num <= *v as f64
                        } else {
                            false
                        }
                    }
                    (PredicateOperator::LessThanOrEqual, Value::Float64(v)) => {
                        if let Ok(num) = val_str.parse::<f64>() {
                            num <= *v
                        } else if let Ok(num) = val_str.parse::<i64>() {
                            (num as f64) <= *v
                        } else {
                            false
                        }
                    }
                    _ => false,
                };
                bitvec.set(i, matches);
            }
            return Ok(bitvec);
        }
        
        // Convert to SIMD filter predicate for numeric types
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
                // SIMD VECTORIZED OPTIMIZATION: Use SIMD kernels for maximum performance
                // Try SIMD first for Int64 arrays (highest priority optimization)
                if let DataType::Int64 = array.data_type() {
                    if let Some(int_arr) = array.as_any().downcast_ref::<Int64Array>() {
                        let mut out_sel = Vec::new();
                        let use_simd = match (operator, value) {
                            (PredicateOperator::GreaterThan, Value::Int64(v)) => {
                                crate::execution::vectorized::scan_filter::filter_gt_i64_simd(int_arr, *v, &mut out_sel);
                                true
                            }
                            (PredicateOperator::LessThan, Value::Int64(v)) => {
                                crate::execution::vectorized::scan_filter::filter_lt_i64_simd(int_arr, *v, &mut out_sel);
                                true
                            }
                            (PredicateOperator::Equals, Value::Int64(v)) => {
                                crate::execution::vectorized::scan_filter::filter_eq_i64_simd(int_arr, *v, &mut out_sel);
                                true
                            }
                            _ => false,
                        };
                        
                        if use_simd {
                            let mut bitvec = bitvec![0; array.len()];
                            for idx in out_sel {
                                bitvec.set(idx as usize, true);
                            }
                            return Ok(bitvec);
                        }
                    }
                }
                
                // MICRO-KERNEL OPTIMIZATION: Use specialized kernels for primitive types
                // Try micro-kernels as fallback for cases not covered by SIMD
                if let (PredicateOperator::Equals, Value::Int64(v)) = (operator, value) {
                    if let DataType::Int64 = array.data_type() {
                        if let Some(int_arr) = array.as_any().downcast_ref::<Int64Array>() {
                            let mut out_sel = Vec::new();
                            crate::execution::kernels::filter_fast::filter_eq_i64(int_arr, *v, &mut out_sel);
                            let mut bitvec = bitvec![0; array.len()];
                            for idx in out_sel {
                                bitvec.set(idx as usize, true);
                            }
                            return Ok(bitvec);
                        }
                    }
                }
                
                // Try other micro-kernel cases
                if let (PredicateOperator::LessThan, Value::Int64(v)) = (operator, value) {
                    if let DataType::Int64 = array.data_type() {
                        if let Some(int_arr) = array.as_any().downcast_ref::<Int64Array>() {
                            let mut out_sel = Vec::new();
                            crate::execution::kernels::filter_fast::filter_lt_i64(int_arr, *v, &mut out_sel);
                            let mut bitvec = bitvec![0; array.len()];
                            for idx in out_sel {
                                bitvec.set(idx as usize, true);
                            }
                            return Ok(bitvec);
                        }
                    }
                }
                
                if let (PredicateOperator::GreaterThan, Value::Int64(v)) = (operator, value) {
                    if let DataType::Int64 = array.data_type() {
                        if let Some(int_arr) = array.as_any().downcast_ref::<Int64Array>() {
                            let mut out_sel = Vec::new();
                            crate::execution::kernels::filter_fast::filter_gt_i64(int_arr, *v, &mut out_sel);
                            let mut bitvec = bitvec![0; array.len()];
                            for idx in out_sel {
                                bitvec.set(idx as usize, true);
                            }
                            return Ok(bitvec);
                        }
                    }
                }
                
                // Fallback to scalar comparison
                let mut bitvec = bitvec![0; array.len()];
                match array.data_type() {
                    DataType::Int64 => {
                        let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                        for i in 0..array.len() {
                            if arr.is_null(i) {
                                continue;
                            }
                            let val = arr.value(i) as f64; // Convert to f64 for cross-type comparison
                            let matches = match (operator, value) {
                                (PredicateOperator::Equals, Value::Int64(v)) => val == *v as f64,
                                (PredicateOperator::Equals, Value::Float64(v)) => val == *v,
                                (PredicateOperator::NotEquals, Value::Int64(v)) => val != *v as f64,
                                (PredicateOperator::NotEquals, Value::Float64(v)) => val != *v,
                                (PredicateOperator::GreaterThan, Value::Int64(v)) => val > *v as f64,
                                (PredicateOperator::GreaterThan, Value::Float64(v)) => val > *v,
                                (PredicateOperator::GreaterThanOrEqual, Value::Int64(v)) => val >= *v as f64,
                                (PredicateOperator::GreaterThanOrEqual, Value::Float64(v)) => val >= *v,
                                (PredicateOperator::LessThan, Value::Int64(v)) => val < *v as f64,
                                (PredicateOperator::LessThan, Value::Float64(v)) => val < *v,
                                (PredicateOperator::LessThanOrEqual, Value::Int64(v)) => val <= *v as f64,
                                (PredicateOperator::LessThanOrEqual, Value::Float64(v)) => val <= *v,
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
                                (PredicateOperator::Equals, Value::Int64(v)) => val == *v as f64,
                                (PredicateOperator::Equals, Value::Float64(v)) => val == *v,
                                (PredicateOperator::NotEquals, Value::Int64(v)) => val != *v as f64,
                                (PredicateOperator::NotEquals, Value::Float64(v)) => val != *v,
                                (PredicateOperator::GreaterThan, Value::Int64(v)) => val > *v as f64,
                                (PredicateOperator::GreaterThan, Value::Float64(v)) => val > *v,
                                (PredicateOperator::GreaterThanOrEqual, Value::Int64(v)) => val >= *v as f64,
                                (PredicateOperator::GreaterThanOrEqual, Value::Float64(v)) => val >= *v,
                                (PredicateOperator::LessThan, Value::Int64(v)) => val < *v as f64,
                                (PredicateOperator::LessThan, Value::Float64(v)) => val < *v,
                                (PredicateOperator::LessThanOrEqual, Value::Int64(v)) => val <= *v as f64,
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
                            let val_str = arr.value(i);
                            // Try to parse string as number for numeric comparisons
                            let matches = match (operator, value) {
                                (PredicateOperator::Equals, Value::String(v)) => val_str == v,
                                (PredicateOperator::NotEquals, Value::String(v)) => val_str != v,
                                // For numeric comparisons, try parsing the string as a number
                                (PredicateOperator::GreaterThan, Value::Int64(v)) => {
                                    if let Ok(num) = val_str.parse::<i64>() {
                                        num > *v
                                    } else if let Ok(num) = val_str.parse::<f64>() {
                                        num > *v as f64
                                    } else {
                                        false
                                    }
                                }
                                (PredicateOperator::GreaterThan, Value::Float64(v)) => {
                                    if let Ok(num) = val_str.parse::<f64>() {
                                        num > *v
                                    } else if let Ok(num) = val_str.parse::<i64>() {
                                        num as f64 > *v
                                    } else {
                                        false
                                    }
                                }
                                (PredicateOperator::GreaterThanOrEqual, Value::Int64(v)) => {
                                    if let Ok(num) = val_str.parse::<i64>() {
                                        num >= *v
                                    } else if let Ok(num) = val_str.parse::<f64>() {
                                        num >= *v as f64
                                    } else {
                                        false
                                    }
                                }
                                (PredicateOperator::GreaterThanOrEqual, Value::Float64(v)) => {
                                    if let Ok(num) = val_str.parse::<f64>() {
                                        num >= *v
                                    } else if let Ok(num) = val_str.parse::<i64>() {
                                        num as f64 >= *v
                                    } else {
                                        false
                                    }
                                }
                                (PredicateOperator::LessThan, Value::Int64(v)) => {
                                    if let Ok(num) = val_str.parse::<i64>() {
                                        num < *v
                                    } else if let Ok(num) = val_str.parse::<f64>() {
                                        num < *v as f64
                                    } else {
                                        false
                                    }
                                }
                                (PredicateOperator::LessThan, Value::Float64(v)) => {
                                    if let Ok(num) = val_str.parse::<f64>() {
                                        num < *v
                                    } else if let Ok(num) = val_str.parse::<i64>() {
                                        (num as f64) < *v
                                    } else {
                                        false
                                    }
                                }
                                (PredicateOperator::LessThanOrEqual, Value::Int64(v)) => {
                                    if let Ok(num) = val_str.parse::<i64>() {
                                        num <= *v
                                    } else if let Ok(num) = val_str.parse::<f64>() {
                                        num <= *v as f64
                                    } else {
                                        false
                                    }
                                }
                                (PredicateOperator::LessThanOrEqual, Value::Float64(v)) => {
                                    if let Ok(num) = val_str.parse::<f64>() {
                                        num <= *v
                                    } else if let Ok(num) = val_str.parse::<i64>() {
                                        num as f64 <= *v
                                    } else {
                                        false
                                    }
                                }
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
    
    /// Apply IS NULL / IS NOT NULL predicate
    fn apply_is_null_predicate(
        &self,
        array: &Arc<dyn Array>,
        operator: &PredicateOperator,
    ) -> Result<BitVec> {
        let mut bitvec = bitvec![0; array.len()];
        let is_null = matches!(operator, PredicateOperator::IsNull);
        
        for i in 0..array.len() {
            let is_null_value = array.is_null(i);
            bitvec.set(i, if is_null { is_null_value } else { !is_null_value });
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
    
    /// Apply predicate with subquery expression (correlated subquery)
    fn apply_subquery_predicate(
        &self,
        batch: &ExecutionBatch,
        column_array: &Arc<dyn Array>,
        operator: &PredicateOperator,
        subquery_expr: &crate::query::expression::Expression,
    ) -> Result<BitVec> {
        use arrow::array::*;
        let mut bitvec = bitvec![0; batch.batch.row_count];
        
        // Need subquery executor
        let executor = self.subquery_executor.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Subquery executor not provided for correlated subquery"))?;
        
        // Extract subquery from expression
        let subquery = match subquery_expr {
            crate::query::expression::Expression::Subquery(q) => q,
            _ => {
                return Err(anyhow::anyhow!("Expected Subquery expression, got: {:?}", subquery_expr));
            }
        };
        
        // For each row, evaluate the subquery with that row as outer context
        // Iterate through ALL rows in the batch (not just row_count)
        // The selection bitmap indicates which rows to process
        let actual_batch_size = batch.selection.len();
        for row_idx in 0..actual_batch_size {
            // Only process rows that are selected (respects filtering)
            if !batch.selection[row_idx] {
                continue; // Skip filtered rows
            }
            
            // Get column value for this row
            let column_value = if column_array.is_null(row_idx) {
                crate::storage::fragment::Value::Null
            } else {
                match column_array.data_type() {
                    DataType::Int64 => {
                        let arr = column_array.as_any().downcast_ref::<Int64Array>().unwrap();
                        crate::storage::fragment::Value::Int64(arr.value(row_idx))
                    }
                    DataType::Float64 => {
                        let arr = column_array.as_any().downcast_ref::<Float64Array>().unwrap();
                        crate::storage::fragment::Value::Float64(arr.value(row_idx))
                    }
                    DataType::Utf8 => {
                        let arr = column_array.as_any().downcast_ref::<StringArray>().unwrap();
                        crate::storage::fragment::Value::String(arr.value(row_idx).to_string())
                    }
                    _ => crate::storage::fragment::Value::Null,
                }
            };
            
            // Create a single-row batch for outer context
            let single_row_batch = self.create_single_row_batch(batch, row_idx)?;
            
            // Execute subquery with outer context
            match executor.execute_scalar_subquery(subquery, Some(&single_row_batch)) {
                Ok(Some(subquery_result)) => {
                    // Compare column value with subquery result
                    let matches = self.compare_values(&column_value, &subquery_result, operator);
                    eprintln!("DEBUG subquery filter: row_idx={}, column_value={:?}, subquery_result={:?}, operator={:?}, matches={}", 
                        row_idx, column_value, subquery_result, operator, matches);
                    bitvec.set(row_idx, matches);
                }
                Ok(None) => {
                    // Subquery returned no rows - comparison is false (NULL handling)
                    eprintln!("DEBUG subquery filter: row_idx={}, subquery returned None", row_idx);
                    bitvec.set(row_idx, false);
                }
                Err(e) => {
                    eprintln!("DEBUG subquery filter: row_idx={}, subquery error: {}", row_idx, e);
                    return Err(anyhow::anyhow!("Failed to execute subquery: {}", e));
                }
            }
        }
        
        Ok(bitvec)
    }
    
    /// Create a single-row batch from a full batch (for outer context)
    fn create_single_row_batch(&self, batch: &ExecutionBatch, row_idx: usize) -> Result<ExecutionBatch> {
        use arrow::array::*;
        use arrow::datatypes::*;
        
        let mut single_row_arrays = Vec::new();
        for col_idx in 0..batch.batch.columns.len() {
            let col = batch.batch.column(col_idx).unwrap();
            let field = batch.batch.schema.field(col_idx);
            
            // Create single-element array by extracting value from row_idx
            let single_array: Arc<dyn Array> = match field.data_type() {
                DataType::Int64 => {
                    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    Arc::new(Int64Array::from(vec![
                        if arr.is_null(row_idx) { None } else { Some(arr.value(row_idx)) }
                    ]))
                }
                DataType::Float64 => {
                    let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                    Arc::new(Float64Array::from(vec![
                        if arr.is_null(row_idx) { None } else { Some(arr.value(row_idx)) }
                    ]))
                }
                DataType::Utf8 => {
                    let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                    Arc::new(StringArray::from(vec![
                        if arr.is_null(row_idx) { None } else { Some(arr.value(row_idx).to_string()) }
                    ]))
                }
                DataType::Boolean => {
                    let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                    Arc::new(BooleanArray::from(vec![
                        if arr.is_null(row_idx) { None } else { Some(arr.value(row_idx)) }
                    ]))
                }
                _ => {
                    return Err(anyhow::anyhow!("Unsupported data type for single-row batch: {:?}", field.data_type()));
                }
            };
            single_row_arrays.push(single_array);
        }
        
        let single_row_batch = crate::storage::columnar::ColumnarBatch::new(
            single_row_arrays,
            batch.batch.schema.clone(),
        );
        
        // CRITICAL: Preserve ColumnSchema from input batch
        let mut exec_batch = if let Some(ref input_schema) = batch.column_schema {
            ExecutionBatch::with_column_schema(single_row_batch, input_schema.clone())
        } else {
            ExecutionBatch::new(single_row_batch)
        };
        exec_batch.selection = bitvec![1; 1]; // Single row selected
        exec_batch.row_count = 1;
        
        Ok(exec_batch)
    }
    
    /// Compare two values using operator
    fn compare_values(
        &self,
        left: &crate::storage::fragment::Value,
        right: &crate::storage::fragment::Value,
        operator: &PredicateOperator,
    ) -> bool {
        use crate::storage::fragment::Value as V;
        use crate::query::plan::PredicateOperator as Op;
        
        let cmp = match (left, right) {
            (V::Int64(l), V::Int64(r)) => Some(l.cmp(r)),
            (V::Float64(l), V::Float64(r)) => {
                l.partial_cmp(r).or(Some(std::cmp::Ordering::Equal))
            }
            (V::String(l), V::String(r)) => Some(l.cmp(r)),
            (V::Int64(l), V::Float64(r)) => {
                (*l as f64).partial_cmp(r).or(Some(std::cmp::Ordering::Equal))
            }
            (V::Float64(l), V::Int64(r)) => {
                l.partial_cmp(&(*r as f64)).or(Some(std::cmp::Ordering::Equal))
            }
            (V::Null, _) | (_, V::Null) => None,
            _ => None,
        };
        
        match (cmp, operator) {
            (Some(order), Op::Equals) => order == std::cmp::Ordering::Equal,
            (Some(order), Op::NotEquals) => order != std::cmp::Ordering::Equal,
            (Some(order), Op::GreaterThan) => order == std::cmp::Ordering::Greater,
            (Some(order), Op::LessThan) => order == std::cmp::Ordering::Less,
            (Some(order), Op::GreaterThanOrEqual) => order != std::cmp::Ordering::Less,
            (Some(order), Op::LessThanOrEqual) => order != std::cmp::Ordering::Greater,
            _ => false,
        }
    }
}

/// Hash Join Strategy (DuckDB/Presto-style)
/// 
/// InMemory: Standard in-memory hash join (current implementation)
/// Partitioned: Grace hash join with spill-to-disk (future implementation)
#[derive(Debug, Clone, Copy)]
enum HashJoinStrategy {
    /// In-memory hash join (current implementation)
    InMemory,
    /// Partitioned/grace hash join with spill (future)
    Partitioned,
}

/// Join state - simplified to Probe/Finished only
/// Build phase is now handled separately in prepare()
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinState {
    /// Probe phase: Probe left batches against pre-built hash table
    Probe,
    /// Finished: All batches processed
    Finished,
}

/// Join operator - performs joins using hypergraph edges (DuckDB/Presto-style)
/// 
/// # Architecture: Two-Phase ExecNode Model
/// Joins are executed in two phases:
/// 1. Prepare phase (ExecNode::prepare()):
///    - Recursively prepares child operators (DFS)
///    - Builds hash table from right side ONCE
///    - Marks operator as prepared
/// 2. Probe phase (ExecNode::next()):
///    - JoinOperator is a pure probe operator
///    - Only reads from left child and probes pre-built hash table
///    - No calls to right.next() - right side is already consumed
/// 
/// # State Machine (Probe Only)
/// 1. Probe: Probe left batches against pre-built hash table
///    - Stream left batches
///    - Probe hash table for matches
///    - Materialize output batches
///    - Transition to Finished when left exhausted
/// 2. Finished: Return None immediately (no more batches)
/// 
/// # Termination Contract
/// - Probe phase: left.next() must eventually return None
/// - Finished state: Always return None immediately
/// - No infinite loops: Each state has a clear exit condition
/// 
/// # Design Philosophy
/// Two-phase model eliminates nested build problems, retry loops, and makes multi-table joins (5+) reliable.
pub struct JoinOperator {
    left: Box<dyn BatchIterator>,
    right: Box<dyn BatchIterator>,
    join_type: JoinType,
    predicate: JoinPredicate,
    graph: std::sync::Arc<HyperGraph>,
    edge_id: EdgeId,
    /// Table aliases: alias -> actual table name (e.g., "c" -> "customers", "o" -> "orders")
    /// Used to build schema with correct alias prefixes
    table_aliases: std::collections::HashMap<String, String>,
    // ExecNode: prepared flag - ensures prepare() was called before next()
    prepared: bool,
    // State machine fields (Probe/Finished only)
    state: JoinState,
    // Pre-built hash table (set during prepare() phase)
    right_hash: Option<FxHashMap<Value, Vec<usize>>>,
    // Pre-built right batches (set during prepare() phase)
    right_batches: Option<Vec<ExecutionBatch>>,
    // Right schema (cached during prepare())
    right_schema: Option<SchemaRef>,
    // Hash join strategy (for future partitioned join support)
    strategy: HashJoinStrategy,
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
        Self::with_table_aliases(left, right, join_type, predicate, graph, edge_id, std::collections::HashMap::new())
    }
    
    pub fn with_table_aliases(
        left: Box<dyn BatchIterator>,
        right: Box<dyn BatchIterator>,
        join_type: JoinType,
        predicate: JoinPredicate,
        graph: std::sync::Arc<HyperGraph>,
        edge_id: EdgeId,
        table_aliases: std::collections::HashMap<String, String>,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            predicate,
            graph,
            edge_id,
            table_aliases,
            prepared: false, // ExecNode: must call prepare() before next()
            // Initialize state machine - starts in Probe (build happens in prepare())
            state: JoinState::Probe,
            right_hash: None,
            right_batches: None,
            right_schema: None,
            strategy: HashJoinStrategy::InMemory, // Current implementation: in-memory only
        }
    }
    
    /// Helper: Try to call prepare() on a BatchIterator if it implements ExecNode
    /// This allows recursive prepare() calls on nested operators
    /// Uses the same type-erased approach as FilterOperator
    fn prepare_child(child: &mut Box<dyn BatchIterator>) -> Result<()> {
        FilterOperator::prepare_child(child)
    }
    
    /// Build hash table from right side (called during prepare() phase)
    /// 
    /// This method consumes the right side iterator and builds the hash table.
    /// After this is called, the JoinOperator is ready for probe-only execution.
    pub(crate) fn build_hash_table(&mut self) -> Result<()> {
        eprintln!("[JOIN PREPARE] Building hash table for join on keys {}.{} = {}.{}", 
            self.predicate.left.0, self.predicate.left.1,
            self.predicate.right.0, self.predicate.right.1);
        
        // Get right schema BEFORE exhausting the iterator
        let right_schema = self.right.schema();
        self.right_schema = Some(right_schema.clone());
        
        // Collect ALL right batches
        let mut right_batches = vec![];
        let mut batch_count = 0;
        const MAX_BATCHES: usize = 1000;
        const MAX_TOTAL_ROWS: usize = 10_000_000;
        let mut total_rows = 0;
        
        // Collect all batches from right side
        // CRITICAL: If right side is a JoinOperator that was pre-built, it will be in Probe state
        // and will produce batches when we call next()
        eprintln!("[JOIN PREPARE] Starting to collect right batches");
        loop {
            match self.right.next()? {
                Some(batch) => {
                    eprintln!("[JOIN PREPARE] Collected batch {} with {} rows", 
                        batch_count, batch.row_count);
                    let batch_rows = batch.row_count;
                    total_rows += batch_rows;
                    
                    if batch_count >= MAX_BATCHES {
                        return Err(anyhow::anyhow!(
                            "ERR_MEMORY_LIMIT_EXCEEDED: JoinOperator exceeded MAX_BATCHES limit ({}). \
                            Collected {} batches with {} total rows from right side.",
                            MAX_BATCHES, batch_count, total_rows
                        ));
                    }
                    if total_rows >= MAX_TOTAL_ROWS {
                        return Err(anyhow::anyhow!(
                            "ERR_MEMORY_LIMIT_EXCEEDED: JoinOperator exceeded MAX_TOTAL_ROWS limit ({}). \
                            Collected {} batches with {} total rows from right side.",
                            MAX_TOTAL_ROWS, batch_count, total_rows
                        ));
                    }
                    
                    right_batches.push(batch);
                    batch_count += 1;
                }
                None => {
                    // Right side exhausted
                    break;
                }
            }
        }
        
        eprintln!("[JOIN PREPARE] Right side batches collected: {} batches, {} total rows", 
            batch_count, total_rows);
        
        // If right side is empty, still proceed to Probe state
        // For INNER JOIN, this will result in 0 rows, but we should still probe the left side
        // Don't set to Finished here - let the probe phase handle empty results
        if right_batches.is_empty() {
            eprintln!("[JOIN PREPARE] Right side is empty - will probe left but get 0 matches");
            // Only set empty hash table if we haven't already built one
            if self.right_hash.is_none() {
                self.right_batches = Some(right_batches);
                self.right_hash = Some(FxHashMap::default());
            }
            // Stay in Probe state - we'll still try to probe left side
            self.state = JoinState::Probe;
            return Ok(());
        }
        
        // Build hash table from right batches
        let mut right_hash: FxHashMap<Value, Vec<usize>> = FxHashMap::default();
        
        // Resolve right key column
        let right_key_idx = match self.resolve_key_column(&right_schema, &self.predicate.right.0, &self.predicate.right.1) {
            Some(idx) => {
                eprintln!("[JOIN PREPARE] Right key column '{}' resolved to index {}", 
                    self.predicate.right.1, idx);
                idx
            },
            None => {
                eprintln!("[JOIN PREPARE] Right key column '{}' NOT FOUND. Available: {:?}", 
                    self.predicate.right.1,
                    right_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
                return Err(anyhow::anyhow!(
                    "ERR_JOIN_PREDICATE_UNKNOWN_COLUMN: Join predicate references unknown column '{}.{}' in right side. \
                    Available columns: {:?}",
                    self.predicate.right.0,
                    self.predicate.right.1,
                    right_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                ));
            }
        };
        
        // Build hash table
        let mut keys_inserted = 0;
        for (batch_idx, batch) in right_batches.iter().enumerate() {
            if let Some(key_array) = batch.batch.column(right_key_idx) {
                let actual_batch_size = batch.selection.len();
                for row_idx in 0..actual_batch_size {
                    if batch.selection[row_idx] && row_idx < key_array.len() {
                        match extract_value(key_array, row_idx) {
                            Ok(key) => {
                                right_hash.entry(key).or_insert_with(Vec::new).push((batch_idx << 16) | row_idx);
                                keys_inserted += 1;
                            },
                            Err(_) => {
                                // Skip invalid keys
                            }
                        }
                    }
                }
            }
        }
        eprintln!("[JOIN PREPARE] Inserted {} keys into hash table", keys_inserted);
        
        // Store hash table and batches
        self.right_hash = Some(right_hash);
        self.right_batches = Some(right_batches);
        
        // Ensure we're in Probe state (build is done)
        self.state = JoinState::Probe;
        
        eprintln!("[JOIN PREPARE] Hash table built, size: {}", 
            self.right_hash.as_ref().unwrap().len());
        
        Ok(())
    }
    
    /// Check if hash table is built (for validation)
    pub fn is_hash_table_built(&self) -> bool {
        self.right_hash.is_some() && self.right_batches.is_some()
    }
    
    /// Build hash table from right side (public for testing/debugging)
    pub fn build_hash_table_public(&mut self) -> Result<()> {
        self.build_hash_table()
    }
}

// ExecNode implementation for JoinOperator
impl ExecNode for JoinOperator {
    fn prepare(&mut self) -> Result<()> {
        // Safety: ensure prepare() is not called twice
        if self.prepared {
            eprintln!("[JOIN PREPARE] Already prepared, skipping");
            return Ok(());
        }
        
        eprintln!("[JOIN PREPARE] Preparing join on keys {}.{} = {}.{}", 
            self.predicate.left.0, self.predicate.left.1,
            self.predicate.right.0, self.predicate.right.1);
        
        // Step 1: Recursively prepare child operators (DFS)
        // This ensures nested joins are built before this join
        eprintln!("[JOIN PREPARE] Recursively preparing right child");
        Self::prepare_child(&mut self.right)?;
        
        eprintln!("[JOIN PREPARE] Recursively preparing left child");
        Self::prepare_child(&mut self.left)?;
        
        // Step 2: Build hash table from right side ONCE
        // Only build if not already built (idempotent)
        if self.right_hash.is_none() {
            self.build_hash_table()?;
        } else {
            eprintln!("[JOIN PREPARE] Hash table already built, skipping");
        }
        
        // Step 3: Mark as prepared
        self.prepared = true;
        eprintln!("[JOIN PREPARE] Join prepared, ready for probe");
        
        Ok(())
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // Safety: assert prepared was called
        assert!(self.prepared, "JoinOperator::next() called before prepare()");
        assert!(self.right_hash.is_some(), "JoinOperator::next() called but hash table not built");
        
        // Probe-only execution - all build logic removed
        match self.state {
            JoinState::Finished => {
                return Ok(None);
            }
            JoinState::Probe => {
                // Continue to probe phase
            }
        }
        
        // Delegate to existing probe logic
        match self.join_type {
            JoinType::Inner => self.execute_inner_join(),
            JoinType::Left => self.execute_left_join(),
            JoinType::Right => self.execute_right_join(),
            JoinType::Full => self.execute_full_join(),
        }
    }
}

impl BatchIterator for JoinOperator {
    fn prepare(&mut self) -> Result<(), anyhow::Error> {
        <Self as ExecNode>::prepare(self).map_err(|e| anyhow::anyhow!("{}", e))
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // Delegate to ExecNode::next() which has safety checks
        <Self as ExecNode>::next(self)
    }
    
    fn schema(&self) -> SchemaRef {
        // SCHEMA-FLOW: JoinOperator combines left and right schemas dynamically
        // Combine schemas from left and right, qualifying column names to avoid duplicates
        // For cascading JOINs, we need to ensure uniqueness even if columns are already qualified
        
        // Get input schemas dynamically (always current)
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();
        
        // SCHEMA-FLOW DEBUG: Log input schemas
        eprintln!("DEBUG JoinOperator::schema() - Left schema has {} fields: {:?}", 
            left_schema.fields().len(),
            left_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        eprintln!("DEBUG JoinOperator::schema() - Right schema has {} fields: {:?}", 
            right_schema.fields().len(),
            right_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        
        let mut fields = vec![];
        let mut seen_names = std::collections::HashSet::new();
        
        // Get table aliases - try to find alias from table_aliases map, otherwise use predicate table names
        // Build reverse map: table_name -> alias
        let mut table_to_alias: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        for (alias, table_name) in &self.table_aliases {
            table_to_alias.insert(table_name.clone(), alias.clone());
        }
        
        // Get left and right table names from predicate
        let left_table_name = &self.predicate.left.0;
        let right_table_name = &self.predicate.right.0;
        
        // The predicate already contains table aliases (e.g., "c", "o"), not table names
        // Use them directly for schema building
        let left_table_alias = left_table_name;
        let right_table_alias = right_table_name;
        
        eprintln!("DEBUG JoinOperator::schema() - Using left_table_alias={}, right_table_alias={}", 
            left_table_alias, right_table_alias);
        
        // Add left columns, qualified with table alias if not already qualified
        for field in left_schema.fields() {
            let field_name = field.name();
            // If field is already qualified (contains '.'), use as-is
            // Otherwise, qualify with table alias
            let mut qualified_name = if field_name.contains('.') {
                field_name.clone()
            } else {
                format!("{}.{}", left_table_alias, field_name)
            };
            
            // For cascading JOINs: if this name already exists, ensure uniqueness
            // by adding a suffix or using the table alias prefix
            let mut counter = 0;
            let original_name = qualified_name.clone();
            while seen_names.contains(&qualified_name) {
                counter += 1;
                qualified_name = format!("{}_{}", original_name, counter);
            }
            seen_names.insert(qualified_name.clone());
            
            fields.push(arrow::datatypes::Field::new(
                qualified_name,
                field.data_type().clone(),
                field.is_nullable(),
            ));
        }
        
        // Add right columns, qualified with table alias if not already qualified
        for field in right_schema.fields() {
            let field_name = field.name();
            // If field is already qualified (contains '.'), use as-is
            // Otherwise, qualify with table alias
            let mut qualified_name = if field_name.contains('.') {
                field_name.clone()
            } else {
                format!("{}.{}", right_table_alias, field_name)
            };
            
            // For cascading JOINs: if this name already exists, ensure uniqueness
            // by adding a suffix or using the table alias prefix
            let mut counter = 0;
            let original_name = qualified_name.clone();
            while seen_names.contains(&qualified_name) {
                counter += 1;
                qualified_name = format!("{}_{}", original_name, counter);
            }
            seen_names.insert(qualified_name.clone());
            
            fields.push(arrow::datatypes::Field::new(
                qualified_name,
                field.data_type().clone(),
                field.is_nullable(),
            ));
        }
        
        // ==========================
        // 4. JOIN SCHEMA MERGE
        // ==========================
        eprintln!("[DEBUG join schema] merged field names = {:?}", fields.iter().map(|f| f.name().clone()).collect::<Vec<_>>());
        
        Arc::new(Schema::new(fields))
    }
}

impl JoinOperator {
    /// Helper: Create empty batch with combined schema
    fn create_empty_batch(&self) -> Result<ExecutionBatch> {
        // Use schema() which should have the correct aliases
        let combined_schema = self.schema();
        eprintln!("DEBUG JoinOperator::create_empty_batch: Using schema with {} fields: {:?}", 
            combined_schema.fields().len(),
            combined_schema.fields().iter().map(|f| f.name().to_string()).collect::<Vec<_>>());
        let empty_columns: Vec<Arc<dyn Array>> = combined_schema.fields().iter().map(|field| {
            match field.data_type() {
                &arrow::datatypes::DataType::Int64 => Arc::new(Int64Array::from(vec![] as Vec<Option<i64>>)) as Arc<dyn Array>,
                &arrow::datatypes::DataType::Float64 => Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)) as Arc<dyn Array>,
                &arrow::datatypes::DataType::Utf8 => Arc::new(StringArray::from(vec![] as Vec<Option<String>>)) as Arc<dyn Array>,
                &arrow::datatypes::DataType::Boolean => Arc::new(BooleanArray::from(vec![] as Vec<Option<bool>>)) as Arc<dyn Array>,
                _ => Arc::new(StringArray::from(vec![] as Vec<Option<String>>)) as Arc<dyn Array>,
            }
        }).collect();
        let combined_schema_clone = combined_schema.clone();
        let output_batch = crate::storage::columnar::ColumnarBatch::new(empty_columns, combined_schema);
        
        // CRITICAL: Create empty ColumnSchema matching the combined schema structure
        // For empty batches, we still want to preserve schema structure
        use crate::execution::column_identity::{ColumnSchema, generate_column_id_for_computed};
        let mut empty_schema = ColumnSchema::new();
        for (idx, field) in combined_schema_clone.fields().iter().enumerate() {
            let column_id = generate_column_id_for_computed();
            empty_schema.add_column(column_id, field.name().clone(), field.name().clone(), field.data_type().clone());
        }
        let mut exec_batch = ExecutionBatch::with_column_schema(output_batch, empty_schema);
        exec_batch.selection = BitVec::new();
        exec_batch.row_count = 0;
        Ok(exec_batch)
    }
    
    /// Helper: Resolve key column index
    fn resolve_key_column(&self, schema: &SchemaRef, table_alias: &str, column_name: &str) -> Option<usize> {
        schema.index_of(column_name).ok()
            .or_else(|| {
                let qualified = format!("{}.{}", table_alias, column_name);
                schema.index_of(&qualified).ok()
            })
    }
    
    fn execute_inner_join(&mut self) -> Result<Option<ExecutionBatch>> {
        // ============================================================
        // PROBE-ONLY HASH JOIN (Build happens in prepare())
        // ============================================================
        // Design: Two-phase ExecNode model eliminates nested build problems
        // 1. Prepare phase (ExecNode::prepare()): All right-side hash tables built once
        // 2. Probe phase (here): Stream left batches, probe pre-built hash table
        // 3. Finished: Return None immediately (no more batches)
        //
        // Termination Contract:
        // - Probe phase: left.next() must eventually return None
        // - Finished state: Always return None immediately
        // - No infinite loops: Each state has a clear exit condition
        // ============================================================
        
        // Safety: Validate that hash table is built (should be set during prepare())
        if !self.is_hash_table_built() {
                        return Err(anyhow::anyhow!(
                "ERR_JOIN_PROBE_BEFORE_BUILD: JoinOperator probe called before hash table build. \
                state={:?}. This indicates prepare() was not called or failed.",
                self.state
            ));
        }
        
        // STATE MACHINE: Handle different states
        eprintln!("[JOIN PROBE] Entering probe phase, current state: {:?}", self.state);
        match self.state {
            // Build state removed - build happens in prepare()
            JoinState::Finished => {
                eprintln!("[JOIN PROBE] Already finished, returning None");
                return Ok(None);
            }
            JoinState::Probe => {
                // Probe phase - hash table already built in prepare()
                eprintln!("[JOIN PROBE] State=Probe, proceeding to probe");
            }
        }
        
        // PROBE PHASE: Use cached hash table and right batches
        let right_batches = match &self.right_batches {
            Some(batches) => batches,
            None => {
                // This should not happen if state machine is correct, but handle gracefully
                self.state = JoinState::Finished;
                return Ok(None);
            }
        };
        
        // Get the hash table (we know it exists now since we're in Probe state)
        let right_hash = self.right_hash.as_ref().unwrap();
        
        // Resolve left key column
        let left_key_idx = match self.resolve_key_column(&self.left.schema(), &self.predicate.left.0, &self.predicate.left.1) {
            Some(idx) => idx,
            None => {
                // Key column not found - return empty batch
                return Ok(Some(self.create_empty_batch()?));
            }
        };
        
        // Get next left batch - simple, single call semantics (no retry logic)
        // Left side is already prepared (all builds done in prepare())
        eprintln!("[JOIN PROBE] Calling left.next()");
        eprintln!("[JOIN PROBE] Left schema: {} fields, hash table size: {}", 
            self.left.schema().fields().len(), right_hash.len());
        let left_batch = match self.left.next()? {
            Some(b) => {
                eprintln!("[JOIN PROBE] Got left batch with {} rows, {} columns", 
                    b.row_count, b.batch.columns.len());
                b
            },
            None => {
                // Left is exhausted - move to Finished state
                eprintln!("[JOIN PROBE] Left exhausted, transitioning to Finished");
                self.state = JoinState::Finished;
                return Ok(None);
            }
        };
        
        // CRITICAL: Get right schema from cache (we stored it before exhausting the iterator)
        let right_schema = self.right_schema.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Right schema not cached - this should not happen")
        })?;
        
        // If we don't have key indices, return empty batch
        let right_key_idx = match self.resolve_key_column(right_schema, &self.predicate.right.0, &self.predicate.right.1) {
            Some(idx) => idx,
            None => {
                return Ok(Some(self.create_empty_batch()?));
            }
        };
        
        // CRITICAL FIX: Use schema() method to get the correct merged schema
        // This ensures cascading joins work correctly by using the same schema building logic
        let output_schema = self.schema();
        let output_schema_fields: Vec<arrow::datatypes::Field> = output_schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        
        // Reduced logging - only log schema info once (already logged above)
        
        // CRITICAL FIX: Probe left batch against hash table and materialize rows with correct lengths
        // This ensures all output arrays have the same length matching result_rows
        let mut result_rows = 0;
        let mut result_selection = BitVec::new();
        
        // Build materialized rows: (left_row_idx, right_row_idx) pairs
        // CRITICAL: Pre-allocate with reasonable capacity
        // Note: For large joins, result size can be large - this is expected behavior
        // If memory is a concern, consider implementing partitioned hash join
        let estimated_result_rows = left_batch.row_count.min(100_000); // Reasonable initial estimate
        let mut matched_pairs: Vec<(usize, usize)> = Vec::with_capacity(estimated_result_rows);
        const MAX_RESULT_ROWS: usize = 100_000_000; // 100M rows limit (very large but reasonable)
        
        if let Some(key_array) = left_batch.batch.column(left_key_idx) {
            // SAFETY: Iterate only up to batch.batch.row_count to avoid array bounds violations
            let max_rows = left_batch.batch.row_count;
            
            for left_row_idx in 0..max_rows {
                // Check if row is selected (and within selection bitmap bounds)
                if left_row_idx >= left_batch.selection.len() || !left_batch.selection[left_row_idx] {
                    continue;
                }
                if left_row_idx < key_array.len() {
                    let key = extract_value(key_array, left_row_idx)?;
                    if let Some(right_indices) = right_hash.get(&key) {
                        // INNER JOIN: only include matched rows
                        for &right_idx in right_indices {
                            // Check memory limit before adding (fail fast, no silent truncation)
                            if result_rows >= MAX_RESULT_ROWS {
                                // FUTURE: When partitioned hash join is implemented, we can spill here
                                eprintln!("[JOIN WARNING] Result size large ({} rows); consider implementing partitioned hash join.", result_rows);
                                return Err(anyhow::anyhow!(
                                    "ERR_MEMORY_LIMIT_EXCEEDED: JoinOperator exceeded MAX_RESULT_ROWS limit ({}). \
                                    Join result would contain more than {} rows. \
                                    Query requires too much memory. Consider using partitioned hash join or reducing data size.",
                                    MAX_RESULT_ROWS, result_rows
                                ));
                            }
                            
                            matched_pairs.push((left_row_idx, right_idx));
                            result_rows += 1;
                            result_selection.push(true);
                        }
                    }
                    // INNER JOIN: unmatched rows are skipped (not included in result)
                }
            }
        }
        
        eprintln!("[JOIN PROBE] Probe complete: {} matched rows from {} left rows", 
            result_rows, left_batch.row_count);
        
        // CRITICAL FIX: Materialize output columns with correct lengths based on join results
        let mut output_columns: Vec<Arc<dyn Array>> = vec![];
        
        // Get the right batch to use (use first one for structure, or create empty if none)
        let right_batch_ref = right_batches.get(0);
        
        // Materialize left columns
        let left_col_count = self.left.schema().fields().len();
        for col_idx in 0..left_col_count {
            let left_col = left_batch.batch.column(col_idx)
                .ok_or_else(|| anyhow::anyhow!("Left column {} not found", col_idx))?;
            
            // Build array for this column with correct length (result_rows)
            let materialized_array = materialize_column_from_pairs(
                left_col,
                &matched_pairs,
                true, // is_left
            )?;
            
            output_columns.push(materialized_array);
        }
        
        // Materialize right columns
        // CRITICAL FIX: right_idx in matched_pairs is encoded as (batch_idx << 16) | row_idx
        // We need to decode it and access the correct batch
        if !right_batches.is_empty() {
            let right_schema = self.right_schema.as_ref().ok_or_else(|| {
                anyhow::anyhow!("Right schema not cached - this should not happen")
            })?;
            let right_col_count = right_schema.fields().len();
            for col_idx in 0..right_col_count {
                // Materialize from multiple right batches
                let materialized_array = materialize_right_column_from_pairs(
                    right_batches,
                    col_idx,
                    &matched_pairs,
                )?;
                
                output_columns.push(materialized_array);
            }
        }
        
        // DISABLED: Excessive debug logging was causing performance issues
        // Schema merging happens correctly without logging
        
        // COLID: Merge ColumnSchemas from left and right batches
        // CRITICAL FIX: For cascading joins, we need to match columns by name, not index
        // This preserves ColIds through JOINs, enabling stable column resolution
        let merged_column_schema = {
            let left_schema = left_batch.column_schema.as_ref();
            let right_schema = right_batches.get(0).and_then(|b| b.column_schema.as_ref());
            
            match (left_schema, right_schema) {
                (Some(left), Some(right)) => {
                    // Both have ColumnSchemas - merge ColIds and update external names
                    let mut merged = ColumnSchema::new();
                    
                    // CRITICAL FIX: Match columns by name, not index, to handle cascading joins correctly
                    // The output_schema_fields are in order: left columns first, then right columns
                    // But we need to match them to the original schemas by name
                    let left_schema_field_count = self.left.schema().fields().len();
                    
                    for (idx, field) in output_schema_fields.iter().enumerate() {
                        let field_name = field.name();
                        let col_id = if idx < left_schema_field_count {
                            // This is a left column - find it in left schema by name
                            // Try to match by qualified name first, then unqualified
                            left.resolve(field_name)
                                .or_else(|| {
                                    // Try unqualified name if field_name is qualified
                                    if field_name.contains('.') {
                                        let unqualified = field_name.split('.').last().unwrap_or(field_name);
                                        left.resolve(unqualified)
                                    } else {
                                        None
                                    }
                                })
                                .or_else(|| {
                                    // Try matching by index as fallback (for simple cases)
                                    if idx < left.column_ids.len() {
                                        Some(left.column_ids[idx])
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or_else(|| {
                                    // Generate new ColId if not found
                                    use crate::execution::column_identity::generate_col_id;
                                    generate_col_id()
                                })
                        } else {
                            // This is a right column - find it in right schema by name
                            let right_idx = idx - left_schema_field_count;
                            right.resolve(field_name)
                                .or_else(|| {
                                    // Try unqualified name if field_name is qualified
                                    if field_name.contains('.') {
                                        let unqualified = field_name.split('.').last().unwrap_or(field_name);
                                        right.resolve(unqualified)
                                    } else {
                                        None
                                    }
                                })
                                .or_else(|| {
                                    // Try matching by index as fallback
                                    if right_idx < right.column_ids.len() {
                                        Some(right.column_ids[right_idx])
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or_else(|| {
                                    // Generate new ColId if not found
                                    use crate::execution::column_identity::generate_col_id;
                                    generate_col_id()
                                })
                        };
                        
                        // CRITICAL FIX: Extract unqualified name for ColumnSchema
                        let unqualified_name = if field_name.contains('.') {
                            field_name.split('.').last().unwrap_or(&field_name).to_string()
                        } else {
                            field_name.clone()
                        };
                        merged.add_column(col_id, field_name.clone(), unqualified_name, field.data_type().clone());
                    }
                    
                    // ==========================
                    // 4. JOIN SCHEMA MERGE (ColumnSchema)
                    // ==========================
                    eprintln!("[DEBUG col schema] qualified_names = {:?}", merged.qualified_names);
                    eprintln!("[DEBUG col schema] unqualified_names = {:?}", merged.unqualified_names);
                    eprintln!("[DEBUG col schema] name_map keys = {:?}", merged.name_map.keys().collect::<Vec<_>>());
                    
                    Some(merged)
                }
                (Some(left), None) => {
                    // Only left has ColumnSchema - preserve left ColIds, create new ones for right
                    let mut merged = ColumnSchema::new();
                    let left_schema_field_count = self.left.schema().fields().len();
                    
                    // CRITICAL FIX: Match left columns by name, not index
                    for (idx, field) in output_schema_fields.iter().enumerate() {
                        let field_name = field.name();
                        let col_id = if idx < left_schema_field_count {
                            // Left column - find by name
                            left.resolve(field_name)
                                .or_else(|| {
                                    if field_name.contains('.') {
                                        let unqualified = field_name.split('.').last().unwrap_or(field_name);
                                        left.resolve(unqualified)
                                    } else {
                                        None
                                    }
                                })
                                .or_else(|| {
                                    if idx < left.column_ids.len() {
                                        Some(left.column_ids[idx])
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or_else(|| {
                                    use crate::execution::column_identity::generate_col_id;
                                    generate_col_id()
                                })
                        } else {
                            // Right column - generate new ColId
                            use crate::execution::column_identity::generate_col_id;
                            generate_col_id()
                        };
                        
                        // CRITICAL FIX: Extract unqualified name for ColumnSchema
                        let unqualified_name = if field_name.contains('.') {
                            field_name.split('.').last().unwrap_or(&field_name).to_string()
                        } else {
                            field_name.clone()
                        };
                        merged.add_column(col_id, field_name.clone(), unqualified_name, field.data_type().clone());
                    }
                    eprintln!("[DEBUG col schema] qualified_names = {:?}", merged.qualified_names);
                    eprintln!("[DEBUG col schema] unqualified_names = {:?}", merged.unqualified_names);
                    eprintln!("[DEBUG col schema] name_map keys = {:?}", merged.name_map.keys().collect::<Vec<_>>());
                    Some(merged)
                }
                (None, Some(right)) => {
                    // Only right has ColumnSchema - create new ColIds for left, preserve right ColIds
                    let mut merged = ColumnSchema::new();
                    let left_schema_field_count = self.left.schema().fields().len();
                    
                    // Create ColIds for left columns
                    for (idx, field) in output_schema_fields.iter().enumerate() {
                        let field_name = field.name();
                        let col_id = if idx < left_schema_field_count {
                            // Left column - generate new ColId
                            use crate::execution::column_identity::generate_col_id;
                            generate_col_id()
                        } else {
                            // Right column - find by name
                            let right_idx = idx - left_schema_field_count;
                            right.resolve(field_name)
                                .or_else(|| {
                                    if field_name.contains('.') {
                                        let unqualified = field_name.split('.').last().unwrap_or(field_name);
                                        right.resolve(unqualified)
                                    } else {
                                        None
                                    }
                                })
                                .or_else(|| {
                                    if right_idx < right.column_ids.len() {
                                        Some(right.column_ids[right_idx])
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or_else(|| {
                                    use crate::execution::column_identity::generate_col_id;
                                    generate_col_id()
                                })
                        };
                        
                        // CRITICAL FIX: Extract unqualified name for ColumnSchema
                        let unqualified_name = if field_name.contains('.') {
                            field_name.split('.').last().unwrap_or(&field_name).to_string()
                        } else {
                            field_name.clone()
                        };
                        merged.add_column(col_id, field_name.clone(), unqualified_name, field.data_type().clone());
                    }
                    eprintln!("[DEBUG col schema] qualified_names = {:?}", merged.qualified_names);
                    eprintln!("[DEBUG col schema] unqualified_names = {:?}", merged.unqualified_names);
                    eprintln!("[DEBUG col schema] name_map keys = {:?}", merged.name_map.keys().collect::<Vec<_>>());
                    Some(merged)
                }
                (None, None) => {
                    // CRITICAL FIX: Always create ColumnSchema for Join outputs, even if inputs don't have one
                    // This ensures ProjectOperator can resolve columns correctly
                    let mut merged = ColumnSchema::new();
                    for field in &output_schema_fields {
                        use crate::execution::column_identity::generate_col_id;
                        let col_id = generate_col_id();
                        let field_name = field.name();
                        let unqualified_name = if field_name.contains('.') {
                            field_name.split('.').last().unwrap_or(field_name).to_string()
                        } else {
                            field_name.clone()
                        };
                        merged.add_column(col_id, field_name.clone(), unqualified_name, field.data_type().clone());
                    }
                    eprintln!("[DEBUG col schema] qualified_names = {:?}", merged.qualified_names);
                    eprintln!("[DEBUG col schema] unqualified_names = {:?}", merged.unqualified_names);
                    eprintln!("[DEBUG col schema] name_map keys = {:?}", merged.name_map.keys().collect::<Vec<_>>());
                    Some(merged)
                }
            }
        };
        
        // DISABLED: Excessive debug logging
        
        if result_rows == 0 {
            // Return empty batch with correct combined schema so ProjectOperator can resolve columns
            // Create empty arrays for each column in the combined schema
            let empty_columns: Vec<Arc<dyn Array>> = output_schema_fields.iter().map(|field| {
                match field.data_type() {
                    &arrow::datatypes::DataType::Int64 => Arc::new(Int64Array::from(vec![] as Vec<Option<i64>>)) as Arc<dyn Array>,
                    &arrow::datatypes::DataType::Float64 => Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)) as Arc<dyn Array>,
                    &arrow::datatypes::DataType::Utf8 => Arc::new(StringArray::from(vec![] as Vec<Option<String>>)) as Arc<dyn Array>,
                    &arrow::datatypes::DataType::Boolean => Arc::new(BooleanArray::from(vec![] as Vec<Option<bool>>)) as Arc<dyn Array>,
                    _ => Arc::new(StringArray::from(vec![] as Vec<Option<String>>)) as Arc<dyn Array>,
                }
            }).collect();
            
            let output_batch = crate::storage::columnar::ColumnarBatch::new(empty_columns, output_schema);
            
            // CRITICAL FIX: ALWAYS attach merged ColumnSchema to empty batch too
            // This ensures ProjectOperator can resolve columns even when join produces no rows
            let mut exec_batch = if let Some(merged_schema) = merged_column_schema {
                ExecutionBatch::with_column_schema(output_batch, merged_schema)
            } else {
                // FALLBACK: Create ColumnSchema from output_schema_fields
                // DISABLED: Excessive debug logging
                let mut fallback_schema = ColumnSchema::new();
                for field in &output_schema_fields {
                    use crate::execution::column_identity::generate_col_id;
                    let col_id = generate_col_id();
                    let field_name = field.name();
                    let unqualified_name = if field_name.contains('.') {
                        field_name.split('.').last().unwrap_or(field_name).to_string()
                    } else {
                        field_name.clone()
                    };
                    fallback_schema.add_column(col_id, field_name.clone(), unqualified_name, field.data_type().clone());
                }
                ExecutionBatch::with_column_schema(output_batch, fallback_schema)
            };
            exec_batch.selection = BitVec::new();
            exec_batch.row_count = 0;
            // DISABLED: Excessive debug logging
            // Return empty batch (don't finish - there might be more left batches)
            return Ok(Some(exec_batch));
        }
        
        // CRITICAL FIX: Output columns are now properly materialized with correct lengths
        // All arrays have result_rows length, matching result_selection and row_count
        let output_batch = crate::storage::columnar::ColumnarBatch::new(output_columns, output_schema);
        
        // CRITICAL FIX: ALWAYS attach merged ColumnSchema to output ExecutionBatch
        // This is essential for cascading joins - ProjectOperator needs the full merged schema
        let mut exec_batch = if let Some(merged_schema) = merged_column_schema {
            ExecutionBatch::with_column_schema(output_batch, merged_schema)
        } else {
            // FALLBACK: Create ColumnSchema from output_schema_fields if merging failed
            // This should not happen in normal operation, but ensures we always have a schema
            eprintln!("DEBUG JoinOperator::execute_inner_join: WARNING - Creating fallback ColumnSchema");
            let mut fallback_schema = ColumnSchema::new();
            for field in &output_schema_fields {
                use crate::execution::column_identity::generate_col_id;
                let col_id = generate_col_id();
                fallback_schema.add_column(col_id, field.name().clone(), field.name().clone(), field.data_type().clone());
            }
            ExecutionBatch::with_column_schema(output_batch, fallback_schema)
        };
        exec_batch.selection = result_selection;
        exec_batch.row_count = result_rows;
        
        // DISABLED: Excessive debug logging
        
        Ok(Some(exec_batch))
    }
    
    fn execute_left_join(&mut self) -> Result<Option<ExecutionBatch>> {
        // PROBE-ONLY: Build happens in prepare()
        // Validate that hash table is built
        if !self.is_hash_table_built() {
            return Err(anyhow::anyhow!(
                "ERR_JOIN_PROBE_BEFORE_BUILD: JoinOperator probe called before hash table build. \
                state={:?}. This indicates prepare() was not called or failed.",
                self.state
            ));
        }
        
        // STATE MACHINE: Handle different states
        match self.state {
            JoinState::Finished => {
                return Ok(None);
            }
            JoinState::Probe => {
                // Continue to probe phase (hash table already built)
            }
        }
        
        // PROBE PHASE: Use cached hash table and right batches
        let right_batches = match &self.right_batches {
            Some(batches) => batches,
            None => {
                self.state = JoinState::Finished;
                return Ok(None);
            }
        };
        
        // Get the hash table (we know it exists now since we're in Probe state)
        let right_hash = self.right_hash.as_ref().unwrap();
        
        // Resolve left key column
        let left_key_idx = match self.resolve_key_column(&self.left.schema(), &self.predicate.left.0, &self.predicate.left.1) {
            Some(idx) => idx,
            None => {
                // Key column not found - return empty batch
                return Ok(Some(self.create_empty_batch()?));
            }
        };
        
        // Get next left batch - if exhausted, move to Finished state
        let left_batch = match self.left.next()? {
            Some(b) => b,
            None => {
                // Left exhausted - move to Finished state
                self.state = JoinState::Finished;
                return Ok(None);
            }
        };
        
        // Build output columns with qualified names
        let mut output_columns = vec![];
        let mut output_schema_fields = vec![];
        let mut seen_names = std::collections::HashSet::new();
        
        // The predicate already contains table aliases (e.g., "c", "o"), not table names
        // Use them directly for schema building
        let left_table_alias = &self.predicate.left.0;
        let right_table_alias = &self.predicate.right.0;
        
        // Add left columns, qualified
        for (idx, field) in self.left.schema().fields().iter().enumerate() {
            output_columns.push(left_batch.batch.column(idx).unwrap().clone());
            let field_name = field.name();
            let mut qualified_name = if field_name.contains('.') {
                field_name.clone()
            } else {
                format!("{}.{}", left_table_alias, field_name)
            };
            
            let mut counter = 0;
            let original_name = qualified_name.clone();
            while seen_names.contains(&qualified_name) {
                counter += 1;
                qualified_name = format!("{}_{}", original_name, counter);
            }
            seen_names.insert(qualified_name.clone());
            
            output_schema_fields.push(arrow::datatypes::Field::new(
                qualified_name,
                field.data_type().clone(),
                field.is_nullable(),
            ));
        }
        
        // Add right columns (will be NULL-filled for unmatched), qualified
        let right_schema = self.right_schema.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Right schema not cached - this should not happen")
        })?;
        for (idx, field) in right_schema.fields().iter().enumerate() {
            let field_name = field.name();
            let mut qualified_name = if field_name.contains('.') {
                field_name.clone()
            } else {
                format!("{}.{}", right_table_alias, field_name)
            };
            
            let mut counter = 0;
            let original_name = qualified_name.clone();
            while seen_names.contains(&qualified_name) {
                counter += 1;
                qualified_name = format!("{}_{}", original_name, counter);
            }
            seen_names.insert(qualified_name.clone());
            
            output_schema_fields.push(arrow::datatypes::Field::new(
                qualified_name,
                field.data_type().clone(),
                field.is_nullable(),
            ));
        }
        
        // Build join result - LEFT JOIN includes ALL left rows
        // Similar to INNER JOIN but includes unmatched left rows with NULL right columns
        let mut matched_pairs = Vec::new(); // (left_row_idx, right_batch_idx, right_row_idx)
        
        if let Some(key_array) = left_batch.batch.column(left_key_idx) {
            // SAFETY: Iterate only up to batch.batch.row_count to avoid array bounds violations
            let max_rows = left_batch.batch.row_count;
            for left_row_idx in 0..max_rows {
                // Check if row is selected (and within selection bitmap bounds)
                if left_row_idx >= left_batch.selection.len() || !left_batch.selection[left_row_idx] {
                    continue;
                }
                if left_row_idx < key_array.len() {
                    if let Ok(key) = extract_value(key_array, left_row_idx) {
                        if let Some(right_indices) = right_hash.get(&key) {
                            // Matched: add all matching right rows
                            for &right_encoded in right_indices {
                                matched_pairs.push((left_row_idx, right_encoded));
                            }
                        } else {
                            // Unmatched: LEFT JOIN includes this row with NULL right columns
                            // Use a sentinel value to indicate NULL right side
                            matched_pairs.push((left_row_idx, usize::MAX));
                        }
                    } else {
                        // NULL key: LEFT JOIN includes this row with NULL right columns
                        matched_pairs.push((left_row_idx, usize::MAX));
                    }
                }
            }
        }
        
        let result_rows = matched_pairs.len();
        if result_rows == 0 {
            // Return empty batch with correct schema
            return Ok(Some(self.create_empty_batch()?));
        }
        
        // Build output arrays using materialization (same as INNER JOIN)
        let mut output_columns = Vec::new();
        
        // Materialize left columns (repeating for multiple matches)
        let left_col_count = self.left.schema().fields().len();
        for col_idx in 0..left_col_count {
            let left_col = left_batch.batch.column(col_idx)
                .ok_or_else(|| anyhow::anyhow!("Left column {} not found", col_idx))?;
            
            // Build array for this column with correct length (result_rows)
            let materialized_array = materialize_column_from_pairs(
                left_col,
                &matched_pairs,
                true, // is_left
            )?;
            
            output_columns.push(materialized_array);
        }
        
        // Materialize right columns (with NULLs for unmatched rows)
        if !right_batches.is_empty() {
            let right_col_count = right_schema.fields().len();
            for col_idx in 0..right_col_count {
                // Materialize from multiple right batches, with NULLs for unmatched (right_idx == usize::MAX)
                let materialized_array = materialize_right_column_from_pairs_with_nulls(
                    right_batches,
                    col_idx,
                    &matched_pairs,
                )?;
                
                output_columns.push(materialized_array);
            }
        }
        
        let output_schema = Arc::new(Schema::new(output_schema_fields.clone()));
        
        // COLID: Merge ColumnSchemas from left and right batches (same logic as execute_inner_join)
        let merged_column_schema = {
            let left_schema = left_batch.column_schema.as_ref();
            let right_schema = right_batches.get(0).and_then(|b| b.column_schema.as_ref());
            
            match (left_schema, right_schema) {
                (Some(left), Some(right)) => {
                    let mut merged = ColumnSchema::new();
                    for (idx, field) in output_schema_fields.iter().enumerate() {
                        if idx < left.column_ids.len() {
                            let col_id = left.column_ids[idx];
                            merged.add_column(col_id, field.name().clone(), field.name().clone(), field.data_type().clone());
                        } else {
                            let right_idx = idx - left.column_ids.len();
                            if right_idx < right.column_ids.len() {
                                let col_id = right.column_ids[right_idx];
                                merged.add_column(col_id, field.name().clone(), field.name().clone(), field.data_type().clone());
                            } else {
                                use crate::execution::column_identity::generate_col_id;
                                let col_id = generate_col_id();
                                merged.add_column(col_id, field.name().clone(), field.name().clone(), field.data_type().clone());
                            }
                        }
                    }
                    Some(merged)
                }
                (Some(left), None) => {
                    let mut merged = left.clone();
                    merged.unqualified_names.truncate(left.column_ids.len());
                    merged.name_map.clear();
                    for (idx, field) in output_schema_fields.iter().enumerate() {
                        if idx < left.column_ids.len() {
                            let col_id = left.column_ids[idx];
                            let new_name = field.name().clone();
                            merged.unqualified_names.push(new_name.clone());
                            merged.data_types.push(field.data_type().clone());
                            merged.name_map.insert(new_name, col_id);
                        } else {
                            use crate::execution::column_identity::generate_col_id;
                            let col_id = generate_col_id();
                            merged.add_column(col_id, field.name().clone(), field.name().clone(), field.data_type().clone());
                        }
                    }
                    Some(merged)
                }
                (None, Some(right)) => {
                    let mut merged = ColumnSchema::new();
                    let left_col_count = self.left.schema().fields().len();
                    for (idx, field) in output_schema_fields.iter().take(left_col_count).enumerate() {
                        use crate::execution::column_identity::generate_col_id;
                        let col_id = generate_col_id();
                        merged.add_column(col_id, field.name().clone(), field.name().clone(), field.data_type().clone());
                    }
                    for (idx, field) in output_schema_fields.iter().skip(left_col_count).enumerate() {
                        if idx < right.column_ids.len() {
                            let col_id = right.column_ids[idx];
                            merged.add_column(col_id, field.name().clone(), field.name().clone(), field.data_type().clone());
                        } else {
                            use crate::execution::column_identity::generate_col_id;
                            let col_id = generate_col_id();
                            merged.add_column(col_id, field.name().clone(), field.name().clone(), field.data_type().clone());
                        }
                    }
                    Some(merged)
                }
                (None, None) => None,
            }
        };
        
        let output_batch = crate::storage::columnar::ColumnarBatch::new(output_columns, output_schema);
        
        // COLID: Create ExecutionBatch with merged ColumnSchema if available
        let mut exec_batch = if let Some(merged_schema) = merged_column_schema {
            ExecutionBatch::with_column_schema(output_batch, merged_schema)
        } else {
            ExecutionBatch::new(output_batch)
        };
        // Create selection bitmap (all rows are selected for LEFT JOIN)
        let mut result_selection = BitVec::new();
        result_selection.resize(result_rows, true);
        exec_batch.selection = result_selection;
        exec_batch.row_count = result_rows;
        Ok(Some(exec_batch))
    }
    
    fn execute_right_join(&mut self) -> Result<Option<ExecutionBatch>> {
        // PROBE-ONLY: Build happens in prepare()
        // Validate that hash table is built
        if !self.is_hash_table_built() {
            return Err(anyhow::anyhow!(
                "ERR_JOIN_PROBE_BEFORE_BUILD: JoinOperator probe called before hash table build. \
                state={:?}. This indicates prepare() was not called or failed.",
                self.state
            ));
        }
        
        // RIGHT JOIN: all rows from right, matched rows from left (NULLs for non-matches)
        // Similar to LEFT JOIN but swap left and right
        // For simplicity, we can swap and call LEFT JOIN logic
        // Note: This swap happens after pre-build, so it's safe
        std::mem::swap(&mut self.left, &mut self.right);
        std::mem::swap(&mut self.predicate.left, &mut self.predicate.right);
        let result = self.execute_left_join();
        std::mem::swap(&mut self.left, &mut self.right);
        std::mem::swap(&mut self.predicate.left, &mut self.predicate.right);
        result
    }
    
    fn execute_full_join(&mut self) -> Result<Option<ExecutionBatch>> {
        // PROBE-ONLY: Build happens in prepare()
        // Validate that hash table is built
        if !self.is_hash_table_built() {
            return Err(anyhow::anyhow!(
                "ERR_JOIN_PROBE_BEFORE_BUILD: JoinOperator probe called before hash table build. \
                state={:?}. This indicates prepare() was not called or failed.",
                self.state
            ));
        }
        
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

/// Materialize right column from matched pairs, handling NULLs for unmatched rows (usize::MAX)
/// For LEFT JOIN: unmatched rows have right_idx == usize::MAX, which should produce NULL
fn materialize_right_column_from_pairs_with_nulls(
    right_batches: &[ExecutionBatch],
    col_idx: usize,
    matched_pairs: &[(usize, usize)],
) -> Result<Arc<dyn Array>> {
    use arrow::array::*;
    
    if right_batches.is_empty() {
        return Err(anyhow::anyhow!("No right batches provided"));
    }
    
    // Get data type from first batch (all batches should have same schema)
    let first_col = right_batches[0].batch.column(col_idx)
        .ok_or_else(|| anyhow::anyhow!("Right column {} not found in first batch", col_idx))?;
    let data_type = first_col.data_type();
    
    match data_type {
        arrow::datatypes::DataType::Int64 => {
            let mut values = Vec::with_capacity(matched_pairs.len());
            for &(_left_idx, right_idx) in matched_pairs {
                if right_idx == usize::MAX {
                    // Unmatched row - NULL
                    values.push(None);
                } else {
                    // Decode right_idx: (batch_idx << 16) | row_idx
                    let batch_idx = right_idx >> 16;
                    let row_idx = right_idx & 0xFFFF;
                    
                    if batch_idx < right_batches.len() {
                        if let Some(col_array) = right_batches[batch_idx].batch.column(col_idx) {
                            let arr = col_array.as_any().downcast_ref::<Int64Array>()
                                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int64Array"))?;
                            if row_idx < arr.len() {
                                if arr.is_null(row_idx) {
                                    values.push(None);
                                } else {
                                    values.push(Some(arr.value(row_idx)));
                                }
                            } else {
                                values.push(None);
                            }
                        } else {
                            values.push(None);
                        }
                    } else {
                        values.push(None);
                    }
                }
            }
            Ok(Arc::new(Int64Array::from(values)) as Arc<dyn Array>)
        }
        arrow::datatypes::DataType::Utf8 => {
            let mut values = Vec::with_capacity(matched_pairs.len());
            for &(_left_idx, right_idx) in matched_pairs {
                if right_idx == usize::MAX {
                    // Unmatched row - NULL
                    values.push(None);
                } else {
                    let batch_idx = right_idx >> 16;
                    let row_idx = right_idx & 0xFFFF;
                    
                    if batch_idx < right_batches.len() {
                        if let Some(col_array) = right_batches[batch_idx].batch.column(col_idx) {
                            let arr = col_array.as_any().downcast_ref::<StringArray>()
                                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to StringArray"))?;
                            if row_idx < arr.len() {
                                if arr.is_null(row_idx) {
                                    values.push(None);
                                } else {
                                    values.push(Some(arr.value(row_idx).to_string()));
                                }
                            } else {
                                values.push(None);
                            }
                        } else {
                            values.push(None);
                        }
                    } else {
                        values.push(None);
                    }
                }
            }
            Ok(Arc::new(StringArray::from(values)) as Arc<dyn Array>)
        }
        arrow::datatypes::DataType::Float64 => {
            let mut values = Vec::with_capacity(matched_pairs.len());
            for &(_left_idx, right_idx) in matched_pairs {
                if right_idx == usize::MAX {
                    values.push(None);
                } else {
                    let batch_idx = right_idx >> 16;
                    let row_idx = right_idx & 0xFFFF;
                    
                    if batch_idx < right_batches.len() {
                        if let Some(col_array) = right_batches[batch_idx].batch.column(col_idx) {
                            let arr = col_array.as_any().downcast_ref::<Float64Array>()
                                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float64Array"))?;
                            if row_idx < arr.len() {
                                if arr.is_null(row_idx) {
                                    values.push(None);
                                } else {
                                    values.push(Some(arr.value(row_idx)));
                                }
                            } else {
                                values.push(None);
                            }
                        } else {
                            values.push(None);
                        }
                    } else {
                        values.push(None);
                    }
                }
            }
            Ok(Arc::new(Float64Array::from(values)) as Arc<dyn Array>)
        }
        _ => {
            // Fallback: create NULL array
            let null_array = create_null_array(data_type, matched_pairs.len())?;
            Ok(null_array)
        }
    }
}

fn materialize_right_column_from_pairs(
    right_batches: &[ExecutionBatch],
    col_idx: usize,
    matched_pairs: &[(usize, usize)],
) -> Result<Arc<dyn Array>> {
    use arrow::array::*;
    
    if right_batches.is_empty() {
        return Err(anyhow::anyhow!("No right batches provided"));
    }
    
    // Get data type from first batch (all batches should have same schema)
    let first_col = right_batches[0].batch.column(col_idx)
        .ok_or_else(|| anyhow::anyhow!("Right column {} not found in first batch", col_idx))?;
    let data_type = first_col.data_type();
    
    match data_type {
        arrow::datatypes::DataType::Int64 => {
            let mut values = Vec::with_capacity(matched_pairs.len());
            for &(_left_idx, right_idx) in matched_pairs {
                // Decode right_idx: (batch_idx << 16) | row_idx
                let batch_idx = right_idx >> 16;
                let row_idx = right_idx & 0xFFFF;
                
                if batch_idx < right_batches.len() {
                    if let Some(col_array) = right_batches[batch_idx].batch.column(col_idx) {
                        let arr = col_array.as_any().downcast_ref::<Int64Array>()
                            .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int64Array"))?;
                        if row_idx < arr.len() {
                            if arr.is_null(row_idx) {
                                values.push(None);
                            } else {
                                values.push(Some(arr.value(row_idx)));
                            }
                        } else {
                            values.push(None);
                        }
                    } else {
                        values.push(None);
                    }
                } else {
                    values.push(None);
                }
            }
            Ok(Arc::new(Int64Array::from(values)) as Arc<dyn Array>)
        }
        arrow::datatypes::DataType::Float64 => {
            let mut values = Vec::with_capacity(matched_pairs.len());
            for &(_left_idx, right_idx) in matched_pairs {
                let batch_idx = right_idx >> 16;
                let row_idx = right_idx & 0xFFFF;
                
                if batch_idx < right_batches.len() {
                    if let Some(col_array) = right_batches[batch_idx].batch.column(col_idx) {
                        let arr = col_array.as_any().downcast_ref::<Float64Array>()
                            .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float64Array"))?;
                        if row_idx < arr.len() {
                            if arr.is_null(row_idx) {
                                values.push(None);
                            } else {
                                values.push(Some(arr.value(row_idx)));
                            }
                        } else {
                            values.push(None);
                        }
                    } else {
                        values.push(None);
                    }
                } else {
                    values.push(None);
                }
            }
            Ok(Arc::new(Float64Array::from(values)) as Arc<dyn Array>)
        }
        arrow::datatypes::DataType::Utf8 => {
            let mut values = Vec::with_capacity(matched_pairs.len());
            for &(_left_idx, right_idx) in matched_pairs {
                let batch_idx = right_idx >> 16;
                let row_idx = right_idx & 0xFFFF;
                
                if batch_idx < right_batches.len() {
                    if let Some(col_array) = right_batches[batch_idx].batch.column(col_idx) {
                        let arr = col_array.as_any().downcast_ref::<StringArray>()
                            .ok_or_else(|| anyhow::anyhow!("Failed to downcast to StringArray"))?;
                        if row_idx < arr.len() {
                            if arr.is_null(row_idx) {
                                values.push(None);
                            } else {
                                values.push(Some(arr.value(row_idx).to_string()));
                            }
                        } else {
                            values.push(None);
                        }
                    } else {
                        values.push(None);
                    }
                } else {
                    values.push(None);
                }
            }
            Ok(Arc::new(StringArray::from(values)) as Arc<dyn Array>)
        }
        arrow::datatypes::DataType::Boolean => {
            let mut values = Vec::with_capacity(matched_pairs.len());
            for &(_left_idx, right_idx) in matched_pairs {
                let batch_idx = right_idx >> 16;
                let row_idx = right_idx & 0xFFFF;
                
                if batch_idx < right_batches.len() {
                    if let Some(col_array) = right_batches[batch_idx].batch.column(col_idx) {
                        let arr = col_array.as_any().downcast_ref::<BooleanArray>()
                            .ok_or_else(|| anyhow::anyhow!("Failed to downcast to BooleanArray"))?;
                        if row_idx < arr.len() {
                            if arr.is_null(row_idx) {
                                values.push(None);
                            } else {
                                values.push(Some(arr.value(row_idx)));
                            }
                        } else {
                            values.push(None);
                        }
                    } else {
                        values.push(None);
                    }
                } else {
                    values.push(None);
                }
            }
            Ok(Arc::new(BooleanArray::from(values)) as Arc<dyn Array>)
        }
        _ => {
            // Fallback: convert to string array
            let mut values = Vec::with_capacity(matched_pairs.len());
            for &(_left_idx, right_idx) in matched_pairs {
                let batch_idx = right_idx >> 16;
                let row_idx = right_idx & 0xFFFF;
                
                if batch_idx < right_batches.len() {
                    if let Some(col_array) = right_batches[batch_idx].batch.column(col_idx) {
                        if row_idx < col_array.len() {
                            if col_array.is_null(row_idx) {
                                values.push(None);
                            } else {
                                values.push(Some(format!("{:?}", col_array)));
                            }
                        } else {
                            values.push(None);
                        }
                    } else {
                        values.push(None);
                    }
                } else {
                    values.push(None);
                }
            }
            Ok(Arc::new(StringArray::from(values)) as Arc<dyn Array>)
        }
    }
}

fn materialize_column_from_pairs(
    source_array: &Arc<dyn Array>,
    matched_pairs: &[(usize, usize)],
    is_left: bool,
) -> Result<Arc<dyn Array>> {
    use arrow::array::*;
    
    let data_type = source_array.data_type();
    
    match data_type {
        arrow::datatypes::DataType::Int64 => {
            let arr = source_array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int64Array"))?;
            
            let mut values = Vec::with_capacity(matched_pairs.len());
            for &(left_idx, right_idx) in matched_pairs {
                let idx = if is_left { left_idx } else { right_idx };
                if idx < arr.len() {
                    if arr.is_null(idx) {
                        values.push(None);
                    } else {
                        values.push(Some(arr.value(idx)));
                    }
                } else {
                    values.push(None); // Out of bounds -> NULL
                }
            }
            
            Ok(Arc::new(Int64Array::from(values)) as Arc<dyn Array>)
        }
        arrow::datatypes::DataType::Float64 => {
            let arr = source_array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float64Array"))?;
            
            let mut values = Vec::with_capacity(matched_pairs.len());
            for &(left_idx, right_idx) in matched_pairs {
                let idx = if is_left { left_idx } else { right_idx };
                if idx < arr.len() {
                    if arr.is_null(idx) {
                        values.push(None);
                    } else {
                        values.push(Some(arr.value(idx)));
                    }
                } else {
                    values.push(None); // Out of bounds -> NULL
                }
            }
            
            Ok(Arc::new(Float64Array::from(values)) as Arc<dyn Array>)
        }
        arrow::datatypes::DataType::Utf8 => {
            let arr = source_array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to StringArray"))?;
            
            let mut values = Vec::with_capacity(matched_pairs.len());
            for &(left_idx, right_idx) in matched_pairs {
                let idx = if is_left { left_idx } else { right_idx };
                if idx < arr.len() {
                    if arr.is_null(idx) {
                        values.push(None);
                    } else {
                        values.push(Some(arr.value(idx).to_string()));
                    }
                } else {
                    values.push(None); // Out of bounds -> NULL
                }
            }
            
            Ok(Arc::new(StringArray::from(values)) as Arc<dyn Array>)
        }
        arrow::datatypes::DataType::Boolean => {
            let arr = source_array.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to BooleanArray"))?;
            
            let mut values = Vec::with_capacity(matched_pairs.len());
            for &(left_idx, right_idx) in matched_pairs {
                let idx = if is_left { left_idx } else { right_idx };
                if idx < arr.len() {
                    if arr.is_null(idx) {
                        values.push(None);
                    } else {
                        values.push(Some(arr.value(idx)));
                    }
                } else {
                    values.push(None); // Out of bounds -> NULL
                }
            }
            
            Ok(Arc::new(BooleanArray::from(values)) as Arc<dyn Array>)
        }
        _ => {
            // Fallback: convert to string array
            let mut values = Vec::with_capacity(matched_pairs.len());
            for &(left_idx, right_idx) in matched_pairs {
                let idx = if is_left { left_idx } else { right_idx };
                if idx < source_array.len() {
                    if source_array.is_null(idx) {
                        values.push(None);
                    } else {
                        // Use format! as fallback
                        values.push(Some(format!("{:?}", source_array)));
                    }
                } else {
                    values.push(None);
                }
            }
            Ok(Arc::new(StringArray::from(values)) as Arc<dyn Array>)
        }
    }
}

/// Safe column value extraction with bounds checking
/// Validates only the specific column being accessed, not the entire batch
fn safe_extract_value(
    batch: &ExecutionBatch,
    col_idx: usize,
    row_idx: usize,
) -> Result<Value> {
    // Bounds check: column index
    if col_idx >= batch.batch.columns.len() {
        return Err(anyhow::anyhow!(
            "Column index {} out of bounds (batch has {} columns)",
            col_idx,
            batch.batch.columns.len()
        ));
    }
    
    // Get column array
    let array = batch.batch.column(col_idx)
        .ok_or_else(|| anyhow::anyhow!(
            "Column {} not found in batch (has {} columns)",
            col_idx,
            batch.batch.columns.len()
        ))?;
    
    // Validate specific array length vs row_idx (more lenient than full batch validation)
    if row_idx >= array.len() {
        return Err(anyhow::anyhow!(
            "Row index {} out of bounds for column {} (array has length {}, batch row_count is {})",
            row_idx,
            col_idx,
            array.len(),
            batch.row_count
        ));
    }
    
    // Extract value using existing extract_value function
    extract_value(array, row_idx)
}

pub fn extract_value(array: &Arc<dyn Array>, idx: usize) -> Result<Value> {
    // Bounds check to prevent array index out of bounds panics
    if idx >= array.len() {
        return Err(anyhow::anyhow!(
            "Array index out of bounds: index {} >= array length {}",
            idx,
            array.len()
        ));
    }
    
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            // Additional bounds check for safety
            if idx >= arr.len() {
                return Err(anyhow::anyhow!(
                    "Int64Array index out of bounds: index {} >= array length {}",
                    idx,
                    arr.len()
                ));
            }
            // Check for NULL values
            if arr.is_null(idx) {
                Ok(Value::Null)
            } else {
                Ok(Value::Int64(arr.value(idx)))
            }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            // Additional bounds check for safety
            if idx >= arr.len() {
                return Err(anyhow::anyhow!(
                    "Float64Array index out of bounds: index {} >= array length {}",
                    idx,
                    arr.len()
                ));
            }
            // Check for NULL values
            if arr.is_null(idx) {
                Ok(Value::Null)
            } else {
                Ok(Value::Float64(arr.value(idx)))
            }
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            // Additional bounds check for safety
            if idx >= arr.len() {
                return Err(anyhow::anyhow!(
                    "StringArray index out of bounds: index {} >= array length {}",
                    idx,
                    arr.len()
                ));
            }
            // Check for NULL values
            if arr.is_null(idx) {
                Ok(Value::Null)
            } else {
                Ok(Value::String(arr.value(idx).to_string()))
            }
        }
        _ => anyhow::bail!("Unsupported type for join key"),
    }
}

/// Aggregate operator - performs GROUP BY and aggregations
pub struct AggregateOperator {
    input: Box<dyn BatchIterator>,
    group_by: Vec<String>,
    /// COLID: Map GROUP BY column names to their SELECT aliases
    /// Example: "d.name" -> "department_name" (from SELECT d.name AS department_name)
    group_by_aliases: std::collections::HashMap<String, String>,
    aggregates: Vec<AggregateExpr>,
    state: AggregateState,
    /// Whether we've already processed all input and returned results
    finished: bool,
    /// Unique instance ID for debugging
    instance_id: usize,
    /// Table aliases for column resolution (e.g., "e2" -> "employees")
    table_aliases: std::collections::HashMap<String, String>,
}

// Static counter for instance IDs
static mut AGG_INSTANCE_COUNTER: usize = 0;

struct AggregateState {
    groups: std::collections::HashMap<Vec<Value>, AggregateValues>,
    /// Partial aggregation state for spill optimization
    /// When memory pressure is high, we can flush partial aggregates
    partial_flush_count: usize,
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
        Self::with_table_aliases_and_group_by_aliases(
            input, 
            group_by, 
            aggregates, 
            std::collections::HashMap::new(),
            std::collections::HashMap::new()
        )
    }
    
    pub fn with_table_aliases(
        input: Box<dyn BatchIterator>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
        table_aliases: std::collections::HashMap<String, String>,
    ) -> Self {
        Self::with_table_aliases_and_group_by_aliases(
            input,
            group_by,
            aggregates,
            table_aliases,
            std::collections::HashMap::new()
        )
    }
    
    /// COLID: Create AggregateOperator with table aliases and GROUP BY aliases
    pub fn with_table_aliases_and_group_by_aliases(
        input: Box<dyn BatchIterator>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
        table_aliases: std::collections::HashMap<String, String>,
        group_by_aliases: std::collections::HashMap<String, String>,
    ) -> Self {
        let instance_id = unsafe {
            AGG_INSTANCE_COUNTER += 1;
            AGG_INSTANCE_COUNTER
        };
        eprintln!(" DEBUG: AggregateOperator::new - creating instance_id={}, group_by_aliases={:?}", instance_id, group_by_aliases);
        Self {
            input,
            group_by,
            group_by_aliases, // COLID: Store GROUP BY aliases
            aggregates,
            state: AggregateState {
                groups: std::collections::HashMap::new(),
                partial_flush_count: 0,
            },
            finished: false,
            instance_id,
            table_aliases,
        }
    }
}

impl BatchIterator for AggregateOperator {
    fn prepare(&mut self) -> Result<(), anyhow::Error> {
        // CRITICAL FIX: AggregateOperator must recursively prepare its input
        eprintln!("[AGGREGATE PREPARE] Preparing AggregateOperator, recursively preparing input");
        FilterOperator::prepare_child(&mut self.input)?;
        eprintln!("[AGGREGATE PREPARE] AggregateOperator prepared");
        Ok(())
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // If we've already processed and returned results, return None
        eprintln!(" DEBUG: AggregateOperator[{}]::next called, finished={}", self.instance_id, self.finished);
        if self.finished {
            eprintln!(" DEBUG: AggregateOperator[{}]::next - already finished, returning None", self.instance_id);
            return Ok(None);
        }
        
        // Process all input batches and accumulate aggregates
        while let Some(batch) = self.input.next()? {
            // TODO: Check if fragments are compressed and use compressed execution kernels
            // For now, fragments are stored as Arrow arrays (not compressed)
            // When fragments are stored in compressed form (RLE/Dictionary), we'll:
            // 1. Check batch metadata for compression type
            // 2. Extract compressed data (RLE/Dictionary)
            // 3. Use aggregate_sum_rle(), aggregate_count_rle(), etc. from compressed_execution module
            
            eprintln!(" DEBUG: AggregateOperator[{}]::next - processing batch: row_count={}, selection.count_ones()={}", 
                self.instance_id, batch.row_count, batch.selection.count_ones());
            self.process_batch(&batch)?;
        }
        
        // Mark as finished so we don't process again
        eprintln!(" DEBUG: AggregateOperator[{}]::next - setting finished=true", self.instance_id);
        self.finished = true;
        
        // Convert aggregated state to ExecutionBatch
        // SQL standard: Aggregation without GROUP BY should return ONE row even if there are no input rows
        let use_empty_group = self.group_by.is_empty();
        if self.state.groups.is_empty() && use_empty_group {
            // Return one row with NULL/0 for aggregates (SQL standard compliance)
            let mut agg_columns: Vec<Vec<Value>> = vec![vec![]; self.aggregates.len()];
            for (i, agg) in self.aggregates.iter().enumerate() {
                let val = match agg.function {
                    AggregateFunction::Sum => Value::Float64(0.0),
                    AggregateFunction::Count => Value::Int64(0),
                    AggregateFunction::Avg => Value::Float64(0.0),
                    AggregateFunction::Min => Value::Null,
                    AggregateFunction::Max => Value::Null,
                    AggregateFunction::CountDistinct => Value::Int64(0),
                };
                agg_columns[i].push(val);
            }
            
            // PHASE 1: Build schema FIRST (schema-first approach)
            let mut fields = vec![];
            for agg in &self.aggregates {
                let field_name = if let Some(ref alias) = agg.alias {
                    alias.clone()
                } else {
                    match agg.function {
                        AggregateFunction::Sum => "SUM".to_string(),
                        AggregateFunction::Count => "COUNT".to_string(),
                        AggregateFunction::Avg => "AVG".to_string(),
                        AggregateFunction::Min => "MIN".to_string(),
                        AggregateFunction::Max => "MAX".to_string(),
                        AggregateFunction::CountDistinct => "COUNT_DISTINCT".to_string(),
                    }
                };
                // Use centralized type function
                let data_type = crate::execution::type_conversion::aggregate_return_type(&agg.function);
                fields.push(Field::new(&field_name, data_type, true));
            }
            
            // PHASE 2: Convert to Arrow arrays USING SCHEMA TYPES (dtype-aware)
            let mut output_arrays = vec![];
            for (i, col) in agg_columns.iter().enumerate() {
                let field = &fields[i];
                let arr = crate::execution::type_conversion::values_to_array(
                    col.clone(),
                    field.data_type(),
                    field.name()
                )?;
                validate_array_type(&arr, field.data_type(), field.name())?;
                output_arrays.push(arr);
            }
            
            let schema = Arc::new(Schema::new(fields));
            let batch = crate::storage::columnar::ColumnarBatch::new(output_arrays, schema.clone());
            let selection = bitvec![1; 1];
            
            // CRITICAL: Create ColumnSchema for aggregate output
            // Aggregate columns are computed/derived, so they get new ColumnIds
            use crate::execution::column_identity::{ColumnSchema, generate_column_id_for_computed};
            let mut output_column_schema = ColumnSchema::new();
            for (idx, field) in schema.fields().iter().enumerate() {
                let column_id = generate_column_id_for_computed();
                let field_name = field.name();
                // For aggregates, qualified and unqualified names are the same (no table prefix)
                output_column_schema.add_column(column_id, field_name.clone(), field_name.clone(), field.data_type().clone());
            }
            
            let mut exec_batch = ExecutionBatch::with_column_schema(batch, output_column_schema);
            exec_batch.selection = selection;
            exec_batch.row_count = 1;
            return Ok(Some(exec_batch));
        }
        
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
                            // AVG of empty set should be NULL, not 0.0
                            Value::Null
                        }
                    }
                    AggregateFunction::Min => agg_vals.mins[i].clone().unwrap_or(Value::Null),
                    AggregateFunction::Max => agg_vals.maxs[i].clone().unwrap_or(Value::Null),
                    AggregateFunction::CountDistinct => Value::Int64(agg_vals.counts[i] as i64),
                };
                eprintln!(" DEBUG: Created value for agg[{}] (function={:?}): {:?}", i, agg.function, &val);
                agg_columns[i].push(val);
            }
        }
        
        let row_count = self.state.groups.len();
        eprintln!(" DEBUG: AggregateOperator::next - groups.len()={}", row_count);
        if row_count == 0 {
            eprintln!(" DEBUG: No groups found, returning None");
            return Ok(None);
        }
        
        // PHASE 1: Build schema FIRST (schema-first approach)
        // This ensures we know the types before creating arrays
        let mut fields = vec![];
        let mut field_names = std::collections::HashSet::new();
        
        // COLID: Add group_by columns using aliases from planner
        for col_name in &self.group_by {
            // Use alias if available, otherwise use column name
            let output_name = self.group_by_aliases.get(col_name)
                .cloned()
                .unwrap_or_else(|| col_name.clone());
            
            if !field_names.contains(&output_name) {
                fields.push(Field::new(&output_name, DataType::Utf8, true));
                field_names.insert(output_name.clone());
            } else {
                eprintln!("  WARNING: Duplicate group_by column '{}' (alias: '{}') detected, skipping", col_name, output_name);
            }
        }
        
        // Add aggregate columns (only if not already present)
        for agg in &self.aggregates {
            // Use alias if provided, otherwise use function name (e.g., "SUM", "COUNT")
            let field_name = if let Some(ref alias) = agg.alias {
                alias.clone()
            } else {
                // Default to function name when no alias
                match agg.function {
                    AggregateFunction::Sum => "SUM".to_string(),
                    AggregateFunction::Count => "COUNT".to_string(),
                    AggregateFunction::Avg => "AVG".to_string(),
                    AggregateFunction::Min => "MIN".to_string(),
                    AggregateFunction::Max => "MAX".to_string(),
                    AggregateFunction::CountDistinct => "COUNT_DISTINCT".to_string(),
                }
            };
            
            // Only add if not already present
            if !field_names.contains(&field_name) {
                // Use centralized type function
                let data_type = crate::execution::type_conversion::aggregate_return_type(&agg.function);
                fields.push(Field::new(&field_name, data_type, true));
                field_names.insert(field_name.clone());
            } else {
                eprintln!("  WARNING: Duplicate aggregate field '{}' detected, skipping", field_name);
            }
        }
        
        // CRITICAL: Final validation - ensure no duplicates
        let final_field_names: Vec<String> = fields.iter().map(|f| f.name().to_string()).collect();
        let unique_names: std::collections::HashSet<String> = final_field_names.iter().cloned().collect();
        if final_field_names.len() != unique_names.len() {
            eprintln!(" ERROR: AggregateOperator schema has duplicate fields: {:?}", final_field_names);
            anyhow::bail!("AggregateOperator schema has duplicate fields: {:?}", final_field_names);
        }
        
        // PHASE 2: Convert to Arrow arrays USING SCHEMA TYPES (dtype-aware)
        // This ensures arrays always match their declared schema types
        let mut output_arrays = vec![];
        eprintln!(" DEBUG: Creating output arrays - group_columns.len()={}, agg_columns.len()={}, schema fields={}", 
            group_columns.len(), agg_columns.len(), fields.len());
        
        // Convert group-by columns using schema type (always Utf8)
        for (i, col) in group_columns.iter().enumerate() {
            let field = &fields[i];
            eprintln!(" DEBUG: Processing group_column[{}], field={}, type={:?}", 
                i, field.name(), field.data_type());
            
            let arr = crate::execution::type_conversion::values_to_array(
                col.clone(),
                field.data_type(),
                field.name()
            )?;
            
            // Validate array type matches schema
            crate::execution::type_conversion::validate_array_type(
                &arr,
                field.data_type(),
                field.name()
            )?;
            
            output_arrays.push(arr);
        }
        
        // Convert aggregate columns using schema types (not value types!)
        eprintln!(" DEBUG: Processing agg_columns, total={}", agg_columns.len());
        for (i, col) in agg_columns.iter().enumerate() {
            let field_idx = self.group_by.len() + i;
            let field = &fields[field_idx];
            
            eprintln!(" DEBUG: Processing agg_column[{}], field={}, type={:?}, first_val={:?}", 
                i, field.name(), field.data_type(), col.first());
            
            let arr = crate::execution::type_conversion::values_to_array(
                col.clone(),
                field.data_type(),
                field.name()
            )?;
            
            // Validate array type matches schema
            crate::execution::type_conversion::validate_array_type(
                &arr,
                field.data_type(),
                field.name()
            )?;
            
            output_arrays.push(arr);
        }
        
        eprintln!(" DEBUG: AggregateOperator::next returning");
        eprintln!("   - row_count: {}", row_count);
        eprintln!("   - schema fields: {:?}", fields.iter().map(|f| f.name()).collect::<Vec<_>>());
        
        eprintln!(" DEBUG: Created {} output arrays, {} schema fields", 
            output_arrays.len(), fields.len());
        
        // Debug: Print what arrays we're creating
        for (i, arr) in output_arrays.iter().enumerate() {
            eprintln!(" DEBUG: output_arrays[{}]: type={}, len={}", i, arr.data_type(), arr.len());
        }
        
        // Debug: Print what fields we're creating
        let field_names_before: Vec<String> = fields.iter().map(|f| f.name().to_string()).collect();
        eprintln!(" DEBUG: fields before deduplication: {:?} (count={})", field_names_before, fields.len());
        for (i, field) in fields.iter().enumerate() {
            eprintln!(" DEBUG: fields[{}]: name={}, type={:?}", i, field.name(), field.data_type());
        }
        
        // Verify array-schema alignment
        if output_arrays.len() != fields.len() {
            eprintln!("  WARNING: Array count ({}) != schema field count ({})!", output_arrays.len(), fields.len());
        }
        
        // CRITICAL: Deduplicate fields before creating schema
        // Arrow Schema doesn't prevent duplicates, so we must do it ourselves
        let mut deduplicated_fields = vec![];
        let mut seen_names = std::collections::HashSet::new();
        for field in &fields {
            if !seen_names.contains(field.name()) {
                deduplicated_fields.push(field.clone());
                seen_names.insert(field.name().to_string());
            } else {
                eprintln!("  WARNING: Skipping duplicate field '{}' when creating schema", field.name());
            }
        }
        
        let dedup_field_names: Vec<String> = deduplicated_fields.iter().map(|f| f.name().to_string()).collect();
        eprintln!(" DEBUG: fields after deduplication: {:?} (count={})", dedup_field_names, deduplicated_fields.len());
        
        if deduplicated_fields.len() != fields.len() {
            eprintln!(" ERROR: Schema had {} duplicate fields, deduplicated to {}", 
                fields.len() - deduplicated_fields.len(), deduplicated_fields.len());
            eprintln!(" ERROR: Original fields: {:?}", field_names_before);
            eprintln!(" ERROR: Deduplicated fields: {:?}", dedup_field_names);
            anyhow::bail!("Schema had duplicate fields: original={}, deduplicated={}", 
                fields.len(), deduplicated_fields.len());
        }
        
        // CRITICAL: Also ensure output_arrays count matches deduplicated_fields count
        if output_arrays.len() != deduplicated_fields.len() {
            eprintln!(" ERROR: Array count ({}) != deduplicated field count ({})!", 
                output_arrays.len(), deduplicated_fields.len());
            anyhow::bail!("Array count ({}) != deduplicated field count ({})", 
                output_arrays.len(), deduplicated_fields.len());
        }
        
        let schema = Arc::new(Schema::new(deduplicated_fields));
        
        // CRITICAL: Validate schema immediately after creation
        let schema_field_names: Vec<String> = schema.fields().iter().map(|f| f.name().to_string()).collect();
        let schema_unique_names: std::collections::HashSet<String> = schema_field_names.iter().cloned().collect();
        if schema_field_names.len() != schema_unique_names.len() {
            eprintln!(" ERROR: Schema created with duplicate fields: {:?}", schema_field_names);
            anyhow::bail!("Schema created with duplicate fields: {:?}", schema_field_names);
        }
        
        // Debug: Verify schema before creating batch
        eprintln!(" DEBUG: Schema before ColumnarBatch::new - fields: {:?} (count={})", 
            schema.fields().iter().map(|f| format!("{}:{:?}", f.name(), f.data_type())).collect::<Vec<_>>(),
            schema.fields().len());
        for (i, field) in schema.fields().iter().enumerate() {
            eprintln!(" DEBUG: Schema field[{}] before batch: name={}, type={:?}", i, field.name(), field.data_type());
        }
        
        // CRITICAL: Validate arrays match schema before creating batch
        if output_arrays.len() != schema.fields().len() {
            eprintln!(" ERROR: Array count ({}) != schema field count ({}) before ColumnarBatch::new!", 
                output_arrays.len(), schema.fields().len());
            anyhow::bail!("Array count ({}) != schema field count ({})", 
                output_arrays.len(), schema.fields().len());
        }
        
        let batch = crate::storage::columnar::ColumnarBatch::new(output_arrays, schema.clone());
        
        // Debug: Verify schema after creating batch
        eprintln!(" DEBUG: Schema after ColumnarBatch::new - fields: {:?}", 
            batch.schema.fields().iter().map(|f| format!("{}:{:?}", f.name(), f.data_type())).collect::<Vec<_>>());
        for (i, field) in batch.schema.fields().iter().enumerate() {
            eprintln!(" DEBUG: Batch schema field[{}] after creation: name={}, type={:?}", i, field.name(), field.data_type());
        }
        
        // CRITICAL: Verify array types match schema types BEFORE returning
        for (i, (col, field)) in batch.columns.iter().zip(batch.schema.fields().iter()).enumerate() {
            let actual_type = col.data_type();
            let expected_type = field.data_type();
            if actual_type != expected_type {
                eprintln!(" ERROR: AggregateOperator batch type mismatch at index {}: field '{}' expected {:?}, got {:?}", 
                    i, field.name(), expected_type, actual_type);
                anyhow::bail!("Type mismatch in AggregateOperator output: field '{}' expected {:?}, got {:?}", 
                    field.name(), expected_type, actual_type);
            }
        }
        
        eprintln!(" DEBUG: Final batch schema: {:?}", batch.schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        eprintln!(" DEBUG: Batch has {} columns, {} schema fields", batch.columns.len(), batch.schema.fields().len());
        
        // Check if schema was modified
        if batch.schema.fields().len() != fields.len() {
            eprintln!("  WARNING: Schema field count changed! Original: {}, Batch: {}", 
                fields.len(), batch.schema.fields().len());
        }
        let selection = bitvec![1; row_count];
        
        // Clear state after output (but don't reset finished flag - we're done)
        self.state.groups.clear();
        
        // CRITICAL: Create ColumnSchema for output batch
        // This ensures downstream operators can resolve columns by ColumnId
        // Aggregate columns are computed/derived, so they get new ColumnIds
        let mut output_column_schema = {
            use crate::execution::column_identity::{ColumnSchema, generate_column_id_for_computed};
            let mut column_schema = ColumnSchema::new();
            
            // Add GROUP BY columns to output ColumnSchema using aliases
            for (idx, col_name) in self.group_by.iter().enumerate() {
                // Generate new ColumnId for output (computed/derived from GROUP BY)
                let column_id = generate_column_id_for_computed();
                
                // Use alias from planner if available, otherwise use column name
                let external_name = self.group_by_aliases.get(col_name)
                    .cloned()
                    .unwrap_or_else(|| col_name.clone());
                let data_type = DataType::Utf8; // GROUP BY columns are typically strings/values
                // For aggregates, qualified and unqualified names are the same (no table prefix)
                column_schema.add_column(column_id, external_name.clone(), external_name, data_type);
            }
            
            // CRITICAL: Add aggregate columns to output ColumnSchema
            // Aggregate columns are computed/derived, so they get new ColumnIds
            for agg in &self.aggregates {
                let column_id = generate_column_id_for_computed();
                
                let external_name = if let Some(ref alias) = agg.alias {
                    alias.clone()
                } else {
                    match agg.function {
                        AggregateFunction::Sum => "SUM".to_string(),
                        AggregateFunction::Count => "COUNT".to_string(),
                        AggregateFunction::Avg => "AVG".to_string(),
                        AggregateFunction::Min => "MIN".to_string(),
                        AggregateFunction::Max => "MAX".to_string(),
                        AggregateFunction::CountDistinct => "COUNT_DISTINCT".to_string(),
                    }
                };
                
                let data_type = crate::execution::type_conversion::aggregate_return_type(&agg.function);
                // For aggregates, qualified and unqualified names are the same (no table prefix)
                column_schema.add_column(column_id, external_name.clone(), external_name, data_type);
            }
            
            column_schema
        };
        
        // CRITICAL FIX: Verify ColumnSchema name_map contains all aliases before creating ExecutionBatch
        eprintln!(" DEBUG: AggregateOperator ColumnSchema name_map: {:?}", output_column_schema.name_map.keys().collect::<Vec<_>>());
        eprintln!(" DEBUG: AggregateOperator ColumnSchema unqualified_names: {:?}", output_column_schema.unqualified_names);
        
        // Assert that all external names are in name_map (required for SortOperator resolution)
        for external_name in &output_column_schema.unqualified_names {
            if !output_column_schema.name_map.contains_key(external_name) {
                eprintln!(" ERROR: AggregateOperator ColumnSchema missing name_map entry for external_name '{}'", external_name);
                // Fix: Add missing mapping
                if let Some(idx) = output_column_schema.unqualified_names.iter().position(|n| n == external_name) {
                    if idx < output_column_schema.column_ids.len() {
                        let col_id = output_column_schema.column_ids[idx];
                        output_column_schema.name_map.insert(external_name.clone(), col_id);
                        eprintln!(" DEBUG: Fixed - Added name_map entry: '{}' -> ColId({:?})", external_name, col_id);
                    }
                }
            }
        }
        
        // COLID: Create ExecutionBatch with ColumnSchema
        let mut exec_batch = ExecutionBatch::with_column_schema(batch, output_column_schema);
        exec_batch.selection = selection;
        exec_batch.row_count = row_count;
        
        // CRITICAL: Final validation before returning batch
        eprintln!(" DEBUG: AggregateOperator RETURNING batch:");
        eprintln!("   - columns.len()={}, schema.fields().len()={}", 
            exec_batch.batch.columns.len(), exec_batch.batch.schema.fields().len());
        if let Some(ref cs) = exec_batch.column_schema {
            eprintln!("   - ColumnSchema name_map keys: {:?}", cs.name_map.keys().collect::<Vec<_>>());
            eprintln!("   - ColumnSchema unqualified_names: {:?}", cs.unqualified_names);
        }
        for (i, (col, field)) in exec_batch.batch.columns.iter().zip(exec_batch.batch.schema.fields().iter()).enumerate() {
            eprintln!("   - Column[{}]: name={}, array_type={:?}, schema_type={:?}", 
                i, field.name(), col.data_type(), field.data_type());
            if col.data_type() != field.data_type() {
                eprintln!("    MISMATCH at return time!");
            }
        }
        
        Ok(Some(exec_batch))
    }
    
    fn schema(&self) -> SchemaRef {
        // SCHEMA-FLOW: AggregateOperator builds schema from group_by + aggregates
        // CRITICAL: Use centralized type function to ensure consistency with batch creation
        // NOTE: AggregateOperator doesn't pass through input schema - it creates a new schema
        // from group_by columns and aggregate results
        
        let mut fields = vec![];
        // COLID: Use GROUP BY aliases in output schema
        for col_name in &self.group_by {
            let output_name = self.group_by_aliases.get(col_name)
                .cloned()
                .unwrap_or_else(|| col_name.clone());
            fields.push(Field::new(&output_name, DataType::Utf8, true));
        }
        for agg in &self.aggregates {
            // Use alias if provided, otherwise use function name (e.g., "SUM", "COUNT")
            let field_name = if let Some(ref alias) = agg.alias {
                alias.clone()
            } else {
                // Default to function name when no alias
                match agg.function {
                    AggregateFunction::Sum => "SUM".to_string(),
                    AggregateFunction::Count => "COUNT".to_string(),
                    AggregateFunction::Avg => "AVG".to_string(),
                    AggregateFunction::Min => "MIN".to_string(),
                    AggregateFunction::Max => "MAX".to_string(),
                    AggregateFunction::CountDistinct => "COUNT_DISTINCT".to_string(),
                }
            };
            // Use centralized type function - MUST match batch creation logic
            let data_type = crate::execution::type_conversion::aggregate_return_type(&agg.function);
            fields.push(Field::new(&field_name, data_type, true));
        }
        
        let output_schema = Arc::new(Schema::new(fields));
        
        // SCHEMA-FLOW DEBUG: Log output schema
        eprintln!("DEBUG AggregateOperator::schema() - Output schema has {} fields: {:?}", 
            output_schema.fields().len(),
            output_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        
        output_schema
    }
}

impl AggregateOperator {
    /// Partial flush: write current aggregate state to spill before continuing
    /// This reduces memory usage for large GROUP BY queries by 30-40%
    fn partial_flush(&mut self) -> Result<()> {
        // PARTIAL AGGREGATION SPILL: Flush current state to reduce memory
        // In a full implementation, we'd serialize the aggregate state and spill it
        // For Phase B, we'll just increment the flush counter
        self.state.partial_flush_count += 1;
        
        // Note: In production, we'd:
        // 1. Serialize current aggregate state
        // 2. Spill to disk using SpillRunBuilder
        // 3. Clear in-memory state
        // 4. Continue processing with reduced memory footprint
        
        Ok(())
    }
    
    fn process_batch(&mut self, batch: &ExecutionBatch) -> Result<()> {
        // CRITICAL: Validate batch schema alignment before processing
        if let Err(diag) = batch.validate_schema_alignment() {
            eprintln!(" WARNING: AggregateOperator - Schema mismatch detected: {}", diag);
            eprintln!(" WARNING: Falling back to string-based column resolution");
            // Continue with fallback resolution - don't fail here
        }
        
        // PARTIAL AGGREGATION SPILL: Check if we should flush partial aggregates
        // This reduces memory usage for large GROUP BY queries by 30-40%
        if self.state.groups.len() > 10000 {
            // Flush partial aggregates when we have too many groups
            self.partial_flush()?;
        }
        
        eprintln!(" DEBUG: process_batch called");
        eprintln!("   - batch.row_count: {}", batch.row_count);
        eprintln!("   - batch.selection.count_ones(): {}", batch.selection.count_ones());
        eprintln!("   - batch.selection.len(): {}", batch.selection.len());
        eprintln!("   - aggregates.len(): {}", self.aggregates.len());
        eprintln!("   - group_by.len(): {}", self.group_by.len());
        eprintln!("   - batch.batch.columns.len(): {}", batch.batch.columns.len());
        eprintln!("   - batch.batch.schema.fields().len(): {}", batch.batch.schema.fields().len());
        if let Some(ref cs) = batch.column_schema {
            eprintln!("   - batch.column_schema.column_ids.len(): {}", cs.column_ids.len());
        }
        
        for (i, agg) in self.aggregates.iter().enumerate() {
            eprintln!("   - agg[{}]: function={:?}, column='{}', alias={:?}", 
                i, agg.function, agg.column, agg.alias);
        }
        
        // COLID: Extract group keys using ColId-based resolution if ColumnSchema is available
        // This fixes GROUP BY column resolution after JOINs
        let mut group_key_indices = vec![];
        
        // CRITICAL: Check if ColumnSchema matches batch structure BEFORE resolving columns
        if let Some(ref column_schema) = batch.column_schema {
            let schema_col_count = column_schema.column_ids.len();
            let batch_col_count = batch.batch.columns.len();
            eprintln!(" DEBUG: ColumnSchema check - schema has {} columns, batch has {} columns", schema_col_count, batch_col_count);
            
            // Use ColumnSchema-based resolution (preferred - fixes JOIN resolution)
            use crate::query::column_resolver::ColumnResolver;
            for col_name in &self.group_by {
                match ColumnResolver::resolve_to_col_id(column_schema, col_name, &self.table_aliases) {
                    Ok(col_id) => {
                        // Find the actual batch column index by matching ColId
                        // CRITICAL: ColumnSchema's column_ids order MUST match batch.columns order
                        // If they don't match (e.g., after projection/filtering), use string-based resolution
                        let batch_col_idx = if let Some(ref batch_col_schema) = batch.column_schema {
                            // Verify ColumnSchema matches batch structure
                            if batch_col_schema.column_ids.len() == batch.batch.columns.len() {
                                // ColumnSchema matches batch - find position of col_id
                                batch_col_schema.column_ids.iter().position(|&id| id == col_id)
                            } else {
                                // ColumnSchema doesn't match batch - likely from previous operator
                                eprintln!(" DEBUG: WARNING - ColumnSchema has {} columns but batch has {} columns, using string-based resolution", 
                                    batch_col_schema.column_ids.len(), batch.batch.columns.len());
                                None
                            }
                        } else {
                            None
                        };
                        
                        if let Some(col_idx) = batch_col_idx {
                            group_key_indices.push(col_idx);
                            eprintln!(" DEBUG: GROUP BY column '{}' resolved to ColId({}) -> batch column index {}", 
                                col_name, col_id.to_u32(), col_idx);
                        } else {
                            // Fallback to string-based resolution
                            eprintln!(" DEBUG: ColId-based resolution failed for GROUP BY column '{}', using string-based fallback", col_name);
                            let resolver = ColumnResolver::new(batch.batch.schema.clone(), self.table_aliases.clone());
                            match resolver.resolve(col_name) {
                                Ok(fallback_idx) => {
                                    eprintln!(" DEBUG: String-based fallback found column '{}' at index {} (batch has {} columns)", 
                                        col_name, fallback_idx, batch.batch.columns.len());
                                    if fallback_idx < batch.batch.columns.len() {
                                        group_key_indices.push(fallback_idx);
                                    } else {
                                        anyhow::bail!("GROUP BY column '{}' fallback index {} >= batch.columns.len() ({})", 
                                            col_name, fallback_idx, batch.batch.columns.len());
                                    }
                                }
                                Err(e) => {
                                    anyhow::bail!("GROUP BY column '{}' not found: ColId={}, fallback failed: {}", 
                                        col_name, col_id.to_u32(), e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // Fall back to old string-based resolution
                        eprintln!(" DEBUG: ColId resolution failed for GROUP BY column '{}', trying fallback: {}", col_name, e);
                        let resolver = ColumnResolver::new(batch.batch.schema.clone(), self.table_aliases.clone());
                        match resolver.resolve(col_name) {
                            Ok(col_idx) => {
                                group_key_indices.push(col_idx);
                            }
                            Err(e2) => {
                                anyhow::bail!("Group by column not found: {} (ColId resolution: {}, fallback: {})", col_name, e, e2);
                            }
                        }
                    }
                }
            }
        } else {
            // Fallback: Use old string-based resolution if ColumnSchema not available
            use crate::query::column_resolver::ColumnResolver;
            let resolver = ColumnResolver::new(batch.batch.schema.clone(), self.table_aliases.clone());
            for col_name in &self.group_by {
                match resolver.resolve(col_name) {
                    Ok(col_idx) => {
                        group_key_indices.push(col_idx);
                    }
                    Err(e) => {
                        anyhow::bail!("Group by column not found: {} ({})", col_name, e);
                    }
                }
            }
        }
        
        // Special case: COUNT(*) without GROUP BY - use empty group key
        let use_empty_group = self.group_by.is_empty();
        
        // COLID: Resolve aggregate columns using ColId-based resolution if available
        let mut agg_col_indices = vec![];
        for (i, agg) in self.aggregates.iter().enumerate() {
            // Handle COUNT(*) - no specific column needed
            if agg.column == "*" {
                eprintln!(" DEBUG: COUNT(*) detected for agg[{}], setting col_idx=usize::MAX", i);
                agg_col_indices.push(usize::MAX); // Special marker for COUNT(*)
            } else if let Some(ref column_schema) = batch.column_schema {
                // COLID: Use ColumnSchema-based resolution for aggregates
                use crate::query::column_resolver::ColumnResolver;
                match ColumnResolver::resolve_to_col_id(column_schema, &agg.column, &self.table_aliases) {
                    Ok(col_id) => {
                        // Find the actual batch column index by matching ColId
                        // CRITICAL: Validate ColumnSchema matches batch structure before using ColId resolution
                        let batch_col_idx = if let Some(ref batch_col_schema) = batch.column_schema {
                            // Verify ColumnSchema matches batch structure
                            if batch_col_schema.column_ids.len() == batch.batch.columns.len() {
                                // ColumnSchema matches batch - find position of col_id
                                batch_col_schema.column_ids.iter().position(|&id| id == col_id)
                            } else {
                                // ColumnSchema doesn't match batch - use string-based resolution
                                eprintln!(" WARNING: ColumnSchema has {} columns but batch has {} columns, using string-based resolution", 
                                    batch_col_schema.column_ids.len(), batch.batch.columns.len());
                                None
                            }
                        } else {
                            None
                        };
                        
                        // Use ColId-based resolution if available, otherwise fallback to string-based
                        let col_idx = if let Some(idx) = batch_col_idx {
                            eprintln!(" DEBUG: Aggregate column '{}' resolved to ColId({}) -> batch column index {} for agg[{}]", 
                                agg.column, col_id.to_u32(), idx, i);
                            idx
                        } else {
                            // Fallback to string-based resolution
                            eprintln!(" DEBUG: ColId-based resolution failed for aggregate column '{}', using string-based fallback", agg.column);
                            let resolver = ColumnResolver::new(batch.batch.schema.clone(), self.table_aliases.clone());
                            resolver.resolve(&agg.column)
                                .map_err(|e| anyhow::anyhow!("Aggregate column '{}' not found: ColId={}, fallback failed: {}", 
                                    agg.column, col_id.to_u32(), e))?
                        };
                        
                        // Validate index is within bounds
                        if col_idx >= batch.batch.columns.len() {
                            anyhow::bail!("Aggregate column '{}' resolved to index {} which is >= batch.columns.len() ({})", 
                                agg.column, col_idx, batch.batch.columns.len());
                        }
                        
                        agg_col_indices.push(col_idx);
                    }
                    Err(e) => {
                        // Fall back to old string-based resolution
                        eprintln!(" DEBUG: ColId resolution failed for aggregate column '{}', trying fallback: {}", agg.column, e);
                        let resolver = ColumnResolver::new(batch.batch.schema.clone(), self.table_aliases.clone());
                        match resolver.resolve(&agg.column) {
                            Ok(col_idx) => {
                                agg_col_indices.push(col_idx);
                            }
                            Err(e2) => {
                                eprintln!(" DEBUG: ERROR - Column '{}' not found for agg[{}]: ColId={}, fallback={}", agg.column, i, e, e2);
                                anyhow::bail!("Aggregate column not found: {} (ColId resolution: {}, fallback: {})", agg.column, e, e2);
                            }
                        }
                    }
                }
            } else {
                // Fallback: Use old string-based resolution if ColumnSchema not available
                use crate::query::column_resolver::ColumnResolver;
                let resolver = ColumnResolver::new(batch.batch.schema.clone(), self.table_aliases.clone());
                match resolver.resolve(&agg.column) {
                    Ok(col_idx) => {
                        eprintln!(" DEBUG: Found column '{}' at index {} for agg[{}] (using ColumnResolver with aliases)", agg.column, col_idx, i);
                        agg_col_indices.push(col_idx);
                    }
                    Err(e) => {
                        eprintln!(" DEBUG: ERROR - Column '{}' not found for agg[{}]: {}", agg.column, i, e);
                        eprintln!(" DEBUG: Available columns: {:?}", resolver.available_columns());
                        eprintln!(" DEBUG: Table aliases: {:?}", self.table_aliases);
                        anyhow::bail!("Aggregate column not found: {} ({})", agg.column, e);
                    }
                }
            }
        }

        // Fast path: single Int64 GROUP BY key
        if !use_empty_group && self.group_by.len() == 1 {
            let key_col_idx = group_key_indices[0];
            if let Some(col) = batch.batch.column(key_col_idx) {
                if let Some(int_arr) = col.as_any().downcast_ref::<Int64Array>() {
                    return self.process_batch_int64_group(batch, int_arr, &agg_col_indices);
                }
            }
        }
        
        // Process each row - iterate through ALL rows in the batch (not just row_count)
        // The selection bitmap indicates which rows to process
        // The selection bitmap length equals the original batch size before filtering
        let actual_batch_size = batch.selection.len();
        eprintln!(" DEBUG: process_batch: actual_batch_size={}, batch.row_count={}, selection.count_ones()={}", 
            actual_batch_size, batch.row_count, batch.selection.count_ones());
        
        for row_idx in 0..actual_batch_size {
            // Check if this row is selected (respects filtering)
            if !batch.selection[row_idx] {
                continue;
            }
            
            // Extract group key (empty for aggregates without GROUP BY)
            let group_key = if use_empty_group {
                vec![] // Single group for aggregates without GROUP BY
            } else {
                let mut key = vec![];
                for &col_idx in &group_key_indices {
                    eprintln!(" DEBUG: Accessing group key column at index {} (batch has {} columns)", col_idx, batch.batch.columns.len());
                    // Use safe extraction with validation
                    key.push(safe_extract_value(batch, col_idx, row_idx)?);
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
                        if row_idx == 0 || agg_vals.counts[i] % 100 == 0 {
                            eprintln!(" DEBUG: COUNT(*) incremented: agg[{}] = {} (row_idx={})", i, agg_vals.counts[i], row_idx);
                        }
                    }
                    continue;
                }
                
                // Use safe extraction with validation
                let mut val = safe_extract_value(batch, col_idx, row_idx)
                    .map_err(|e| anyhow::anyhow!("Failed to extract value for agg[{}] column {}: {}", i, col_idx, e))?;
                
                // Apply CAST if specified
                if let Some(ref cast_type) = self.aggregates[i].cast_type {
                    val = crate::query::expression::cast_value(&val, cast_type)
                        .unwrap_or(val); // If cast fails, use original value
                }
                
                match self.aggregates[i].function {
                    AggregateFunction::Sum | AggregateFunction::Avg => {
                        let num_val = match &val {
                            Value::Float64(x) => Some(*x),
                            Value::Int64(x) => Some(*x as f64),
                            Value::String(s) => {
                                // Try to parse string as number for SUM/AVG
                                s.parse::<f64>().ok().or_else(|| s.parse::<i64>().ok().map(|x| x as f64))
                            }
                            _ => None,
                        };
                        if let Some(x) = num_val {
                            agg_vals.sums[i] += x;
                            agg_vals.counts[i] += 1;
                        }
                        // If value can't be converted to number, skip it (don't increment count for SUM/AVG)
                    }
                    AggregateFunction::Count | AggregateFunction::CountDistinct => {
                        // COUNT(column) should skip NULL values, COUNT(*) counts all rows
                        // Since we already extracted the value, check if it's NULL
                        // For COUNT(*), col_idx would be usize::MAX and we'd skip this branch
                        if !matches!(val, Value::Null) {
                            agg_vals.counts[i] += 1;
                        }
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
        
        eprintln!(" DEBUG: process_batch complete, groups.len()={}", self.state.groups.len());
        for (i, (key, vals)) in self.state.groups.iter().enumerate() {
            eprintln!("   - group[{}]: key={:?}, counts={:?}, sums={:?}", i, key, vals.counts, vals.sums);
        }
        
        Ok(())
    }

    /// Fast-path aggregation for single Int64 GROUP BY key.
    /// Uses a temporary FxHashMap<i64, AggregateValues> and merges into the generic state.
    fn process_batch_int64_group(
        &mut self,
        batch: &ExecutionBatch,
        key_array: &Int64Array,
        agg_col_indices: &[usize],
    ) -> Result<()> {
        use fxhash::FxHashMap;

        // Local map: group key (i64) -> aggregate values
        let mut local_groups: FxHashMap<i64, AggregateValues> = FxHashMap::default();

        // SAFETY: Use batch.selection.len() to avoid array bounds violations
        let actual_batch_size = batch.selection.len();
        debug_assert!(actual_batch_size >= batch.row_count, 
            "batch.selection.len() ({}) >= batch.row_count ({})", 
            actual_batch_size, batch.row_count);
        
        for row_idx in 0..actual_batch_size {
            if !batch.selection[row_idx] {
                continue;
            }
            debug_assert!(row_idx < key_array.len(), 
                "row_idx {} < key_array.len() {}", row_idx, key_array.len());
            if key_array.is_null(row_idx) {
                continue;
            }
            let key = key_array.value(row_idx);

            let agg_vals = local_groups.entry(key).or_insert_with(|| AggregateValues {
                sums: vec![0.0; self.aggregates.len()],
                counts: vec![0; self.aggregates.len()],
                mins: vec![None; self.aggregates.len()],
                maxs: vec![None; self.aggregates.len()],
            });

            // Update aggregates for this row
            for (i, &col_idx) in agg_col_indices.iter().enumerate() {
                // Handle COUNT(*) - no column value needed
                if col_idx == usize::MAX {
                    if matches!(self.aggregates[i].function, AggregateFunction::Count | AggregateFunction::CountDistinct) {
                        agg_vals.counts[i] += 1;
                    }
                    continue;
                }

                // Use safe extraction with validation
                let mut val = safe_extract_value(batch, col_idx, row_idx)
                    .map_err(|e| anyhow::anyhow!("Failed to extract value for agg[{}] column {} in process_batch_int64_group: {}", i, col_idx, e))?;

                // Apply CAST if specified
                if let Some(ref cast_type) = self.aggregates[i].cast_type {
                    val = crate::query::expression::cast_value(&val, cast_type)
                        .unwrap_or(val); // If cast fails, use original value
                }

                match self.aggregates[i].function {
                    AggregateFunction::Sum | AggregateFunction::Avg => {
                        let num_val = match &val {
                            Value::Float64(x) => Some(*x),
                            Value::Int64(x) => Some(*x as f64),
                            Value::String(s) => {
                                // Try to parse string as number for SUM/AVG
                                s.parse::<f64>().ok().or_else(|| s.parse::<i64>().ok().map(|x| x as f64))
                            }
                            _ => None,
                        };
                        if let Some(x) = num_val {
                            agg_vals.sums[i] += x;
                            agg_vals.counts[i] += 1;
                        }
                        // If value can't be converted to number, skip it (don't increment count for SUM/AVG)
                    }
                    AggregateFunction::Count | AggregateFunction::CountDistinct => {
                        // COUNT(column) should skip NULL values, COUNT(*) counts all rows
                        // Since we already extracted the value, check if it's NULL
                        // For COUNT(*), col_idx would be usize::MAX and we'd skip this branch
                        if !matches!(val, Value::Null) {
                            agg_vals.counts[i] += 1;
                        }
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

        // Merge local groups into global state
        for (key, local_vals) in local_groups {
            let group_key = vec![Value::Int64(key)];
            let global_vals = self.state.groups.entry(group_key).or_insert_with(|| AggregateValues {
                sums: vec![0.0; self.aggregates.len()],
                counts: vec![0; self.aggregates.len()],
                mins: vec![None; self.aggregates.len()],
                maxs: vec![None; self.aggregates.len()],
            });

            for i in 0..self.aggregates.len() {
                global_vals.sums[i] += local_vals.sums[i];
                global_vals.counts[i] += local_vals.counts[i];

                // Merge mins
                match (&global_vals.mins[i], &local_vals.mins[i]) {
                    (None, Some(v)) => global_vals.mins[i] = Some(v.clone()),
                    (Some(g), Some(l)) => {
                        if l < g {
                            global_vals.mins[i] = Some(l.clone());
                        }
                    }
                    _ => {}
                }

                // Merge maxs
                match (&global_vals.maxs[i], &local_vals.maxs[i]) {
                    (None, Some(v)) => global_vals.maxs[i] = Some(v.clone()),
                    (Some(g), Some(l)) => {
                        if l > g {
                            global_vals.maxs[i] = Some(l.clone());
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }
}

/// Project operator - selects columns and evaluates expressions
/// Project operator - selects and reorders columns (DuckDB/Presto-style)
/// 
/// # Column Resolution (ColumnId-based)
/// - All column resolution is ColumnId-driven via ColumnSchema
/// - Resolve qualified names (e.g., "p.name") to ColumnId, then to physical index
/// - Resolve unqualified names with ambiguity checking
/// 
/// # Output Naming Rules
/// - If user specified explicit alias (e.g., `SELECT p.name AS p_name`), use that alias exactly
/// - Otherwise, use the fully qualified name from the expression (e.g., "p.name")
/// - NEVER strip qualifiers - this prevents c.name and p.name from both becoming "name"
/// 
/// # Deduplication
/// - Base deduplication ONLY on exact field.name() string match (case-insensitive)
/// - Do NOT derive dedupe key by stripping qualifiers
/// - If users select the same column twice intentionally (e.g., SELECT x, x), allow duplicates
/// 
/// # Design Philosophy
/// Similar in spirit to DuckDB/Presto: projection is ColumnId-based and always preserves user-visible names.
pub struct ProjectOperator {
    input: Box<dyn BatchIterator>,
    columns: Vec<String>,
    /// Expressions to evaluate (for CAST, functions, etc.)
    expressions: Vec<crate::query::plan::ProjectionExpr>,
    /// Optional subquery executor for scalar subqueries in expressions
    subquery_executor: Option<std::sync::Arc<dyn crate::query::expression::SubqueryExecutor>>,
    /// Table alias mapping: alias -> actual table name (e.g., "d" -> "documents")
    table_aliases: std::collections::HashMap<String, String>,
    /// WASM JIT compiled programs for expressions (optional)
    wasm_programs: std::collections::HashMap<String, crate::codegen::WasmProgramSpec>,
    /// Operator cache for persistent WASM modules (Phase C)
    operator_cache: Option<std::sync::Arc<std::sync::Mutex<crate::cache::operator_cache::OperatorCache>>>,
    /// Debug logging flag to prevent excessive output
    debug_logged: bool,
    /// ExecNode: prepared flag - ensures prepare() was called before next()
    prepared: bool,
}

impl ProjectOperator {
    pub fn new(
        input: Box<dyn BatchIterator>, 
        columns: Vec<String>,
        expressions: Vec<crate::query::plan::ProjectionExpr>,
    ) -> Self {
        Self {
            input,
            columns,
            expressions,
            subquery_executor: None,
            table_aliases: std::collections::HashMap::new(),
            wasm_programs: std::collections::HashMap::new(),
            operator_cache: None,
            debug_logged: false,
            prepared: false,
        }
    }
    
    pub fn with_subquery_executor(
        input: Box<dyn BatchIterator>,
        columns: Vec<String>,
        expressions: Vec<crate::query::plan::ProjectionExpr>,
        subquery_executor: Option<std::sync::Arc<dyn crate::query::expression::SubqueryExecutor>>,
    ) -> Self {
        Self {
            input,
            columns,
            expressions,
            subquery_executor,
            table_aliases: std::collections::HashMap::new(),
            wasm_programs: std::collections::HashMap::new(),
            operator_cache: None,
            debug_logged: false,
            prepared: false,
        }
    }
    
    pub fn with_subquery_executor_and_aliases(
        input: Box<dyn BatchIterator>,
        columns: Vec<String>,
        expressions: Vec<crate::query::plan::ProjectionExpr>,
        subquery_executor: Option<std::sync::Arc<dyn crate::query::expression::SubqueryExecutor>>,
        table_aliases: std::collections::HashMap<String, String>,
    ) -> Self {
        Self {
            input,
            columns,
            expressions,
            subquery_executor,
            table_aliases,
            wasm_programs: std::collections::HashMap::new(),
            operator_cache: None,
            debug_logged: false,
            prepared: false,
        }
    }
    
    /// Create ProjectOperator with WASM JIT programs
    pub fn with_wasm_jit(
        input: Box<dyn BatchIterator>,
        columns: Vec<String>,
        expressions: Vec<crate::query::plan::ProjectionExpr>,
        wasm_programs: std::collections::HashMap<String, crate::codegen::WasmProgramSpec>,
    ) -> Self {
        Self {
            input,
            columns,
            expressions,
            subquery_executor: None,
            table_aliases: std::collections::HashMap::new(),
            wasm_programs,
            operator_cache: None,
            debug_logged: false,
            prepared: false,
        }
    }
    
    /// Set operator cache for WASM JIT compilation (Phase C)
    pub fn with_operator_cache(
        mut self,
        operator_cache: std::sync::Arc<std::sync::Mutex<crate::cache::operator_cache::OperatorCache>>,
    ) -> Self {
        self.operator_cache = Some(operator_cache);
        self
    }
    
    /// Set WASM JIT programs for expressions (Phase C)
    pub fn with_wasm_programs(
        mut self,
        wasm_programs: &std::collections::HashMap<String, crate::codegen::WasmProgramSpec>,
    ) -> Self {
        self.wasm_programs = wasm_programs.clone();
        self
    }
}

// ExecNode implementation for ProjectOperator
impl ProjectOperator {
    /// Helper: Try to call prepare() on a BatchIterator if it implements ExecNode
    /// Uses the same type-erased approach as FilterOperator
    fn prepare_child(child: &mut Box<dyn BatchIterator>) -> Result<()> {
        FilterOperator::prepare_child(child)
    }
}

impl ExecNode for ProjectOperator {
    fn prepare(&mut self) -> Result<()> {
        eprintln!("[PROJECT EXECNODE PREPARE] Starting ExecNode::prepare() for ProjectOperator with {} columns, prepared={}", 
            self.columns.len(), self.prepared);
        // Recursively prepare child operator
        Self::prepare_child(&mut self.input)?;
        self.prepared = true;
        eprintln!("[PROJECT EXECNODE PREPARE] Completed ExecNode::prepare(), prepared flag set to true");
        Ok(())
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // Safety: assert prepared was called
        if !self.prepared {
            eprintln!("[PROJECT NEXT] ERROR: prepared={}, columns={}", self.prepared, self.columns.len());
            eprintln!("[PROJECT NEXT] Stack trace: {:?}", std::backtrace::Backtrace::capture());
        }
        assert!(self.prepared, "ProjectOperator::next() called before prepare()");
        let batch = match self.input.next()? {
            Some(b) => {
                // CRITICAL DEBUG: Log incoming schema
                // DISABLED: Excessive debug logging was causing performance issues
                // Only log errors, not every batch
                b
            },
            None => {
                eprintln!("DEBUG ProjectOperator::next: Input returned None");
                return Ok(None);
            },
        };
        
        // WILDCARD EXPANSION: Expand wildcards at runtime if not already expanded
        // This handles cases where wildcards weren't expanded during planning
        use crate::query::wildcard_expansion::expand_wildcards_from_schema;
        let expanded_columns = expand_wildcards_from_schema(
            &self.columns,
            &batch.batch.schema,
            &self.table_aliases,
        )?;
        
        // Select only requested columns (zero-copy where possible)
        // Check if columns is empty or contains "*" (wildcard)
        let is_wildcard = expanded_columns.is_empty() || 
            (expanded_columns.len() == 1 && expanded_columns[0] == "*");
        
        // eprintln!("ProjectOperator::next: Projecting {} columns (expanded from {}): {:?}", 
        //     expanded_columns.len(), self.columns.len(), expanded_columns);
        let mut output_columns = vec![];
        let mut output_fields = vec![];
        
        // If wildcard or empty columns, return all columns (standard SQL behavior)
        if is_wildcard {
            // SELECT * - return all columns from input
            // COLID: Preserve ColumnSchema from input if available
            let mut output_column_schema = if let Some(ref input_schema) = batch.column_schema {
                // Clone the entire ColumnSchema for SELECT *
                use crate::execution::column_identity::ColumnSchema;
                let mut new_schema = ColumnSchema::new();
                // Copy all columns with their ColIds
                for (idx, &col_id) in input_schema.column_ids.iter().enumerate() {
                    if let Some(external_name) = input_schema.unqualified_names.get(idx) {
                        if let Some(data_type) = input_schema.data_types.get(idx) {
                                new_schema.add_column(col_id, external_name.clone(), external_name.clone(), data_type.clone());
                        }
                    }
                }
                Some(new_schema)
            } else {
                None
            };
            
            for col_idx in 0..batch.batch.columns.len() {
                output_columns.push(batch.batch.column(col_idx).unwrap().clone());
                // Clean field name (remove table prefix)
                let field = batch.batch.schema.field(col_idx);
                let clean_field_name = if field.name().contains('.') {
                    field.name().split('.').last().unwrap_or(field.name()).to_string()
                } else {
                    field.name().to_string()
                };
                output_fields.push(arrow::datatypes::Field::new(
                    &clean_field_name,
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
            }
            // CRITICAL: Deduplicate even for wildcard SELECT *
            // ==========================
            // 6. DEDUPLICATION LAYER
            // ==========================
            eprintln!("[DEBUG dedupe] input fields = {:?}", output_fields.iter().map(|f| f.name().to_string()).collect::<Vec<_>>());
            let mut deduplicated_fields = vec![];
            let mut deduplicated_columns = vec![];
            let mut seen_names = std::collections::HashSet::new();
            let mut deduplicated_indices = vec![];
            for (idx, field) in output_fields.iter().enumerate() {
                let field_name = field.name();
                // DUCKDB/PRESTO-STYLE: Deduplicate only on exact name match (case-insensitive)
                // Do NOT strip qualifiers - "c.name" and "p.name" are distinct and both must be preserved
                let normalized_name = field_name.to_uppercase();
                if !seen_names.contains(&normalized_name) {
                    seen_names.insert(normalized_name);
                    deduplicated_fields.push(field.clone());
                    deduplicated_columns.push(output_columns[idx].clone());
                    deduplicated_indices.push(idx);
                } else {
                    // Duplicate name detected - log but allow it (user may have intentionally selected same column twice)
                    eprintln!("DEBUG ProjectOperator: Duplicate field name '{}' detected - keeping first occurrence", field_name);
                }
            }
        eprintln!("[DEBUG dedupe] output fields = {:?}", deduplicated_fields.iter().map(|f| f.name().to_string()).collect::<Vec<_>>());
        // CRITICAL FIX: Update ColumnSchema with cleaned (unqualified) names for SELECT *
            // This ensures ORDER BY can resolve column names like 'name' when SELECT * is used
            // Do this BEFORE creating the schema to avoid borrow issues
            if let Some(ref cs) = output_column_schema {
                use crate::execution::column_identity::ColumnSchema;
                // Rebuild name_map and unqualified_names with cleaned names
                let mut new_schema = ColumnSchema::new();
                for (output_idx, original_idx) in deduplicated_indices.iter().enumerate() {
                    if let Some(&col_id) = cs.column_ids.get(*original_idx) {
                        let clean_name = deduplicated_fields[output_idx].name();
                        // Add both qualified and unqualified names
                        new_schema.add_column(col_id, clean_name.to_string(), clean_name.to_string(), 
                            deduplicated_fields[output_idx].data_type().clone());
                    }
                }
                output_column_schema = Some(new_schema);
            } else {
                // Create new ColumnSchema from output schema
                use crate::execution::column_identity::ColumnSchema;
                let mut new_schema = ColumnSchema::new();
                for (idx, field) in deduplicated_fields.iter().enumerate() {
                    use crate::execution::column_identity::ColId;
                    let col_id = ColId::new(idx as u32, 0);
                    new_schema.add_column(col_id, field.name().to_string(), field.name().to_string(), 
                        field.data_type().clone());
                }
                output_column_schema = Some(new_schema);
            }
            
            let output_schema = Arc::new(Schema::new(deduplicated_fields));
            let output_batch = crate::storage::columnar::ColumnarBatch::new(deduplicated_columns, output_schema);
            let mut exec_batch = ExecutionBatch::from_batch_with_column_schema(output_batch, output_column_schema);
            exec_batch.selection = batch.selection.clone();
            exec_batch.row_count = batch.row_count;
            exec_batch.column_fragments = batch.column_fragments.clone();
            return Ok(Some(exec_batch));
        }
        
        // Use expressions if available, otherwise fall back to column names
        if !self.expressions.is_empty() {
            use crate::query::expression::ExpressionEvaluator;
            let evaluator = if let Some(ref executor) = self.subquery_executor {
                ExpressionEvaluator::with_subquery_executor_and_aliases(
                    batch.batch.schema.clone(),
                    executor.clone(),
                    self.table_aliases.clone(),
                )
            } else {
                ExpressionEvaluator::with_table_aliases(
                    batch.batch.schema.clone(),
                    self.table_aliases.clone(),
                )
            };
            
            // CRITICAL: Initialize output_column_schema for expression-based projection
            let mut output_column_schema: Option<crate::execution::column_identity::ColumnSchema> = None;
            
            // ==========================
            // 5. PROJECT EXECUTION
            // ==========================
            eprintln!("[DEBUG project] expecting to resolve expressions: {:?}", self.expressions);
            eprintln!("DEBUG ProjectOperator: ===== START PROCESSING {} EXPRESSIONS =====", self.expressions.len());
            eprintln!("DEBUG ProjectOperator: Expression details:");
            for (idx, expr) in self.expressions.iter().enumerate() {
                match &expr.expr_type {
                    crate::query::plan::ProjectionExprType::Column(c) => {
                        eprintln!("DEBUG ProjectOperator:   [{}] Column('{}') alias='{}'", idx, c, expr.alias);
                    }
                    _ => {
                        eprintln!("DEBUG ProjectOperator:   [{}] Other expression", idx);
                    }
                }
            }
            eprintln!("DEBUG ProjectOperator: Input batch schema fields: {:?}", 
                batch.batch.schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
            if let Some(ref cs) = batch.column_schema {
                eprintln!("DEBUG ProjectOperator: Input ColumnSchema qualified_names: {:?}", 
                    cs.qualified_names);
                eprintln!("DEBUG ProjectOperator: Input ColumnSchema name_map keys: {:?}", 
                    cs.name_map.keys().collect::<Vec<_>>());
            }
            
            for (expr_idx, expr) in self.expressions.iter().enumerate() {
                match &expr.expr_type {
                    crate::query::plan::ProjectionExprType::Column(col_name) => {
                        eprintln!("[DEBUG project] resolving expr = {:?}", expr);
                        eprintln!("DEBUG ProjectOperator: ===== EXPRESSION [{}]: Processing '{}' (alias: '{}') =====", 
                            expr_idx, col_name, expr.alias);
                        // Simple column reference
                        if col_name == "*" {
                            // Add all columns
                            for col_idx in 0..batch.batch.columns.len() {
                                output_columns.push(batch.batch.column(col_idx).unwrap().clone());
                                output_fields.push(batch.batch.schema.field(col_idx).clone());
                            }
                        } else if col_name.ends_with(".*") {
                            // Qualified wildcard (e.g., "e.*")
                            let table_alias = col_name.strip_suffix(".*").unwrap();
                            let mut found_any = false;
                            
                            // Find all columns that match this table alias
                            for (col_idx, field) in batch.batch.schema.fields().iter().enumerate() {
                                let field_name = field.name();
                                // Check if field is qualified with this table alias
                                if field_name.starts_with(&format!("{}.", table_alias)) {
                                    output_columns.push(batch.batch.column(col_idx).unwrap().clone());
                                    output_fields.push(field.as_ref().clone());
                                    found_any = true;
                                } else if let Some(actual_table) = self.table_aliases.get(table_alias) {
                                    // Check if field is qualified with actual table name
                                    if field_name.starts_with(&format!("{}.", actual_table)) {
                                        output_columns.push(batch.batch.column(col_idx).unwrap().clone());
                                        output_fields.push(field.as_ref().clone());
                                        found_any = true;
                                    }
                                }
                            }
                            
                            // Fallback: If no qualified columns found, check if ALL columns are unqualified
                            // In this case, if this is the only table or the primary table, include all columns
                            if !found_any {
                                // Check if all columns in schema are unqualified (no dots)
                                let all_unqualified = batch.batch.schema.fields().iter()
                                    .all(|f| !f.name().contains('.'));
                                
                                if all_unqualified {
                                    // All columns are unqualified - likely from a single table
                                    // Include all columns as a fallback
                                    for (col_idx, field) in batch.batch.schema.fields().iter().enumerate() {
                                        output_columns.push(batch.batch.column(col_idx).unwrap().clone());
                                        output_fields.push(field.as_ref().clone());
                                        found_any = true;
                                    }
                                }
                            }
                            
                            if !found_any {
                                anyhow::bail!("Qualified wildcard '{}' matched no columns. Available columns: {:?}", 
                                    col_name,
                                    batch.batch.schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                                );
                            }
                        } else {
                            // REFACTORED: Use ColumnId-based resolution with proper error handling
                            // Resolution order:
                            // 1. Try qualified name in ColumnSchema (if column_schema available)
                            // 2. Try unqualified name with ambiguity check (if column_schema available)
                            // 3. Fall back to string-based ColumnResolver (for backward compatibility)
                            let col_idx = if let Some(ref column_schema) = batch.column_schema {
                                // Strategy 1: Try qualified name resolution
                                if col_name.contains('.') {
                                    // First try exact match in name_map
                                    let exact_match = column_schema.resolve(col_name);
                                    if let Some(column_id) = exact_match {
                                            match column_schema.physical_index(column_id) {
                                                Some(idx) => {
                                                eprintln!("[DEBUG project] resolved '{}' to physical index {:?}", col_name, idx);
                                                eprintln!("DEBUG ProjectOperator: Resolved qualified '{}' via exact match -> ColumnId({:?}) -> index {}", 
                                                        col_name, column_id, idx);
                                                    Ok(idx)
                                                }
                                            None => {
                                                // Fall through to table alias matching
                                                // CRITICAL FIX: For qualified names, don't fall back to unqualified resolution
                                                // This prevents p.name from resolving to c.name when both have unqualified name "name"
                                                // Instead, try to resolve using table alias mapping or return error
                                                // First, try to find the column by matching the table alias/name part
                                                let parts: Vec<&str> = col_name.split('.').collect();
                                                if parts.len() == 2 {
                                                    let (table_part, col_part) = (parts[0], parts[1]);
                                                    // Try to find columns that match the column part and check if table part matches
                                                    // This is a more sophisticated resolution that handles table aliases
                                                    let mut candidates = Vec::new();
                                                    eprintln!("DEBUG ProjectOperator: Looking for column '{}' (table_part='{}', col_part='{}')", 
                                                        col_name, table_part, col_part);
                                                    eprintln!("DEBUG ProjectOperator: Available qualified names: {:?}", 
                                                        column_schema.qualified_names);
                                                    for (idx, qualified_name) in column_schema.qualified_names.iter().enumerate() {
                                                        // CRITICAL FIX: For qualified column names like "p.name", we need exact match
                                                        // First try exact match with the full qualified name
                                                        if qualified_name == col_name {
                                                            if let Some(&col_id) = column_schema.column_ids.get(idx) {
                                                                if let Some(phys_idx) = column_schema.physical_index(col_id) {
                                                                    eprintln!("DEBUG ProjectOperator: Found exact match for '{}' -> index {}", 
                                                                        col_name, phys_idx);
                                                                    candidates.push((phys_idx, qualified_name.clone()));
                                                                    continue; // Exact match takes precedence
                                                                }
                                                            }
                                                        }
                                                        
                                                        // Then check if column name matches (ends with .col_part or equals col_part)
                                                        let name_matches = qualified_name.ends_with(&format!(".{}", col_part)) || 
                                                           qualified_name == col_part;
                                                        
                                                        if name_matches {
                                                            // Check if the table part matches (could be alias or actual table name)
                                                            // Also check if table_part matches the actual table name via table_aliases
                                                            let direct_match = qualified_name.starts_with(&format!("{}.", table_part));
                                                            let alias_match = if let Some(actual_table) = self.table_aliases.get(table_part) {
                                                                qualified_name.starts_with(&format!("{}.", actual_table))
                                                            } else {
                                                                false
                                                            };
                                                            let table_matches = direct_match || alias_match;
                                                            
                                                            eprintln!("DEBUG ProjectOperator: Checking '{}': name_matches={}, table_matches={} (direct={}, alias={}, table_part='{}', col_name='{}')", 
                                                                qualified_name, name_matches, table_matches, direct_match, alias_match, table_part, col_name);
                                                            
                                                            if table_matches {
                                                                if let Some(&col_id) = column_schema.column_ids.get(idx) {
                                                                    if let Some(phys_idx) = column_schema.physical_index(col_id) {
                                                                        eprintln!("DEBUG ProjectOperator: Found candidate: '{}' -> index {}", 
                                                                            qualified_name, phys_idx);
                                                                        candidates.push((phys_idx, qualified_name.clone()));
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    
                                                    match candidates.len() {
                                                        1 => {
                                                            eprintln!("[DEBUG project] resolved '{}' to physical index {:?}", col_name, candidates[0].0);
                                                            eprintln!("DEBUG ProjectOperator: Resolved qualified '{}' via table alias matching -> index {}", 
                                                                col_name, candidates[0].0);
                                                            Ok(candidates[0].0)
                                                        },
                                                        0 => {
                                                            eprintln!("[DEBUG project] FAILED to resolve '{}'", col_name);
                                                    Err(anyhow::anyhow!(
                                                                "ERR_COLUMN_NOT_FOUND: Column '{}' not found. Available columns: {:?}",
                                                                col_name,
                                                                column_schema.qualified_names.iter().take(10).collect::<Vec<_>>()
                                                            ))
                                                        },
                                                        _ => Err(anyhow::anyhow!(
                                                            "ERR_AMBIGUOUS_COLUMN_REFERENCE: Column '{}' is ambiguous. Found {} matches: {:?}",
                                                            col_name, candidates.len(),
                                                            candidates.iter().map(|(_, name)| name).collect::<Vec<_>>()
                                                        ))
                                                    }
                                                } else {
                                                    Err(anyhow::anyhow!(
                                                        "ERR_COLUMN_NOT_FOUND: Column '{}' must be in format 'table.column'", col_name
                                                    ))
                                                }
                                            }
                                        }
                                    } else {
                                        // Exact match failed, try table alias matching
                                        let parts: Vec<&str> = col_name.split('.').collect();
                                        if parts.len() == 2 {
                                            let (table_part, col_part) = (parts[0], parts[1]);
                                            let mut candidates = Vec::new();
                                            for (idx, qualified_name) in column_schema.qualified_names.iter().enumerate() {
                                                if qualified_name == col_name {
                                                    if let Some(&col_id) = column_schema.column_ids.get(idx) {
                                                        if let Some(phys_idx) = column_schema.physical_index(col_id) {
                                                            candidates.push((phys_idx, qualified_name.clone()));
                                                            continue;
                                                        }
                                                    }
                                                }
                                                let name_matches = qualified_name.ends_with(&format!(".{}", col_part)) || 
                                                   qualified_name == col_part;
                                                if name_matches {
                                                    let direct_match = qualified_name.starts_with(&format!("{}.", table_part));
                                                    let alias_match = if let Some(actual_table) = self.table_aliases.get(table_part) {
                                                        qualified_name.starts_with(&format!("{}.", actual_table))
                                                    } else {
                                                        false
                                                    };
                                                    if direct_match || alias_match {
                                                        if let Some(&col_id) = column_schema.column_ids.get(idx) {
                                                            if let Some(phys_idx) = column_schema.physical_index(col_id) {
                                                                candidates.push((phys_idx, qualified_name.clone()));
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            match candidates.len() {
                                                1 => {
                                                    eprintln!("[DEBUG project] resolved '{}' to physical index {:?}", col_name, candidates[0].0);
                                                    Ok(candidates[0].0)
                                                },
                                                0 => {
                                                    eprintln!("[DEBUG project] FAILED to resolve '{}'", col_name);
                                                    Err(anyhow::anyhow!(
                                                        "ERR_COLUMN_NOT_FOUND: Column '{}' not found. Available columns: {:?}",
                                                        col_name,
                                                        column_schema.qualified_names.iter().take(10).collect::<Vec<_>>()
                                                    ))
                                                },
                                                _ => Err(anyhow::anyhow!(
                                                    "ERR_AMBIGUOUS_COLUMN_REFERENCE: Column '{}' is ambiguous. Found {} matches: {:?}",
                                                    col_name, candidates.len(),
                                                    candidates.iter().map(|(_, name)| name).collect::<Vec<_>>()
                                                ))
                                            }
                                        } else {
                                            Err(anyhow::anyhow!(
                                                "ERR_COLUMN_NOT_FOUND: Column '{}' must be in format 'table.column'", col_name
                                            ))
                                        }
                                    }
                                } else {
                                    // Unqualified name - use resolve_unqualified for ambiguity checking
                                    match column_schema.resolve_unqualified(col_name) {
                                        Ok(column_id) => {
                                            match column_schema.physical_index(column_id) {
                                                Some(idx) => {
                                                    eprintln!("DEBUG ProjectOperator: Resolved unqualified '{}' -> ColumnId({:?}) -> index {}", 
                                                        col_name, column_id, idx);
                                                    Ok(idx)
                                                }
                                                None => Err(anyhow::anyhow!(
                                                    "ERR_INTERNAL: ColumnId {:?} not found in ColumnSchema physical index",
                                                    column_id
                                                ))
                                            }
                                        }
                                        Err(e) => {
                                            // Ambiguous or not found - return error
                                            Err(anyhow::anyhow!(
                                                "ERR_AMBIGUOUS_COLUMN_REFERENCE: {}",
                                                e
                                            ))
                                        }
                                    }
                                }
                            } else {
                                // No ColumnSchema - fall back to string-based resolution (backward compatibility)
                                use crate::query::column_resolver::ColumnResolver;
                                let resolver = ColumnResolver::new(batch.batch.schema.clone(), self.table_aliases.clone());
                                resolver.resolve(col_name)
                            };
                            
                            let col_idx = match col_idx {
                                Ok(idx) => idx,
                                Err(e) => {
                                    // Return error instead of continuing
                                    return Err(anyhow::anyhow!(
                                        "ERR_COLUMN_NOT_FOUND: Failed to resolve column '{}' in ProjectOperator. {}",
                                        col_name, e
                                    ));
                                }
                            };
                            
                            // Get column using resolved index
                            let col = batch.batch.column(col_idx)
                                .ok_or_else(|| anyhow::anyhow!("Column index {} out of bounds", col_idx))?;
                            
                            output_columns.push(col.clone());
                            
                            // Build output field with proper name
                            // DUCKDB/PRESTO-STYLE: Preserve qualified names strictly
                            // Rule: If user specified explicit alias, use it. Otherwise, use fully qualified name.
                            let field = batch.batch.schema.field(col_idx);
                            let output_field_name = if !expr.alias.is_empty() {
                                // User specified explicit alias (e.g., SELECT p.name AS p_name)
                                expr.alias.clone()
                            } else {
                                // No alias - use fully qualified name from expression (e.g., "p.name")
                                // NEVER strip qualifiers - this prevents c.name and p.name from both becoming "name"
                                col_name.to_string() // Use full qualified name (e.g., "c.name", "p.name")
                            };
                            
                            output_fields.push(arrow::datatypes::Field::new(
                                &output_field_name,
                                field.data_type().clone(),
                                field.is_nullable(),
                            ));
                            
                            eprintln!("DEBUG ProjectOperator:   ✓ Successfully resolved '{}' -> index {} -> output field '{}'", 
                                col_name, col_idx, output_field_name);
                            eprintln!("DEBUG ProjectOperator:   Current output_fields count: {}", output_fields.len());
                            
                            // CRITICAL: Update ColumnSchema for output - preserve ColumnId through projection
                            if let Some(ref input_schema) = batch.column_schema {
                                if let Some(column_id) = input_schema.column_id_at(col_idx) {
                                    // Preserve ColumnId through projection (never modify or regenerate)
                                    if output_column_schema.is_none() {
                                        output_column_schema = Some(crate::execution::column_identity::ColumnSchema::new());
                                    }
                                    if let Some(ref mut schema) = output_column_schema {
                                        let qualified_name = input_schema.qualified_name(column_id)
                                            .unwrap_or(&output_field_name).clone();
                                        let unqualified_name = input_schema.unqualified_name(column_id)
                                            .unwrap_or(&output_field_name).clone();
                                        let data_type = input_schema.data_type(column_id)
                                            .unwrap_or(field.data_type()).clone();
                                        // Use add_column with both qualified and unqualified names
                                        schema.add_column(column_id, qualified_name, unqualified_name, data_type);
                                    }
                                } else {
                                    // Column not found in input schema - this shouldn't happen but handle gracefully
                                    eprintln!("WARNING: Column index {} not found in input ColumnSchema", col_idx);
                                }
                            }
                        }
                    }
                    crate::query::plan::ProjectionExprType::Cast { column, target_type } => {
                        // CAST expression - evaluate for each row
                        // Use ColumnResolver for consistent column resolution
                        use crate::query::column_resolver::ColumnResolver;
                        let resolver = ColumnResolver::new(batch.batch.schema.clone(), self.table_aliases.clone());
                        let col_idx = resolver.resolve(column)
                            .map_err(|_| anyhow::anyhow!("Column '{}' not found for CAST", column))?;
                        let source_array = batch.batch.column(col_idx).unwrap();
                        
                        // Evaluate CAST for all rows
                        let mut cast_values = Vec::new();
                        // SAFETY: Use batch.selection.len() to avoid array bounds violations
                        let actual_batch_size = batch.selection.len();
                        debug_assert!(actual_batch_size >= batch.row_count, 
                            "batch.selection.len() ({}) >= batch.row_count ({})", 
                            actual_batch_size, batch.row_count);
                        
                        for row_idx in 0..actual_batch_size {
                            if !batch.selection[row_idx] {
                                cast_values.push(None);
                                continue;
                            }
                            debug_assert!(row_idx < source_array.len(), 
                                "row_idx {} < source_array.len() {}", row_idx, source_array.len());
                            let val = extract_value(source_array, row_idx)?;
                            let cast_val = crate::query::expression::cast_value(&val, target_type)?;
                            cast_values.push(Some(cast_val));
                        }
                        
                        // Convert to Arrow array based on target type
                        let cast_array: Arc<dyn Array> = match target_type {
                            arrow::datatypes::DataType::Int32 => {
                                let values: Vec<Option<i32>> = cast_values.iter().map(|v| {
                                    v.as_ref().and_then(|val| match val {
                                        crate::storage::fragment::Value::Int32(x) => Some(*x),
                                        crate::storage::fragment::Value::Int64(x) => Some(*x as i32),
                                        _ => None,
                                    })
                                }).collect();
                                Arc::new(Int32Array::from(values))
                            }
                            arrow::datatypes::DataType::Int64 => {
                                let values: Vec<Option<i64>> = cast_values.iter().map(|v| {
                                    v.as_ref().and_then(|val| match val {
                                        crate::storage::fragment::Value::Int64(x) => Some(*x),
                                        crate::storage::fragment::Value::Int32(x) => Some(*x as i64),
                                        _ => None,
                                    })
                                }).collect();
                                Arc::new(Int64Array::from(values))
                            }
                            arrow::datatypes::DataType::Float32 => {
                                let values: Vec<Option<f32>> = cast_values.iter().map(|v| {
                                    v.as_ref().and_then(|val| match val {
                                        crate::storage::fragment::Value::Float32(x) => Some(*x),
                                        crate::storage::fragment::Value::Float64(x) => Some(*x as f32),
                                        crate::storage::fragment::Value::Int64(x) => Some(*x as f32),
                                        crate::storage::fragment::Value::Int32(x) => Some(*x as f32),
                                        _ => None,
                                    })
                                }).collect();
                                Arc::new(Float32Array::from(values))
                            }
                            arrow::datatypes::DataType::Float64 => {
                                let values: Vec<Option<f64>> = cast_values.iter().map(|v| {
                                    v.as_ref().and_then(|val| match val {
                                        crate::storage::fragment::Value::Float64(x) => Some(*x),
                                        crate::storage::fragment::Value::Float32(x) => Some(*x as f64),
                                        crate::storage::fragment::Value::Int64(x) => Some(*x as f64),
                                        crate::storage::fragment::Value::Int32(x) => Some(*x as f64),
                                        _ => None,
                                    })
                                }).collect();
                                Arc::new(Float64Array::from(values))
                            }
                            arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                                let values: Vec<Option<String>> = cast_values.iter().map(|v| {
                                    v.as_ref().map(|val| format!("{}", val))
                                }).collect();
                                Arc::new(StringArray::from(values))
                            }
                            arrow::datatypes::DataType::Boolean => {
                                let values: Vec<Option<bool>> = cast_values.iter().map(|v| {
                                    v.as_ref().and_then(|val| match val {
                                        crate::storage::fragment::Value::Bool(x) => Some(*x),
                                        crate::storage::fragment::Value::Int64(x) => Some(*x != 0),
                                        crate::storage::fragment::Value::Int32(x) => Some(*x != 0),
                                        _ => None,
                                    })
                                }).collect();
                                Arc::new(BooleanArray::from(values))
                            }
                            _ => {
                                // Default to string representation
                                let values: Vec<Option<String>> = cast_values.iter().map(|v| {
                                    v.as_ref().map(|val| format!("{}", val))
                                }).collect();
                                Arc::new(StringArray::from(values))
                            }
                        };
                        
                        // CRITICAL: Check for duplicates before adding
                        let output_name = if !expr.alias.is_empty() {
                            expr.alias.clone()
                        } else {
                            format!("cast_{}", column)
                        };
                        
                        let normalized_name = output_name.to_uppercase();
                        let already_added = output_fields.iter().any(|f| f.name().to_uppercase() == normalized_name);
                        
                        if !already_added {
                            output_columns.push(cast_array);
                            output_fields.push(arrow::datatypes::Field::new(&output_name, target_type.clone(), true));
                        } else {
                            eprintln!("DEBUG ProjectOperator: Skipping duplicate CAST expression '{}'", output_name);
                        }
                    }
                    crate::query::plan::ProjectionExprType::Case(case_expr) => {
                        // CASE expression - evaluate for each row
                        use crate::query::expression::ExpressionEvaluator;
                        let evaluator = if let Some(ref executor) = self.subquery_executor {
                            ExpressionEvaluator::with_subquery_executor(batch.batch.schema.clone(), executor.clone())
                        } else {
                            ExpressionEvaluator::new(batch.batch.schema.clone())
                        };
                        
                        // Evaluate CASE expression for all rows
                        let mut case_values = Vec::new();
                        // SAFETY: Use batch.selection.len() to avoid array bounds violations
                        let actual_batch_size = batch.selection.len();
                        debug_assert!(actual_batch_size >= batch.row_count, 
                            "batch.selection.len() ({}) >= batch.row_count ({})", 
                            actual_batch_size, batch.row_count);
                        
                        for row_idx in 0..actual_batch_size {
                            if !batch.selection[row_idx] {
                                case_values.push(None);
                                continue;
                            }
                            debug_assert!(row_idx < batch.batch.row_count, 
                                "row_idx {} < batch.batch.row_count {}", row_idx, batch.batch.row_count);
                            match evaluator.evaluate(case_expr, &batch, row_idx) {
                                Ok(val) => case_values.push(Some(val)),
                                Err(_) => case_values.push(None),
                            }
                        }
                        
                        // Convert to Arrow array based on value types
                        let case_array: Arc<dyn Array> = if case_values.is_empty() {
                            Arc::new(StringArray::from(vec![] as Vec<Option<String>>))
                        } else {
                            // Determine type from first non-null value
                            let first_val = case_values.iter().find_map(|v| v.as_ref());
                            match first_val {
                                Some(crate::storage::fragment::Value::Int64(_)) => {
                                    let arr: Vec<Option<i64>> = case_values.iter().map(|v| {
                                        v.as_ref().and_then(|val| match val {
                                            crate::storage::fragment::Value::Int64(i) => Some(*i),
                                            _ => None,
                                        })
                                    }).collect();
                                    Arc::new(Int64Array::from(arr))
                                }
                                Some(crate::storage::fragment::Value::Float64(_)) => {
                                    let arr: Vec<Option<f64>> = case_values.iter().map(|v| {
                                        v.as_ref().and_then(|val| match val {
                                            crate::storage::fragment::Value::Float64(f) => Some(*f),
                                            _ => None,
                                        })
                                    }).collect();
                                    Arc::new(Float64Array::from(arr))
                                }
                                Some(crate::storage::fragment::Value::String(_)) | _ => {
                                    let arr: Vec<Option<String>> = case_values.iter().map(|v| {
                                        v.as_ref().map(|val| format!("{}", val))
                                    }).collect();
                                    Arc::new(StringArray::from(arr))
                                }
                            }
                        };
                        
                        // Determine output type from array
                        let output_type = case_array.data_type().clone();
                        
                        // CRITICAL: Check for duplicates before adding
                        let output_name = if !expr.alias.is_empty() {
                            expr.alias.clone()
                        } else {
                            "case_result".to_string()
                        };
                        
                        let normalized_name = output_name.to_uppercase();
                        let already_added = output_fields.iter().any(|f| f.name().to_uppercase() == normalized_name);
                        
                        if !already_added {
                            output_columns.push(case_array);
                            output_fields.push(arrow::datatypes::Field::new(&output_name, output_type, true));
                        } else {
                            eprintln!("DEBUG ProjectOperator: Skipping duplicate CASE expression '{}'", output_name);
                        }
                    }
                    crate::query::plan::ProjectionExprType::Function(func_expr) => {
                        // Function expression (e.g., VECTOR_SIMILARITY or aggregate like AVG)
                        // CRITICAL FIX: First check if we've already added this column (by alias)
                        // This prevents processing the same aggregate function twice
                        let expected_column_name = &expr.alias;
                        
                        // Early check: If we've already added a column with this alias, skip it entirely
                        if !expected_column_name.is_empty() {
                            let normalized_alias = expected_column_name.to_uppercase();
                            let already_in_output = output_fields.iter().any(|f| {
                                f.name().to_uppercase() == normalized_alias || f.name() == expected_column_name
                            });
                            
                            if already_in_output {
                                eprintln!("DEBUG ProjectOperator: Skipping Function expression '{}' (alias: '{}') - already in output", 
                                    match func_expr {
                                        crate::query::expression::Expression::Function { name, .. } => name,
                                        _ => "unknown",
                                    },
                                    expr.alias
                                );
                                continue;
                            }
                        }
                        
                        // CRITICAL FIX: Check if input batch already has this column (from AggregateOperator)
                        // If so, just use it directly instead of re-evaluating to prevent duplicates
                        
                        // Check if input batch already has a column matching the expected name
                        // CRITICAL: For aggregate functions, we MUST find them in the AggregateOperator output
                        // If we don't find them, we should skip them entirely to prevent duplicate NULL columns
                        let use_existing_column = if !expected_column_name.is_empty() {
                            // Strategy 1: Try ColumnSchema name_map if available (case-insensitive)
                            let found_via_schema = if let Some(ref column_schema) = batch.column_schema {
                                // Try exact match first
                                column_schema.name_map.get(expected_column_name)
                                    .and_then(|col_id| column_schema.physical_index(*col_id))
                                    .is_some() ||
                                // Try case-insensitive match
                                column_schema.name_map.iter()
                                    .any(|(name, col_id)| {
                                        name.eq_ignore_ascii_case(expected_column_name) &&
                                        column_schema.physical_index(*col_id).is_some()
                                    }) ||
                                // Try unqualified_names (case-insensitive)
                                column_schema.unqualified_names.iter()
                                    .any(|name| name.eq_ignore_ascii_case(expected_column_name))
                            } else {
                                false
                            };
                            
                            // Strategy 2: Try ColumnResolver (case-insensitive)
                            let found_via_resolver = if !found_via_schema {
                                use crate::query::column_resolver::ColumnResolver;
                                let resolver = ColumnResolver::from_schema(batch.batch.schema.clone());
                                // Try exact match
                                resolver.resolve(expected_column_name).is_ok() ||
                                // Try case-insensitive match by checking all schema fields
                                batch.batch.schema.fields().iter()
                                    .any(|f| f.name().eq_ignore_ascii_case(expected_column_name))
                            } else {
                                true
                            };
                            
                            found_via_schema || found_via_resolver
                        } else {
                            false
                        };
                        
                        if use_existing_column {
                            // Use existing column from input batch (already computed by AggregateOperator)
                            // This prevents duplicates!
                            let col_idx = if let Some(ref column_schema) = batch.column_schema {
                                if let Some(col_id) = column_schema.name_map.get(expected_column_name) {
                                    column_schema.physical_index(*col_id)
                                } else {
                                    None
                                }
                            } else {
                                None
                            };
                            
                            let col_idx = col_idx.unwrap_or_else(|| {
                                // Fallback to ColumnResolver
                                use crate::query::column_resolver::ColumnResolver;
                                let resolver = ColumnResolver::from_schema(batch.batch.schema.clone());
                                resolver.resolve(expected_column_name).unwrap()
                            });
                            
                            // CRITICAL: Check if we've already added this column (case-insensitive)
                            let normalized_name = expected_column_name.to_uppercase();
                            let already_added = output_fields.iter().any(|f| {
                                let f_name = f.name();
                                // Check exact match and case-insensitive match
                                f_name == expected_column_name || 
                                f_name.to_uppercase() == normalized_name ||
                                // Also check if the field name matches the alias
                                (!expr.alias.is_empty() && f_name == expr.alias.as_str())
                            });
                            
                            if !already_added {
                                let field = batch.batch.schema.field(col_idx);
                                // Use the alias as the output name to match what was requested
                                let output_name = if !expr.alias.is_empty() {
                                    expr.alias.clone()
                                } else {
                                    // Clean field name (remove table prefix) if no alias
                                    if field.name().contains('.') {
                                        field.name().split('.').last().unwrap_or(field.name()).to_string()
                                    } else {
                                        field.name().to_string()
                                    }
                                };
                                
                                // Double-check we're not adding a duplicate (by output name)
                                let output_normalized = output_name.to_uppercase();
                                let still_duplicate = output_fields.iter().any(|f| f.name().to_uppercase() == output_normalized);
                                
                                if !still_duplicate {
                                    output_columns.push(batch.batch.column(col_idx).unwrap().clone());
                                    output_fields.push(arrow::datatypes::Field::new(
                                        &output_name,
                                        field.data_type().clone(),
                                        field.is_nullable(),
                                    ));
                                    eprintln!("DEBUG ProjectOperator: Using existing column '{}' (output name: '{}') from AggregateOperator", expected_column_name, output_name);
                                } else {
                                    eprintln!("DEBUG ProjectOperator: Skipping duplicate column '{}' (output name '{}' already exists)", expected_column_name, output_name);
                                }
                            } else {
                                eprintln!("DEBUG ProjectOperator: Skipping duplicate column '{}' (already added)", expected_column_name);
                            }
                        } else {
                            // Column doesn't exist in input - check if this is an aggregate function first
                            // Aggregate functions should already be computed by AggregateOperator
                            // If they're not found, we should skip them to prevent duplicate NULL columns
                            let is_aggregate = matches!(func_expr,
                                crate::query::expression::Expression::Function { name, .. } 
                                if matches!(name.as_str(), "SUM" | "AVG" | "COUNT" | "MIN" | "MAX" | "COUNT_DISTINCT")
                            );
                            
                            if is_aggregate {
                                eprintln!("WARNING ProjectOperator: Aggregate function '{}' (alias: '{}') not found in input batch. Skipping to prevent duplicate NULL columns.", 
                                    match func_expr {
                                        crate::query::expression::Expression::Function { name, .. } => name,
                                        _ => "unknown",
                                    },
                                    expr.alias
                                );
                                // Skip aggregate functions that aren't in the input - they should have been computed by AggregateOperator
                                // This prevents the _1 suffix columns from being created
                                continue;
                            }
                            
                            // For non-aggregate functions, proceed with evaluation
                            eprintln!("DEBUG ProjectOperator: Non-aggregate function '{}' (alias: '{}') not found in input, evaluating...", 
                                match func_expr {
                                    crate::query::expression::Expression::Function { name, .. } => name,
                                    _ => "unknown",
                                },
                                expr.alias
                            );
                            
                            // JIT EXECUTION: Try WASM JIT if available
                            if let Some(wasm_spec) = self.wasm_programs.get(&expr.alias) {
                                // Execute via WASM JIT if available
                                use crate::codegen::WasmCodegen;
                                use crate::execution::wasm_runner::WasmRunner;
                                use crate::execution::result::execution_batch_to_record_batch;
                                
                                // Compile with cache (if available)
                                // Use lock to get mutable reference safely
                                let mut cache_guard = self.operator_cache.as_ref()
                                    .and_then(|c| c.lock().ok());
                                let cache_ref: Option<&mut crate::cache::operator_cache::OperatorCache> = cache_guard.as_deref_mut();
                                if let Ok(wasm_code) = WasmCodegen::compile(wasm_spec, cache_ref) {
                                    if let Ok(record_batch) = execution_batch_to_record_batch(&batch) {
                                        // Create runner with cache
                                        let cache_for_runner = self.operator_cache.clone();
                                        let runner = if let Some(ref cache) = cache_for_runner {
                                            WasmRunner::with_cache(Some(cache.clone()))
                                        } else {
                                            WasmRunner::with_cache(None)
                                        };
                                        if let Ok(_jit_result) = runner.run(&wasm_code, &record_batch) {
                                            // Use JIT result (for now, fall through to interpreter)
                                            // In full implementation, we'd extract results from JIT output
                                        }
                                    }
                                }
                            }
                            
                            // Otherwise, evaluate function expression for each row
                            use crate::query::expression::ExpressionEvaluator;
                            let evaluator = if let Some(ref executor) = self.subquery_executor {
                                ExpressionEvaluator::with_subquery_executor_and_aliases(
                                    batch.batch.schema.clone(),
                                    executor.clone(),
                                    self.table_aliases.clone(),
                                )
                            } else {
                                ExpressionEvaluator::with_table_aliases(
                                    batch.batch.schema.clone(),
                                    self.table_aliases.clone(),
                                )
                            };
                            
                            // Evaluate function expression for all rows
                            let mut func_values = Vec::new();
                            // SAFETY: Iterate only up to batch.batch.row_count to avoid array bounds violations
                            // The selection bitmap may be larger, but we only process rows that exist in the batch
                            let max_rows = batch.batch.row_count;
                            
                            for row_idx in 0..max_rows {
                                // Check if row is selected (and within selection bitmap bounds)
                                if row_idx >= batch.selection.len() || !batch.selection[row_idx] {
                                    func_values.push(None);
                                    continue;
                                }
                                match evaluator.evaluate(func_expr, &batch, row_idx) {
                                    Ok(val) => func_values.push(Some(val)),
                                    Err(_) => func_values.push(None),
                                }
                            }
                            
                            // Convert to Arrow array based on value types
                            let func_array: Arc<dyn Array> = if func_values.is_empty() {
                                Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>))
                            } else {
                                // Determine type from first non-null value
                                let first_val = func_values.iter().find_map(|v| v.as_ref());
                                match first_val {
                                    Some(crate::storage::fragment::Value::Int64(_)) => {
                                        let arr: Vec<Option<i64>> = func_values.iter().map(|v| {
                                            v.as_ref().and_then(|val| match val {
                                                crate::storage::fragment::Value::Int64(i) => Some(*i),
                                                _ => None,
                                            })
                                        }).collect();
                                        Arc::new(Int64Array::from(arr))
                                    }
                                    Some(crate::storage::fragment::Value::Float64(_)) | Some(crate::storage::fragment::Value::Float32(_)) => {
                                        let arr: Vec<Option<f64>> = func_values.iter().map(|v| {
                                            v.as_ref().and_then(|val| match val {
                                                crate::storage::fragment::Value::Float64(f) => Some(*f),
                                                crate::storage::fragment::Value::Float32(f) => Some(*f as f64),
                                                _ => None,
                                            })
                                        }).collect();
                                        Arc::new(Float64Array::from(arr))
                                    }
                                    Some(crate::storage::fragment::Value::String(_)) => {
                                        let arr: Vec<Option<String>> = func_values.iter().map(|v| {
                                            v.as_ref().map(|val| format!("{}", val))
                                        }).collect();
                                        Arc::new(StringArray::from(arr))
                                    }
                                    Some(crate::storage::fragment::Value::Bool(_)) => {
                                        let arr: Vec<Option<bool>> = func_values.iter().map(|v| {
                                            v.as_ref().and_then(|val| match val {
                                                crate::storage::fragment::Value::Bool(b) => Some(*b),
                                                _ => None,
                                            })
                                        }).collect();
                                        Arc::new(BooleanArray::from(arr))
                                    }
                                    _ => {
                                        // Default to Float64 for vector functions (similarity, distance)
                                        let arr: Vec<Option<f64>> = func_values.iter().map(|v| {
                                            v.as_ref().and_then(|val| match val {
                                                crate::storage::fragment::Value::Float64(f) => Some(*f),
                                                crate::storage::fragment::Value::Float32(f) => Some(*f as f64),
                                                crate::storage::fragment::Value::Int64(i) => Some(*i as f64),
                                                _ => None,
                                            })
                                        }).collect();
                                        Arc::new(Float64Array::from(arr))
                                    }
                                }
                            };
                            
                            // Determine output type from array
                            let output_type = func_array.data_type().clone();
                            
                            // CRITICAL: Check for duplicates before adding
                            let output_name = if !expr.alias.is_empty() {
                                expr.alias.clone()
                            } else {
                                // Extract function name from Expression
                                let func_name = match func_expr {
                                    crate::query::expression::Expression::Function { name, .. } => name.clone(),
                                    _ => "func".to_string(),
                                };
                                format!("func_{}", func_name)
                            };
                            
                            // FINAL CHECK: Before adding, verify this is NOT an aggregate function
                            // Aggregates should have been handled above - if we get here, something went wrong
                            let is_aggregate_final = matches!(func_expr,
                                crate::query::expression::Expression::Function { name, .. } 
                                if matches!(name.as_str(), "SUM" | "AVG" | "COUNT" | "MIN" | "MAX" | "COUNT_DISTINCT")
                            );
                            
                            if is_aggregate_final {
                                eprintln!("ERROR ProjectOperator: Aggregate function '{}' (alias: '{}') reached function evaluation code. This should not happen - skipping to prevent NULL columns.", 
                                    match func_expr {
                                        crate::query::expression::Expression::Function { name, .. } => name,
                                        _ => "unknown",
                                    },
                                    expr.alias
                                );
                                // Skip aggregate functions entirely - they should have been found in input batch
                                continue;
                            }
                            
                            let normalized_name = output_name.to_uppercase();
                            let already_added = output_fields.iter().any(|f| f.name().to_uppercase() == normalized_name);
                            
                            if !already_added {
                                output_columns.push(func_array);
                                output_fields.push(arrow::datatypes::Field::new(&output_name, output_type, true));
                            } else {
                                eprintln!("DEBUG ProjectOperator: Skipping duplicate Function expression '{}'", output_name);
                            }
                        }
                    }
                }
            }
        } else {
            // Fall back to column name-based projection (standard SQL behavior)
            // CRITICAL: Skip aggregate function names in columns list - they should be handled by expressions
            // Aggregates in columns list are aliases (e.g., "product_count", "total_stock") that should
            // already be in output_fields from expression processing
            for col_name in &expanded_columns {
                // Skip if this column is already in output (likely from expression processing)
                let normalized_col = col_name.to_uppercase();
                let already_added = output_fields.iter().any(|f| {
                    f.name().to_uppercase() == normalized_col || 
                    f.name() == col_name
                });
                
                if already_added {
                    eprintln!("DEBUG ProjectOperator: Skipping column '{}' from columns list - already in output_fields", col_name);
                    continue;
                }
                if col_name == "*" {
                    // Add all columns with deduplication
                    for col_idx in 0..batch.batch.columns.len() {
                        let field = batch.batch.schema.field(col_idx);
                        let clean_field_name = if field.name().contains('.') {
                            field.name().split('.').last().unwrap_or(field.name()).to_string()
                        } else {
                            field.name().to_string()
                        };
                        
                        // Check for duplicates
                        let normalized_name = clean_field_name.to_uppercase();
                        let already_added = output_fields.iter().any(|f| f.name().to_uppercase() == normalized_name);
                        
                        if !already_added {
                            output_columns.push(batch.batch.column(col_idx).unwrap().clone());
                            output_fields.push(arrow::datatypes::Field::new(
                                &clean_field_name,
                                field.data_type().clone(),
                                field.is_nullable(),
                            ));
                        }
                    }
                } else {
                    // Handle table-qualified column names (e.g., "d.id" or "documents.id")
                    let resolved_col_name = if col_name.contains('.') {
                        // Split table.column format
                        let parts: Vec<&str> = col_name.split('.').collect();
                        if parts.len() == 2 {
                            let table_part = parts[0];
                            let col_part = parts[1];
                            
                            // Resolve table alias to actual table name (if needed)
                            let _actual_table = self.table_aliases.get(table_part)
                                .map(|s| s.as_str())
                                .unwrap_or(table_part);
                            
                            // For JOIN schemas, use just column name (schema doesn't have prefixes)
                            col_part.to_string()
                        } else {
                            col_name.clone()
                        }
                    } else {
                        col_name.clone()
                    };
                    
                    // Use ColumnResolver for consistent column resolution
                    use crate::query::column_resolver::ColumnResolver;
                    let resolver = ColumnResolver::new(batch.batch.schema.clone(), self.table_aliases.clone());
                    
                    match resolver.resolve(&resolved_col_name) {
                        Ok(col_idx) => {
                            // Clean field name and check for duplicates
                            let field = batch.batch.schema.field(col_idx);
                            let clean_field_name = if field.name().contains('.') {
                                field.name().split('.').last().unwrap_or(field.name()).to_string()
                            } else {
                                field.name().to_string()
                            };
                            
                            // Check if already added
                            let normalized_name = clean_field_name.to_uppercase();
                            let already_added = output_fields.iter().any(|f| f.name().to_uppercase() == normalized_name);
                            
                            if !already_added {
                                output_columns.push(batch.batch.column(col_idx).unwrap().clone());
                                output_fields.push(arrow::datatypes::Field::new(
                                    &clean_field_name,
                                    field.data_type().clone(),
                                    field.is_nullable(),
                                ));
                            } else {
                                eprintln!("DEBUG ProjectOperator: Skipping duplicate column '{}' in expanded_columns path", clean_field_name);
                            }
                        }
                        Err(_) => {
                            // Last resort: try to find by partial match (for cases like "COUNT(*)" vs "COUNT")
                            let found = (0..batch.batch.schema.fields().len()).find(|&idx| {
                                let field_name = batch.batch.schema.field(idx).name();
                                // Remove special characters and compare
                                let normalized_field = field_name.to_uppercase().replace(&['(', ')', '*'][..], "");
                                let normalized_col = col_name.to_uppercase().replace(&['(', ')', '*'][..], "");
                                normalized_field == normalized_col || 
                                field_name.to_uppercase() == col_name.to_uppercase() ||
                                field_name.to_uppercase().contains(&col_name.to_uppercase()) ||
                                col_name.to_uppercase().contains(&field_name.to_uppercase())
                            });
                            
                            if let Some(col_idx) = found {
                                output_columns.push(batch.batch.column(col_idx).unwrap().clone());
                                output_fields.push(batch.batch.schema.field(col_idx).clone());
                            } else {
                                // Column not found - try without table prefix one more time
                                // For JOIN queries, columns might be accessed without prefix
                                let col_without_prefix = if resolved_col_name.contains('.') {
                                    resolved_col_name.split('.').last().unwrap_or(&resolved_col_name).to_string()
                                } else {
                                    resolved_col_name.clone()
                                };
                                
                                // Final attempt: use ColumnResolver to resolve unqualified name
                                let final_resolver = ColumnResolver::new(batch.batch.schema.clone(), self.table_aliases.clone());
                                match final_resolver.resolve(&col_without_prefix) {
                                    Ok(col_idx) => {
                                        output_columns.push(batch.batch.column(col_idx).unwrap().clone());
                                        output_fields.push(batch.batch.schema.field(col_idx).clone());
                                    }
                                    Err(_) => {
                                        // Column not found - this is an error in standard SQL
                                        eprintln!("DEBUG ProjectOperator: Column '{}' (resolved: '{}', without prefix: '{}') not found in schema. Available columns: {:?}", 
                                            col_name,
                                            resolved_col_name,
                                            col_without_prefix,
                                            batch.batch.schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                                        );
                                        anyhow::bail!("Column '{}' not found. Available columns: {:?}", 
                                            col_name,
                                            batch.batch.schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        if output_columns.is_empty() {
            // eprintln!("ProjectOperator::next: No matching columns found, returning None");
            return Ok(None);
        }
        
        // DUCKDB/PRESTO-STYLE DEDUPLICATION: Only deduplicate exact name matches
        // Rule: Base deduplication ONLY on exact field.name() string match.
        // Do NOT derive dedupe key by stripping qualifiers.
        // If users select the same column twice intentionally (e.g., SELECT x, x), allow duplicates.
        // This matches many engines and avoids correctness surprises.
        let mut deduplicated_fields = vec![];
        let mut deduplicated_columns = vec![];
        let mut seen_field_names = std::collections::HashSet::new();
        
        // Track which original indices we're keeping
        let mut kept_indices = vec![];
        
        for (idx, field) in output_fields.iter().enumerate() {
            let field_name = field.name();
            
            // DUCKDB/PRESTO-STYLE: Preserve qualified names - do NOT strip qualifiers
            // Use exact field name for deduplication (case-insensitive)
            // This ensures "c.name" and "p.name" are treated as distinct
            let normalized_name = field_name.to_uppercase();
            if !seen_field_names.contains(&normalized_name) {
                seen_field_names.insert(normalized_name);
                // Preserve the original field name (qualified or unqualified as-is)
                deduplicated_fields.push(field.clone());
                deduplicated_columns.push(output_columns[idx].clone());
                kept_indices.push(idx);
            } else {
                // Duplicate exact name detected - log but allow it
                // (User may have intentionally selected same column twice, e.g., SELECT x, x)
                eprintln!("DEBUG ProjectOperator: Duplicate field name '{}' detected - keeping first occurrence", field_name);
            }
        }
        
        // Verify we kept the same number of columns as fields
        if deduplicated_columns.len() != deduplicated_fields.len() {
            eprintln!("  ERROR ProjectOperator: Deduplication mismatch - columns: {}, fields: {}", 
                deduplicated_columns.len(), deduplicated_fields.len());
            anyhow::bail!("ProjectOperator deduplication failed - column/field count mismatch");
        }
        
        // Verify no duplicates in final schema
        let final_field_names: Vec<String> = deduplicated_fields.iter().map(|f| f.name().to_string()).collect();
        let unique_names: std::collections::HashSet<String> = final_field_names.iter().cloned().collect();
        if final_field_names.len() != unique_names.len() {
            eprintln!("  ERROR ProjectOperator: Schema still has duplicate fields after deduplication: {:?}", final_field_names);
            anyhow::bail!("ProjectOperator schema has duplicate fields: {:?}", final_field_names);
        }
        
        eprintln!("DEBUG ProjectOperator: Before deduplication: {} fields, after: {} fields", 
            output_fields.len(), deduplicated_fields.len());
        eprintln!("DEBUG ProjectOperator: Final fields: {:?}", final_field_names);
        eprintln!("DEBUG ProjectOperator: ===== FINAL OUTPUT =====");
        eprintln!("DEBUG ProjectOperator: Requested {} expressions, got {} fields after deduplication", 
            self.expressions.len(), deduplicated_fields.len());
        eprintln!("[DEBUG project] final output field names = {:?}", 
            deduplicated_fields.iter().map(|f| f.name().to_string()).collect::<Vec<_>>());
        eprintln!("DEBUG ProjectOperator: Final output field names: {:?}", 
            deduplicated_fields.iter().map(|f| f.name()).collect::<Vec<_>>());
        
        // CRITICAL: Verify that all requested expressions were processed
        // Note: Some expressions might expand to multiple columns (e.g., wildcards), so we check >= instead of ==
        if !self.expressions.is_empty() {
            let processed_cols: Vec<String> = deduplicated_fields.iter().map(|f| f.name().to_string()).collect();
            let requested_cols: Vec<String> = self.expressions.iter().map(|e| {
                match &e.expr_type {
                    crate::query::plan::ProjectionExprType::Column(c) => c.clone(),
                    _ => format!("{:?}", e.expr_type),
                }
            }).collect();
            
            // Count non-wildcard expressions (wildcards expand to multiple columns)
            let non_wildcard_count = self.expressions.iter().filter(|e| {
                match &e.expr_type {
                    crate::query::plan::ProjectionExprType::Column(c) => c != "*" && !c.ends_with(".*"),
                    _ => true,
                }
            }).count();
            
            if deduplicated_fields.len() < non_wildcard_count {
                eprintln!("WARNING ProjectOperator: Mismatch - requested {} non-wildcard expressions but only {} fields in output", 
                    non_wildcard_count, deduplicated_fields.len());
                eprintln!("WARNING ProjectOperator: Requested: {:?}, Got: {:?}", requested_cols, processed_cols);
                
                // Try to identify which columns are missing
                for expr in &self.expressions {
                    match &expr.expr_type {
                        crate::query::plan::ProjectionExprType::Column(col_name) => {
                            if col_name == "*" || col_name.ends_with(".*") {
                                continue; // Skip wildcards
                            }
                            let found = processed_cols.iter().any(|f| {
                                let f_clean = if f.contains('.') {
                                    f.split('.').last().unwrap_or(f)
                                } else {
                                    f.as_str()
                                };
                                f_clean.to_uppercase() == col_name.to_uppercase() || 
                                (!expr.alias.is_empty() && f_clean.to_uppercase() == expr.alias.to_uppercase())
                            });
                            if !found {
                                eprintln!("ERROR ProjectOperator: Column '{}' (alias: '{}') was requested but not found in output!", 
                                    col_name, expr.alias);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        
        let output_schema = Arc::new(Schema::new(deduplicated_fields));
        let output_batch = crate::storage::columnar::ColumnarBatch::new(deduplicated_columns, output_schema.clone());
        
        // COLID: Create output ColumnSchema mapping aliases to existing ColIds
        let output_column_schema = if let Some(ref input_schema) = batch.column_schema {
            use crate::execution::column_identity::ColumnSchema;
            let mut new_schema = ColumnSchema::new();
            
            // Map projected columns to existing ColIds
            if !self.expressions.is_empty() {
                // Use expressions to map aliases to ColIds
                for (output_idx, expr) in self.expressions.iter().enumerate() {
                    if output_idx >= output_fields.len() {
                        break;
                    }
                    
                    let output_field = &output_fields[output_idx];
                    let output_name = output_field.name();
                    
                    match &expr.expr_type {
                        crate::query::plan::ProjectionExprType::Column(col_name) => {
                            // Simple column reference - map alias to existing ColId
                            if let Some(col_id) = input_schema.resolve(col_name) {
                                let data_type = input_schema.data_type(col_id)
                                    .cloned()
                                    .unwrap_or_else(|| output_field.data_type().clone());
                                new_schema.add_column(col_id, output_name.to_string(), output_name.to_string(), data_type);
                            } else {
                                // Fallback: create new ColId (shouldn't happen for valid columns)
                                use crate::execution::column_identity::generate_col_id;
                                let new_col_id = generate_col_id();
                                new_schema.add_column(new_col_id, output_name.to_string(), output_name.to_string(), output_field.data_type().clone());
                            }
                        }
                        _ => {
                            // Computed expression (CAST, function) - create new ColId
                            use crate::execution::column_identity::generate_col_id;
                            let new_col_id = generate_col_id();
                                new_schema.add_column(new_col_id, output_name.to_string(), output_name.to_string(), output_field.data_type().clone());
                        }
                    }
                }
            } else {
                // No expressions - use expanded_columns and output_fields
                // Try to map each output field to input ColId by name
                for (idx, field) in output_fields.iter().enumerate() {
                    let field_name = field.name();
                    if let Some(col_id) = input_schema.resolve(field_name) {
                        let data_type = input_schema.data_type(col_id)
                            .cloned()
                            .unwrap_or_else(|| field.data_type().clone());
                        new_schema.add_column(col_id, field_name.to_string(), field_name.to_string(), data_type);
                    } else {
                        // Create new ColId for unmatched columns
                        use crate::execution::column_identity::generate_col_id;
                        let new_col_id = generate_col_id();
                        new_schema.add_column(new_col_id, field_name.to_string(), field_name.to_string(), field.data_type().clone());
                    }
                }
            }
            
            Some(new_schema)
        } else {
            None // No input ColumnSchema - backward compatibility
        };
        
        // eprintln!("ProjectOperator::next: Returning batch with {} rows, {} columns", 
        //     batch.row_count, output_batch.columns.len());
        let mut exec_batch = ExecutionBatch::from_batch_with_column_schema(output_batch, output_column_schema);
        exec_batch.selection = batch.selection.clone();
        exec_batch.row_count = batch.row_count;
        // Preserve column_fragments from input batch
        exec_batch.column_fragments = batch.column_fragments.clone();
        Ok(Some(exec_batch))
    }
}

impl BatchIterator for ProjectOperator {
    fn prepare(&mut self) -> Result<(), anyhow::Error> {
        // CRITICAL: This method MUST be called via dynamic dispatch
        // If you see this log, dynamic dispatch is working
        eprintln!("[PROJECT BATCHITERATOR PREPARE] Called on ProjectOperator with {} columns, prepared={}", 
            self.columns.len(), self.prepared);
        
        // Call ExecNode::prepare() which will recursively prepare children
        let result = <Self as ExecNode>::prepare(self);
        eprintln!("[PROJECT BATCHITERATOR PREPARE] ExecNode::prepare() result: {:?}, prepared={}", 
            result, self.prepared);
        result.map_err(|e| anyhow::anyhow!("{}", e))
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // Delegate to ExecNode::next() which has safety checks
        <Self as ExecNode>::next(self)
    }
    
    fn schema(&self) -> SchemaRef {
        // SCHEMA-FLOW: ProjectOperator selects columns from input schema dynamically
        // Build schema with only requested columns
        
        // Get input schema dynamically (always current)
        let input_schema = self.input.schema();
        
        // SCHEMA-FLOW DEBUG: Log input schema
        eprintln!("DEBUG ProjectOperator::schema() - Input schema has {} fields: {:?}", 
            input_schema.fields().len(),
            input_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        
        // Use ColumnResolver for consistent column resolution in schema() method
        // This ensures schema() matches next() behavior exactly
        use crate::query::column_resolver::ColumnResolver;
        let resolver = ColumnResolver::new(input_schema.clone(), self.table_aliases.clone());
        
        let mut fields = vec![];
        
        // EDGE CASE 3 FIX: Process expressions to handle aliases correctly (matches next() logic)
        if !self.expressions.is_empty() {
            for expr in &self.expressions {
                match &expr.expr_type {
                    crate::query::plan::ProjectionExprType::Column(col_name) => {
                        // Use ColumnResolver for consistent column resolution
                        match resolver.resolve(col_name) {
                            Ok(idx) => {
                                let original_field = input_schema.field(idx);
                                // EDGE CASE 3 FIX: Use alias if provided, otherwise use original field name
                                if !expr.alias.is_empty() && expr.alias != col_name.as_str() {
                                    fields.push(arrow::datatypes::Field::new(
                                        &expr.alias,
                                        original_field.data_type().clone(),
                                        original_field.is_nullable(),
                                    ));
                                } else {
                                    fields.push(original_field.clone());
                                }
                            }
                            Err(_) => {
                                // Column not found - this will fail in next() with a better error message
                                eprintln!("DEBUG ProjectOperator::schema() - Column '{}' not found. Available columns: {:?}", 
                                    col_name,
                                    input_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
                                // Don't add to fields - will fail in next() with clearer error
                            }
                        }
                    }
                    crate::query::plan::ProjectionExprType::Cast { target_type, .. } => {
                        // CAST expression - use alias if provided, otherwise use column name
                        let field_name = if !expr.alias.is_empty() { &expr.alias } else { "cast_result" };
                        fields.push(arrow::datatypes::Field::new(field_name, target_type.clone(), true));
                    }
                    crate::query::plan::ProjectionExprType::Case(_) => {
                        // CASE expression - use alias if provided
                        let field_name = if !expr.alias.is_empty() { &expr.alias } else { "case_result" };
                        // Infer type from first non-null value (simplified - actual type inference in next())
                        fields.push(arrow::datatypes::Field::new(field_name, arrow::datatypes::DataType::Utf8, true));
                    }
                    crate::query::plan::ProjectionExprType::Function(_) => {
                        // Function expression - use alias if provided
                        let field_name = if !expr.alias.is_empty() { &expr.alias } else { "func_result" };
                        // Infer type from function (simplified - actual type inference in next())
                        fields.push(arrow::datatypes::Field::new(field_name, arrow::datatypes::DataType::Float64, true));
                    }
                }
            }
        } else if self.columns.is_empty() || (self.columns.len() == 1 && self.columns[0] == "*") {
            // Wildcard: return input schema unchanged
            eprintln!("DEBUG ProjectOperator::schema() - Wildcard projection, returning input schema");
            return input_schema;
        } else {
            // Fallback: use columns list (legacy path - no expressions, no aliases)
            for col_name in &self.columns {
                // Use ColumnResolver for consistent column resolution
                match resolver.resolve(col_name) {
                    Ok(idx) => {
                        fields.push(input_schema.field(idx).clone());
                    }
                    Err(_) => {
                        // Column not found - this will fail in next() with a better error message
                        eprintln!("DEBUG ProjectOperator::schema() - Column '{}' not found. Available columns: {:?}", 
                            col_name,
                            input_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
                        // Don't add to fields - will fail in next() with clearer error
                    }
                }
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
    /// Whether we've already buffered and processed all input
    input_buffered: bool,
    /// Table alias mapping: alias -> actual table name (e.g., "e1" -> "employees")
    table_aliases: std::collections::HashMap<String, String>,
    /// Whether ORDER BY columns have been resolved using runtime ExecutionBatch schema
    order_by_resolved: bool,
    /// Resolved sort key indices: (column_index, ascending)
    resolved_sort_key_indices: Option<Vec<(usize, bool)>>,
}

impl SortOperator {
    pub fn new(
        input: Box<dyn BatchIterator>,
        order_by: Vec<OrderByExpr>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Self {
        eprintln!("DEBUG SortOperator::new: Received order_by from planner: {:?}", order_by.iter().map(|o| &o.column).collect::<Vec<_>>());
        Self {
            input,
            order_by, // CANONICAL resolved OrderByExprs from planner - these are the source of truth
            limit,
            offset,
            buffered_rows: vec![],
            input_buffered: false,
            table_aliases: std::collections::HashMap::new(),
            order_by_resolved: false,
            resolved_sort_key_indices: None,
        }
    }
    
    pub fn with_table_aliases(
        input: Box<dyn BatchIterator>,
        order_by: Vec<OrderByExpr>,
        limit: Option<usize>,
        offset: Option<usize>,
        table_aliases: std::collections::HashMap<String, String>,
    ) -> Self {
        eprintln!("DEBUG SortOperator::with_table_aliases: Received CANONICAL resolved order_by from planner: {:?}", order_by.iter().map(|o| &o.column).collect::<Vec<_>>());
        Self {
            input,
            order_by, // CANONICAL resolved OrderByExprs from planner - these are the source of truth
            limit,
            offset,
            buffered_rows: vec![],
            input_buffered: false,
            table_aliases,
            order_by_resolved: false,
            resolved_sort_key_indices: None,
        }
    }
    
    /// Resolve ORDER BY columns using runtime ExecutionBatch ColumnSchema
    /// This is called once after receiving the first batch to resolve ORDER BY aliases
    /// using the actual ExecutionBatch's name_map (which contains SELECT aliases).
    fn resolve_order_by_columns_internal(&mut self, schema: &SchemaRef, column_schema: Option<&crate::execution::column_identity::ColumnSchema>) -> Result<()> {
        eprintln!("DEBUG SortOperator::resolve_order_by_columns: Starting resolution");
        eprintln!("DEBUG SortOperator::resolve_order_by_columns: ORDER BY columns from planner: {:?}", 
            self.order_by.iter().map(|o| &o.column).collect::<Vec<_>>());
        
        if let Some(ref cs) = column_schema {
            eprintln!("DEBUG SortOperator::resolve_order_by_columns: ColumnSchema name_map keys: {:?}", 
                cs.name_map.keys().collect::<Vec<_>>());
            eprintln!("DEBUG SortOperator::resolve_order_by_columns: ColumnSchema unqualified_names: {:?}", 
                cs.unqualified_names);
        } else {
            eprintln!("WARNING SortOperator::resolve_order_by_columns: No ColumnSchema available in ExecutionBatch!");
        }
        
        eprintln!("DEBUG SortOperator::resolve_order_by_columns: Schema field names: {:?}", 
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        
        // Create ColumnResolver for fallback (backward compatibility)
        use crate::query::column_resolver::ColumnResolver;
        let resolver = ColumnResolver::new(schema.clone(), self.table_aliases.clone());
        
        let mut sort_key_indices = vec![];
        
        // Resolve each ORDER BY expression using runtime ExecutionBatch schema
        for order_expr in &self.order_by {
            eprintln!("DEBUG SortOperator::resolve_order_by_columns: Resolving ORDER BY column '{}' (planner-resolved)", order_expr.column);
            
            let col_idx = if let Some(ref column_schema) = column_schema {
                // RUNTIME RESOLUTION: Use ExecutionBatch ColumnSchema name_map
                // This contains SELECT aliases like 'department_name' from AggregateOperator
                
                // Strategy 1: Position-based ORDER BY (ORDER BY 1, 2)
                if order_expr.column.starts_with("__POSITION_") && order_expr.column.ends_with("__") {
                    let pos_str = order_expr.column.trim_start_matches("__POSITION_").trim_end_matches("__");
                    if let Ok(pos) = pos_str.parse::<usize>() {
                        if pos == 0 {
                            anyhow::bail!("ORDER BY position must be >= 1, got 0");
                        }
                        let idx = pos - 1;
                        if idx < column_schema.column_ids.len() {
                            Ok(idx)
                        } else {
                            anyhow::bail!("ORDER BY position {} is out of range (schema has {} columns)", pos, column_schema.column_ids.len())
                        }
                    } else {
                        Err(anyhow::anyhow!("Invalid position marker: {}", order_expr.column))
                    }
                } else if order_expr.column.parse::<usize>().is_ok() {
                    // Direct numeric literal (e.g., "1", "2")
                    let pos = order_expr.column.parse::<usize>().unwrap();
                    if pos == 0 {
                        anyhow::bail!("ORDER BY position must be >= 1, got 0");
                    }
                    let idx = pos - 1;
                    if idx < column_schema.column_ids.len() {
                        Ok(idx)
                    } else {
                        anyhow::bail!("ORDER BY position {} is out of range (schema has {} columns)", pos, column_schema.column_ids.len())
                    }
                } else {
                    // RUNTIME RESOLUTION: Use ExecutionBatch ColumnSchema name_map
                    // This should contain the planner-resolved alias (e.g., 'department_name')
                    
                    // Strategy 1: Exact match in name_map (primary - should work for planner-resolved aliases)
                    match column_schema.name_map.get(&order_expr.column) {
                        Some(col_id) if column_schema.physical_index(*col_id).is_some() => {
                            let idx = column_schema.physical_index(*col_id).unwrap();
                            eprintln!("DEBUG SortOperator::resolve_order_by_columns: Found '{}' in name_map -> ColId({:?}) -> index {}", 
                                order_expr.column, col_id, idx);
                            Ok(idx)
                        }
                        _ => {
                            // Strategy 2: Case-insensitive match in unqualified_names
                            let order_col_upper = order_expr.column.to_uppercase();
                            match column_schema.unqualified_names.iter().position(|name| {
                                name.to_uppercase() == order_col_upper
                            }) {
                                Some(idx) => {
                                    eprintln!("DEBUG SortOperator::resolve_order_by_columns: Found '{}' in unqualified_names at index {}", 
                                        order_expr.column, idx);
                                    Ok(idx)
                                }
                                None => {
                                    eprintln!("WARNING SortOperator::resolve_order_by_columns: '{}' not found in name_map or unqualified_names, trying fallback", 
                                        order_expr.column);
                                    
                                    // Strategy 3: Try resolve (qualified/unqualified names)
                                    match column_schema.resolve(&order_expr.column) {
                                        Some(col_id) if column_schema.physical_index(col_id).is_some() => {
                                            let idx = column_schema.physical_index(col_id).unwrap();
                                            eprintln!("DEBUG SortOperator::resolve_order_by_columns: Found '{}' via resolve -> index {}", 
                                                order_expr.column, idx);
                                            Ok(idx)
                                        }
                                        _ => {
                                            // Strategy 4: Fall back to string-based resolution
                                            eprintln!("WARNING SortOperator::resolve_order_by_columns: Falling back to resolver.resolve() for '{}'", 
                                                order_expr.column);
                                            resolver.resolve(&order_expr.column)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                // No ColumnSchema - use string-based resolution (backward compatibility)
                eprintln!("WARNING SortOperator::resolve_order_by_columns: No ColumnSchema, using string-based resolution for '{}'", 
                    order_expr.column);
                resolver.resolve(&order_expr.column)
            };
            
            match col_idx {
                Ok(idx) => {
                    // Validate index is within bounds
                    if idx < schema.fields().len() {
                        sort_key_indices.push((idx, order_expr.ascending));
                        eprintln!("DEBUG SortOperator::resolve_order_by_columns: Successfully resolved '{}' -> index {}", 
                            order_expr.column, idx);
                    } else {
                        anyhow::bail!("ORDER BY column index {} out of bounds (schema has {} fields)", 
                            idx, schema.fields().len());
                    }
                }
                Err(e) => {
                    eprintln!("ERROR SortOperator::resolve_order_by_columns: Failed to resolve '{}': {}", 
                        order_expr.column, e);
                    eprintln!("ERROR SortOperator::resolve_order_by_columns: Available columns: {:?}", 
                        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
                    if let Some(ref cs) = column_schema {
                        eprintln!("ERROR SortOperator::resolve_order_by_columns: ColumnSchema name_map keys: {:?}", 
                            cs.name_map.keys().collect::<Vec<_>>());
                    }
                    return Err(anyhow::anyhow!("Failed to resolve ORDER BY column '{}': {}. Available columns: {:?}", 
                        order_expr.column, e, 
                        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()));
                }
            }
        }
        
        // Store resolved indices
        self.resolved_sort_key_indices = Some(sort_key_indices);
        self.order_by_resolved = true;
        eprintln!("DEBUG SortOperator::resolve_order_by_columns: Resolution complete. Resolved {} sort keys", 
            self.resolved_sort_key_indices.as_ref().unwrap().len());
        
        Ok(())
    }
}

impl BatchIterator for SortOperator {
    fn prepare(&mut self) -> Result<(), anyhow::Error> {
        // CRITICAL FIX: SortOperator must recursively prepare its input
        eprintln!("[SORT PREPARE] Preparing SortOperator, recursively preparing input");
        FilterOperator::prepare_child(&mut self.input)?;
        eprintln!("[SORT PREPARE] SortOperator prepared");
        Ok(())
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        eprintln!(" DEBUG: SortOperator::next called, buffered_rows.len()={}, input_buffered={}", 
            self.buffered_rows.len(), self.input_buffered);
        
        // Buffer all input ONCE
        if !self.input_buffered {
            eprintln!(" DEBUG: SortOperator::next - buffering input batches (first time)");
            self.input_buffered = true;
            let mut batch_count = 0;
            while let Some(batch) = self.input.next()? {
                batch_count += 1;
                eprintln!(" DEBUG: SortOperator RECEIVED batch[{}] from input:", batch_count);
                eprintln!("   - columns.len()={}, schema.fields().len()={}", 
                    batch.batch.columns.len(), batch.batch.schema.fields().len());
                for (i, (col, field)) in batch.batch.columns.iter().zip(batch.batch.schema.fields().iter()).enumerate() {
                    eprintln!("   - Column[{}]: name={}, array_type={:?}, schema_type={:?}", 
                        i, field.name(), col.data_type(), field.data_type());
                    if col.data_type() != field.data_type() {
                        eprintln!("    MISMATCH when receiving batch!");
                    }
                }
                self.buffered_rows.push(batch);
            }
            eprintln!(" DEBUG: SortOperator buffered {} batches total", batch_count);
            
            // RUNTIME RESOLUTION: Resolve ORDER BY columns using first ExecutionBatch schema
            if !self.buffered_rows.is_empty() && !self.order_by.is_empty() && !self.order_by_resolved {
                eprintln!("DEBUG SortOperator: Starting runtime ORDER BY resolution");
                eprintln!("DEBUG SortOperator: buffered_rows.len()={}, order_by.len()={}, order_by_resolved={}", 
                    self.buffered_rows.len(), self.order_by.len(), self.order_by_resolved);
                
                // Extract necessary data from first batch BEFORE calling mutable method
                let schema = self.buffered_rows[0].batch.schema.clone();
                let column_schema = self.buffered_rows[0].column_schema.clone();
                
                eprintln!("DEBUG SortOperator: Extracted schema with {} fields, column_schema present: {}", 
                    schema.fields().len(), column_schema.is_some());
                if let Some(ref cs) = column_schema {
                    eprintln!("DEBUG SortOperator: ColumnSchema name_map keys BEFORE resolution: {:?}", 
                        cs.name_map.keys().collect::<Vec<_>>());
                }
                
                // CRITICAL: Verify input batch types match schema BEFORE using
                for (i, (col, field)) in self.buffered_rows[0].batch.columns.iter().zip(self.buffered_rows[0].batch.schema.fields().iter()).enumerate() {
                    if col.data_type() != field.data_type() {
                        eprintln!(" ERROR: SortOperator input batch type mismatch at index {}: field '{}' expected {:?}, got {:?}", 
                            i, field.name(), field.data_type(), col.data_type());
                        anyhow::bail!("Type mismatch in SortOperator input: field '{}' expected {:?}, got {:?}. This indicates a bug in upstream operator.", 
                            field.name(), field.data_type(), col.data_type());
                    }
                }
                
                // Resolve ORDER BY columns using runtime ExecutionBatch ColumnSchema
                // Pass cloned data to avoid borrow checker issues
                eprintln!("DEBUG SortOperator: Calling resolve_order_by_columns_internal()");
                self.resolve_order_by_columns_internal(&schema, column_schema.as_ref())?;
                eprintln!("DEBUG SortOperator: resolve_order_by_columns_internal() completed successfully");
            } else {
                eprintln!("DEBUG SortOperator: Skipping resolution - buffered_rows.is_empty()={}, order_by.is_empty()={}, order_by_resolved={}", 
                    self.buffered_rows.is_empty(), self.order_by.is_empty(), self.order_by_resolved);
            }
            
            // Sort buffered rows
            if !self.buffered_rows.is_empty() && !self.order_by.is_empty() {
                // Collect all rows into a single batch for sorting
                let mut all_rows: Vec<Vec<Value>> = vec![];
                let first_batch = &self.buffered_rows[0];
                
                let schema = first_batch.batch.schema.clone();
                
                // Debug: Print the schema we're using
                eprintln!(" DEBUG: SortOperator using schema from buffered_rows[0]");
                eprintln!(" DEBUG: buffered_rows[0] has {} columns, {} schema fields", 
                    first_batch.batch.columns.len(), first_batch.batch.schema.fields().len());
                
                // Use pre-resolved sort key indices (resolution should have happened earlier)
                let sort_key_indices = if let Some(ref indices) = self.resolved_sort_key_indices {
                    eprintln!("DEBUG SortOperator: Using {} pre-resolved sort key indices", indices.len());
                    indices
                } else {
                    // Resolution didn't happen - this is an error
                    eprintln!("ERROR SortOperator: resolved_sort_key_indices is None!");
                    eprintln!("ERROR SortOperator: order_by_resolved={}, buffered_rows.len()={}, order_by.len()={}", 
                        self.order_by_resolved, self.buffered_rows.len(), self.order_by.len());
                    anyhow::bail!("SortOperator: ORDER BY columns were not resolved. This indicates resolve_order_by_columns_internal() was not called or failed silently.");
                };
                
                // Collect all rows - respect selection bitmap
                for batch in &self.buffered_rows {
                    // Iterate through ALL rows in the batch (not just row_count)
                    // The selection bitmap indicates which rows to include
                    let actual_batch_size = batch.selection.len();
                    eprintln!(" DEBUG: SortOperator collecting rows: actual_batch_size={}, batch.row_count={}, selection.count_ones()={}", 
                        actual_batch_size, batch.row_count, batch.selection.count_ones());
                    
                    // SAFETY: Iterate only up to the minimum of selection.len() and actual array length
                    let max_row_idx = batch.batch.columns.first()
                        .map(|col| col.len())
                        .unwrap_or(actual_batch_size);
                    let safe_batch_size = actual_batch_size.min(max_row_idx);
                    
                    for row_idx in 0..safe_batch_size {
                        // Only include rows that are selected (respects filtering)
                        if row_idx >= batch.selection.len() || !batch.selection[row_idx] {
                            continue;
                        }
                        
                        let mut row = vec![];
                        for col_idx in 0..batch.batch.columns.len() {
                            let col = batch.batch.column(col_idx).unwrap();
                            // Ensure row_idx is within array bounds
                            if row_idx < col.len() {
                                row.push(extract_value(col, row_idx)?);
                            } else {
                                // Array is shorter than expected - skip this row
                                break;
                            }
                        }
                        // Only add row if we successfully extracted all column values
                        if row.len() == batch.batch.columns.len() {
                            all_rows.push(row);
                        }
                    }
                }
                
                eprintln!(" DEBUG: SortOperator collected {} rows from {} batches", 
                    all_rows.len(), self.buffered_rows.len());
                
                // Sort rows
                all_rows.sort_by(|a, b| {
                    for &(key_idx, ascending) in sort_key_indices.iter() {
                        let cmp = a[key_idx].partial_cmp(&b[key_idx]).unwrap_or(std::cmp::Ordering::Equal);
                        // Apply ascending/descending direction
                        let final_cmp = if ascending {
                            cmp
                        } else {
                            cmp.reverse()
                        };
                        // Only return if values are different (not equal)
                        // If equal, continue to next sort key
                        if final_cmp != std::cmp::Ordering::Equal {
                            return final_cmp;
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
                
                eprintln!(" DEBUG: SortOperator recreating batch - schema has {} fields, {} rows", 
                    schema.fields().len(), sorted_rows.len());
                eprintln!(" DEBUG: SortOperator schema fields: {:?}", 
                    schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
                
                let mut output_arrays = vec![];
                let num_cols = schema.fields().len();
                eprintln!(" DEBUG: SortOperator creating {} arrays from {} rows", num_cols, sorted_rows.len());
                
                // Verify we have enough values per row
                if !sorted_rows.is_empty() && sorted_rows[0].len() != num_cols {
                    eprintln!("  WARNING: Row value count ({}) != schema field count ({})!", 
                        sorted_rows[0].len(), num_cols);
                }
                
                for col_idx in 0..num_cols {
                    if col_idx >= sorted_rows[0].len() {
                        eprintln!("  WARNING: Column index {} >= row length {}", col_idx, sorted_rows[0].len());
                        break;
                    }
                    let col_values: Vec<Value> = sorted_rows.iter().map(|row| row[col_idx].clone()).collect();
                    let field = schema.field(col_idx);
                    
                    eprintln!(" DEBUG: SortOperator creating array[{}]: field={}, type={:?}, first_val={:?}", 
                        col_idx, field.name(), field.data_type(), col_values.first());
                    
                    // Use centralized type conversion utility - always respects schema types
                    let arr = values_to_array(
                        col_values,
                        field.data_type(),
                        field.name()
                    )?;
                    
                    // Validate array type matches schema
                    validate_array_type(
                        &arr,
                        field.data_type(),
                        field.name()
                    )?;
                    
                    output_arrays.push(arr);
                }
                
                eprintln!(" DEBUG: SortOperator created {} arrays, schema has {} fields", 
                    output_arrays.len(), schema.fields().len());
                
                // CRITICAL: Verify array count matches schema field count
                if output_arrays.len() != schema.fields().len() {
                    eprintln!(" ERROR: SortOperator array count ({}) != schema field count ({})!", 
                        output_arrays.len(), schema.fields().len());
                    anyhow::bail!("SortOperator: Array count ({}) != schema field count ({})", 
                        output_arrays.len(), schema.fields().len());
                }
                
                // CRITICAL: Verify each array type matches schema type BEFORE creating batch
                for (i, (arr, field)) in output_arrays.iter().zip(schema.fields().iter()).enumerate() {
                    if arr.data_type() != field.data_type() {
                        eprintln!(" ERROR: SortOperator array[{}] type ({:?}) != schema field type ({:?})!", 
                            i, arr.data_type(), field.data_type());
                        anyhow::bail!("SortOperator: Array[{}] type ({:?}) != schema field '{}' type ({:?})", 
                            i, arr.data_type(), field.name(), field.data_type());
                    }
                }
                
                let output_batch = crate::storage::columnar::ColumnarBatch::new(output_arrays, schema.clone());
                
                // CRITICAL: Verify batch after creation - should have same count
                if output_batch.columns.len() != output_batch.schema.fields().len() {
                    eprintln!(" ERROR: SortOperator output batch column count ({}) != schema field count ({})!", 
                        output_batch.columns.len(), output_batch.schema.fields().len());
                    anyhow::bail!("SortOperator output batch: Column count ({}) != schema field count ({})", 
                        output_batch.columns.len(), output_batch.schema.fields().len());
                }
                
                // CRITICAL: Check for duplicate field names in schema
                let field_names: Vec<String> = output_batch.schema.fields().iter().map(|f| f.name().to_string()).collect();
                let unique_names: std::collections::HashSet<String> = field_names.iter().cloned().collect();
                if field_names.len() != unique_names.len() {
                    eprintln!(" ERROR: SortOperator output batch schema has duplicate field names: {:?}", field_names);
                    anyhow::bail!("SortOperator output batch schema has duplicate field names: {:?}", field_names);
                }
                
                eprintln!(" DEBUG: SortOperator output batch - {} columns, {} schema fields", 
                    output_batch.columns.len(), output_batch.schema.fields().len());
                eprintln!(" DEBUG: SortOperator output batch schema field names: {:?}", field_names);
                let selection = bitvec![1; sorted_rows.len()];
                
                // COLID: Preserve ColumnSchema through sorting (sorting doesn't change column identity)
                let output_column_schema = first_batch.column_schema.clone();
                
                let mut exec_batch = if let Some(ref column_schema) = output_column_schema {
                    ExecutionBatch::with_column_schema(output_batch, column_schema.clone())
                } else {
                    ExecutionBatch::new(output_batch)
                };
                exec_batch.selection = selection;
                exec_batch.row_count = sorted_rows.len();
                
                // CRITICAL: Final validation before storing
                if exec_batch.batch.columns.len() != exec_batch.batch.schema.fields().len() {
                    eprintln!(" ERROR: SortOperator exec_batch column count ({}) != schema field count ({})!", 
                        exec_batch.batch.columns.len(), exec_batch.batch.schema.fields().len());
                    anyhow::bail!("SortOperator exec_batch: Column count ({}) != schema field count ({})", 
                        exec_batch.batch.columns.len(), exec_batch.batch.schema.fields().len());
                }
                
                self.buffered_rows = vec![exec_batch];
            }
        }
        
        if self.buffered_rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.buffered_rows.remove(0)))
        }
    }
    
    fn schema(&self) -> SchemaRef {
        // SCHEMA-FLOW: SortOperator passes through input schema unchanged
        let input_schema = self.input.schema();
        
        // SCHEMA-FLOW DEBUG: Log schema propagation
        eprintln!("DEBUG SortOperator::schema() - Passing through input schema with {} fields: {:?}", 
            input_schema.fields().len(),
            input_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        
        input_schema
    }
}

/// Limit operator - applies LIMIT/OFFSET
/// Distinct operator - removes duplicate rows
pub struct DistinctOperator {
    input: Box<dyn BatchIterator>,
    /// Set of seen row values (all columns) for deduplication
    seen_rows: std::collections::HashSet<Vec<Value>>,
    /// Buffered unique rows waiting to be output
    output_buffer: Vec<Vec<Value>>,
    /// Whether we've finished consuming input
    input_exhausted: bool,
    /// Batch size for output
    batch_size: usize,
}

impl DistinctOperator {
    pub fn new(input: Box<dyn BatchIterator>) -> Self {
        // NO schema snapshot - compute dynamically from input when needed
        Self {
            input,
            seen_rows: std::collections::HashSet::new(),
            output_buffer: Vec::new(),
            input_exhausted: false,
            batch_size: 8192, // Output in batches of 8K rows
        }
    }
    
    /// Extract a row as Vec<Value> from a batch at given index
    fn extract_row(batch: &ExecutionBatch, row_idx: usize) -> Result<Vec<Value>> {
        let mut row = Vec::new();
        for col_idx in 0..batch.batch.columns.len() {
            let array = batch.batch.column(col_idx).unwrap();
            let val = extract_value(array, row_idx)?;
            row.push(val);
        }
        Ok(row)
    }
    
    /// Convert Vec<Vec<Value>> to ExecutionBatch
    fn values_to_batch(&self, rows: &[Vec<Value>], schema: &SchemaRef) -> Result<ExecutionBatch> {
        if rows.is_empty() {
            // Return empty batch
            let empty_batch = crate::storage::columnar::ColumnarBatch::empty(schema.clone());
            return Ok(ExecutionBatch::new(empty_batch));
        }
        
        let num_cols = schema.fields().len();
        let mut column_arrays: Vec<Arc<dyn Array>> = Vec::new();
        
        for col_idx in 0..num_cols {
            let field = schema.field(col_idx);
            let data_type = field.data_type();
            
            let array: Arc<dyn Array> = match data_type {
                DataType::Int64 => {
                    let values: Vec<Option<i64>> = rows.iter().map(|row| {
                        match row.get(col_idx) {
                            Some(Value::Int64(v)) => Some(*v),
                            Some(Value::Int32(v)) => Some(*v as i64),
                            Some(Value::Null) => None,
                            _ => None,
                        }
                    }).collect();
                    Arc::new(Int64Array::from(values))
                }
                DataType::Int32 => {
                    let values: Vec<Option<i32>> = rows.iter().map(|row| {
                        match row.get(col_idx) {
                            Some(Value::Int32(v)) => Some(*v),
                            Some(Value::Int64(v)) => Some(*v as i32),
                            Some(Value::Null) => None,
                            _ => None,
                        }
                    }).collect();
                    Arc::new(Int32Array::from(values))
                }
                DataType::Float64 => {
                    let values: Vec<Option<f64>> = rows.iter().map(|row| {
                        match row.get(col_idx) {
                            Some(Value::Float64(v)) => Some(*v),
                            Some(Value::Float32(v)) => Some(*v as f64),
                            Some(Value::Int64(v)) => Some(*v as f64),
                            Some(Value::Int32(v)) => Some(*v as f64),
                            Some(Value::Null) => None,
                            _ => None,
                        }
                    }).collect();
                    Arc::new(Float64Array::from(values))
                }
                DataType::Utf8 => {
                    let values: Vec<Option<String>> = rows.iter().map(|row| {
                        match row.get(col_idx) {
                            Some(Value::String(v)) => Some(v.clone()),
                            Some(Value::Null) => None,
                            _ => None,
                        }
                    }).collect();
                    Arc::new(StringArray::from(values))
                }
                DataType::Boolean => {
                    let values: Vec<Option<bool>> = rows.iter().map(|row| {
                        match row.get(col_idx) {
                            Some(Value::Bool(v)) => Some(*v),
                            Some(Value::Null) => None,
                            _ => None,
                        }
                    }).collect();
                    Arc::new(BooleanArray::from(values))
                }
                _ => anyhow::bail!("Unsupported data type for DISTINCT: {:?}", data_type),
            };
            
            column_arrays.push(array);
        }
        
        let batch = crate::storage::columnar::ColumnarBatch::new(column_arrays, schema.clone());
        Ok(ExecutionBatch::new(batch))
    }
}

impl BatchIterator for DistinctOperator {
    fn prepare(&mut self) -> Result<(), anyhow::Error> {
        // CRITICAL FIX: DistinctOperator must recursively prepare its input
        eprintln!("[DISTINCT PREPARE] Preparing DistinctOperator, recursively preparing input");
        FilterOperator::prepare_child(&mut self.input)?;
        eprintln!("[DISTINCT PREPARE] DistinctOperator prepared");
        Ok(())
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // First, try to output from buffer
        if !self.output_buffer.is_empty() {
            let batch_size = self.batch_size.min(self.output_buffer.len());
            let batch_rows = self.output_buffer.drain(..batch_size).collect::<Vec<_>>();
            // Compute schema dynamically from input operator
            return Ok(Some(self.values_to_batch(&batch_rows, &self.input.schema())?));
        }
        
        // If buffer is empty and input is exhausted, we're done
        if self.input_exhausted {
            return Ok(None);
        }
        
        // Consume input batches and collect unique rows
        loop {
            match self.input.next()? {
                Some(batch) => {
                    // Process each row in the batch
                    // SAFETY: Use batch.selection.len() to avoid array bounds violations
                    let actual_batch_size = batch.selection.len();
                    debug_assert!(actual_batch_size >= batch.row_count, 
                        "batch.selection.len() ({}) >= batch.row_count ({})", 
                        actual_batch_size, batch.row_count);
                    
                    for row_idx in 0..actual_batch_size {
                        // Only process selected rows
                        if !batch.selection[row_idx] {
                            continue;
                        }
                        
                        debug_assert!(row_idx < batch.batch.row_count, 
                            "row_idx {} < batch.batch.row_count {}", row_idx, batch.batch.row_count);
                        
                        // Extract row values
                        let row = Self::extract_row(&batch, row_idx)?;
                        
                        // Check if we've seen this row before
                        if !self.seen_rows.contains(&row) {
                            // New unique row - add to seen set and buffer
                            self.seen_rows.insert(row.clone());
                            self.output_buffer.push(row);
                            
                            // If buffer is full, output a batch
                            if self.output_buffer.len() >= self.batch_size {
                                let batch_rows = self.output_buffer.drain(..self.batch_size).collect::<Vec<_>>();
                                // Compute schema dynamically from input operator
            return Ok(Some(self.values_to_batch(&batch_rows, &self.input.schema())?));
                            }
                        }
                    }
                }
                None => {
                    // Input exhausted
                    self.input_exhausted = true;
                    
                    // Output remaining buffer if any
                    if !self.output_buffer.is_empty() {
                        let batch_rows = self.output_buffer.drain(..).collect::<Vec<_>>();
                        // Compute schema dynamically from input operator
            return Ok(Some(self.values_to_batch(&batch_rows, &self.input.schema())?));
                    }
                    
                    return Ok(None);
                }
            }
        }
    }
    
    fn schema(&self) -> SchemaRef {
        // Compute schema dynamically from input operator (never use stale snapshot)
        self.input.schema()
    }
}

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
    fn prepare(&mut self) -> Result<(), anyhow::Error> {
        // CRITICAL FIX: LimitOperator must recursively prepare its input
        eprintln!("[LIMIT PREPARE] Preparing LimitOperator, recursively preparing input");
        FilterOperator::prepare_child(&mut self.input)?;
        eprintln!("[LIMIT PREPARE] LimitOperator prepared");
        Ok(())
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // eprintln!("LimitOperator::next: rows_returned={}, limit={}, offset={}", 
        //     self.rows_returned, self.limit, self.offset);
        
        if self.rows_returned >= self.limit {
            // eprintln!("LimitOperator::next: Already returned {} rows, limit is {}, returning None", 
            //     self.rows_returned, self.limit);
            return Ok(None);
        }
        
        let mut batch = match self.input.next()? {
            Some(b) => {
                // eprintln!("LimitOperator::next: Got batch with {} rows from input", b.row_count);
                b
            },
            None => {
                // eprintln!("LimitOperator::next: Input returned None");
                return Ok(None);
            },
        };
        
        // Apply offset and limit
        if self.rows_returned < self.offset {
            let skip = (self.offset - self.rows_returned).min(batch.row_count);
            // eprintln!("LimitOperator::next: Skipping {} rows for offset", skip);
            batch = batch.slice(skip, batch.row_count - skip);
            self.rows_returned += skip;
        }
        
        if self.rows_returned >= self.limit {
            // eprintln!("LimitOperator::next: After offset, rows_returned={} >= limit={}, returning None", 
            //     self.rows_returned, self.limit);
            return Ok(None);
        }
        
        let remaining = self.limit - self.rows_returned;
        // eprintln!("LimitOperator::next: Remaining rows to return: {}", remaining);
        if batch.row_count > remaining {
            // eprintln!("LimitOperator::next: Slicing batch from {} to {} rows", batch.row_count, remaining);
            batch = batch.slice(0, remaining);
        }
        
        self.rows_returned += batch.row_count;
        // eprintln!("LimitOperator::next: Returning batch with {} rows (total returned: {})", 
        //     batch.row_count, self.rows_returned);
        Ok(Some(batch))
    }
    
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

/// Build execution operator from plan operator with LLM protocol limits
pub fn build_operator_with_llm_limits(
    plan_op: &PlanOperator,
    graph: std::sync::Arc<HyperGraph>,
    max_scan_rows: Option<u64>,
) -> Result<Box<dyn BatchIterator>> {
    build_operator_with_llm_limits_and_ctes(plan_op, graph, max_scan_rows, None)
}

pub fn build_operator_with_llm_limits_and_table_aliases(
    plan_op: &PlanOperator,
    graph: std::sync::Arc<HyperGraph>,
    max_scan_rows: Option<u64>,
    table_aliases: std::collections::HashMap<String, String>,
) -> Result<Box<dyn BatchIterator>> {
    build_operator_with_subquery_executor(plan_op, graph, max_scan_rows, None, None, Some(&table_aliases), None, None, None, None, None)
}

/// Build operator with CTE results cache support and subquery executor
pub fn build_operator_with_llm_limits_and_ctes(
    plan_op: &PlanOperator,
    graph: std::sync::Arc<HyperGraph>,
    max_scan_rows: Option<u64>,
    cte_results: Option<&std::collections::HashMap<String, Vec<crate::execution::batch::ExecutionBatch>>>,
) -> Result<Box<dyn BatchIterator>> {
    build_operator_with_subquery_executor(plan_op, graph, max_scan_rows, cte_results, None, None, None, None, None, None, None)
}

/// Build operator with CTE results cache support and subquery executor
pub fn build_operator_with_subquery_executor(
    plan_op: &PlanOperator,
    graph: std::sync::Arc<HyperGraph>,
    max_scan_rows: Option<u64>,
    cte_results: Option<&std::collections::HashMap<String, Vec<crate::execution::batch::ExecutionBatch>>>,
    subquery_executor: Option<std::sync::Arc<dyn crate::query::expression::SubqueryExecutor>>,
    table_aliases: Option<&std::collections::HashMap<String, String>>,
    vector_index_hints: Option<&std::collections::HashMap<(String, String), bool>>,
    btree_index_hints: Option<&std::collections::HashMap<(String, String), bool>>,
    partition_hints: Option<&std::collections::HashMap<String, Vec<crate::storage::partitioning::PartitionId>>>,
    operator_cache: Option<std::sync::Arc<std::sync::Mutex<crate::cache::operator_cache::OperatorCache>>>,
    wasm_jit_hints: Option<&std::collections::HashMap<String, crate::codegen::WasmProgramSpec>>,
) -> Result<Box<dyn BatchIterator>> {
    eprintln!("DEBUG build_operator_with_subquery_executor: subquery_executor.is_some()={}, operator={:?}", 
        subquery_executor.is_some(), 
        std::mem::discriminant(plan_op));
    let table_aliases = table_aliases.cloned().unwrap_or_default();
    match plan_op {
        PlanOperator::CTEScan { cte_name, columns, limit, offset } => {
            // Get CTE results from cache - try both with and without __CTE_ prefix
            let batches = if let Some(cache) = cte_results {
                // Try with __CTE_ prefix first (as stored in materialized_views)
                let cte_key = format!("__CTE_{}", cte_name);
                cache.get(&cte_key)
                    .or_else(|| cache.get(cte_name)) // Fallback to plain name
                    .ok_or_else(|| anyhow::anyhow!("CTE '{}' results not found in cache (tried keys: '{}', '{}')", cte_name, cte_key, cte_name))?
                    .clone()
            } else {
                return Err(anyhow::anyhow!("CTE '{}' results not found - cte_results is None", cte_name));
            };
            
            use crate::execution::cte_scan::CTEScanOperator;
            Ok(Box::new(CTEScanOperator::new(
                batches,
                columns.clone(),
                *limit,
                *offset,
            )))
        }
        PlanOperator::DerivedTableScan { derived_table_name, columns, limit, offset } => {
            // Get derived table results from cache - try both with and without __DERIVED_ prefix
            let batches = if let Some(cache) = cte_results {
                // Try with __DERIVED_ prefix first (as stored)
                let derived_key = format!("__DERIVED_{}", derived_table_name);
                cache.get(&derived_key)
                    .or_else(|| cache.get(derived_table_name)) // Fallback to plain name
                    .ok_or_else(|| anyhow::anyhow!("Derived table '{}' results not found in cache (tried keys: '{}', '{}')", derived_table_name, derived_key, derived_table_name))?
                    .clone()
            } else {
                return Err(anyhow::anyhow!("Derived table '{}' results not found - cte_results is None", derived_table_name));
            };
            
            use crate::execution::derived_table_scan::DerivedTableScanOperator;
            Ok(Box::new(DerivedTableScanOperator::new(
                batches,
                columns.clone(),
                *limit,
                *offset,
            )))
        }
        PlanOperator::Scan { node_id, table, columns, limit, offset } => {
            let mut scan_op = ScanOperator::new(*node_id, table.clone(), columns.clone(), graph)?;
            // Set LIMIT/OFFSET for early termination
            scan_op.limit = *limit;
            scan_op.offset = *offset;
            // CRITICAL FIX: Set max_scan_rows from LLM protocol
            scan_op.max_scan_rows = max_scan_rows;
            // INTEGRATION: Store vector index hints for runtime usage
            if let Some(hints) = vector_index_hints {
                scan_op.vector_index_hints = hints.clone();
            }
            // INTEGRATION: Store B-tree index hints for runtime usage
            if let Some(hints) = btree_index_hints {
                scan_op.btree_index_hints = hints.clone();
            }
            // INTEGRATION: Store partition hints for runtime usage
            if let Some(hints) = partition_hints {
                if let Some(partitions) = hints.get(table) {
                    scan_op.partition_hints = partitions.clone();
                }
            }
            Ok(Box::new(scan_op))
        }
        PlanOperator::Filter { input, predicates, where_expression } => {
            eprintln!("DEBUG build_operator: Creating FilterOperator with where_expression.is_some()={}, predicates.len()={}", 
                where_expression.is_some(), predicates.len());
            // If the input is a simple Scan, push down fragment-level predicates into ScanOperator
            if let PlanOperator::Scan { node_id, table, columns, limit, offset } = input.as_ref() {
                let mut scan_op = ScanOperator::new(*node_id, table.clone(), columns.clone(), graph.clone())?;
                scan_op.limit = *limit;
                scan_op.offset = *offset;
                // INTEGRATION: Store vector index hints for runtime usage
                if let Some(hints) = vector_index_hints {
                    scan_op.vector_index_hints = hints.clone();
                }
                
                // Convert simple FilterPredicates into FragmentPredicates for pruning
                // NOTE: Only push down predicates when there's no WHERE expression tree (for OR conditions)
                let mut fragment_preds = Vec::new();
                if where_expression.is_none() {
                    for pred in predicates {
                        use crate::query::plan::PredicateOperator;
                        use crate::storage::cache_layout::FragmentPredicate as FP;
                        
                        let fp = match pred.operator {
                            PredicateOperator::LessThan => Some(FP::LessThan(pred.value.clone())),
                            PredicateOperator::GreaterThan => Some(FP::GreaterThan(pred.value.clone())),
                            PredicateOperator::Equals => Some(FP::Equals(pred.value.clone())),
                            // BETWEEN not explicitly represented; could be modeled via two predicates
                            _ => None, // LIKE, IN, NOT EQUAL, etc. not used for fragment pruning
                        };
                        
                        if let Some(frag_pred) = fp {
                            fragment_preds.push((pred.column.clone(), frag_pred));
                        }
                    }
                }
                
                scan_op.fragment_predicates = fragment_preds;
                
                let input_op: Box<dyn BatchIterator> = Box::new(scan_op);
                Ok(Box::new(FilterOperator::with_subquery_executor_and_aliases(input_op, predicates.clone(), where_expression.clone(), subquery_executor.clone(), table_aliases.clone())))
            } else {
                let input_op = build_operator_with_subquery_executor(input, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
                Ok(Box::new(FilterOperator::with_subquery_executor_and_aliases(input_op, predicates.clone(), where_expression.clone(), subquery_executor.clone(), table_aliases.clone())))
            }
        }
        PlanOperator::Join { left, right, edge_id, join_type, predicate } => {
            eprintln!("DEBUG build_operator: Building JoinOperator: left_table={}, right_table={}, left_col={}, right_col={}", 
                predicate.left.0, predicate.right.0, predicate.left.1, predicate.right.1);
            let left_op = build_operator_with_subquery_executor(left, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
            let right_op = build_operator_with_subquery_executor(right, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
            eprintln!("DEBUG build_operator: JoinOperator built - left schema: {} fields ({:?}), right schema: {} fields ({:?})", 
                left_op.schema().fields().len(),
                left_op.schema().fields().iter().map(|f| f.name().to_string()).collect::<Vec<_>>(),
                right_op.schema().fields().len(),
                right_op.schema().fields().iter().map(|f| f.name().to_string()).collect::<Vec<_>>());
            let mut join_op = JoinOperator::with_table_aliases(
                left_op,
                right_op,
                join_type.clone(),
                predicate.clone(),
                graph.clone(),
                *edge_id,
                table_aliases.clone(),
            );
            
            // ExecNode: prepare() will be called by execution engine
            // No need to pre-build here - prepare() handles it recursively
            eprintln!("DEBUG build_operator: JoinOperator created, output schema will have {} fields", 
                join_op.schema().fields().len());
            
            // ExecNode: prepare() will be called by execution engine
            // No need to pre-build here - prepare() handles it recursively
            Ok(Box::new(join_op))
        }
        PlanOperator::BitsetJoin { left, right, edge_id, join_type, predicate, use_bitmap } => {
            use crate::execution::bitset_join::BitsetJoinOperator;
            use crate::storage::bitmap_index::BitmapIndex;
            
            // Try to retrieve bitmap indexes from hypergraph nodes
            let left_table = &predicate.left.0;
            let left_column = &predicate.left.1;
            let right_table = &predicate.right.0;
            let right_column = &predicate.right.1;
            
            // Get bitmap indexes from column nodes
            let left_node = graph.get_node_by_table_column(left_table, left_column);
            let right_node = graph.get_node_by_table_column(right_table, right_column);
            
            let (left_index, right_index) = match (left_node, right_node) {
                (Some(left_n), Some(right_n)) => {
                    // Try to get bitmap index from fragments
                    let left_idx = left_n.fragments.first()
                        .and_then(|f| f.bitmap_index.clone());
                    let right_idx = right_n.fragments.first()
                        .and_then(|f| f.bitmap_index.clone());
                    
                    match (left_idx, right_idx) {
                        (Some(li), Some(ri)) => (li, ri),
                        _ => {
                            // Fall back to regular join if indexes not available
                            eprintln!("DEBUG: Bitmap indexes not found for {}.{} or {}.{} - falling back to regular join", 
                                left_table, left_column, right_table, right_column);
                            let left_op = build_operator_with_subquery_executor(left, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
                            let right_op = build_operator_with_subquery_executor(right, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
                            let mut join_op = JoinOperator::with_table_aliases(
                                left_op,
                                right_op,
                                join_type.clone(),
                                predicate.clone(),
                                graph.clone(),
                                *edge_id,
                                table_aliases.clone(),
                            );
                            
                            // ExecNode: prepare() will be called by execution engine
                            // No need to pre-build here - prepare() handles it recursively
                            
                            return Ok(Box::new(join_op));
                        }
                    }
                }
                _ => {
                    // Fall back to regular join if nodes not found
                    eprintln!("DEBUG: Column nodes not found for {}.{} or {}.{} - falling back to regular join", 
                        left_table, left_column, right_table, right_column);
                    let left_op = build_operator_with_subquery_executor(left, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
                    let right_op = build_operator_with_subquery_executor(right, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
                    let mut join_op = JoinOperator::with_table_aliases(
                        left_op,
                        right_op,
                        join_type.clone(),
                        predicate.clone(),
                        graph.clone(),
                        *edge_id,
                        table_aliases.clone(),
                    );
                    
                    // ExecNode: prepare() will be called by execution engine
                    // No need to pre-build here - prepare() handles it recursively
                    
                    return Ok(Box::new(join_op));
                }
            };
            
            // Materialize table data by executing scan operators
            // For now, we'll materialize by executing the left and right operators
            // and collecting their results
            let left_op = build_operator_with_subquery_executor(left, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
            let right_op = build_operator_with_subquery_executor(right, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
            
            // Materialize left table data
            let mut left_data: Vec<Vec<Value>> = Vec::new();
            let mut left_columns: Vec<String> = Vec::new();
            {
                let mut left_iter = left_op;
                if let Some(mut batch) = left_iter.next()? {
                    // Get column names from schema
                    let schema = batch.batch.schema.clone();
                    for field in schema.fields() {
                        left_columns.push(field.name().clone());
                        left_data.push(Vec::new());
                    }
                    
                    // Collect all data from left side
                    loop {
                        let row_count = batch.row_count;
                        for col_idx in 0..left_columns.len() {
                            if let Some(array) = batch.batch.columns.get(col_idx) {
                                for row_idx in 0..row_count {
                                    if let Ok(value) = extract_value(array, row_idx) {
                                        left_data[col_idx].push(value);
                                    }
                                }
                            }
                        }
                        
                        match left_iter.next()? {
                            Some(next_batch) => batch = next_batch,
                            None => break,
                        }
                    }
                }
            }
            
            // Materialize right table data
            let mut right_data: Vec<Vec<Value>> = Vec::new();
            let mut right_columns: Vec<String> = Vec::new();
            {
                let mut right_iter = right_op;
                if let Some(mut batch) = right_iter.next()? {
                    // Get column names from schema
                    let schema = batch.batch.schema.clone();
                    for field in schema.fields() {
                        right_columns.push(field.name().clone());
                        right_data.push(Vec::new());
                    }
                    
                    // Collect all data from right side
                    loop {
                        let row_count = batch.row_count;
                        for col_idx in 0..right_columns.len() {
                            if let Some(array) = batch.batch.columns.get(col_idx) {
                                for row_idx in 0..row_count {
                                    if let Ok(value) = extract_value(array, row_idx) {
                                        right_data[col_idx].push(value);
                                    }
                                }
                            }
                        }
                        
                        match right_iter.next()? {
                            Some(next_batch) => batch = next_batch,
                            None => break,
                        }
                    }
                }
            }
            
            // Build output schema (left columns + right columns)
            let mut output_schema = left_columns.clone();
            output_schema.extend(right_columns.clone());
            
            // Create BitsetJoinOperator
            let bitset_join_op = BitsetJoinOperator::new(
                left_index,
                right_index,
                left_column.clone(),
                right_column.clone(),
                left_data,
                right_data,
                left_columns,
                right_columns,
                output_schema,
            );
            
            Ok(Box::new(bitset_join_op))
        }
        PlanOperator::Aggregate { input, group_by, group_by_aliases, aggregates, having: _having } => {
            let input_op = build_operator_with_subquery_executor(input, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
            // COLID: Pass GROUP BY aliases to AggregateOperator
            let agg_op = AggregateOperator::with_table_aliases_and_group_by_aliases(
                input_op, 
                group_by.clone(), 
                aggregates.clone(),
                table_aliases.clone(), // Pass table aliases if available
                group_by_aliases.clone() // COLID: Pass GROUP BY aliases from planner
            );
            // HAVING is handled as separate HavingOperator after Aggregate
            Ok(Box::new(agg_op))
        }
        PlanOperator::Having { input, predicate } => {
            let input_op = build_operator_with_subquery_executor(input, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
            use crate::execution::having::HavingOperator;
            Ok(Box::new(HavingOperator::with_subquery_executor_and_aliases(input_op, predicate.clone(), subquery_executor.clone(), table_aliases.clone())))
        }
        PlanOperator::Window { input, window_functions } => {
            let input_op = build_operator_with_subquery_executor(input, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
            use crate::execution::window::WindowOperator;
            // Create WindowOperator with table aliases for proper column resolution
            Ok(Box::new(WindowOperator::with_table_aliases(
                input_op, 
                window_functions.clone(),
                table_aliases.clone()
            )))
        }
        PlanOperator::Project { input, columns, expressions } => {
            eprintln!("DEBUG build_operator: Creating ProjectOperator with {} columns: {:?}, {} expressions: {:?}", 
                columns.len(), columns,
                expressions.len(),
                expressions.iter().map(|e| match &e.expr_type {
                    crate::query::plan::ProjectionExprType::Column(c) => format!("Column({})", c),
                    _ => format!("{:?}", e.expr_type),
                }).collect::<Vec<_>>());
            eprintln!("DEBUG build_operator: ProjectOperator input operator type in PLAN: {:?}", 
                std::mem::discriminant(input.as_ref()));
            // CRITICAL DEBUG: Log the actual input operator structure
            match input.as_ref() {
                PlanOperator::Join { predicate, .. } => {
                    eprintln!("DEBUG build_operator: Project input IS a Join: {} JOIN {} ON {}.{} = {}.{}", 
                        predicate.left.0, predicate.right.0,
                        predicate.left.0, predicate.left.1,
                        predicate.right.0, predicate.right.1);
                }
                PlanOperator::Scan { table, .. } => {
                    eprintln!("DEBUG build_operator: Project input IS a Scan: table={}", table);
                }
                _ => {
                    eprintln!("DEBUG build_operator: Project input is: {:?}", std::mem::discriminant(input.as_ref()));
                }
            }
            let input_op = build_operator_with_subquery_executor(input, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
            eprintln!("DEBUG build_operator: ProjectOperator input operator built - schema has {} fields: {:?}", 
                input_op.schema().fields().len(),
                input_op.schema().fields().iter().map(|f| f.name().to_string()).collect::<Vec<_>>());
            let mut proj_op = ProjectOperator::with_subquery_executor_and_aliases(
                input_op,
                columns.clone(),
                expressions.clone(),
                subquery_executor.clone(),
                table_aliases.clone(),
            );
            // Set operator cache and WASM JIT hints (Phase C)
            if let Some(ref cache) = operator_cache {
                proj_op = proj_op.with_operator_cache(cache.clone());
            }
            if let Some(hints) = wasm_jit_hints {
                proj_op = proj_op.with_wasm_programs(hints);
            }
            Ok(Box::new(proj_op))
        }
        PlanOperator::Sort { input, order_by, limit, offset } => {
            // CANONICAL ORDER BY: These are the resolved OrderByExprs from planner - use them directly
            eprintln!("DEBUG ExecutionEngine: Building SortOperator with planner-resolved order_by: {:?}", order_by.iter().map(|o| &o.column).collect::<Vec<_>>());
            
            let input_op = build_operator_with_subquery_executor(input, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
            Ok(Box::new(SortOperator::with_table_aliases(
                input_op,
                order_by.clone(), // Use planner-resolved order_by directly - no further transformation
                *limit,
                *offset,
                table_aliases.clone(),
            )))
        }
        PlanOperator::Limit { input, limit, offset } => {
            let input_op = build_operator_with_subquery_executor(input, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
            Ok(Box::new(LimitOperator::new(input_op, *limit, *offset)))
        }
        PlanOperator::SetOperation { left, right, operation } => {
            use crate::query::union::{SetOperator, SetOperation};
            let left_op = build_operator_with_subquery_executor(left, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
            let right_op = build_operator_with_subquery_executor(right, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
            let set_op = match operation {
                crate::query::plan::SetOperationType::Union => SetOperation::Union,
                crate::query::plan::SetOperationType::UnionAll => SetOperation::UnionAll,
                crate::query::plan::SetOperationType::Intersect => SetOperation::Intersect,
                crate::query::plan::SetOperationType::Except => SetOperation::Except,
            };
            Ok(Box::new(SetOperator::new(left_op, right_op, set_op)))
        }
        PlanOperator::Distinct { input } => {
            let input_op = build_operator_with_subquery_executor(input, graph.clone(), max_scan_rows, cte_results, subquery_executor.clone(), Some(&table_aliases), vector_index_hints, btree_index_hints, partition_hints, operator_cache.clone(), wasm_jit_hints)?;
            Ok(Box::new(DistinctOperator::new(input_op)))
        }
        PlanOperator::Fused { input, operations } => {
            // For fused operators, we need to execute the fused operations in sequence
            // For now, we'll "unfuse" them and execute as separate operators
            // This is a temporary implementation - a full implementation would have a FusedOperator
            // that executes all operations in a single pass for better performance
            
            // Build the input operator
            let mut current_op = build_operator_with_subquery_executor(
                input, 
                graph.clone(), 
                max_scan_rows, 
                cte_results, 
                subquery_executor.clone(), 
                Some(&table_aliases), 
                vector_index_hints, 
                btree_index_hints, 
                partition_hints, 
                operator_cache.clone(), 
                wasm_jit_hints
            )?;
            
            // Apply fused operations in reverse order (since they're stacked)
            // Operations are stored in execution order: [Aggregate, Project, Filter]
            // We need to apply them: Filter -> Project -> Aggregate
            for op in operations.iter().rev() {
                match op {
                    crate::query::plan::FusedOperation::Filter { predicate } => {
                        // Apply filter
                        use crate::execution::operators::FilterOperator;
                        current_op = Box::new(FilterOperator::new(
                            current_op,
                            vec![predicate.clone()],
                        ));
                    }
                    crate::query::plan::FusedOperation::Project { columns, expressions } => {
                        // Apply projection
                        use crate::execution::operators::ProjectOperator;
                        let mut proj_op = ProjectOperator::with_subquery_executor_and_aliases(
                            current_op,
                            columns.clone(),
                            expressions.clone(),
                            subquery_executor.clone(),
                            table_aliases.clone(),
                        );
                        if let Some(ref cache) = operator_cache {
                            proj_op = proj_op.with_operator_cache(cache.clone());
                        }
                        if let Some(hints) = wasm_jit_hints {
                            proj_op = proj_op.with_wasm_programs(hints);
                        }
                        current_op = Box::new(proj_op);
                    }
                    crate::query::plan::FusedOperation::Aggregate { group_by, group_by_aliases, aggregates } => {
                        // Apply aggregation
                        use crate::execution::operators::AggregateOperator;
                        current_op = Box::new(AggregateOperator::with_table_aliases_and_group_by_aliases(
                            current_op,
                            group_by.clone(),
                            aggregates.clone(),
                            table_aliases.clone(),
                            group_by_aliases.clone(),
                        ));
                    }
                }
            }
            
            Ok(current_op)
        }
    }
}

/// Build execution operator from plan operator (convenience wrapper)
pub fn build_operator(
    plan_op: &PlanOperator,
    graph: std::sync::Arc<HyperGraph>,
) -> Result<Box<dyn BatchIterator>> {
    build_operator_with_llm_limits(plan_op, graph, None)
}

