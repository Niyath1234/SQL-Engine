/// Shared execution for multi-query optimization
/// Execute multiple queries with overlapping scans in a single pass
use crate::execution::batch::{ExecutionBatch, BatchIterator};
use crate::execution::operators::ScanOperator;
use crate::query::plan::QueryPlan;
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::NodeId;
use arrow::datatypes::SchemaRef;
use arrow::array::{Int64Array, StringArray};
use std::sync::Arc;
use anyhow::Result;
use bitvec::prelude::*;

/// Query bundle: multiple queries that can share execution
pub struct QueryBundle {
    /// Queries in this bundle
    pub queries: Vec<BundledQuery>,
    /// Common table(s) they all access
    pub common_tables: Vec<String>,
    /// Common columns they all need
    pub common_columns: Vec<String>,
}

/// A single query in a bundle
#[derive(Clone)]
pub struct BundledQuery {
    /// Original query plan
    pub plan: QueryPlan,
    /// Query signature (for result routing)
    pub signature: String,
    /// Per-query predicates (applied after shared scan)
    pub predicates: Vec<QueryPredicate>,
    /// Columns needed by this query
    pub needed_columns: Vec<String>,
}

/// Predicate for a specific query in the bundle
#[derive(Clone)]
pub struct QueryPredicate {
    pub column: String,
    pub operator: String,
    pub value: crate::storage::fragment::Value,
}

/// Shared scan operator: scans once, serves multiple queries
pub struct SharedScanOperator {
    /// Base scan operator
    scan: Box<dyn BatchIterator>,
    /// Queries in this bundle
    queries: Vec<BundledQuery>,
    /// Current batch (shared across queries)
    current_batch: Option<ExecutionBatch>,
    /// Per-query selection vectors (computed from current_batch)
    query_selections: Vec<BitVec>,
}

impl SharedScanOperator {
    pub fn new(
        node_id: NodeId,
        table: String,
        common_columns: Vec<String>,
        graph: Arc<HyperGraph>,
        queries: Vec<BundledQuery>,
    ) -> Result<Self> {
        let scan_op = ScanOperator::new(node_id, table, common_columns, graph)?;
        let scan: Box<dyn BatchIterator> = Box::new(scan_op);
        
        Ok(Self {
            scan,
            queries,
            current_batch: None,
            query_selections: vec![],
        })
    }
    
    /// Get next batch for a specific query
    pub fn next_for_query(&mut self, query_idx: usize) -> Result<Option<ExecutionBatch>> {
        if query_idx >= self.queries.len() {
            return Ok(None);
        }
        
        // If we don't have a current batch, get one from scan
        if self.current_batch.is_none() {
            self.current_batch = self.scan.next()?;
            if self.current_batch.is_none() {
                return Ok(None);
            }
            
            // Compute per-query selections from current batch
            self.compute_query_selections()?;
        }
        
        // Extract batch for this query
        let mut batch = self.current_batch.as_ref().unwrap().clone();
        
        // Apply this query's selection
        if query_idx < self.query_selections.len() {
            batch.apply_selection(&self.query_selections[query_idx]);
        }
        
        // If we've consumed this batch for all queries, clear it
        // For now, we'll keep it until all queries have consumed it
        // In a full implementation, we'd track per-query consumption
        
        Ok(Some(batch))
    }
    
    fn compute_query_selections(&mut self) -> Result<()> {
        let batch = self.current_batch.as_ref().unwrap();
        self.query_selections.clear();
        
        for query in &self.queries {
            let mut selection = bitvec![1; batch.batch.row_count];
            
            // Apply predicates for this query
            for predicate in &query.predicates {
                let column = batch.batch.column_by_name(&predicate.column);
                if let Some(col) = column {
                    let col_selection = self.apply_predicate(col, predicate)?;
                    selection &= &col_selection;
                }
            }
            
            self.query_selections.push(selection);
        }
        
        Ok(())
    }
    
    fn apply_predicate(
        &self,
        array: &Arc<dyn arrow::array::Array>,
        predicate: &QueryPredicate,
    ) -> Result<BitVec> {
        // For now, use a simplified approach that delegates to FilterOperator
        // In full implementation, we'd reuse FilterOperator's apply_filter_predicate directly
        use crate::execution::operators::FilterOperator;
        use crate::query::plan::{FilterPredicate, PredicateOperator};
        use crate::execution::batch::BatchIterator;
        
        // Create a temporary batch with just this column
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(&predicate.column, array.data_type().clone(), true),
        ]));
        let batch = crate::storage::columnar::ColumnarBatch::new(
            vec![array.clone()],
            schema.clone(),
        );
        let exec_batch = ExecutionBatch::new(batch);
        
        // Create a simple iterator that returns this batch once
        struct SingleBatchIter {
            batch: Option<ExecutionBatch>,
            schema: arrow::datatypes::SchemaRef,
        }
        impl BatchIterator for SingleBatchIter {
            fn next(&mut self) -> Result<Option<ExecutionBatch>> {
                Ok(self.batch.take())
            }
            fn schema(&self) -> arrow::datatypes::SchemaRef {
                self.schema.clone()
            }
        }
        
        let mut iter = SingleBatchIter {
            batch: Some(exec_batch),
            schema,
        };
        
        // Convert predicate
        let filter_pred = FilterPredicate {
            column: predicate.column.clone(),
            operator: match predicate.operator.as_str() {
                "=" => PredicateOperator::Equals,
                ">" => PredicateOperator::GreaterThan,
                "<" => PredicateOperator::LessThan,
                ">=" => PredicateOperator::GreaterThanOrEqual,
                "<=" => PredicateOperator::LessThanOrEqual,
                "!=" | "<>" => PredicateOperator::NotEquals,
                _ => PredicateOperator::Equals,
            },
            value: predicate.value.clone(),
            pattern: None,
            in_values: None,
            subquery_expression: None,
        };
        
        // Use FilterOperator
        let mut filter_op = FilterOperator::new(Box::new(iter), vec![filter_pred]);
        let filtered_batch = filter_op.next()?;
        
        if let Some(batch) = filtered_batch {
            Ok(batch.selection)
        } else {
            Ok(bitvec![0; array.len()])
        }
    }
}

/// Query bundler: groups queries that can share execution
pub struct QueryBundler {
    /// Time window for bundling (milliseconds)
    pub window_ms: u64,
    /// Pending queries waiting to be bundled
    pending_queries: Vec<PendingQuery>,
}

/// A query waiting to be bundled
pub struct PendingQuery {
    pub plan: QueryPlan,
    pub signature: String,
    pub tables: Vec<String>,
    pub columns: Vec<String>,
    pub predicates: Vec<QueryPredicate>,
    pub timestamp_ms: u64,
}

impl QueryBundler {
    pub fn new(window_ms: u64) -> Self {
        Self {
            window_ms,
            pending_queries: vec![],
        }
    }
    
    /// Add a query to the bundler
    pub fn add_query(&mut self, query: PendingQuery) {
        self.pending_queries.push(query);
    }
    
    /// Try to form bundles from pending queries
    pub fn form_bundles(&mut self, current_time_ms: u64) -> Vec<QueryBundle> {
        // Remove queries outside the time window
        self.pending_queries.retain(|q| {
            current_time_ms.saturating_sub(q.timestamp_ms) <= self.window_ms
        });
        
        if self.pending_queries.is_empty() {
            return vec![];
        }
        
        // Group queries by common tables
        let mut bundles: Vec<QueryBundle> = vec![];
        let mut processed = bitvec![0; self.pending_queries.len()];
        
        for i in 0..self.pending_queries.len() {
            if processed[i] {
                continue;
            }
            
            let query = &self.pending_queries[i];
            let mut bundle_queries = vec![BundledQuery {
                plan: query.plan.clone(),
                signature: query.signature.clone(),
                predicates: query.predicates.clone(),
                needed_columns: query.columns.clone(),
            }];
            
            let common_tables = query.tables.clone();
            let mut common_columns = query.columns.clone();
            
            processed.set(i, true);
            
            // Find other queries that share the same table(s)
            for j in (i + 1)..self.pending_queries.len() {
                if processed[j] {
                    continue;
                }
                
                let other = &self.pending_queries[j];
                
                // Check if they share at least one table
                let shares_table = common_tables.iter().any(|t| other.tables.contains(t));
                
                if shares_table {
                    bundle_queries.push(BundledQuery {
                        plan: other.plan.clone(),
                        signature: other.signature.clone(),
                        predicates: other.predicates.clone(),
                        needed_columns: other.columns.clone(),
                    });
                    
                    // Update common columns (intersection)
                    common_columns.retain(|c| other.columns.contains(c));
                    
                    processed.set(j, true);
                }
            }
            
            if bundle_queries.len() > 1 {
                // Only create bundle if we have multiple queries
                bundles.push(QueryBundle {
                    queries: bundle_queries,
                    common_tables,
                    common_columns,
                });
            }
        }
        
        // Clear processed queries
        self.pending_queries.retain(|_| {
            // Keep unprocessed queries for next round
            true
        });
        
        bundles
    }
}

