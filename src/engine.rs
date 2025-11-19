use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::graph::FragmentStats as HGFragmentStats;
use crate::hypergraph::node::{HyperNode, NodeId};
use crate::hypergraph::edge::{HyperEdge, EdgeId, JoinType as EdgeJoinType, JoinPredicate as EdgeJoinPredicate, PredicateOperator as EdgePredicateOperator};
use crate::query::parser::parse_sql;
use crate::query::parser_enhanced::extract_query_info_enhanced;
use crate::query::dml::{extract_insert, extract_update, extract_delete, InsertStatement, UpdateStatement, DeleteStatement};
use crate::query::ddl::{extract_create_table, extract_drop_table, CreateTableStatement};
use arrow::array::*;
use arrow::datatypes::DataType as ArrowDataType;
use crate::query::planner::QueryPlanner;
use crate::query::cache::{PlanCache, QuerySignature};
use crate::query::optimizer::HypergraphOptimizer;
use crate::execution::engine::ExecutionEngine;
use crate::execution::adaptive::{AdaptiveExecutionEngine, RuntimeStatistics};
use crate::execution::wcoj::should_use_wcoj;
use crate::cache::result_cache::ResultCache;
use crate::storage::fragment::ColumnFragment;
use crate::storage::memory_tier::MultiTierMemoryManager;
use crate::storage::adaptive_fragment::AdaptiveFragmentManager;
use crate::storage::tiered_index::TieredIndexManager;
use std::sync::Arc;
use std::collections::HashMap;
use anyhow::Result;

/// Main SQL engine interface
pub struct HypergraphSQLEngine {
    graph: Arc<HyperGraph>,
    planner: QueryPlanner,
    optimizer: HypergraphOptimizer,
    plan_cache: PlanCache,
    result_cache: ResultCache,
    execution_engine: ExecutionEngine,
    adaptive_engine: Option<AdaptiveExecutionEngine>,
    use_adaptive: bool,
    /// Multi-tier memory manager (RAM as L1)
    memory_manager: Arc<MultiTierMemoryManager>,
    /// Adaptive fragment manager (for dynamic fragmenting and recompression)
    adaptive_fragment_manager: Arc<AdaptiveFragmentManager>,
    /// Tiered index manager (learned + tiered indexes as memory filters)
    tiered_index_manager: Arc<TieredIndexManager>,
    /// Trace-based specialization hits per query signature (for JIT-like fast paths)
    trace_hits: HashMap<QuerySignature, u64>,
}

impl HypergraphSQLEngine {
    /// Create a new engine instance
    pub fn new() -> Self {
        let graph = Arc::new(HyperGraph::new());
        let planner = QueryPlanner::from_arc(graph.clone());
        // Optimizer can work with a clone for now (it's only used for join reordering)
        let optimizer = HypergraphOptimizer::new((*graph).clone());
        let plan_cache = PlanCache::new(1000); // Cache up to 1000 plans
        let result_cache = ResultCache::new(100, 300); // Cache 100 results for 5 minutes
        let execution_engine = ExecutionEngine::from_arc(graph.clone());
        let execution_engine_for_adaptive = ExecutionEngine::from_arc(graph.clone());
        let adaptive_engine = Some(AdaptiveExecutionEngine::new(execution_engine_for_adaptive));
        
        // Initialize multi-tier memory manager
        // Default: 4GB L1 (RAM), 8GB L2 (compressed RAM)
        // These can be configured based on available system memory
        let memory_manager = Arc::new(MultiTierMemoryManager::new(4096, 8192));
        
        // Initialize adaptive fragment manager
        let adaptive_fragment_manager = Arc::new(AdaptiveFragmentManager::new());
        
        // Initialize tiered index manager
        let tiered_index_manager = Arc::new(TieredIndexManager::new());
        
        Self {
            graph,
            planner,
            optimizer,
            plan_cache,
            result_cache,
            execution_engine,
            adaptive_engine,
            use_adaptive: true,
            memory_manager,
            adaptive_fragment_manager,
            tiered_index_manager,
            trace_hits: HashMap::new(),
        }
    }
    
    /// Create engine with custom memory tier configuration
    pub fn new_with_memory(l1_capacity_mb: usize, l2_capacity_mb: usize) -> Self {
        let mut engine = Self::new();
        engine.memory_manager = Arc::new(MultiTierMemoryManager::new(l1_capacity_mb, l2_capacity_mb));
        engine
    }
    
    /// Create engine without adaptive processing
    pub fn new_simple() -> Self {
        let mut engine = Self::new();
        engine.use_adaptive = false;
        engine
    }
    
    /// Execute a SQL query
    pub fn execute_query(&mut self, sql: &str) -> Result<QueryResult> {
        self.execute_query_with_cache(sql, true)
    }
    
    /// Execute a SQL query with optional result caching
    pub fn execute_query_with_cache(&mut self, sql: &str, use_result_cache: bool) -> Result<QueryResult> {
        // Parse SQL to determine statement type
        let ast = parse_sql(sql)?;
        
        // Super-fast SQL-level fast path: SELECT COUNT(*) FROM <table> with no WHERE/GROUP BY
        if let Some(result) = self.try_fast_path_sql(&ast) {
            return Ok(result);
        }
        
        // Route to appropriate handler based on statement type
        match &ast {
            sqlparser::ast::Statement::Query(_) => {
                // SELECT query - use existing path
                self.execute_select_query(sql, use_result_cache)
            }
            sqlparser::ast::Statement::Insert { .. } => {
                // INSERT statement
                let insert_stmt = extract_insert(&ast)?;
                self.execute_insert(insert_stmt)
            }
            sqlparser::ast::Statement::Update { .. } => {
                // UPDATE statement
                let update_stmt = extract_update(&ast)?;
                self.execute_update(update_stmt)
            }
            sqlparser::ast::Statement::Delete { .. } => {
                // DELETE statement
                let delete_stmt = extract_delete(&ast)?;
                self.execute_delete(delete_stmt)
            }
            sqlparser::ast::Statement::CreateTable { .. } => {
                // CREATE TABLE statement
                let create_stmt = extract_create_table(&ast)?;
                self.execute_create_table(create_stmt)
            }
            sqlparser::ast::Statement::Drop { .. } => {
                // DROP TABLE statement
                let table_name = extract_drop_table(&ast)?;
                self.execute_drop_table(&table_name)
            }
            _ => anyhow::bail!("Unsupported statement type: {:?}", ast),
        }
    }
    
    /// Execute a SELECT query (existing implementation)
    fn execute_select_query(&mut self, sql: &str, use_result_cache: bool) -> Result<QueryResult> {
        // 1. Check result cache (if enabled)
        let signature = QuerySignature::from_sql(sql);
        if use_result_cache {
            if let Some(cached_result) = self.result_cache.get(&signature) {
                let row_count = cached_result.iter().map(|b| b.row_count).sum();
                return Ok(QueryResult {
                    batches: cached_result,
                    row_count,
                    execution_time_ms: 0.0, // Cached - no execution time
                });
            }
        }
        
        // 2. Check plan cache
        let plan = if let Some(cached_plan) = self.plan_cache.get(&signature) {
            cached_plan.clone()
        } else {
        // 3. Parse SQL using enhanced parser
        let ast = parse_sql(sql)?;
        let parsed = extract_query_info_enhanced(&ast)?;
        
        // Debug: check if tables were extracted
        if parsed.tables.is_empty() {
            anyhow::bail!("No tables found in query. Query: {}", sql);
        }
        
        // 3.5. Extract CTE context if present
        let cte_context = if let sqlparser::ast::Statement::Query(query) = &ast {
            if query.with.is_some() {
                Some(crate::query::cte::CTEContext::from_query(query)?)
            } else {
                None
            }
        } else {
            None
        };
        
        // 4. Plan query with CTE context
        let planner = if let Some(cte_ctx) = cte_context {
            self.planner.clone().with_cte_context(cte_ctx)
        } else {
            self.planner.clone()
        };
        let mut plan = planner.plan(&parsed)?;
        
        // 4.5. Optimize plan using hypergraph optimizer
        if let Some(path) = self.find_path_for_query(&parsed.tables)? {
            if let Ok(optimal_order) = self.optimizer.optimize_joins(&path) {
                // Reorder joins in plan if needed
                // TODO: Apply optimal order to plan
            }
        }
        
        // 5. Cache plan
        self.plan_cache.insert(signature.clone(), plan.clone());
        
        plan
        };

        // 2.5. Try trace-based specialized execution for hot, simple queries
        if let Some(result) = self.try_execute_trace_specialized(&signature, &plan) {
            return Ok(result);
        }
        
        // 6. Execute plan (use adaptive if enabled)
        let result = if self.use_adaptive && self.adaptive_engine.is_some() {
            let stats = Arc::new(RuntimeStatistics::new());
            self.adaptive_engine.as_ref().unwrap().execute_adaptive(&plan, stats)?
        } else {
            self.execution_engine.execute(&plan)?
        };
        
        // 7. Cache result (if enabled)
        if use_result_cache {
            let batches_for_cache: Vec<crate::execution::batch::ExecutionBatch> = result.batches.iter().cloned().collect();
            self.result_cache.insert(signature, batches_for_cache);
        }
        
        // 8. Sync fragment access to memory manager (for tier management)
        self.sync_fragment_access();
        
        // 9. Process adaptive fragmenting and recompression (periodically)
        // Only process every N queries to avoid overhead
        static QUERY_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let count = QUERY_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if count % 10 == 0 {
            // Process every 10 queries
            self.process_adaptive_fragments();
        }
        
        // Return result (QueryResult types should match)
        Ok(QueryResult {
            batches: result.batches,
            row_count: result.row_count,
            execution_time_ms: result.execution_time_ms,
        })
    }

    /// SQL-level fast path: recognize extremely simple patterns directly from AST,
    /// before planning and adaptive execution. Currently supports:
    ///   SELECT COUNT(*) FROM <table>
    fn try_fast_path_sql(
        &self,
        ast: &sqlparser::ast::Statement,
    ) -> Option<QueryResult> {
        use sqlparser::ast::{Statement, SetExpr, Select, SelectItem, Expr, Function, FunctionArg, FunctionArgExpr, ObjectName, TableFactor};

        // Only handle simple SELECT queries
        let query = match ast {
            Statement::Query(q) => q,
            _ => return None,
        };

        // Body must be a simple SELECT (no set operations)
        let select = match &*query.body {
            SetExpr::Select(s) => s,
            _ => return None,
        };

        // No WHERE, GROUP BY, HAVING for this fast path
        if select.selection.is_some() || !matches!(select.group_by, sqlparser::ast::GroupByExpr::All) || select.having.is_some() {
            return None;
        }

        // Projection must be exactly COUNT(*)
        if select.projection.len() != 1 {
            return None;
        }

        let is_count_star = match &select.projection[0] {
            SelectItem::UnnamedExpr(Expr::Function(Function { name, args, .. })) |
            SelectItem::ExprWithAlias { expr: Expr::Function(Function { name, args, .. }), .. } => {
                let func_name = name.to_string().to_uppercase();
                if func_name != "COUNT" {
                    false
                } else {
                    // COUNT(*) has either no args or a single wildcard arg
                    if args.is_empty() {
                        true
                    } else if args.len() == 1 {
                        matches!(
                            &args[0],
                            FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                                | FunctionArg::Named { arg: FunctionArgExpr::Wildcard, .. }
                        )
                    } else {
                        false
                    }
                }
            }
            _ => false,
        };

        if !is_count_star {
            return None;
        }

        // FROM must be a single base table with simple name
        if select.from.len() != 1 {
            return None;
        }

        let table_factor = &select.from[0].relation;
        let table_name = match table_factor {
            TableFactor::Table { name: ObjectName(idents), .. } if idents.len() == 1 => {
                idents[0].value.clone()
            }
            _ => return None,
        };

        // Use specialized COUNT(*) implementation
        match self.execute_specialized_count_star(&table_name) {
            Ok(result) => Some(result),
            Err(_) => None,
        }
    }

    /// Trace-based specialization: try to execute a hot query using a specialized fast path.
    /// Currently supports: SELECT COUNT(*) FROM <single_table> (no WHERE/GROUP BY/JOIN).
    fn try_execute_trace_specialized(
        &mut self,
        signature: &QuerySignature,
        plan: &crate::query::plan::QueryPlan,
    ) -> Option<QueryResult> {
        // Increase hit count for this query signature
        let entry = self.trace_hits.entry(signature.clone()).or_insert(0);
        *entry += 1;
        let hits = *entry;

        // Only specialize after a few executions (hot query)
        const HOT_THRESHOLD: u64 = 3;
        if hits < HOT_THRESHOLD {
            return None;
        }

        // Detect simple COUNT(*) over a single table without joins/filters
        let table_name = match Self::analyze_simple_count_star(&plan.root) {
            Some(t) => t,
            None => return None,
        };

        // Execute specialized COUNT(*) using hypergraph node statistics
        match self.execute_specialized_count_star(&table_name) {
            Ok(result) => Some(result),
            Err(_) => None,
        }
    }

    /// Analyze plan to see if it matches: SELECT COUNT(*) FROM <table> [optional LIMIT]
    /// Shape: (optional Limit/Project/Sort) -> Aggregate(Count(*)) -> Scan(table)
    fn analyze_simple_count_star(plan_root: &crate::query::plan::PlanOperator) -> Option<String> {
        use crate::query::plan::{PlanOperator, AggregateFunction};

        // Walk down wrapper operators until we reach Aggregate or something unsupported
        let mut op = plan_root;
        loop {
            match op {
                PlanOperator::Limit { input, .. } => {
                    op = input;
                }
                PlanOperator::Sort { input, .. } => {
                    op = input;
                }
                PlanOperator::Project { input, .. } => {
                    op = input;
                }
                PlanOperator::Aggregate { input, group_by, aggregates, .. } => {
                    // Must be a single COUNT(*) aggregate with no GROUP BY
                    if !group_by.is_empty() || aggregates.len() != 1 {
                        return None;
                    }
                    let agg = &aggregates[0];
                    if !matches!(agg.function, AggregateFunction::Count) || agg.column != "*" {
                        return None;
                    }

                    // Under the aggregate we expect a simple Scan
                    match input.as_ref() {
                        PlanOperator::Scan { table, .. } => {
                            return Some(table.clone());
                        }
                        _ => return None,
                    }
                }
                // Any other operator shapes (Filter, Join, etc.) are not handled by this fast path
                _ => return None,
            }
        }
    }

    /// Specialized COUNT(*) implementation using hypergraph node statistics.
    /// This avoids scanning all rows and instead uses precomputed fragment row counts.
    fn execute_specialized_count_star(&self, table_name: &str) -> Result<QueryResult> {
        use std::time::Instant;
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let start = Instant::now();

        // Find table node in hypergraph
        let table_node = self.graph.get_table_node(table_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' not found in hypergraph", table_name))?;

        // Use node statistics for total row count
        let total_rows = table_node.total_rows() as i64;

        // Build a single-row Arrow batch with COUNT(*) result
        let array = Int64Array::from(vec![Some(total_rows)]);
        let field = Field::new("count", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = crate::storage::columnar::ColumnarBatch::new(vec![Arc::new(array)], schema);
        let exec_batch = crate::execution::batch::ExecutionBatch::new(batch);

        let elapsed = start.elapsed();

        Ok(QueryResult {
            batches: vec![exec_batch],
            row_count: 1,
            execution_time_ms: elapsed.as_secs_f64() * 1000.0,
        })
    }
    
    /// Load data into the engine
    pub fn load_table(&mut self, table_name: &str, columns: Vec<(String, ColumnFragment)>) -> Result<NodeId> {
        // Create table node
        let table_node_id = self.graph.next_node_id();
        let mut table_node = HyperNode::new_table(table_node_id, table_name.to_string());
        
        // Store column names in order in metadata
        let column_names: Vec<String> = columns.iter().map(|(name, _)| name.clone()).collect();
        table_node.metadata.insert("column_names".to_string(), serde_json::to_string(&column_names)?);
        
        // Create column nodes and add fragments
        for (fragment_idx, (column_name, fragment)) in columns.iter().enumerate() {
            let column_node_id = self.graph.next_node_id();
            let mut column_node = HyperNode::new_column(
                column_node_id,
                table_name.to_string(),
                column_name.clone(),
            );
            // Clone fragment for column node
            column_node.add_fragment(fragment.clone());
            self.graph.add_node(column_node);
            
            // Add fragment to table node as well
            table_node.add_fragment(fragment.clone());
            
            // Register fragment in memory manager (L1 by default)
            let fragment_size = fragment.metadata.memory_size.max(1024); // At least 1KB
            self.memory_manager.register_l1(table_node_id, fragment_size);
            
            // Build tiered index for this fragment (as memory filter)
            self.tiered_index_manager.build_index(table_node_id, fragment_idx, fragment);
        }
        
        // Add table node to graph
        self.graph.add_node(table_node);
        
        println!("✓ Loaded table '{}' into hypergraph (node_id: {:?}) with {} columns", table_name, table_node_id, columns.len());
        
        Ok(table_node_id)
    }
    
    /// Add a join relationship
    pub fn add_join(
        &mut self,
        left_table: &str,
        left_column: &str,
        right_table: &str,
        right_column: &str,
    ) -> Result<EdgeId> {
        // Find or create nodes
        let left_node = self.graph.get_node_by_table_column(left_table, left_column)
            .ok_or_else(|| anyhow::anyhow!("Left node not found: {}.{}", left_table, left_column))?;
        let right_node = self.graph.get_node_by_table_column(right_table, right_column)
            .ok_or_else(|| anyhow::anyhow!("Right node not found: {}.{}", right_table, right_column))?;
        
        // Create edge
        let edge_id = self.graph.next_edge_id();
        let edge = HyperEdge::new(
            edge_id,
            left_node.id,
            right_node.id,
            EdgeJoinType::Inner,
            EdgeJoinPredicate {
                left: (left_table.to_string(), left_column.to_string()),
                right: (right_table.to_string(), right_column.to_string()),
                operator: EdgePredicateOperator::Equals,
            },
        );
        
        self.graph.add_edge(edge);
        
        Ok(edge_id)
    }
    
    /// Find path for query tables
    fn find_path_for_query(&self, tables: &[String]) -> Result<Option<crate::hypergraph::path::HyperPath>> {
        if tables.len() < 2 {
            return Ok(None);
        }
        
        // Try to find existing path
        // For now, return None - full implementation would search path cache
        Ok(None)
    }
    
    /// Get the hypergraph (for inspection)
    pub fn graph(&self) -> HyperGraph {
        (*self.graph).clone()
    }
    
    /// Clear result cache (for benchmarking)
    pub fn clear_result_cache(&mut self) {
        self.result_cache.clear();
    }
    
    /// Clear all caches (result cache and plan cache)
    pub fn clear_all_caches(&mut self) {
        self.result_cache.clear();
        self.plan_cache.clear();
    }
    
    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            plan_cache_size: self.plan_cache.len(),
            result_cache_size: self.result_cache.len(),
        }
    }
    
    /// Get top-N hot fragments from the underlying hypergraph
    pub fn hot_fragments(&self, top_n: usize) -> Vec<((NodeId, usize), HGFragmentStats)> {
        self.graph.hot_fragments(top_n)
    }
    
    /// Get memory tier statistics
    pub fn memory_tier_stats(&self) -> crate::storage::memory_tier::TierStatistics {
        self.memory_manager.get_tier_stats()
    }
    
    /// Get hot fragments from memory manager
    pub fn memory_hot_fragments(&self, top_n: usize) -> Vec<(NodeId, crate::storage::memory_tier::FragmentAccessStats)> {
        self.memory_manager.get_hot_fragments(top_n)
    }
    
    /// Process prefetch queue (promote hot fragments to L1)
    pub fn process_memory_tiers(&self) {
        self.memory_manager.process_prefetch_queue();
    }
    
    /// Sync fragment access statistics from graph to memory manager
    /// This should be called periodically or after query execution
    pub fn sync_fragment_access(&self) {
        // Get hot fragments from graph
        let hot_fragments = self.graph.hot_fragments(1000); // Get up to 1000 fragments
        
        for ((node_id, _fragment_idx), stats) in hot_fragments {
            // Record access in memory manager
            self.memory_manager.record_access(node_id, stats.bytes);
        }
    }
    
    /// Process adaptive fragmenting and recompression
    /// Analyzes access patterns and adjusts fragment sizes/compression
    pub fn process_adaptive_fragments(&self) {
        // Get memory manager stats
        let memory_stats = self.memory_manager.get_hot_fragments(1000);
        
        // Get nodes from graph
        for (node_id, access_stats) in memory_stats {
            if let Some(node) = self.graph.get_node(node_id) {
                // Analyze each fragment in the node
                for (fragment_idx, fragment) in node.fragments.iter().enumerate() {
                    // Get access pattern
                    let pattern = crate::storage::adaptive_fragment::FragmentAccessPattern::from_stats(
                        &access_stats,
                        fragment.metadata.memory_size,
                        fragment.metadata.compression.clone(),
                    );
                    
                    // Check if fragment should be split
                    if self.adaptive_fragment_manager.should_split_fragment(&pattern) {
                        let optimal_size = self.adaptive_fragment_manager.optimal_fragment_size(&pattern);
                        // TODO: Actually split fragments (requires node mutation)
                        // For now, we just track the decision
                    }
                    
                    // Check if fragment should be recompressed
                    let data_type = fragment.get_array()
                        .map(|a| a.data_type().clone())
                        .unwrap_or(arrow::datatypes::DataType::Int64);
                    let recompression_decision = self.adaptive_fragment_manager.make_recompression_decision(
                        &pattern,
                        &data_type,
                    );
                    
                    if recompression_decision.should_recompress {
                        // TODO: Actually recompress fragments (requires node mutation)
                        // For now, we just track the decision
                    }
                }
            }
        }
    }
    
    /// Get adaptive fragment manager
    pub fn adaptive_fragment_manager(&self) -> Arc<AdaptiveFragmentManager> {
        self.adaptive_fragment_manager.clone()
    }
    
    /// Get memory manager (for advanced usage)
    pub fn memory_manager(&self) -> Arc<MultiTierMemoryManager> {
        self.memory_manager.clone()
    }
    
    /// Get tiered index manager
    pub fn tiered_index_manager(&self) -> Arc<TieredIndexManager> {
        self.tiered_index_manager.clone()
    }
    
    /// Get tiered index statistics
    pub fn tiered_index_stats(&self) -> Vec<((NodeId, usize), crate::storage::tiered_index::TieredIndexStats)> {
        self.tiered_index_manager.get_all_indexes()
    }
    
    /// Execute INSERT statement
    fn execute_insert(&mut self, insert: InsertStatement) -> Result<QueryResult> {
        use std::time::Instant;
        let start = Instant::now();
        
        // Find table node
        let table_node = self.graph.iter_nodes()
            .find(|(_, node)| node.table_name.as_ref() == Some(&insert.table_name) && matches!(node.node_type, crate::hypergraph::node::NodeType::Table))
            .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", insert.table_name))?;
        
        let (table_node_id, table_node) = table_node;
        
        // Get column order from metadata or use provided columns
        let column_names: Vec<String> = if !insert.columns.is_empty() {
            insert.columns.clone()
        } else {
            serde_json::from_str(table_node.metadata.get("column_names").ok_or_else(|| anyhow::anyhow!("Table has no column metadata"))?)?
        };
        
        // Create new fragments for each column with inserted values
        let mut new_fragments = vec![];
        for (col_idx, col_name) in column_names.iter().enumerate() {
            // Get existing fragment to determine data type
            let existing_fragment = table_node.fragments.get(col_idx)
                .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in table", col_name))?;
            
            // Extract values for this column
            let column_values: Vec<Option<crate::storage::fragment::Value>> = insert.values.iter()
                .map(|row| row.get(col_idx).cloned())
                .collect();
            
            // Convert to Arrow array
            let data_type = existing_fragment.get_array()
                .map(|a| a.data_type().clone())
                .ok_or_else(|| anyhow::anyhow!("Fragment has no array to determine data type"))?;
            let array = values_to_arrow_array(&column_values, &data_type)?;
            
            // Create new fragment
            let new_fragment = ColumnFragment {
                array: Some(Arc::from(array)),
                mmap: None,
                mmap_offset: 0,
                metadata: crate::storage::fragment::FragmentMetadata {
                    row_count: column_values.len(),
                    min_value: None,
                    max_value: None,
                    cardinality: column_values.len(),
                    compression: crate::storage::fragment::CompressionType::None,
                    memory_size: 0, // Will be calculated
                },
                bloom_filter: None,
                bitmap_index: None,
                is_sorted: false,
            };
            
            new_fragments.push(new_fragment);
        }
        
        // Append new fragments to existing ones (for now, we'll merge them)
        // TODO: Properly append to existing fragments
        let mut updated_fragments = table_node.fragments.clone();
        for (idx, new_frag) in new_fragments.iter().enumerate() {
            if let Some(existing_frag) = updated_fragments.get_mut(idx) {
                // Merge fragments by concatenating arrays
                let merged = merge_fragments(existing_frag, new_frag)?;
                *existing_frag = merged;
            } else {
                updated_fragments.push(new_frag.clone());
            }
        }
        
        // Update node with new fragments
        self.graph.update_node_fragments(table_node_id, updated_fragments);
        
        let elapsed = start.elapsed();
        Ok(QueryResult {
            batches: vec![],
            row_count: insert.values.len(),
            execution_time_ms: elapsed.as_secs_f64() * 1000.0,
        })
    }
    
    /// Execute UPDATE statement
    fn execute_update(&mut self, update: UpdateStatement) -> Result<QueryResult> {
        use std::time::Instant;
        let start = Instant::now();
        
        // Find table node
        let table_node = self.graph.iter_nodes()
            .find(|(_, node)| node.table_name.as_ref() == Some(&update.table_name) && matches!(node.node_type, crate::hypergraph::node::NodeType::Table))
            .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", update.table_name))?;
        
        let (table_node_id, _) = table_node;
        
        // TODO: Implement UPDATE by:
        // 1. Finding rows matching WHERE clause
        // 2. Updating values in fragments
        // 3. Rebuilding indexes if needed
        
        // For now, return success
        let elapsed = start.elapsed();
        Ok(QueryResult {
            batches: vec![],
            row_count: 0,
            execution_time_ms: elapsed.as_secs_f64() * 1000.0,
        })
    }
    
    /// Execute DELETE statement
    fn execute_delete(&mut self, delete: DeleteStatement) -> Result<QueryResult> {
        use std::time::Instant;
        let start = Instant::now();
        
        // Find table node
        let table_node = self.graph.iter_nodes()
            .find(|(_, node)| node.table_name.as_ref() == Some(&delete.table_name) && matches!(node.node_type, crate::hypergraph::node::NodeType::Table))
            .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", delete.table_name))?;
        
        let (table_node_id, _) = table_node;
        
        // TODO: Implement DELETE by:
        // 1. Finding rows matching WHERE clause
        // 2. Removing rows from fragments
        // 3. Rebuilding indexes if needed
        
        // For now, return success
        let elapsed = start.elapsed();
        Ok(QueryResult {
            batches: vec![],
            row_count: 0,
            execution_time_ms: elapsed.as_secs_f64() * 1000.0,
        })
    }
    
    /// Execute CREATE TABLE statement
    fn execute_create_table(&mut self, create: CreateTableStatement) -> Result<QueryResult> {
        use std::time::Instant;
        let start = Instant::now();
        
        // Create empty fragments for each column
        let mut columns = vec![];
        for col_def in &create.columns {
            // Create empty array based on data type
            let array: Arc<dyn arrow::array::Array> = match col_def.data_type {
                ArrowDataType::Int64 => Arc::new(Int64Array::from(vec![] as Vec<Option<i64>>)),
                ArrowDataType::Float64 => Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
                ArrowDataType::Utf8 => Arc::new(StringArray::from(vec![] as Vec<Option<String>>)),
                ArrowDataType::Boolean => Arc::new(BooleanArray::from(vec![] as Vec<Option<bool>>)),
                _ => anyhow::bail!("Unsupported data type: {:?}", col_def.data_type),
            };
            
            let fragment = ColumnFragment {
                array: Some(array),
                mmap: None,
                mmap_offset: 0,
                metadata: crate::storage::fragment::FragmentMetadata {
                    row_count: 0,
                    min_value: None,
                    max_value: None,
                    cardinality: 0,
                    compression: crate::storage::fragment::CompressionType::None,
                    memory_size: 0,
                },
                bloom_filter: None,
                bitmap_index: None,
                is_sorted: false,
            };
            
            columns.push((col_def.name.clone(), fragment));
        }
        
        // Use existing load_table method
        self.load_table(&create.table_name, columns)?;
        
        let elapsed = start.elapsed();
        Ok(QueryResult {
            batches: vec![],
            row_count: 0,
            execution_time_ms: elapsed.as_secs_f64() * 1000.0,
        })
    }
    
    /// Execute DROP TABLE statement
    fn execute_drop_table(&mut self, table_name: &str) -> Result<QueryResult> {
        use std::time::Instant;
        let start = Instant::now();
        
        // Find and remove all nodes for this table
        let nodes_to_remove: Vec<NodeId> = self.graph.iter_nodes()
            .filter(|(_, node)| node.table_name.as_ref() == Some(&table_name.to_string()))
            .map(|(id, _)| id)
            .collect();
        
        for node_id in nodes_to_remove {
            // Remove from graph (requires mutable access)
            // TODO: Add remove_node method to HyperGraph
            // For now, we'll mark as deleted in metadata
        }
        
        let elapsed = start.elapsed();
        Ok(QueryResult {
            batches: vec![],
            row_count: 0,
            execution_time_ms: elapsed.as_secs_f64() * 1000.0,
        })
    }
}

/// Convert values to Arrow array
fn values_to_arrow_array(values: &[Option<crate::storage::fragment::Value>], data_type: &ArrowDataType) -> Result<Box<dyn arrow::array::Array>> {
    match data_type {
        ArrowDataType::Int64 => {
            let arr: Vec<Option<i64>> = values.iter().map(|v| match v {
                Some(crate::storage::fragment::Value::Int64(i)) => Some(*i),
                Some(crate::storage::fragment::Value::Int32(i)) => Some(*i as i64),
                _ => None,
            }).collect();
            Ok(Box::new(Int64Array::from(arr)))
        }
        ArrowDataType::Float64 => {
            let arr: Vec<Option<f64>> = values.iter().map(|v| match v {
                Some(crate::storage::fragment::Value::Float64(f)) => Some(*f),
                Some(crate::storage::fragment::Value::Float32(f)) => Some(*f as f64),
                Some(crate::storage::fragment::Value::Int64(i)) => Some(*i as f64),
                _ => None,
            }).collect();
            Ok(Box::new(Float64Array::from(arr)))
        }
        ArrowDataType::Utf8 => {
            let arr: Vec<Option<String>> = values.iter().map(|v| match v {
                Some(crate::storage::fragment::Value::String(s)) => Some(s.clone()),
                _ => None,
            }).collect();
            Ok(Box::new(StringArray::from(arr)))
        }
        ArrowDataType::Boolean => {
            let arr: Vec<Option<bool>> = values.iter().map(|v| match v {
                Some(crate::storage::fragment::Value::Bool(b)) => Some(*b),
                _ => None,
            }).collect();
            Ok(Box::new(BooleanArray::from(arr)))
        }
        _ => anyhow::bail!("Unsupported data type: {:?}", data_type),
    }
}

/// Merge two fragments by concatenating arrays
fn merge_fragments(frag1: &ColumnFragment, frag2: &ColumnFragment) -> Result<ColumnFragment> {
    use arrow::compute::concat;
    
    let arr1 = frag1.get_array().ok_or_else(|| anyhow::anyhow!("Fragment 1 has no array"))?;
    let arr2 = frag2.get_array().ok_or_else(|| anyhow::anyhow!("Fragment 2 has no array"))?;
    let merged_array = concat(&[arr1.as_ref(), arr2.as_ref()])?;
    
    Ok(ColumnFragment {
        array: Some(Arc::new(merged_array)),
        mmap: None, // Merged fragments don't use mmap
        mmap_offset: 0,
        metadata: crate::storage::fragment::FragmentMetadata {
            row_count: frag1.metadata.row_count + frag2.metadata.row_count,
            min_value: None, // Recalculate
            max_value: None, // Recalculate
            cardinality: frag1.metadata.cardinality + frag2.metadata.cardinality,
            compression: frag1.metadata.compression.clone(),
            memory_size: frag1.metadata.memory_size + frag2.metadata.memory_size,
        },
        bloom_filter: None,
        bitmap_index: None,
        is_sorted: false,
    })
}

/// Cache statistics
pub struct CacheStats {
    pub plan_cache_size: usize,
    pub result_cache_size: usize,
}

impl Default for HypergraphSQLEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Query result
pub struct QueryResult {
    pub batches: Vec<crate::execution::batch::ExecutionBatch>,
    pub row_count: usize,
    pub execution_time_ms: f64,
}

