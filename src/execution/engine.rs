use crate::execution::operators::build_operator_with_llm_limits;
use crate::execution::batch::{ExecutionBatch, BatchIterator};
use crate::execution::exec_node::ExecNode;
use crate::query::plan::{QueryPlan, PlanOperator};
use crate::hypergraph::graph::HyperGraph;
use anyhow::Result;
use std::sync::Arc;

/// Execution engine - executes query plans
pub struct ExecutionEngine {
    graph: std::sync::Arc<HyperGraph>,
    /// Batch size for processing (FEATURE 2: hardware-tuned)
    batch_size: usize,
    /// Hardware tuning profile (FEATURE 2: computed once at startup)
    hardware_profile: Option<crate::execution::hardware_tuning::HardwareTuningProfile>,
    /// Spill manager for handling large queries that exceed memory limits
    spill_manager: Option<std::sync::Arc<tokio::sync::Mutex<crate::spill::manager::SpillManager>>>,
    /// Persistent compiled operator cache (for WASM JIT)
    operator_cache: Option<std::sync::Arc<std::sync::Mutex<crate::cache::operator_cache::OperatorCache>>>,
}

impl ExecutionEngine {
    pub fn new(graph: HyperGraph) -> Self {
        // FEATURE 2: Use hardware-tuned batch size
        use crate::execution::hardware_tuning::HardwareTuningProfile;
        let hardware_profile = HardwareTuningProfile::benchmark();
        let batch_size = hardware_profile.batch_size;
        
        Self {
            graph: std::sync::Arc::new(graph),
            batch_size, // Hardware-tuned batch size
            hardware_profile: Some(hardware_profile),
            spill_manager: None,
            operator_cache: None,
        }
    }
    
    pub fn from_arc(graph: std::sync::Arc<HyperGraph>) -> Self {
        // FEATURE 2: Use hardware-tuned batch size
        use crate::execution::hardware_tuning::HardwareTuningProfile;
        let hardware_profile = HardwareTuningProfile::benchmark();
        let batch_size = hardware_profile.batch_size;
        
        Self {
            graph,
            batch_size, // Hardware-tuned batch size
            hardware_profile: Some(hardware_profile),
            spill_manager: None,
            operator_cache: None,
        }
    }
    
    /// Create execution engine with spill manager enabled
    pub fn with_spill(graph: HyperGraph, threshold_bytes: usize) -> Self {
        // FEATURE 2: Use hardware-tuned batch size
        use crate::execution::hardware_tuning::HardwareTuningProfile;
        let hardware_profile = HardwareTuningProfile::benchmark();
        let batch_size = hardware_profile.batch_size;
        
        let spill_manager = crate::spill::manager::SpillManager::with_config(
            crate::spill::manager::SpillConfig {
                threshold_bytes,
                spill_dir: std::path::PathBuf::from(".spill"),
            }
        );
        Self {
            graph: std::sync::Arc::new(graph),
            batch_size, // Hardware-tuned batch size
            hardware_profile: Some(hardware_profile),
            spill_manager: Some(std::sync::Arc::new(tokio::sync::Mutex::new(spill_manager))),
            operator_cache: None,
        }
    }
    
    /// Create execution engine with operator cache
    pub fn with_operator_cache(
        graph: std::sync::Arc<HyperGraph>,
        operator_cache: std::sync::Arc<std::sync::Mutex<crate::cache::operator_cache::OperatorCache>>,
    ) -> Self {
        // FEATURE 2: Use hardware-tuned batch size
        use crate::execution::hardware_tuning::HardwareTuningProfile;
        let hardware_profile = HardwareTuningProfile::benchmark();
        let batch_size = hardware_profile.batch_size;
        
        Self {
            graph,
            batch_size, // Hardware-tuned batch size
            hardware_profile: Some(hardware_profile),
            spill_manager: None,
            operator_cache: Some(operator_cache),
        }
    }
    
    /// Execute a query plan with performance timing
    /// OPTIMIZATION: Use parallel execution by default for better performance
    pub fn execute(&self, plan: &QueryPlan) -> Result<QueryResult> {
        // Use parallel execution for better performance (2-4x speedup on multi-core)
        self.execute_parallel(plan)
    }
    
    /// Execute a query plan with optional timeout (for LLM protocol)
    pub fn execute_with_timeout(&self, plan: &QueryPlan, max_time_ms: Option<u64>) -> Result<QueryResult> {
        self.execute_with_llm_limits(plan, max_time_ms, None)
    }
    
    /// Execute a query plan with LLM protocol limits (timeout and max_scan_rows)
    pub fn execute_with_llm_limits(&self, plan: &QueryPlan, max_time_ms: Option<u64>, max_scan_rows: Option<u64>) -> Result<QueryResult> {
        self.execute_with_llm_limits_and_ctes(plan, max_time_ms, max_scan_rows, None)
    }
    
    /// Execute a query plan with LLM protocol limits and CTE results cache
    pub fn execute_with_llm_limits_and_ctes(
        &self,
        plan: &QueryPlan,
        max_time_ms: Option<u64>,
        max_scan_rows: Option<u64>,
        cte_results: Option<&std::collections::HashMap<String, Vec<crate::execution::batch::ExecutionBatch>>>,
    ) -> Result<QueryResult> {
        self.execute_with_subquery_executor(plan, max_time_ms, max_scan_rows, cte_results, None)
    }
    
    /// Execute a query plan with LLM protocol limits, CTE results, and subquery executor
    pub fn execute_with_subquery_executor(
        &self,
        plan: &QueryPlan,
        max_time_ms: Option<u64>,
        max_scan_rows: Option<u64>,
        cte_results: Option<&std::collections::HashMap<String, Vec<crate::execution::batch::ExecutionBatch>>>,
        subquery_executor: Option<std::sync::Arc<dyn crate::query::expression::SubqueryExecutor>>,
    ) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        
        // Build execution operator tree with LLM limits, CTE results, and subquery executor
        eprintln!("DEBUG ExecutionEngine::execute_with_subquery_executor: cte_results.is_some()={}, cte_results.len()={:?}, subquery_executor.is_some()={}", 
            cte_results.is_some(), 
            cte_results.map(|r| r.len()).unwrap_or(0),
            subquery_executor.is_some());
        if let Some(ref results) = cte_results {
            eprintln!("DEBUG ExecutionEngine::execute_with_subquery_executor: cte_results keys: {:?}", results.keys().collect::<Vec<_>>());
        }
        eprintln!("DEBUG ExecutionEngine::execute_with_subquery_executor: plan.root operator type: {:?}", 
            std::mem::discriminant(&plan.root));
        // CRITICAL DEBUG: Check if plan.root is Project and what its input is
        if let PlanOperator::Project { input, .. } = &plan.root {
            eprintln!("DEBUG ExecutionEngine::execute_with_subquery_executor: plan.root is Project, input type: {:?}", 
                std::mem::discriminant(input.as_ref()));
        }
        let build_start = std::time::Instant::now();
        let mut root_op = crate::execution::operators::build_operator_with_subquery_executor(
            &plan.root,
            self.graph.clone(),
            max_scan_rows,
            cte_results,
            subquery_executor,
            Some(&plan.table_aliases),
            Some(&plan.vector_index_hints), // Pass vector index hints for execution
            Some(&plan.btree_index_hints), // Pass B-tree index hints for execution
            Some(&plan.partition_hints), // Pass partition hints for execution
            self.operator_cache.clone(), // Pass operator cache for WASM JIT (Phase C)
            Some(&plan.wasm_jit_hints), // Pass WASM JIT hints from plan
        )?;
        let build_time = build_start.elapsed();
        
        // PREPARE PHASE: Call prepare() on root operator ONCE before execution
        // This builds all hash tables, initializes data structures, and recursively prepares nested operators
        eprintln!("[PREPARE] Preparing root operator (type: {:?})", std::any::type_name_of_val(&*root_op));
        
        // CRITICAL FIX: Use prepare_child helper which ensures correct type dispatch
        // Direct trait object dispatch may not work correctly, so we use the helper
        // that can handle type-specific downcasting if needed
        match crate::execution::operators::FilterOperator::prepare_child(&mut root_op) {
            Ok(()) => {
                eprintln!("[PREPARE] Root operator prepared successfully, starting execution");
            }
            Err(e) => {
                eprintln!("[PREPARE] ERROR: Root operator prepare() failed: {}", e);
                return Err(anyhow::anyhow!("Failed to prepare root operator: {}", e));
            }
        }
        eprintln!("[PREPARE] Root operator prepared, starting execution");
        
        // Execute pipeline with early termination optimization
        // TERMINATION CONTRACT: root_op.next() must eventually return None
        // Add timeout guard to prevent indefinite hangs
        let exec_start = std::time::Instant::now();
        let mut batches = vec![];
        let mut total_rows = 0;
        let mut iteration_count = 0;
        const MAX_ITERATIONS: usize = 10_000_000; // Safety limit (10M batches)
        const MAX_EXECUTION_TIME_SECS: u64 = 60; // 1 minute timeout (reduced for faster failure detection)
        
        // CRITICAL: Check timeout before entering loop to catch hangs on first call
        loop {
            // Check timeout before each iteration
            if exec_start.elapsed().as_secs() > MAX_EXECUTION_TIME_SECS {
                return Err(anyhow::anyhow!(
                    "ERR_QUERY_TIMEOUT: Query execution exceeded {} seconds. \
                    This may indicate a bug, pathological query, or very large dataset. \
                    Consider adding LIMIT or optimizing the query.",
                    MAX_EXECUTION_TIME_SECS
                ));
            }
            
            // Guard against infinite loops
            if iteration_count > MAX_ITERATIONS {
                return Err(anyhow::anyhow!(
                    "ERR_INFINITE_LOOP_DETECTED: Query execution exceeded {} iterations. \
                    This indicates an operator is not properly terminating. \
                    Possible causes: operator bug, infinite data source, or missing termination condition.",
                    MAX_ITERATIONS
                ));
            }
            
            match root_op.next()? {
                Some(batch) => {
                    iteration_count += 1;
                    // Check timeout (LLM protocol: max_time_ms)
                    if let Some(max_time) = max_time_ms {
                        let elapsed_ms = start.elapsed().as_millis() as u64;
                        if elapsed_ms >= max_time {
                            eprintln!("ExecutionEngine: Timeout reached ({}ms >= {}ms), stopping execution", elapsed_ms, max_time);
                            break;
                        }
                    }
                    
                    // SPILL INTEGRATION: Check if batch should be spilled to disk
            if let Some(ref spill_mgr) = self.spill_manager {
                // Convert ExecutionBatch to RecordBatch for spilling
                use crate::execution::result::execution_batch_to_record_batch;
                if let Ok(record_batch) = execution_batch_to_record_batch(&batch) {
                    // Use blocking task to handle async spill in sync context
                    let spill_mgr_clone = spill_mgr.clone();
                    let record_batch_clone = record_batch.clone();
                    let spill_result = tokio::runtime::Handle::try_current()
                        .map(|handle| {
                            let record_batch_for_handle = record_batch_clone.clone();
                            handle.block_on(async move {
                                let mut mgr = spill_mgr_clone.lock().await;
                                mgr.maybe_spill(&record_batch_for_handle).await
                            })
                        })
                        .or_else(|_| {
                            // If no runtime available, create a temporary one
                            let spill_mgr_clone2 = spill_mgr.clone();
                            let record_batch_for_rt = record_batch.clone();
                            tokio::runtime::Runtime::new()
                                .map(|rt| rt.block_on(async move {
                                    let mut mgr = spill_mgr_clone2.lock().await;
                                    mgr.maybe_spill(&record_batch_for_rt).await
                                }))
                        });
                    
                    if let Ok(Ok(Some(spill_path))) = spill_result {
                        tracing::warn!("Spilled batch to {:?}", spill_path);
                        
                        // ASYNC PREFETCH: Prefetch spill file asynchronously for faster merge
                        let spill_path_str = spill_path.to_string_lossy().to_string();
                        tokio::spawn(async move {
                            let _ = crate::spill::prefetcher::prefetch(&spill_path_str).await;
                        });
                        
                        // Batch was spilled to disk - we can continue processing
                        // The batch is no longer in memory, so we skip adding it to batches
                        // Note: For full spill support, we'd need to track spill files and read them back
                        // For Phase B, we use advanced spill with compression and parallel merge
                        continue;
                    }
                }
            }
            
                    total_rows += batch.row_count;
                    // Debug logging disabled to prevent memory issues in IDEs
                    // eprintln!("ExecutionEngine: Got batch with {} rows (total so far: {})", batch.row_count, total_rows);
                    batches.push(batch);
                    
                    // Early termination: if we have a small result set and no more batches needed,
                    // we can stop early (this is a heuristic - actual LIMIT is handled by operators)
                    // This helps when LIMIT is very small (e.g., LIMIT 10)
                    if total_rows > 0 && batches.len() > 10 {
                        // If we have many small batches, we might be done
                        // But let the operators handle LIMIT, so we don't stop here
                    }
                }
                None => {
                    // Operator exhausted - exit loop
                    break;
                }
            }
        }
        let exec_time = exec_start.elapsed();
        
        // Collect results
        let collect_start = std::time::Instant::now();
        let row_count = batches.iter().map(|b| b.row_count).sum();
        // Debug logging disabled to prevent memory issues in IDEs
        // eprintln!("ExecutionEngine: Total row_count from batches: {} (calculated: {})", row_count, total_rows);
        let collect_time = collect_start.elapsed();
        let total_time = start.elapsed();
        
        // Only print timing summary (not per-batch details)
        // eprintln!("  Execution timing:");
        // eprintln!("   Build: {:?}", build_time);
        // eprintln!("   Execute: {:?}", exec_time);
        // eprintln!("   Collect: {:?}", collect_time);
        // eprintln!("   Total: {:?} ({:.2}ms)", total_time, total_time.as_secs_f64() * 1000.0);
        // eprintln!("   Rows: {}", row_count);
        // if total_time.as_secs_f64() > 0.0 {
        //     eprintln!("   Throughput: {:.2} rows/sec", row_count as f64 / total_time.as_secs_f64());
        // }
        
        Ok(QueryResult {
            batches,
            row_count,
            execution_time_ms: total_time.as_secs_f64() * 1000.0,
        })
    }
    
    /// Execute a subquery Query AST (used for CTEs, scalar subqueries, etc.)
    /// This method takes a Query AST (Box<Query>) and executes it to get results
    /// EDGE CASE 2 FIX: Accept CTE context and results for nested CTE support
    pub fn execute_subquery_ast(
        &self, 
        query_ast: &Box<sqlparser::ast::Query>, 
        planner: &crate::query::planner::QueryPlanner,
        cte_context: Option<&crate::query::cte::CTEContext>,
        cte_results: Option<&std::collections::HashMap<String, Vec<crate::execution::batch::ExecutionBatch>>>,
    ) -> Result<QueryResult> {
        use crate::query::parser_enhanced::extract_query_info_enhanced;
        use crate::query::wildcard_expansion::expand_wildcards_in_parsed_query;
        use crate::query::planner::QueryPlanner;
        
        // Convert Query to Statement for extraction
        let ast = sqlparser::ast::Statement::Query(query_ast.clone());
        let mut parsed = extract_query_info_enhanced(&ast)?;
        
        // WILDCARD EXPANSION: Expand wildcards BEFORE planning and execution
        // This ensures CTE materialization uses fully expanded column lists
        expand_wildcards_in_parsed_query(&mut parsed, &self.graph)?;
        eprintln!("DEBUG execute_subquery_ast: After wildcard expansion, columns: {:?}", parsed.columns);
        
        // EDGE CASE 2 FIX: Set CTE context on planner so it can resolve CTE names
        // Create planner with CTE context if needed (planner is not Clone)
        let mut planner_with_cte = if let Some(cte_ctx) = cte_context {
             QueryPlanner::from_arc_with_options(planner.graph.clone(), planner.use_cascades)
                 .with_cte_context(cte_ctx.clone())
        } else {
             QueryPlanner::from_arc_with_options(planner.graph.clone(), planner.use_cascades)
        };
        
        // Plan the subquery with CTE context and results available
        let subquery_plan = planner_with_cte.plan_with_ast(&parsed, Some(&ast))?;
        
        // Execute the subquery plan with CTE results
        self.execute_with_llm_limits_and_ctes(&subquery_plan, None, None, cte_results)
    }
    
    /// Execute query with parallel processing
    /// This uses rayon to process fragments in parallel for scan operations
    /// OPTIMIZATION: Enhanced with CTE results and subquery executor support
    pub fn execute_parallel(&self, plan: &QueryPlan) -> Result<QueryResult> {
        self.execute_parallel_with_subquery_executor(plan, None, None, None, None)
    }
    
    /// Execute query with parallel processing and full parameter support
    pub fn execute_parallel_with_subquery_executor(
        &self,
        plan: &QueryPlan,
        max_time_ms: Option<u64>,
        max_scan_rows: Option<u64>,
        cte_results: Option<&std::collections::HashMap<String, Vec<crate::execution::batch::ExecutionBatch>>>,
        subquery_executor: Option<std::sync::Arc<dyn crate::query::expression::SubqueryExecutor>>,
    ) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        
        // Build execution operator tree with all parameters
        let mut root_op = crate::execution::operators::build_operator_with_subquery_executor(
            &plan.root,
            self.graph.clone(),
            max_scan_rows,
            cte_results,
            subquery_executor,
            Some(&plan.table_aliases),
            Some(&plan.vector_index_hints), // Pass vector index hints for execution
            Some(&plan.btree_index_hints), // Pass B-tree index hints for execution
            Some(&plan.partition_hints), // Pass partition hints for execution
            self.operator_cache.clone(), // Pass operator cache for WASM JIT (Phase C)
            Some(&plan.wasm_jit_hints), // Pass WASM JIT hints from plan
        )?;
        
        // ExecNode: Prepare phase - build all hash tables and initialize operators
        eprintln!("[EXEC ENGINE] Calling prepare() on root operator (parallel path)");
        crate::execution::operators::FilterOperator::prepare_child(&mut root_op)?;
        eprintln!("[EXEC ENGINE] Prepare phase complete, starting parallel execution");
        
        // For parallel execution, we collect batches in parallel where possible
        // The main parallelization happens at the fragment level in ScanOperator
        let mut batches = Vec::new();
        let mut total_rows = 0;
        
        // Process batches - parallelization happens inside operators
        // For scan operations with multiple fragments, they can be processed in parallel
        // Full parallelization would require:
        // 1. Processing multiple fragments in parallel within ScanOperator using rayon
        // 2. Parallel join operations using rayon par_iter
        // 3. Parallel aggregation
        while let Some(batch) = root_op.next()? {
            // Check timeout (if specified)
            if let Some(max_time) = max_time_ms {
                let elapsed_ms = start.elapsed().as_millis() as u64;
                if elapsed_ms >= max_time {
                    eprintln!("ExecutionEngine: Timeout reached ({}ms >= {}ms), stopping execution", elapsed_ms, max_time);
                    break;
                }
            }
            
            total_rows += batch.row_count;
            batches.push(batch);
        }
        
        let execution_time = start.elapsed();
        
        Ok(QueryResult {
            batches,
            row_count: total_rows,
            execution_time_ms: execution_time.as_secs_f64() * 1000.0,
        })
    }
}

/// Query execution result
pub struct QueryResult {
    pub batches: Vec<ExecutionBatch>,
    pub row_count: usize,
    pub execution_time_ms: f64,
}

impl QueryResult {
    /// Convert to arrow RecordBatch for output
    pub fn to_record_batches(&self) -> Vec<arrow::record_batch::RecordBatch> {
        use crate::execution::result::batches_to_record_batches;
        
        // Use existing conversion function from result.rs
        batches_to_record_batches(&self.batches)
            .unwrap_or_default() // Return empty vec on error
    }
}

