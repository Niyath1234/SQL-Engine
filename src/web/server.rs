use crate::engine::{HypergraphSQLEngine, LlmQueryMode, LlmQueryRequest, LlmQueryResponse, QueryClass};
use std::sync::Arc;
use tokio::sync::RwLock;
use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, Json},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;

/// Shared application state
pub type AppState = Arc<RwLock<HypergraphSQLEngine>>;

/// Start the web server
pub async fn start_server(engine: HypergraphSQLEngine, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let state: AppState = Arc::new(RwLock::new(engine));
    
    let app = Router::new()
        .route("/", get(index))
        .route("/api/execute", post(execute_query))
        .route("/api/llm/execute", post(execute_llm_query))
        .route("/api/tables", get(list_tables))
        .route("/api/table/:name", get(get_table_info))
        .route("/api/stats", get(get_stats))
        .route("/api/load_csv", post(load_csv))
        .route("/api/clear_cache", post(clear_cache))
        .layer(CorsLayer::permissive())
        .with_state(state);
    
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    println!(" Hypergraph SQL Engine Admin UI running on http://localhost:{}", port);
    println!(" Open your browser to start querying!");
    
    axum::serve(listener, app).await?;
    Ok(())
}

/// Serve the main HTML page
async fn index() -> Html<&'static str> {
    Html(include_str!("../../static/index.html"))
}

/// Execute a SQL query
#[derive(Deserialize)]
struct QueryRequest {
    sql: String,
}

#[derive(Serialize)]
struct QueryResponse {
    success: bool,
    rows: Vec<Vec<String>>,
    columns: Vec<String>,
    row_count: usize,
    execution_time_ms: f64,
    error: Option<String>,
}

/// LLM-focused query request - richer protocol than the simple SQL-only endpoint.
#[derive(Deserialize)]
struct LlmQueryApiRequest {
    /// Optional session identifier (conversation id)
    session_id: Option<String>,
    /// SQL text to execute
    sql: String,
    /// Mode: "exact" or "approx" (default: exact)
    mode: Option<String>,
    /// Optional soft limit on rows scanned
    max_scan_rows: Option<u64>,
    /// Optional soft limit on execution time (ms)
    max_time_ms: Option<u64>,
    /// Result format: "full", "summary", "metadata", "sample:N", "representative:N" (High Priority #8)
    result_format: Option<String>,
}

#[derive(Serialize)]
struct LlmQueryApiResponse {
    success: bool,
    rows: Vec<Vec<String>>,
    columns: Vec<String>,
    row_count: usize,
    execution_time_ms: f64,
    /// High-level class of query (ScanLimit, FilterLimit, GroupBy, etc.)
    query_class: String,
    /// Tables referenced by the query
    tables: Vec<String>,
    /// Columns referenced by the query
    columns_used: Vec<String>,
    /// Whether approximate mode was requested
    approx_mode: bool,
    /// Whether execution was truncated due to resource limits (future)
    truncated: bool,
    error: Option<String>,
}

async fn execute_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, StatusCode> {
    let start = std::time::Instant::now();
    
    let mut engine = state.write().await;
    let result = engine.execute_query(&req.sql);
    
    match result {
        Ok(query_result) => {
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            
            // Convert batches to rows
            let mut rows = Vec::new();
            let mut columns = Vec::new();
            
            if !query_result.batches.is_empty() {
                let first_batch = &query_result.batches[0];
                let schema = &first_batch.batch.schema;
                
                // Extract column names
                for field in schema.fields() {
                    columns.push(field.name().clone());
                }
                
                // Extract rows
                for batch in &query_result.batches {
                    let row_count = batch.row_count;
                    let col_count = columns.len();
                    
                    // Safety check: skip if batch has no rows
                    if row_count == 0 || col_count == 0 {
                        continue;
                    }
                    
                    for row_idx in 0..row_count {
                        // Safety check: ensure row_idx is valid
                        if row_idx >= batch.selection.len() || !batch.selection[row_idx] {
                            continue;
                        }
                        
                        let mut row = Vec::new();
                        for col_idx in 0..col_count {
                            let col = batch.batch.column(col_idx);
                            if let Some(array) = col {
                                let value = format_value(array, row_idx);
                                row.push(value);
                            } else {
                                row.push("NULL".to_string());
                            }
                        }
                        rows.push(row);
                    }
                }
            }
            
            Ok(Json(QueryResponse {
                success: true,
                rows,
                columns,
                row_count: query_result.row_count,
                execution_time_ms: elapsed,
                error: None,
            }))
        }
        Err(e) => {
            Ok(Json(QueryResponse {
                success: false,
                rows: vec![],
                columns: vec![],
                row_count: 0,
                execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
                error: Some(e.to_string()),
            }))
        }
    }
}

/// Execute a SQL query using the LLM-aware protocol
async fn execute_llm_query(
    State(state): State<AppState>,
    Json(req): Json<LlmQueryApiRequest>,
) -> Result<Json<LlmQueryApiResponse>, StatusCode> {
    let start = std::time::Instant::now();

    // Map string mode to enum
    let mode = match req.mode.as_deref() {
        Some("approx") | Some("APPROX") => LlmQueryMode::Approx,
        _ => LlmQueryMode::Exact,
    };

    // High Priority #8: Extract result_format from request (default to Full if not specified)
    let result_format = if let Some(ref format_str) = req.result_format {
        match format_str.as_str() {
            "summary" => crate::result_format::ResultFormat::Summary,
            "metadata" => crate::result_format::ResultFormat::Metadata,
            _ => crate::result_format::ResultFormat::Full,
        }
    } else {
        crate::result_format::ResultFormat::Full
    };
    
    let llm_req = LlmQueryRequest {
        session_id: req.session_id.clone(),
        sql: req.sql.clone(),
        mode,
        max_scan_rows: req.max_scan_rows,
        max_time_ms: req.max_time_ms,
        result_format,
    };

    let mut engine = state.write().await;
    let result = engine.execute_llm_query(llm_req);

    match result {
        Ok(LlmQueryResponse {
            result,
            formatted_result,
            query_class,
            tables,
            columns,
            approx_mode,
            truncated,
        }) => {
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;

            // Convert batches to rows (reuse logic from execute_query)
            let mut rows = Vec::new();
            let mut column_names = Vec::new();

            if !result.batches.is_empty() {
                let first_batch = &result.batches[0];
                let schema = &first_batch.batch.schema;

                for field in schema.fields() {
                    column_names.push(field.name().clone());
                }

                for batch in &result.batches {
                    let row_count = batch.row_count;
                    let col_count = column_names.len();

                    // Safety check: skip if batch has no rows
                    if row_count == 0 || col_count == 0 {
                        continue;
                    }

                    for row_idx in 0..row_count {
                        // Safety check: ensure row_idx is valid
                        if row_idx >= batch.selection.len() || !batch.selection[row_idx] {
                            continue;
                        }

                        let mut row = Vec::new();
                        for col_idx in 0..col_count {
                            let col = batch.batch.column(col_idx);
                            if let Some(array) = col {
                                let value = format_value(array, row_idx);
                                row.push(value);
                            } else {
                                row.push("NULL".to_string());
                            }
                        }
                        rows.push(row);
                    }
                }
            }

            Ok(Json(LlmQueryApiResponse {
                success: true,
                rows,
                columns: column_names,
                row_count: result.row_count,
                execution_time_ms: elapsed,
                query_class: match query_class {
                    QueryClass::ScanLimit => "ScanLimit".to_string(),
                    QueryClass::FilterLimit => "FilterLimit".to_string(),
                    QueryClass::GroupBy => "GroupBy".to_string(),
                    QueryClass::MetricLookup => "MetricLookup".to_string(),
                    QueryClass::JoinQuery => "JoinQuery".to_string(),
                    QueryClass::Other => "Other".to_string(),
                },
                tables,
                columns_used: columns,
                approx_mode,
                truncated,
                error: None,
            }))
        }
        Err(e) => Ok(Json(LlmQueryApiResponse {
            success: false,
            rows: vec![],
            columns: vec![],
            row_count: 0,
            execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
            query_class: "Other".to_string(),
            tables: vec![],
            columns_used: vec![],
            approx_mode: false,
            truncated: false,
            error: Some(e.to_string()),
        })),
    }
}

/// Format a value from an Arrow array
fn format_value(array: &arrow::array::ArrayRef, idx: usize) -> String {
    use arrow::array::*;
    
    // Safety check: return NULL if array is empty or index is out of bounds
    if array.len() == 0 || idx >= array.len() {
        return "NULL".to_string();
    }
    
    match array.data_type() {
        arrow::datatypes::DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            if arr.is_null(idx) {
                "NULL".to_string()
            } else {
                arr.value(idx).to_string()
            }
        }
        arrow::datatypes::DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            if arr.is_null(idx) {
                "NULL".to_string()
            } else {
                format!("{:.2}", arr.value(idx))
            }
        }
        arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            if arr.is_null(idx) {
                "NULL".to_string()
            } else {
                arr.value(idx).to_string()
            }
        }
        arrow::datatypes::DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            if arr.is_null(idx) {
                "NULL".to_string()
            } else {
                arr.value(idx).to_string()
            }
        }
        _ => format!("{:?}", array),
    }
}

/// List all tables
#[derive(Serialize)]
struct TablesResponse {
    tables: Vec<String>,
}

async fn list_tables(State(state): State<AppState>) -> Result<Json<TablesResponse>, StatusCode> {
    let engine = state.read().await;
    let graph = engine.graph();
    
    // Get all table nodes
    let mut tables = Vec::new();
    for (_, node) in graph.iter_nodes() {
        if matches!(node.node_type, crate::hypergraph::node::NodeType::Table) {
            if let Some(table_name) = &node.table_name {
                tables.push(table_name.clone());
            }
        }
    }
    
    tables.sort();
    Ok(Json(TablesResponse { tables }))
}

/// Get table information
#[derive(Serialize)]
struct TableInfoResponse {
    name: String,
    columns: Vec<ColumnInfo>,
    row_count: usize,
}

#[derive(Serialize)]
struct ColumnInfo {
    name: String,
    data_type: String,
}

async fn get_table_info(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Result<Json<TableInfoResponse>, StatusCode> {
    let engine = state.read().await;
    let graph = engine.graph();
    
    // Get table node
    let table_node = graph.get_table_node(&name)
        .ok_or_else(|| StatusCode::NOT_FOUND)?;
    
    // Get column nodes
    let mut columns = Vec::new();
    for col_node in graph.get_column_nodes(&name) {
        if let Some(col_name) = &col_node.column_name {
            // Get data type from first fragment
            let data_type = if let Some(fragment) = col_node.fragments.first() {
                if let Some(array) = fragment.get_array() {
                    format!("{:?}", array.data_type())
                } else {
                    "Unknown".to_string()
                }
            } else {
                "Unknown".to_string()
            };
            
            columns.push(ColumnInfo {
                name: col_name.clone(),
                data_type,
            });
        }
    }
    
    columns.sort_by_key(|c| c.name.clone());
    
    Ok(Json(TableInfoResponse {
        name: name.clone(),
        columns,
        row_count: table_node.total_rows(),
    }))
}

/// Get engine statistics
#[derive(Serialize)]
struct StatsResponse {
    cache_stats: CacheStats,
    memory_tier_stats: String,
}

#[derive(Serialize)]
struct CacheStats {
    plan_cache_size: usize,
    result_cache_size: usize,
}

async fn get_stats(State(state): State<AppState>) -> Result<Json<StatsResponse>, StatusCode> {
    let engine = state.read().await;
    let cache_stats = engine.cache_stats();
    let memory_stats = engine.memory_tier_stats();
    
    Ok(Json(StatsResponse {
        cache_stats: CacheStats {
            plan_cache_size: cache_stats.plan_cache_size,
            result_cache_size: cache_stats.result_cache_size,
        },
        memory_tier_stats: format!("{:?}", memory_stats),
    }))
}

/// Load CSV file into the engine
#[derive(Deserialize)]
struct LoadCsvRequest {
    csv_path: String,
    /// Optional table name (defaults to CSV filename without extension)
    #[serde(default)]
    table_name: Option<String>,
}

#[derive(Serialize)]
struct LoadCsvResponse {
    success: bool,
    message: String,
    rows_loaded: Option<usize>,
    error: Option<String>,
}

async fn load_csv(
    State(state): State<AppState>,
    Json(req): Json<LoadCsvRequest>,
) -> Result<Json<LoadCsvResponse>, StatusCode> {
    let mut engine = state.write().await;
    
    // Auto-generate table name from CSV filename if not provided
    let table_name = req.table_name.unwrap_or_else(|| {
        std::path::Path::new(&req.csv_path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("csv_table")
            .to_string()
            .replace("-", "_")
            .replace(" ", "_")
            .to_lowercase()
    });
    
    // Load CSV using the same logic as compare_engines
    match load_csv_file(&req.csv_path) {
        Ok((row_count, _column_names, column_fragments, _rows)) => {
            match engine.load_table(&table_name, column_fragments) {
                Ok(_) => {
                    Ok(Json(LoadCsvResponse {
                        success: true,
                        message: format!("Successfully loaded {} rows into table '{}'", row_count, table_name),
                        rows_loaded: Some(row_count),
                        error: None,
                    }))
                }
                Err(e) => {
                    Ok(Json(LoadCsvResponse {
                        success: false,
                        message: format!("Failed to load table: {}", e),
                        rows_loaded: None,
                        error: Some(e.to_string()),
                    }))
                }
            }
        }
        Err(e) => {
            Ok(Json(LoadCsvResponse {
                success: false,
                message: format!("Failed to read CSV file: {}", e),
                rows_loaded: None,
                error: Some(e.to_string()),
            }))
        }
    }
}

/// Load CSV file and create column fragments
fn load_csv_file(path: &str) -> Result<(usize, Vec<String>, Vec<(String, crate::storage::fragment::ColumnFragment)>, Vec<csv::StringRecord>), Box<dyn std::error::Error>> {
    use arrow::array::*;
    use std::sync::Arc;
    use crate::storage::fragment::{ColumnFragment, FragmentMetadata, CompressionType};
    
    let mut reader = csv::Reader::from_path(path)?;
    let headers = reader.headers()?.clone();
    let column_names: Vec<String> = headers.iter().map(|s| s.to_string()).collect();
    
    let mut rows = Vec::new();
    for result in reader.records() {
        let record = result?;
        rows.push(record);
    }
    
    // Create arrays for each column
    let mut column_arrays: Vec<Arc<dyn arrow::array::Array>> = vec![];
    
    for (col_idx, _col_name) in column_names.iter().enumerate() {
        let mut int_values = Vec::new();
        let mut float_values = Vec::new();
        let mut string_values = Vec::new();
        let mut is_int = true;
        let mut is_float = true;
        
        for row in rows.iter() {
            if let Some(field) = row.get(col_idx) {
                if field.is_empty() {
                    if is_int {
                        int_values.push(None);
                    } else if is_float {
                        float_values.push(None);
                    } else {
                        string_values.push(None);
                    }
                    continue;
                }
                if is_int {
                    match field.parse::<i64>() {
                        Ok(v) => int_values.push(Some(v)),
                        Err(_) => {
                            is_int = false;
                            if is_float {
                                match field.parse::<f64>() {
                                    Ok(v) => float_values.push(Some(v)),
                                    Err(_) => {
                                        is_float = false;
                                        string_values.push(Some(field.to_string()));
                                    }
                                }
                            } else {
                                string_values.push(Some(field.to_string()));
                            }
                        }
                    }
                } else if is_float {
                    match field.parse::<f64>() {
                        Ok(v) => float_values.push(Some(v)),
                        Err(_) => {
                            is_float = false;
                            string_values.push(Some(field.to_string()));
                        }
                    }
                } else {
                    string_values.push(Some(field.to_string()));
                }
            }
        }
        
        // Fill remaining with None if needed
        while is_int && int_values.len() < rows.len() {
            int_values.push(None);
        }
        while is_float && !is_int && float_values.len() < rows.len() {
            float_values.push(None);
        }
        while !is_int && !is_float && string_values.len() < rows.len() {
            string_values.push(None);
        }
        
        let array: Arc<dyn arrow::array::Array> = if is_int && !int_values.is_empty() {
            Arc::new(Int64Array::from(int_values))
        } else if is_float && !float_values.is_empty() {
            Arc::new(Float64Array::from(float_values))
        } else {
            Arc::new(StringArray::from(string_values))
        };
        
        column_arrays.push(array);
    }
    
    // Create fragments
    let mut column_fragments = Vec::new();
    for (col_idx, col_name) in column_names.iter().enumerate() {
        let fragment = ColumnFragment::new(
            column_arrays[col_idx].clone(),
            FragmentMetadata {
                row_count: rows.len(),
                min_value: None,
                max_value: None,
                cardinality: rows.len(),
                compression: CompressionType::None,
                memory_size: 0,
            },
        );
        column_fragments.push((col_name.clone(), fragment));
    }
    
    Ok((rows.len(), column_names, column_fragments, rows))
}

/// Clear cache and program-created files
#[derive(Serialize)]
struct ClearCacheResponse {
    success: bool,
    message: String,
    cleared_items: Vec<String>,
    error: Option<String>,
}

async fn clear_cache() -> Result<Json<ClearCacheResponse>, StatusCode> {
    use std::fs;
    use std::path::Path;
    
    let mut cleared_items = Vec::new();
    let mut errors = Vec::new();
    
    // Clear .query_patterns/ directory
    let query_patterns_dir = Path::new(".query_patterns");
    if query_patterns_dir.exists() {
        match fs::remove_dir_all(query_patterns_dir) {
            Ok(_) => {
                cleared_items.push(".query_patterns/ (pattern learning database)".to_string());
            }
            Err(e) => {
                errors.push(format!("Failed to remove .query_patterns/: {}", e));
            }
        }
    }
    
    // Clear .wal/ directory (transaction logs)
    let wal_dir = Path::new(".wal");
    if wal_dir.exists() {
        match fs::remove_dir_all(wal_dir) {
            Ok(_) => {
                cleared_items.push(".wal/ (transaction logs)".to_string());
            }
            Err(e) => {
                errors.push(format!("Failed to remove .wal/: {}", e));
            }
        }
    }
    
    // Clear log files
    let log_files = vec![".wal.log", "wal.log"];
    for log_file in log_files {
        let log_path = Path::new(log_file);
        if log_path.exists() {
            match fs::remove_file(log_path) {
                Ok(_) => {
                    cleared_items.push(format!("{} (log file)", log_file));
                }
                Err(e) => {
                    errors.push(format!("Failed to remove {}: {}", log_file, e));
                }
            }
        }
    }
    
    if errors.is_empty() {
        Ok(Json(ClearCacheResponse {
            success: true,
            message: if cleared_items.is_empty() {
                "No cache files to clear".to_string()
            } else {
                format!("Successfully cleared {} item(s)", cleared_items.len())
            },
            cleared_items,
            error: None,
        }))
    } else {
        Ok(Json(ClearCacheResponse {
            success: false,
            message: format!("Partially cleared. {} error(s) occurred", errors.len()),
            cleared_items,
            error: Some(errors.join("; ")),
        }))
    }
}

