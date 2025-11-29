use crate::engine::{HypergraphSQLEngine, LlmQueryMode, LlmQueryRequest, LlmQueryResponse, QueryClass};
use std::sync::Arc;
use tokio::sync::RwLock;
use axum::{
    extract::{State, Path},
    http::StatusCode,
    response::{Html, Json},
    routing::{get, post, put, delete},
    Router,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

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
        .route("/api/relationships", get(get_relationships))
        .route("/api/stats", get(get_stats))
        .route("/api/load_csv", post(load_csv))
        .route("/api/clear_cache", post(clear_cache))
        .route("/api/schema", get(get_schema))
        .route("/api/saved", get(list_saved_queries).post(create_saved_query))
        .route("/api/saved/:id", put(update_saved_query).delete(delete_saved_query))
        .route("/api/health", get(health_check))
        .layer(CorsLayer::permissive())
        .with_state(state);
    
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    println!("🚀 Hypergraph SQL Engine Admin UI running on http://localhost:{}", port);
    println!("📊 Open your browser to start querying!");
    
    axum::serve(listener, app).await?;
    Ok(())
}

/// Serve the main HTML page (served by Vite dev server or built files)
async fn index() -> Html<&'static str> {
    // In production, this will serve the built React app from static/
    // In development, Vite dev server handles this
    Html(r#"
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SQL Engine - Querybook</title>
</head>
<body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
</body>
</html>
    "#)
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

/// LLM-focused query request
#[derive(Deserialize)]
struct LlmQueryApiRequest {
    session_id: Option<String>,
    sql: String,
    mode: Option<String>,
    max_scan_rows: Option<u64>,
    max_time_ms: Option<u64>,
    result_format: Option<String>,
}

#[derive(Serialize)]
struct LlmQueryApiResponse {
    rows: Vec<Vec<String>>,
    columns: Vec<String>,
    row_count: usize,
    execution_time_ms: f64,
    query_class: String,
    tables: Vec<String>,
    columns_used: Vec<String>,
    approx_mode: bool,
    truncated: bool,
    error: Option<String>,
}

async fn execute_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, StatusCode> {
    let start = std::time::Instant::now();
    
    let mut engine = state.write().await;
    let result = engine.execute_query_with_cache(&req.sql, false);
    
    match result {
        Ok(query_result) => {
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            
            let mut rows = Vec::new();
            let mut columns = Vec::new();
            
            if !query_result.batches.is_empty() {
                let first_batch = &query_result.batches[0];
                let schema = &first_batch.batch.schema;
                
                for field in schema.fields() {
                    columns.push(field.name().clone());
                }
                
                for batch in &query_result.batches {
                    let col_count = columns.len();
                    if col_count == 0 || batch.batch.columns.is_empty() {
                        continue;
                    }
                    
                    let row_count = batch.row_count;
                    for row_idx in 0..row_count {
                        let mut row = Vec::with_capacity(col_count);
                        for col_idx in 0..col_count {
                            if let Some(array) = batch.batch.columns.get(col_idx) {
                                let value = crate::result_format::format_array_value(array, row_idx);
                                row.push(value);
                            } else {
                                row.push("NULL".to_string());
                            }
                        }
                        rows.push(row);
                    }
                }
            }
            
            let row_count = rows.len();
            Ok(Json(QueryResponse {
                success: true,
                rows,
                columns,
                row_count,
                execution_time_ms: elapsed,
                error: None,
            }))
        }
        Err(e) => {
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            Ok(Json(QueryResponse {
                success: false,
                rows: Vec::new(),
                columns: Vec::new(),
                row_count: 0,
                execution_time_ms: elapsed,
                error: Some(e.to_string()),
            }))
        }
    }
}

async fn execute_llm_query(
    State(state): State<AppState>,
    Json(req): Json<LlmQueryApiRequest>,
) -> Result<Json<LlmQueryApiResponse>, StatusCode> {
    let start = std::time::Instant::now();
    let mut engine = state.write().await;
    
    let mode = req.mode.as_deref().unwrap_or("exact");
    let llm_mode = match mode {
        "approx" => LlmQueryMode::Approx,
        _ => LlmQueryMode::Exact,
    };
    
    let llm_req = LlmQueryRequest {
        session_id: req.session_id.clone(),
        sql: req.sql.clone(),
        mode: llm_mode,
        max_scan_rows: req.max_scan_rows,
        max_time_ms: req.max_time_ms,
        result_format: req.result_format.as_ref().map(|s| {
            use crate::result_format::ResultFormat;
            match s.as_str() {
                "summary" => ResultFormat::Summary,
                "metadata" => ResultFormat::Metadata,
                s if s.starts_with("sample:") => {
                    let n: usize = s.strip_prefix("sample:").and_then(|x| x.parse().ok()).unwrap_or(10);
                    ResultFormat::Sample(n)
                },
                s if s.starts_with("representative:") => {
                    let n: usize = s.strip_prefix("representative:").and_then(|x| x.parse().ok()).unwrap_or(10);
                    ResultFormat::Representative(n)
                },
                _ => ResultFormat::Full,
            }
        }).unwrap_or(crate::result_format::ResultFormat::Full),
    };
    
    match engine.execute_llm_query(llm_req) {
        Ok(response) => {
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            
            // Convert response to API format
            let formatted = response.formatted_result.as_ref();
            let (api_rows, api_columns) = if let Some(fmt) = formatted {
                if fmt.is_summary {
                    (fmt.sample_rows.clone(), fmt.columns.clone())
                } else {
                    (fmt.full_rows.clone(), fmt.columns.clone())
                }
            } else {
                // Fallback: convert batches to rows
                let mut rows = Vec::new();
                let mut columns = Vec::new();
                
                if !response.result.batches.is_empty() {
                    let first_batch = &response.result.batches[0];
                    let schema = &first_batch.batch.schema;
                    
                    for field in schema.fields() {
                        columns.push(field.name().clone());
                    }
                    
                    for batch in &response.result.batches {
                        let col_count = columns.len();
                        if col_count == 0 || batch.batch.columns.is_empty() {
                            continue;
                        }
                        
                        let row_count = batch.row_count;
                        for row_idx in 0..row_count {
                            let mut row = Vec::with_capacity(col_count);
                            for col_idx in 0..col_count {
                                if let Some(array) = batch.batch.columns.get(col_idx) {
                                    let value = crate::result_format::format_array_value(array, row_idx);
                                    row.push(value);
                                } else {
                                    row.push("NULL".to_string());
                                }
                            }
                            rows.push(row);
                        }
                    }
                }
                
                (rows, columns)
            };
            
            Ok(Json(LlmQueryApiResponse {
                rows: api_rows,
                columns: api_columns,
                row_count: response.result.row_count,
                execution_time_ms: elapsed,
                query_class: format!("{:?}", response.query_class),
                tables: response.tables,
                columns_used: response.columns,
                approx_mode: response.approx_mode,
                truncated: response.truncated,
                error: None,
            }))
        }
        Err(e) => {
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            Ok(Json(LlmQueryApiResponse {
                rows: Vec::new(),
                columns: Vec::new(),
                row_count: 0,
                execution_time_ms: elapsed,
                query_class: "Error".to_string(),
                tables: Vec::new(),
                columns_used: Vec::new(),
                approx_mode: false,
                truncated: false,
                error: Some(e.to_string()),
            }))
        }
    }
}

#[derive(Serialize)]
struct TablesResponse {
    tables: Vec<String>,
}

async fn list_tables(State(state): State<AppState>) -> Result<Json<TablesResponse>, StatusCode> {
    let engine = state.read().await;
    let graph = engine.graph();
    
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
    Path(name): Path<String>,
) -> Result<Json<TableInfoResponse>, StatusCode> {
    let engine = state.read().await;
    let graph = engine.graph();
    
    let table_node = graph.get_table_node(&name)
        .ok_or_else(|| StatusCode::NOT_FOUND)?;
    
    let mut columns = Vec::new();
    for col_node in graph.get_column_nodes(&name) {
        if let Some(col_name) = &col_node.column_name {
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
    
    Ok(Json(TableInfoResponse {
        name: table_node.table_name.clone().unwrap_or_default(),
        columns,
        row_count: 0, // TODO: Get actual row count
    }))
}

#[derive(Serialize)]
struct RelationshipsResponse {
    relationships: Vec<RelationshipInfo>,
}

#[derive(Serialize)]
struct RelationshipInfo {
    from_table: String,
    to_table: String,
    from_column: String,
    join_type: String,
    cardinality: Option<f64>,
    selectivity: Option<f64>,
}

async fn get_relationships(State(state): State<AppState>) -> Result<Json<RelationshipsResponse>, StatusCode> {
    let engine = state.read().await;
    let graph = engine.graph();
    
    let mut relationships = Vec::new();
    for (_edge_id, edge) in graph.iter_edges() {
        relationships.push(RelationshipInfo {
            from_table: edge.predicate.left.0.clone(),
            to_table: edge.predicate.right.0.clone(),
            from_column: edge.predicate.left.1.clone(),
            join_type: format!("{:?}", edge.join_type),
            cardinality: None,
            selectivity: None,
        });
    }
    
    Ok(Json(RelationshipsResponse { relationships }))
}

async fn get_stats(State(state): State<AppState>) -> Result<Json<serde_json::Value>, StatusCode> {
    let engine = state.read().await;
    let graph = engine.graph();
    
    let mut total_rows = 0;
    let mut total_tables = 0;
    
    for (_, node) in graph.iter_nodes() {
        if matches!(node.node_type, crate::hypergraph::node::NodeType::Table) {
            total_tables += 1;
        }
    }
    
    Ok(Json(serde_json::json!({
        "total_tables": total_tables,
        "total_rows": total_rows,
        "memory_stats": {
            "total_memory_usage_mb": 0,
            "total_memory_capacity_mb": 0,
        }
    })))
}

async fn load_csv(State(_state): State<AppState>) -> Result<Json<serde_json::Value>, StatusCode> {
    // TODO: Implement CSV loading
    Ok(Json(serde_json::json!({ "success": true, "message": "CSV loading not yet implemented" })))
}

#[derive(Serialize)]
struct ClearCacheResponse {
    success: bool,
    message: String,
    cleared_items: Vec<String>,
    error: Option<String>,
}

async fn clear_cache(_state: State<AppState>) -> Result<Json<ClearCacheResponse>, StatusCode> {
    use std::fs;
    use std::path::Path;
    
    let mut cleared_items = Vec::new();
    let mut errors = Vec::new();
    
    // Clear .operator_cache/
    let cache_dir = Path::new(".operator_cache");
    if cache_dir.exists() {
        match fs::remove_dir_all(cache_dir) {
            Ok(_) => cleared_items.push(".operator_cache/".to_string()),
            Err(e) => errors.push(format!("Failed to remove .operator_cache/: {}", e)),
        }
    }
    
    // Clear .spill/
    let spill_dir = Path::new(".spill");
    if spill_dir.exists() {
        match fs::remove_dir_all(spill_dir) {
            Ok(_) => cleared_items.push(".spill/".to_string()),
            Err(e) => errors.push(format!("Failed to remove .spill/: {}", e)),
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

// New endpoints for Querybook UI

#[derive(Serialize)]
struct SchemaResponse {
    tables: Vec<TableSchema>,
}

#[derive(Serialize)]
struct TableSchema {
    name: String,
    columns: Vec<ColumnInfo>,
}

async fn get_schema(State(state): State<AppState>) -> Result<Json<SchemaResponse>, StatusCode> {
    let engine = state.read().await;
    let graph = engine.graph();
    
    let mut tables = Vec::new();
    for (_, node) in graph.iter_nodes() {
        if matches!(node.node_type, crate::hypergraph::node::NodeType::Table) {
            if let Some(table_name) = &node.table_name {
                let mut columns = Vec::new();
                for col_node in graph.get_column_nodes(table_name) {
                    if let Some(col_name) = &col_node.column_name {
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
                
                tables.push(TableSchema {
                    name: table_name.clone(),
                    columns,
                });
            }
        }
    }
    
    Ok(Json(SchemaResponse { tables }))
}

// Saved Queries storage (in-memory for now, can be persisted later)
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
static SAVED_QUERIES: OnceLock<StdMutex<HashMap<String, SavedQuery>>> = OnceLock::new();

fn get_saved_queries() -> &'static StdMutex<HashMap<String, SavedQuery>> {
    SAVED_QUERIES.get_or_init(|| StdMutex::new(HashMap::new()))
}

#[derive(Serialize, Deserialize, Clone)]
struct SavedQuery {
    id: String,
    title: String,
    sql: String,
    folder: Option<String>,
    created_at: String,
    updated_at: String,
}

async fn list_saved_queries() -> Result<Json<Vec<SavedQuery>>, StatusCode> {
    let queries = get_saved_queries().lock().unwrap();
    Ok(Json(queries.values().cloned().collect()))
}

#[derive(Deserialize)]
struct CreateSavedQueryRequest {
    title: String,
    sql: String,
    folder: Option<String>,
}

async fn create_saved_query(Json(req): Json<CreateSavedQueryRequest>) -> Result<Json<SavedQuery>, StatusCode> {
    let id = format!("query-{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos());
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs().to_string();
    
    let query = SavedQuery {
        id: id.clone(),
        title: req.title,
        sql: req.sql,
        folder: req.folder,
        created_at: now.clone(),
        updated_at: now,
    };
    
    let mut queries = get_saved_queries().lock().unwrap();
    queries.insert(id.clone(), query.clone());
    
    Ok(Json(query))
}

#[derive(Deserialize)]
struct UpdateSavedQueryRequest {
    title: Option<String>,
    sql: Option<String>,
    folder: Option<String>,
}

async fn update_saved_query(
    Path(id): Path<String>,
    Json(req): Json<UpdateSavedQueryRequest>,
) -> Result<Json<SavedQuery>, StatusCode> {
    let mut queries = get_saved_queries().lock().unwrap();
    
    if let Some(mut query) = queries.get(&id).cloned() {
        if let Some(title) = req.title {
            query.title = title;
        }
        if let Some(sql) = req.sql {
            query.sql = sql;
        }
        if let Some(folder) = req.folder {
            query.folder = Some(folder);
        }
        query.updated_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs().to_string();
        
        queries.insert(id.clone(), query.clone());
        Ok(Json(query))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn delete_saved_query(Path(id): Path<String>) -> Result<StatusCode, StatusCode> {
    let mut queries = get_saved_queries().lock().unwrap();
    if queries.remove(&id).is_some() {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn health_check() -> Result<Json<serde_json::Value>, StatusCode> {
    Ok(Json(serde_json::json!({ "status": "ok" })))
}
