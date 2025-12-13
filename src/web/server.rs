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
use tower_http::services::ServeDir;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Shared application state
pub type AppState = Arc<RwLock<HypergraphSQLEngine>>;

/// Start the web server
pub async fn start_server(engine: HypergraphSQLEngine, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let state: AppState = Arc::new(RwLock::new(engine));
    
    // Build all API routes first (they take precedence over static files)
    let api_routes = Router::new()
        .route("/execute", post(execute_query))
        .route("/llm/execute", post(execute_llm_query))
        .route("/tables", get(list_tables))
        .route("/table/:name", get(get_table_info))
        .route("/relationships", get(get_relationships))
        .route("/stats", get(get_stats))
        .route("/load_csv", post(load_csv))
        .route("/clear_cache", post(clear_cache))
        .route("/clear_all", post(clear_all_data))
        .route("/schema", get(get_schema))
        .route("/schema/sync", post(schema_sync))
        .route("/saved", get(list_saved_queries).post(create_saved_query))
        .route("/saved/:id", put(update_saved_query).delete(delete_saved_query))
        .route("/health", get(health_check))
        // Phase 2: Ingestion API endpoints
        .route("/ingest/simulate", post(ingest_simulate))
        .route("/ingest/load_sample_data", post(load_sample_data))
        // Phase 5: LLM Cortex API endpoints
        .route("/ask", post(ask_llm))
        .with_state(state.clone());
    
    // Try to serve static files from static/ directory (if built)
    let static_dir = std::path::Path::new("static");
    let app = if static_dir.exists() && static_dir.is_dir() {
        // Production mode: serve built static files, with API routes taking precedence
        Router::new()
            .nest("/api", api_routes)
            .nest_service("/", ServeDir::new("static"))
    } else {
        // Development mode: serve basic HTML (Vite dev server should be running on port 3000)
        Router::new()
            .nest("/api", api_routes)
            .route("/", get(index))
    };
    
    let app = app
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

#[derive(Serialize)]
struct ClearAllResponse {
    success: bool,
    message: String,
    tables_dropped: Vec<String>,
    cleared_items: Vec<String>,
    error: Option<String>,
}

async fn clear_all_data(State(state): State<AppState>) -> Result<Json<ClearAllResponse>, StatusCode> {
    use std::fs;
    use std::path::Path;
    
    let mut engine = state.write().await;
    let mut tables_dropped = Vec::new();
    let mut cleared_items = Vec::new();
    let mut errors = Vec::new();
    
    // Get all table names first
    let table_names: Vec<String> = {
        let graph = engine.graph();
        let mut names = Vec::new();
        for (_, node) in graph.iter_nodes() {
            if matches!(node.node_type, crate::hypergraph::node::NodeType::Table) {
                if let Some(table_name) = &node.table_name {
                    names.push(table_name.clone());
                }
            }
        }
        names
    };
    
    // Drop all tables
    for table_name in &table_names {
        match engine.execute_query(&format!("DROP TABLE IF EXISTS {}", table_name)) {
            Ok(_) => {
                tables_dropped.push(table_name.clone());
            }
            Err(e) => {
                errors.push(format!("Failed to drop table {}: {}", table_name, e));
            }
        }
    }
    
    // Clear all caches
    engine.clear_all_caches();
    cleared_items.push("result_cache".to_string());
    cleared_items.push("plan_cache".to_string());
    
    // Clear cache directories
    let cache_dirs = [".operator_cache", ".spill", ".query_patterns", ".wal"];
    for dir in &cache_dirs {
        let path = Path::new(dir);
        if path.exists() {
            match fs::remove_dir_all(path) {
                Ok(_) => cleared_items.push(format!("{}/", dir)),
                Err(e) => errors.push(format!("Failed to remove {}: {}", dir, e)),
            }
        }
    }
    
    // Clear WorldState (reset to empty)
    {
        let mut world_state = engine.world_state_mut();
        *world_state = crate::worldstate::WorldState::new();
    }
    
    if errors.is_empty() {
        Ok(Json(ClearAllResponse {
            success: true,
            message: format!("Successfully cleared all data: {} tables dropped, {} cache items cleared", 
                tables_dropped.len(), cleared_items.len()),
            tables_dropped,
            cleared_items,
            error: None,
        }))
    } else {
        Ok(Json(ClearAllResponse {
            success: false,
            message: format!("Partially cleared. {} error(s) occurred", errors.len()),
            tables_dropped,
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

#[derive(Deserialize)]
struct SimulateIngestionRequest {
    source_id: Option<String>,
    table_name: Option<String>,
    schema_type: Option<String>,
    batch_size: Option<usize>,
    #[serde(default)]
    payloads: Option<Vec<serde_json::Value>>,
}

async fn ingest_simulate(
    State(state): State<AppState>,
    Json(req): Json<SimulateIngestionRequest>,
) -> Result<Json<crate::ingestion::IngestionResult>, StatusCode> {
    let mut engine = state.write().await;
    let source_id = req.source_id.unwrap_or_else(|| "simulator_1".to_string());
    
    let result = if let Some(payloads) = req.payloads {
        let connector = crate::ingestion::JsonConnector::new(source_id.clone(), payloads);
        engine.ingest(Box::new(connector), req.table_name)
    } else {
        let schema_type = match req.schema_type.as_deref().unwrap_or("flat") {
            "nested" => crate::ingestion::SimulatorSchema::Nested,
            "with_arrays" => crate::ingestion::SimulatorSchema::WithArrays,
            "ecommerce" => crate::ingestion::SimulatorSchema::ECommerce,
            _ => crate::ingestion::SimulatorSchema::Flat,
        };
        let connector = crate::ingestion::SimulatorConnector::new(source_id.clone(), schema_type)
            .with_batch_size(req.batch_size.unwrap_or(100));
        engine.ingest(Box::new(connector), req.table_name)
    };
    
    match result {
        Ok(ingestion_result) => {
            // Auto-sync schema after successful ingestion to keep WorldState in sync
            let tables: Vec<String> = engine.graph().iter_nodes()
                .filter_map(|(_, node)| {
                    if matches!(node.node_type, crate::hypergraph::node::NodeType::Table) {
                        node.table_name.clone()
                    } else {
                        None
                    }
                })
                .collect();
            let _ = engine.sync_worldstate_schema_from_hypergraph();
            Ok(Json(ingestion_result))
        }
        Err(e) => {
            eprintln!("Ingestion failed: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn load_sample_data(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    use serde_json::json;
    let mut engine = state.write().await;
    
    let sample_tables = vec![
        ("products", "products_api", json!([
            {"id": 1, "name": "Laptop Pro", "price": 1299.99, "category_id": 1, "created_at": "2024-01-15"},
            {"id": 2, "name": "Wireless Mouse", "price": 29.99, "category_id": 1, "created_at": "2024-01-16"},
            {"id": 3, "name": "Mechanical Keyboard", "price": 149.99, "category_id": 1, "created_at": "2024-01-17"},
            {"id": 4, "name": "Office Chair", "price": 299.99, "category_id": 2, "created_at": "2024-01-18"},
            {"id": 5, "name": "Standing Desk", "price": 599.99, "category_id": 2, "created_at": "2024-01-19"},
            {"id": 6, "name": "Monitor 27\"", "price": 399.99, "category_id": 1, "created_at": "2024-01-20"},
            {"id": 7, "name": "USB-C Hub", "price": 79.99, "category_id": 1, "created_at": "2024-01-21"},
            {"id": 8, "name": "Desk Lamp", "price": 49.99, "category_id": 2, "created_at": "2024-01-22"},
            {"id": 9, "name": "Webcam HD", "price": 89.99, "category_id": 1, "created_at": "2024-01-23"},
            {"id": 10, "name": "Noise Cancelling Headphones", "price": 249.99, "category_id": 1, "created_at": "2024-01-24"},
        ])),
        ("categories", "categories_api", json!([
            {"id": 1, "name": "Electronics", "description": "Electronic devices and accessories"},
            {"id": 2, "name": "Furniture", "description": "Office and home furniture"},
            {"id": 3, "name": "Software", "description": "Software licenses and subscriptions"},
        ])),
        ("customers", "customers_api", json!([
            {"id": 1, "name": "John Doe", "email": "john@example.com", "created_at": "2024-01-10"},
            {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "created_at": "2024-01-11"},
            {"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "created_at": "2024-01-12"},
            {"id": 4, "name": "Alice Williams", "email": "alice@example.com", "created_at": "2024-01-13"},
            {"id": 5, "name": "Charlie Brown", "email": "charlie@example.com", "created_at": "2024-01-14"},
        ])),
        ("orders", "orders_api", json!([
            {"id": 1, "customer_id": 1, "order_date": "2024-01-20", "status": "completed", "total_amount": 1329.98},
            {"id": 2, "customer_id": 2, "order_date": "2024-01-21", "status": "pending", "total_amount": 449.98},
            {"id": 3, "customer_id": 3, "order_date": "2024-01-22", "status": "completed", "total_amount": 899.98},
            {"id": 4, "customer_id": 1, "order_date": "2024-01-23", "status": "completed", "total_amount": 79.99},
            {"id": 5, "customer_id": 4, "order_date": "2024-01-24", "status": "pending", "total_amount": 249.99},
            {"id": 6, "customer_id": 2, "order_date": "2024-01-25", "status": "completed", "total_amount": 1299.99},
            {"id": 7, "customer_id": 5, "order_date": "2024-01-26", "status": "completed", "total_amount": 599.99},
        ])),
        ("order_items", "order_items_api", json!([
            {"order_id": 1, "product_id": 1, "quantity": 1, "price": 1299.99},
            {"order_id": 1, "product_id": 2, "quantity": 1, "price": 29.99},
            {"order_id": 2, "product_id": 3, "quantity": 1, "price": 149.99},
            {"order_id": 2, "product_id": 6, "quantity": 1, "price": 399.99},
            {"order_id": 3, "product_id": 4, "quantity": 1, "price": 299.99},
            {"order_id": 3, "product_id": 5, "quantity": 1, "price": 599.99},
            {"order_id": 4, "product_id": 7, "quantity": 1, "price": 79.99},
            {"order_id": 5, "product_id": 10, "quantity": 1, "price": 249.99},
            {"order_id": 6, "product_id": 1, "quantity": 1, "price": 1299.99},
            {"order_id": 7, "product_id": 5, "quantity": 1, "price": 599.99},
        ])),
    ];
    
    let mut results = Vec::new();
    for (table_name, source_id, payloads) in sample_tables {
        let payloads_vec: Vec<serde_json::Value> = serde_json::from_value(payloads)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let connector = crate::ingestion::JsonConnector::new(source_id.to_string(), payloads_vec);
        match engine.ingest(Box::new(connector), Some(table_name.to_string())) {
            Ok(result) => {
                // Auto-sync schema after each table ingestion
                let tables: Vec<String> = engine.graph().iter_nodes()
                    .filter_map(|(_, node)| {
                        if matches!(node.node_type, crate::hypergraph::node::NodeType::Table) {
                            node.table_name.clone()
                        } else {
                            None
                        }
                    })
                    .collect();
                let _ = engine.sync_worldstate_schema_from_hypergraph();
                results.push(serde_json::json!({
                    "table": table_name,
                    "status": "success",
                    "records": result.records_ingested,
                }));
            }
            Err(e) => {
                eprintln!("Failed to ingest {}: {}", table_name, e);
                results.push(serde_json::json!({
                    "table": table_name,
                    "status": "error",
                    "error": e.to_string(),
                }));
            }
        }
    }
    
    // Final schema sync after all tables are loaded
    let _ = engine.sync_worldstate_schema_from_hypergraph();
    
    let join_rules = vec![
        ("products", "category_id", "categories", "id"),
        ("orders", "customer_id", "customers", "id"),
        ("order_items", "order_id", "orders", "id"),
        ("order_items", "product_id", "products", "id"),
    ];
    
    let join_rules_count = join_rules.len();
    for (left_table, left_key, right_table, right_key) in join_rules {
        if let Ok(rule_id) = engine.create_join_rule(
            left_table.to_string(),
            vec![left_key.to_string()],
            right_table.to_string(),
            vec![right_key.to_string()],
            "inner".to_string(),
            format!("{} joins to {}", left_table, right_table),
        ) {
            let _ = engine.approve_join_rule(&rule_id);
        }
    }
    
    let _ = engine.sync_all_approved_rules_to_hypergraph();
    
    Ok(Json(serde_json::json!({
        "status": "success",
        "tables_loaded": results,
        "join_rules_created": join_rules_count,
    })))
}

#[derive(Deserialize)]
struct AskRequest {
    intent: String,
    user_id: Option<String>,
    ollama_url: Option<String>,
    model: Option<String>,
}

async fn ask_llm(
    State(state): State<AppState>,
    Json(req): Json<AskRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mut engine = state.write().await;
    // Build metadata pack using hypergraph execution schema (single source of truth for columns/types)
    let metadata_pack = engine.export_metadata_pack_execution_truth(req.user_id.as_deref());

    // Hard schema sync gate: block LLM queries if WorldState schema != hypergraph schema.
    let tables_for_gate: Vec<String> = metadata_pack.tables.iter().map(|t| t.name.clone()).collect();
    // Compare execution-equivalent hashes (columns/types only) to avoid false mismatches
    let world_exec_schema_hash = engine.worldstate_execution_schema_hash(&tables_for_gate);
    let hyper_exec_schema_hash = engine.hypergraph_schema_hash(&tables_for_gate);
    if world_exec_schema_hash != hyper_exec_schema_hash {
        return Ok(Json(serde_json::json!({
            "status": "schema_out_of_sync",
            "reason": "WorldState schema does not match hypergraph execution schema. Run /api/schema/sync to repair.",
            "tables_checked": tables_for_gate,
            "worldstate_execution_schema_hash": world_exec_schema_hash,
            "hypergraph_execution_schema_hash": hyper_exec_schema_hash,
            "suggestions": [
                "Call POST /api/schema/sync to rebuild WorldState schema registry from hypergraph execution schema",
                "If this keeps happening, ensure ingestion writes schema into both hypergraph and WorldState consistently"
            ]
        })));
    }
    let policy = {
        let world_state = engine.world_state();
        world_state.policy_registry.get_query_policy(req.user_id.as_deref()).clone()
    };
    
    // NEW PIPELINE: LLM → IntentSpec → Compiler → StructuredPlan
    let intent_spec_generator = crate::llm::IntentSpecGenerator::new(
        req.ollama_url.clone(),
        req.model.clone(),
    );
    
    let intent_spec = match intent_spec_generator.generate_intent_spec(&req.intent, &metadata_pack).await {
        Ok(spec) => spec,
        Err(e) => {
            eprintln!("IntentSpec generation failed: {}", e);
            return Ok(Json(serde_json::json!({
                "status": "intent_spec_generation_failed",
                "reason": e.to_string(),
                "suggestions": ["LLM failed to generate IntentSpec - check Ollama connection"]
            })));
        }
    };
    
    // Validate IntentSpec doesn't contain schema references (strict mode)
    let schema_errors = intent_spec.validate_no_schema_references();
    if !schema_errors.is_empty() {
        return Ok(Json(serde_json::json!({
            "status": "intent_spec_validation_failed",
            "reason": "IntentSpec contains schema references (table/column names)",
            "errors": schema_errors,
            "suggestions": ["LLM should output generic entities, not table/column names"]
        })));
    }
    
    // Compile IntentSpec → StructuredPlan (deterministic, hypergraph-aware)
    let mut compiler = crate::llm::IntentCompiler::with_llm_scoring(
        req.intent.clone(),
        req.ollama_url.clone(),
        req.model.clone(),
    );
    let mut structured_plan = match compiler.compile(&intent_spec, &metadata_pack).await {
        Ok(plan) => plan,
        Err(e) => {
            eprintln!("Intent compilation failed: {}", e);
            return Ok(Json(serde_json::json!({
                "status": "compilation_failed",
                "reason": e.to_string(),
                "intent_spec": intent_spec,
                "suggestions": ["Compiler could not resolve IntentSpec to tables/columns - check hypergraph"]
            })));
        }
    };
    
    let compiler_explanations = compiler.get_explanations().iter()
        .map(|e| serde_json::json!({
            "step": e.step,
            "decision": e.decision,
            "reason": e.reason
        }))
        .collect::<Vec<_>>();
    
    let validator = crate::llm::PlanValidator::new();
    let sql_generator = crate::llm::SQLGenerator::new();
    let audit_log = crate::llm::AuditLog::default();

    // Skip normalization for now - compiler should produce correct plans
    // TODO: Re-enable normalization after fixing issues
    /*
    let normalizer = crate::llm::PlanNormalizer::new();
    if let Err(e) = normalizer.normalize(&mut structured_plan, &metadata_pack) {
        eprintln!("WARNING: Normalization failed after compilation: {}", e);
        // For MVP: continue anyway (compiler should have produced correct plan)
    }
    */
    match validator.validate(&structured_plan, &metadata_pack, &policy) {
        crate::llm::ValidationResult::Valid => {}
        crate::llm::ValidationResult::Invalid { reason, suggestions } => {
            return Ok(Json(serde_json::json!({
                "status": "validation_failed",
                "reason": reason,
                "suggestions": suggestions,
                "structured_plan": structured_plan,
            })));
        }
    }
    let sql = sql_generator.generate_sql(&structured_plan, &policy);
    let world_hash = {
        let world_state = engine.world_state();
        world_state.world_hash_global()
    };
    let query_hash = {
        use crate::query::cache::QuerySignature;
        QuerySignature::from_sql(&sql).hash()
    };
    let entry_id = audit_log.log(
        req.intent.clone(),
        structured_plan.clone(),
        sql.clone(),
        world_hash,
        query_hash,
    );
    let execution_result = engine.execute_query(&sql);
    match execution_result {
        Ok(result) => {
            audit_log.update_execution_stats(
                &entry_id,
                result.row_count as u64,
                result.execution_time_ms,
                false,
            );
            Ok(Json(serde_json::json!({
                "status": "success",
                "entry_id": entry_id,
                "structured_plan": structured_plan,
                "sql": sql,
                "result": {
                    "row_count": result.row_count,
                    "execution_time_ms": result.execution_time_ms,
                },
            })))
        }
        Err(e) => {
            Ok(Json(serde_json::json!({
                "status": "execution_failed",
                "error": e.to_string(),
                "sql": sql,
            })))
        }
    }
}

#[derive(Deserialize)]
struct SchemaSyncRequest {
    // Reserved for future: direction/mode. For now we only support worldstate_from_hypergraph.
    mode: Option<String>,
}

async fn schema_sync(
    State(state): State<AppState>,
    Json(req): Json<Option<SchemaSyncRequest>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let mut engine = state.write().await;
    let mode = req.and_then(|r| r.mode).unwrap_or_else(|| "worldstate_from_hypergraph".to_string());
    if mode != "worldstate_from_hypergraph" {
        return Ok(Json(serde_json::json!({
            "status": "error",
            "error": format!("Unsupported mode '{}'. Supported: worldstate_from_hypergraph", mode),
        })));
    }

    if let Err(e) = engine.sync_worldstate_schema_from_hypergraph() {
        eprintln!("Schema sync failed: {}", e);
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let pack = engine.export_metadata_pack_execution_truth(None);
    let tables: Vec<String> = pack.tables.iter().map(|t| t.name.clone()).collect();
    let world_exec_schema_hash = engine.worldstate_execution_schema_hash(&tables);
    let hyper_exec_schema_hash = engine.hypergraph_schema_hash(&tables);

    Ok(Json(serde_json::json!({
        "status": "success",
        "mode": mode,
        "tables": tables,
        "worldstate_execution_schema_hash": world_exec_schema_hash,
        "hypergraph_execution_schema_hash": hyper_exec_schema_hash,
    })))
}
