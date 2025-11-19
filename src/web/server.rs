use actix_web::{web, App, HttpServer, HttpResponse, Result as ActixResult};
use actix_files::Files;
use crate::engine::HypergraphSQLEngine;
use crate::web::csv_loader;
use crate::web::benchmark;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

#[derive(Serialize)]
struct LoadResponse {
    success: bool,
    rows_loaded: usize,
    message: String,
}

#[derive(Serialize)]
struct BenchmarkResponse {
    success: bool,
    query: String,
    hypergraph_time_ms: f64,
    postgres_time_ms: f64,
    hypergraph_rows: usize,
    postgres_rows: usize,
    speedup: f64,
    message: String,
}

#[derive(Deserialize)]
struct QueryRequest {
    query: String,
    postgres_conn: String,
}

pub struct AppState {
    engine: Mutex<HypergraphSQLEngine>,
}

async fn load_csv(
    data: web::Data<AppState>,
    path: web::Path<(String,)>,
) -> ActixResult<HttpResponse> {
    let table_name = &path.0;
    let csv_path = "/Users/niyathnair/Downloads/LQS/annual-enterprise-survey-2024-financial-year-provisional.csv";
    
    let mut engine = data.engine.lock().unwrap();
    
    match csv_loader::load_csv_to_hypergraph(&mut *engine, csv_path, table_name) {
        Ok(rows) => {
            Ok(HttpResponse::Ok().json(LoadResponse {
                success: true,
                rows_loaded: rows,
                message: format!("Loaded {} rows into hypergraph engine", rows),
            }))
        }
        Err(e) => {
            Ok(HttpResponse::InternalServerError().json(LoadResponse {
                success: false,
                rows_loaded: 0,
                message: format!("Error: {}", e),
            }))
        }
    }
}

async fn load_postgres(
    path: web::Path<(String,)>,
    query: web::Query<std::collections::HashMap<String, String>>,
) -> ActixResult<HttpResponse> {
    let table_name = &path.0;
    let csv_path = "/Users/niyathnair/Downloads/LQS/annual-enterprise-survey-2024-financial-year-provisional.csv";
    let postgres_conn = query.get("conn").unwrap_or(&"postgresql://localhost/postgres".to_string());
    
    match csv_loader::load_csv_to_postgres(csv_path, table_name, postgres_conn).await {
        Ok(rows) => {
            Ok(HttpResponse::Ok().json(LoadResponse {
                success: true,
                rows_loaded: rows,
                message: format!("Loaded {} rows into PostgreSQL", rows),
            }))
        }
        Err(e) => {
            Ok(HttpResponse::InternalServerError().json(LoadResponse {
                success: false,
                rows_loaded: 0,
                message: format!("Error: {}", e),
            }))
        }
    }
}

async fn run_benchmark(
    data: web::Data<AppState>,
    req: web::Json<QueryRequest>,
) -> ActixResult<HttpResponse> {
    let mut engine = data.engine.lock().unwrap();
    
    match benchmark::benchmark_query(&mut *engine, &req.postgres_conn, &req.query).await {
        Ok(result) => {
            Ok(HttpResponse::Ok().json(BenchmarkResponse {
                success: true,
                query: result.query,
                hypergraph_time_ms: result.hypergraph_time_ms,
                postgres_time_ms: result.postgres_time_ms,
                hypergraph_rows: result.hypergraph_rows,
                postgres_rows: result.postgres_rows,
                speedup: result.speedup,
                message: format!(
                    "Hypergraph: {:.2}ms ({} rows), PostgreSQL: {:.2}ms ({} rows), Speedup: {:.2}x",
                    result.hypergraph_time_ms,
                    result.hypergraph_rows,
                    result.postgres_time_ms,
                    result.postgres_rows,
                    result.speedup
                ),
            }))
        }
        Err(e) => {
            Ok(HttpResponse::InternalServerError().json(BenchmarkResponse {
                success: false,
                query: req.query.clone(),
                hypergraph_time_ms: 0.0,
                postgres_time_ms: 0.0,
                hypergraph_rows: 0,
                postgres_rows: 0,
                speedup: 0.0,
                message: format!("Error: {}", e),
            }))
        }
    }
}

pub async fn start_server() -> std::io::Result<()> {
    let app_state = web::Data::new(AppState {
        engine: Mutex::new(HypergraphSQLEngine::new()),
    });
    
    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .route("/api/load/hypergraph/{table}", web::post().to(load_csv))
            .route("/api/load/postgres/{table}", web::post().to(load_postgres))
            .route("/api/benchmark", web::post().to(run_benchmark))
            .service(Files::new("/", "./static").index_file("index.html"))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

