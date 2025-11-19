use crate::engine::HypergraphSQLEngine;
use std::time::Instant;
use anyhow::Result;

pub struct BenchmarkResult {
    pub query: String,
    pub hypergraph_time_ms: f64,
    pub postgres_time_ms: f64,
    pub hypergraph_rows: usize,
    pub postgres_rows: usize,
    pub speedup: f64,
}

pub async fn benchmark_query(
    engine: &mut HypergraphSQLEngine,
    postgres_conn: &str,
    query: &str,
) -> Result<BenchmarkResult> {
    // Benchmark Hypergraph Engine
    let start = Instant::now();
    let hypergraph_result = engine.execute_query(query)?;
    let hypergraph_time = start.elapsed();
    let hypergraph_time_ms = hypergraph_time.as_secs_f64() * 1000.0;
    
    // Benchmark PostgreSQL
    let (client, connection) = tokio_postgres::connect(postgres_conn, tokio_postgres::NoTls).await?;
    
    // Spawn connection task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {}", e);
        }
    });
    
    let start = Instant::now();
    let rows = client.query(query, &[]).await?;
    let postgres_time = start.elapsed();
    let postgres_time_ms = postgres_time.as_secs_f64() * 1000.0;
    
    let speedup = if hypergraph_time_ms > 0.0 {
        postgres_time_ms / hypergraph_time_ms
    } else {
        0.0
    };
    
    Ok(BenchmarkResult {
        query: query.to_string(),
        hypergraph_time_ms,
        postgres_time_ms,
        hypergraph_rows: hypergraph_result.row_count,
        postgres_rows: rows.len(),
        speedup,
    })
}

