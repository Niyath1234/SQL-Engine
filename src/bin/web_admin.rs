use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use hypergraph_sql_engine::web::server::start_server;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get port from environment or use default
    let port = env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    
    // Create engine instance
    let engine = HypergraphSQLEngine::new();
    
    // Start web server
    start_server(engine, port).await?;
    
    Ok(())
}

