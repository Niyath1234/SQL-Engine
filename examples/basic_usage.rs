use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use anyhow::Result;

fn main() -> Result<()> {
    println!("Hypergraph SQL Engine - Basic Usage Example");
    
    // Create engine instance
    let mut engine = HypergraphSQLEngine::new();
    
    // Example queries from the use cases
    let queries = vec![
        "SELECT r.region_name, SUM(f.sales_amount) 
         FROM fact_sales f 
         JOIN dim_region r ON f.region_id = r.id 
         WHERE f.sale_date BETWEEN '2025-01-01' AND '2025-01-31' 
         GROUP BY r.region_name",
        
        "SELECT r.region_name, SUM(f.sales_amount) 
         FROM fact_sales f 
         JOIN dim_region r ON f.region_id = r.id 
         WHERE f.sale_date BETWEEN '2025-02-01' AND '2025-02-28' 
         GROUP BY r.region_name",
    ];
    
    for (i, sql) in queries.iter().enumerate() {
        println!("\n--- Query {} ---", i + 1);
        println!("SQL: {}", sql);
        
        match engine.execute_query(sql) {
            Ok(result) => {
                println!("✓ Query executed successfully");
                println!("  Result rows: {}", result.row_count);
                println!("  Batches: {}", result.batches.len());
            }
            Err(e) => {
                println!("✗ Query execution error: {}", e);
            }
        }
    }
    
    println!("\nNote: This example demonstrates the engine interface.");
    println!("Full implementation requires data loading and join definitions.");
    
    Ok(())
}

