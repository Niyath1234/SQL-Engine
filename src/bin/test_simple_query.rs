use hypergraph_sql_engine::engine::HypergraphSQLEngine;

fn main() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║         Hypergraph SQL Engine - Simple Query Test           ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    
    // Create engine
    println!("[1/4] Initializing engine...");
    let mut engine = HypergraphSQLEngine::new();
    println!("      ✅ Engine initialized");
    println!();
    
    // Create a simple table
    println!("[2/4] Creating test table 'employees'...");
    let create_table_sql = r#"
        CREATE TABLE employees (
            id INT,
            name VARCHAR,
            age INT,
            salary FLOAT
        )
    "#;
    
    match engine.execute_query(create_table_sql) {
        Ok(_) => println!("      ✅ Table 'employees' created"),
        Err(e) => {
            println!("      ⚠️  Table creation: {:?}", e);
            // Continue anyway - table might already exist
        }
    }
    println!();
    
    // Insert some test data
    println!("[3/4] Inserting test data...");
    let insert_sql = r#"
        INSERT INTO employees (id, name, age, salary) VALUES
        (1, 'Alice', 30, 75000.0),
        (2, 'Bob', 25, 60000.0),
        (3, 'Charlie', 35, 90000.0),
        (4, 'Diana', 28, 65000.0),
        (5, 'Eve', 32, 80000.0)
    "#;
    
    match engine.execute_query(insert_sql) {
        Ok(result) => println!("      ✅ Inserted {} rows", result.row_count),
        Err(e) => {
            println!("      ⚠️  Insert error: {:?}", e);
            // Try alternative insert format
            println!("      Trying alternative insert format...");
            let alt_insert = "INSERT INTO employees VALUES (1, 'Alice', 30, 75000.0)";
            if let Ok(result) = engine.execute_query(alt_insert) {
                println!("      ✅ Inserted {} rows (alternative format)", result.row_count);
            }
        }
    }
    println!();
    
    // Run a test query
    println!("[4/4] Executing test query...");
    println!("      SQL: SELECT id, name, age, salary FROM employees WHERE age > 27 ORDER BY salary DESC");
    println!();
    
    let query_sql = "SELECT id, name, age, salary FROM employees WHERE age > 27 ORDER BY salary DESC";
    
    match engine.execute_query(query_sql) {
        Ok(result) => {
            println!("╔══════════════════════════════════════════════════════════════╗");
            println!("║                    QUERY RESULT                              ║");
            println!("╚══════════════════════════════════════════════════════════════╝");
            println!();
            println!("Rows returned: {}", result.row_count);
            println!("Execution time: {:.3} ms", result.execution_time_ms);
            println!();
            
            if !result.batches.is_empty() {
                let batch = &result.batches[0];
                println!("Batch info:");
                println!("  - Row count: {}", batch.row_count);
                println!("  - Columns: {}", batch.batch.columns.len());
                println!("  - Schema fields: {}", batch.batch.schema.fields().len());
                println!();
                
                // Print column names
                let field_names: Vec<String> = batch.batch.schema.fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                println!("Columns: {:?}", field_names);
                println!();
                
                // Try to print some data
                println!("Data (first {} rows):", result.row_count.min(10));
                println!("┌─────┬─────────┬─────┬──────────┐");
                println!("│ id  │ name    │ age │ salary   │");
                println!("├─────┼─────────┼─────┼──────────┤");
                
                // Print rows (simplified - just show we got data)
                for i in 0..result.row_count.min(10) {
                    println!("│ ... │ ...     │ ... │ ...      │");
                }
                println!("└─────┴─────────┴─────┴──────────┘");
                println!();
                println!("✅ Query executed successfully!");
            } else {
                println!("⚠️  No batches returned (but query succeeded)");
            }
        }
        Err(e) => {
            println!("❌ Query execution failed:");
            println!("   Error: {:?}", e);
            return Err(e);
        }
    }
    
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                    TEST COMPLETE                              ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    
    Ok(())
}

