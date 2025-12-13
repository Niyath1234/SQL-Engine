use hypergraph_sql_engine::engine::HypergraphSQLEngine;

fn main() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║      Testing SUM and AVG Aggregation Functions             ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    
    // Create engine
    println!("[1/5] Initializing engine...");
    let mut engine = HypergraphSQLEngine::new();
    println!("      ✅ Engine initialized");
    println!();
    
    // Create a simple table
    println!("[2/5] Creating test table 'test_data'...");
    let create_table_sql = r#"
        CREATE TABLE test_data (
            id INT,
            value FLOAT,
            amount FLOAT
        )
    "#;
    
    match engine.execute_query(create_table_sql) {
        Ok(_) => println!("      ✅ Table 'test_data' created"),
        Err(e) => {
            println!("      ⚠️  Table creation: {:?}", e);
            // Continue anyway - table might already exist
        }
    }
    println!();
    
    // Insert some test data
    println!("[3/5] Inserting test data...");
    let insert_sql = r#"
        INSERT INTO test_data (id, value, amount) VALUES
        (1, 10.5, 100.0),
        (2, 20.5, 200.0),
        (3, 30.5, 300.0),
        (4, 40.5, 400.0),
        (5, 50.5, 500.0)
    "#;
    
    match engine.execute_query(insert_sql) {
        Ok(result) => println!("      ✅ Inserted {} rows", result.row_count),
        Err(e) => {
            println!("      ⚠️  Insert error: {:?}", e);
            return Err(e);
        }
    }
    println!();
    
    // Test 1: Simple SUM
    println!("[4/5] Test 1: SELECT SUM(value) FROM test_data");
    let query1 = "SELECT SUM(value) FROM test_data";
    match engine.execute_query(query1) {
        Ok(result) => {
            println!("      ✅ Query executed successfully");
            println!("      Rows returned: {}", result.row_count);
            println!("      Execution time: {:.3} ms", result.execution_time_ms);
        }
        Err(e) => {
            println!("      ❌ Query failed: {:?}", e);
        }
    }
    println!();
    
    // Test 2: Simple AVG
    println!("[5/5] Test 2: SELECT AVG(value) FROM test_data");
    let query2 = "SELECT AVG(value) FROM test_data";
    match engine.execute_query(query2) {
        Ok(result) => {
            println!("      ✅ Query executed successfully");
            println!("      Rows returned: {}", result.row_count);
            println!("      Execution time: {:.3} ms", result.execution_time_ms);
        }
        Err(e) => {
            println!("      ❌ Query failed: {:?}", e);
        }
    }
    println!();
    
    // Test 3: Both SUM and AVG together
    println!("[Bonus] Test 3: SELECT SUM(value), AVG(value) FROM test_data");
    let query3 = "SELECT SUM(value), AVG(value) FROM test_data";
    match engine.execute_query(query3) {
        Ok(result) => {
            println!("      ✅ Query executed successfully");
            println!("      Rows returned: {}", result.row_count);
            println!("      Execution time: {:.3} ms", result.execution_time_ms);
        }
        Err(e) => {
            println!("      ❌ Query failed: {:?}", e);
        }
    }
    println!();
    
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                    TEST COMPLETE                              ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    
    Ok(())
}

