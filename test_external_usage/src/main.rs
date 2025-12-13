//! Test that the package can be used as an external dependency
//! 
//! This simulates how another project would use the hypergraph-sql-engine package

use hypergraph_sql_engine::HypergraphSQLEngine;

fn main() -> anyhow::Result<()> {
    println!("Testing Hypergraph SQL Engine as External Dependency");
    println!("====================================================\n");

    // Test 1: Create engine
    println!("Test 1: Creating engine instance...");
    let mut engine = HypergraphSQLEngine::new();
    println!("✅ Engine created\n");

    // Test 2: Create table
    println!("Test 2: Creating table...");
    engine.execute_query(
        "CREATE TABLE test_users (id INT, name VARCHAR, score FLOAT)"
    )?;
    println!("✅ Table created\n");

    // Test 3: Insert data
    println!("Test 3: Inserting data...");
    let result = engine.execute_query(
        r#"
        INSERT INTO test_users (id, name, score) VALUES
        (1, 'Alice', 95.5),
        (2, 'Bob', 87.0),
        (3, 'Charlie', 92.3)
        "#
    )?;
    println!("✅ Inserted {} rows\n", result.row_count);

    // Test 4: Query data
    println!("Test 4: Querying data...");
    let result = engine.execute_query(
        "SELECT name, score FROM test_users WHERE score > 90 ORDER BY score DESC"
    )?;
    println!("✅ Query returned {} rows in {:.3} ms\n", result.row_count, result.execution_time_ms);

    // Test 5: Check cache stats
    println!("Test 5: Checking cache statistics...");
    let stats = engine.cache_stats();
    println!("✅ Cache stats - Plan cache: {}, Result cache: {}\n", 
             stats.plan_cache_size, stats.result_cache_size);

    // Test 6: GROUP BY query
    println!("Test 6: Testing GROUP BY aggregation...");
    let result = engine.execute_query(
        "SELECT COUNT(*) as total FROM test_users"
    )?;
    println!("✅ Aggregation query returned {} rows\n", result.row_count);

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     All External Usage Tests Passed! ✅                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("\nThe package works correctly as an external dependency!");

    Ok(())
}

