//! Basic usage example for Hypergraph SQL Engine
//! 
//! Run with: `cargo run --example basic_usage`

use hypergraph_sql_engine::HypergraphSQLEngine;

fn main() -> anyhow::Result<()> {
    println!("Hypergraph SQL Engine - Basic Usage Example");
    println!("==========================================\n");

    // Create a new engine instance
    let mut engine = HypergraphSQLEngine::new();
    println!("✅ Engine initialized\n");

    // Create a table
    println!("Creating 'employees' table...");
    engine.execute_query(
        r#"
        CREATE TABLE employees (
            id INT,
            name VARCHAR,
            department_id INT,
            age INT,
            salary FLOAT
        )
        "#
    )?;
    println!("✅ Table created\n");

    // Insert data
    println!("Inserting sample data...");
    engine.execute_query(
        r#"
        INSERT INTO employees (id, name, department_id, age, salary) VALUES
        (1, 'Alice', 1, 30, 75000.0),
        (2, 'Bob', 2, 25, 60000.0),
        (3, 'Charlie', 1, 35, 90000.0),
        (4, 'Diana', 3, 28, 65000.0),
        (5, 'Eve', 1, 32, 80000.0)
        "#
    )?;
    println!("✅ Data inserted\n");

    // Simple SELECT query
    println!("Query 1: Simple SELECT");
    let result = engine.execute_query(
        "SELECT id, name, salary FROM employees"
    )?;
    println!("   Found {} rows in {:.3} ms\n", result.row_count, result.execution_time_ms);

    // WHERE clause
    println!("Query 2: WHERE clause");
    let result = engine.execute_query(
        "SELECT name, salary FROM employees WHERE salary > 70000"
    )?;
    println!("   Found {} rows in {:.3} ms\n", result.row_count, result.execution_time_ms);

    // GROUP BY aggregation
    println!("Query 3: GROUP BY with aggregation");
    let result = engine.execute_query(
        "SELECT department_id, COUNT(*) as count, AVG(salary) as avg_salary FROM employees GROUP BY department_id"
    )?;
    println!("   Found {} rows in {:.3} ms\n", result.row_count, result.execution_time_ms);

    // ORDER BY
    println!("Query 4: ORDER BY");
    let result = engine.execute_query(
        "SELECT name, salary FROM employees ORDER BY salary DESC"
    )?;
    println!("   Found {} rows in {:.3} ms\n", result.row_count, result.execution_time_ms);

    println!("✅ All queries executed successfully!");
    Ok(())
}

