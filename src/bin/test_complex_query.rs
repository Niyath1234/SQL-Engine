use hypergraph_sql_engine::engine::HypergraphSQLEngine;

fn main() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║      Hypergraph SQL Engine - Complex Query Test             ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    
    // Create engine
    println!("[1/6] Initializing engine...");
    let mut engine = HypergraphSQLEngine::new();
    println!("      ✅ Engine initialized");
    println!();
    
    // Create employees table
    println!("[2/6] Creating 'employees' table...");
    let create_employees = r#"
        CREATE TABLE employees (
            id INT,
            name VARCHAR,
            department_id INT,
            age INT,
            salary FLOAT,
            hire_date VARCHAR
        )
    "#;
    
    match engine.execute_query(create_employees) {
        Ok(_) => println!("      ✅ Table 'employees' created"),
        Err(e) => println!("      ⚠️  Employees table: {:?}", e),
    }
    println!();
    
    // Create departments table
    println!("[3/6] Creating 'departments' table...");
    let create_departments = r#"
        CREATE TABLE departments (
            id INT,
            name VARCHAR,
            budget FLOAT,
            location VARCHAR
        )
    "#;
    
    match engine.execute_query(create_departments) {
        Ok(_) => println!("      ✅ Table 'departments' created"),
        Err(e) => println!("      ⚠️  Departments table: {:?}", e),
    }
    println!();
    
    // Insert employees data
    println!("[4/6] Inserting employees data...");
    let insert_employees = r#"
        INSERT INTO employees (id, name, department_id, age, salary, hire_date) VALUES
        (1, 'Alice', 1, 30, 75000.0, '2020-01-15'),
        (2, 'Bob', 2, 25, 60000.0, '2021-03-20'),
        (3, 'Charlie', 1, 35, 90000.0, '2019-06-10'),
        (4, 'Diana', 3, 28, 65000.0, '2022-02-14'),
        (5, 'Eve', 1, 32, 80000.0, '2020-11-05'),
        (6, 'Frank', 2, 29, 70000.0, '2021-08-30'),
        (7, 'Grace', 3, 27, 62000.0, '2022-05-12'),
        (8, 'Henry', 1, 40, 95000.0, '2018-04-22'),
        (9, 'Iris', 2, 26, 58000.0, '2023-01-08'),
        (10, 'Jack', 3, 33, 72000.0, '2021-09-15')
    "#;
    
    match engine.execute_query(insert_employees) {
        Ok(result) => println!("      ✅ Inserted {} employees", result.row_count),
        Err(e) => println!("      ⚠️  Insert error: {:?}", e),
    }
    println!();
    
    // Insert departments data
    println!("[5/6] Inserting departments data...");
    let insert_departments = r#"
        INSERT INTO departments (id, name, budget, location) VALUES
        (1, 'Engineering', 500000.0, 'San Francisco'),
        (2, 'Sales', 300000.0, 'New York'),
        (3, 'Marketing', 250000.0, 'Los Angeles')
    "#;
    
    match engine.execute_query(insert_departments) {
        Ok(result) => println!("      ✅ Inserted {} departments", result.row_count),
        Err(e) => println!("      ⚠️  Insert error: {:?}", e),
    }
    println!();
    
    // Complex Query 1: JOIN with multiple conditions
    println!("[6/6] Executing COMPLEX QUERY 1: JOIN with multiple WHERE conditions...");
    println!("      SQL: SELECT e.name, e.salary, d.name, d.budget");
    println!("           FROM employees e");
    println!("           JOIN departments d ON e.department_id = d.id");
    println!("           WHERE e.salary > 65000 AND e.age > 28 AND d.budget > 250000");
    println!("           ORDER BY e.salary DESC");
    println!();
    
    let complex_query1 = r#"
        SELECT e.name, e.salary, d.name, d.budget
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        WHERE e.salary > 65000 AND e.age > 28 AND d.budget > 250000
        ORDER BY e.salary DESC
    "#;
    
    match engine.execute_query(complex_query1) {
        Ok(result) => {
            println!("╔══════════════════════════════════════════════════════════════╗");
            println!("║              COMPLEX QUERY 1 RESULTS                        ║");
            println!("╚══════════════════════════════════════════════════════════════╝");
            println!();
            println!("Rows returned: {}", result.row_count);
            println!("Execution time: {:.3} ms", result.execution_time_ms);
            println!();
            
            if !result.batches.is_empty() {
                let batch = &result.batches[0];
                let field_names: Vec<String> = batch.batch.schema.fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                println!("Columns: {:?}", field_names);
                println!();
                println!("✅ Complex Query 1 executed successfully!");
            }
        }
        Err(e) => {
            println!("❌ Complex Query 1 failed:");
            println!("   Error: {:?}", e);
        }
    }
    println!();
    
    // Complex Query 2: Aggregation with GROUP BY
    println!("[BONUS] Executing COMPLEX QUERY 2: Aggregation with GROUP BY...");
    println!("        SQL: SELECT department_id, COUNT(*), AVG(salary), MAX(salary)");
    println!("             FROM employees");
    println!("             WHERE salary > 60000");
    println!("             GROUP BY department_id");
    println!("             ORDER BY AVG(salary) DESC");
    println!();
    
    let complex_query2 = r#"
        SELECT department_id, COUNT(*), AVG(salary), MAX(salary)
        FROM employees
        WHERE salary > 60000
        GROUP BY department_id
        ORDER BY AVG(salary) DESC
    "#;
    
    match engine.execute_query(complex_query2) {
        Ok(result) => {
            println!("╔══════════════════════════════════════════════════════════════╗");
            println!("║              COMPLEX QUERY 2 RESULTS                        ║");
            println!("╚══════════════════════════════════════════════════════════════╝");
            println!();
            println!("Rows returned: {}", result.row_count);
            println!("Execution time: {:.3} ms", result.execution_time_ms);
            println!();
            
            if !result.batches.is_empty() {
                let batch = &result.batches[0];
                let field_names: Vec<String> = batch.batch.schema.fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                println!("Columns: {:?}", field_names);
                println!();
                println!("✅ Complex Query 2 executed successfully!");
            }
        }
        Err(e) => {
            println!("❌ Complex Query 2 failed:");
            println!("   Error: {:?}", e);
        }
    }
    println!();
    
    // Complex Query 3: Multiple table JOIN with complex filtering
    println!("[BONUS] Executing COMPLEX QUERY 3: Multi-table JOIN with range conditions...");
    println!("        SQL: SELECT e.name, e.age, e.salary, d.name, d.location");
    println!("             FROM employees e");
    println!("             JOIN departments d ON e.department_id = d.id");
    println!("             WHERE e.age BETWEEN 28 AND 35");
    println!("               AND e.salary BETWEEN 65000 AND 85000");
    println!("               AND d.budget > 200000");
    println!("             ORDER BY e.salary DESC, e.name");
    println!();
    
    let complex_query3 = r#"
        SELECT e.name, e.age, e.salary, d.name, d.location
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        WHERE e.age BETWEEN 28 AND 35
          AND e.salary BETWEEN 65000 AND 85000
          AND d.budget > 200000
        ORDER BY e.salary DESC, e.name
    "#;
    
    match engine.execute_query(complex_query3) {
        Ok(result) => {
            println!("╔══════════════════════════════════════════════════════════════╗");
            println!("║              COMPLEX QUERY 3 RESULTS                        ║");
            println!("╚══════════════════════════════════════════════════════════════╝");
            println!();
            println!("Rows returned: {}", result.row_count);
            println!("Execution time: {:.3} ms", result.execution_time_ms);
            println!();
            
            if !result.batches.is_empty() {
                let batch = &result.batches[0];
                let field_names: Vec<String> = batch.batch.schema.fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                println!("Columns: {:?}", field_names);
                println!();
                println!("✅ Complex Query 3 executed successfully!");
            }
        }
        Err(e) => {
            println!("❌ Complex Query 3 failed:");
            println!("   Error: {:?}", e);
        }
    }
    println!();
    
    // Complex Query 4: Nested conditions with OR
    println!("[BONUS] Executing COMPLEX QUERY 4: Complex WHERE with OR conditions...");
    println!("        SQL: SELECT name, department_id, age, salary");
    println!("             FROM employees");
    println!("             WHERE (age > 30 AND salary > 70000) OR (age < 28 AND salary < 65000)");
    println!("             ORDER BY department_id, salary DESC");
    println!();
    
    let complex_query4 = r#"
        SELECT name, department_id, age, salary
        FROM employees
        WHERE (age > 30 AND salary > 70000) OR (age < 28 AND salary < 65000)
        ORDER BY department_id, salary DESC
    "#;
    
    match engine.execute_query(complex_query4) {
        Ok(result) => {
            println!("╔══════════════════════════════════════════════════════════════╗");
            println!("║              COMPLEX QUERY 4 RESULTS                        ║");
            println!("╚══════════════════════════════════════════════════════════════╝");
            println!();
            println!("Rows returned: {}", result.row_count);
            println!("Execution time: {:.3} ms", result.execution_time_ms);
            println!();
            
            if !result.batches.is_empty() {
                let batch = &result.batches[0];
                let field_names: Vec<String> = batch.batch.schema.fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                println!("Columns: {:?}", field_names);
                println!();
                println!("✅ Complex Query 4 executed successfully!");
            }
        }
        Err(e) => {
            println!("❌ Complex Query 4 failed:");
            println!("   Error: {:?}", e);
        }
    }
    println!();
    
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║              ALL COMPLEX QUERIES COMPLETE                    ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    
    Ok(())
}
