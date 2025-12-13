use hypergraph_sql_engine::engine::HypergraphSQLEngine;

fn main() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║    Hypergraph SQL Engine - Easy to Medium SQL Test Suite   ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    
    // Create engine
    println!("[SETUP] Initializing engine...");
    let mut engine = HypergraphSQLEngine::new();
    println!("        ✅ Engine initialized");
    println!();
    
    // Create tables
    println!("[SETUP] Creating tables...");
    
    let create_employees = r#"
        CREATE TABLE employees (
            id INT,
            name VARCHAR,
            department_id INT,
            age INT,
            salary FLOAT,
            hire_date VARCHAR,
            manager_id INT
        )
    "#;
    engine.execute_query(create_employees)?;
    println!("        ✅ Table 'employees' created");
    
    let create_departments = r#"
        CREATE TABLE departments (
            id INT,
            name VARCHAR,
            budget FLOAT,
            location VARCHAR
        )
    "#;
    engine.execute_query(create_departments)?;
    println!("        ✅ Table 'departments' created");
    
    let create_projects = r#"
        CREATE TABLE projects (
            id INT,
            name VARCHAR,
            department_id INT,
            budget FLOAT,
            status VARCHAR
        )
    "#;
    engine.execute_query(create_projects)?;
    println!("        ✅ Table 'projects' created");
    println!();
    
    // Insert data
    println!("[SETUP] Inserting test data...");
    
    let insert_employees = r#"
        INSERT INTO employees (id, name, department_id, age, salary, hire_date, manager_id) VALUES
        (1, 'Alice', 1, 30, 75000.0, '2020-01-15', NULL),
        (2, 'Bob', 2, 25, 60000.0, '2021-03-20', 1),
        (3, 'Charlie', 1, 35, 90000.0, '2019-06-10', NULL),
        (4, 'Diana', 3, 28, 65000.0, '2022-02-14', 3),
        (5, 'Eve', 1, 32, 80000.0, '2020-11-05', 3),
        (6, 'Frank', 2, 29, 70000.0, '2021-08-30', 1),
        (7, 'Grace', 3, 27, 62000.0, '2022-05-12', 3),
        (8, 'Henry', 1, 40, 95000.0, '2018-04-22', NULL),
        (9, 'Iris', 2, 26, 58000.0, '2023-01-08', 1),
        (10, 'Jack', 3, 33, 72000.0, '2021-09-15', 3)
    "#;
    let result = engine.execute_query(insert_employees)?;
    println!("        ✅ Inserted {} employees", result.row_count);
    
    let insert_departments = r#"
        INSERT INTO departments (id, name, budget, location) VALUES
        (1, 'Engineering', 500000.0, 'San Francisco'),
        (2, 'Sales', 300000.0, 'New York'),
        (3, 'Marketing', 250000.0, 'Los Angeles')
    "#;
    let result = engine.execute_query(insert_departments)?;
    println!("        ✅ Inserted {} departments", result.row_count);
    
    let insert_projects = r#"
        INSERT INTO projects (id, name, department_id, budget, status) VALUES
        (1, 'Project Alpha', 1, 100000.0, 'Active'),
        (2, 'Project Beta', 1, 150000.0, 'Active'),
        (3, 'Project Gamma', 2, 80000.0, 'Completed'),
        (4, 'Project Delta', 3, 60000.0, 'Active')
    "#;
    let result = engine.execute_query(insert_projects)?;
    println!("        ✅ Inserted {} projects", result.row_count);
    println!();
    
    // Test Suite - Easy to Medium SQL Queries
    let mut passed = 0;
    let mut failed = 0;
    let mut test_num = 0;
    
    // ========== EASY QUERIES ==========
    
    // Test 1: Simple SELECT
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: Simple SELECT (EASY)                                 ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = "SELECT id, name, salary FROM employees LIMIT 5";
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 2: WHERE clause with single condition
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: WHERE clause - single condition (EASY)              ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = "SELECT name, salary FROM employees WHERE salary > 70000";
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 3: WHERE with AND
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: WHERE with AND (EASY)                              ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = "SELECT name, age, salary FROM employees WHERE age > 30 AND salary > 75000";
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 4: ORDER BY single column
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: ORDER BY single column (EASY)                      ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = "SELECT name, salary FROM employees ORDER BY salary DESC";
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 5: ORDER BY multiple columns
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: ORDER BY multiple columns (EASY)                    ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = "SELECT name, department_id, salary FROM employees ORDER BY department_id ASC, salary DESC";
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 6: WHERE with BETWEEN
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: WHERE with BETWEEN (EASY)                          ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = "SELECT name, salary FROM employees WHERE salary BETWEEN 65000 AND 80000";
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 7: WHERE with IN
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: WHERE with IN clause (EASY)                        ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = "SELECT name, department_id FROM employees WHERE department_id IN (1, 3)";
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // ========== MEDIUM QUERIES ==========
    
    // Test 8: Simple JOIN
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: Simple JOIN (MEDIUM)                               ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = r#"
        SELECT e.name, e.salary, d.name as department
        FROM employees e
        JOIN departments d ON e.department_id = d.id
    "#;
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 9: JOIN with WHERE
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: JOIN with WHERE (MEDIUM)                           ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = r#"
        SELECT e.name, e.salary, d.name as department
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        WHERE e.salary > 70000 AND d.budget > 300000
    "#;
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 10: Multiple JOINs
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: Multiple JOINs (MEDIUM)                            ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = r#"
        SELECT e.name, d.name as dept, p.name as project
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        JOIN projects p ON d.id = p.department_id
    "#;
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 11: LEFT JOIN
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: LEFT JOIN (MEDIUM)                                  ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = r#"
        SELECT d.name, e.name as employee
        FROM departments d
        LEFT JOIN employees e ON d.id = e.department_id
    "#;
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 12: Simple GROUP BY
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: Simple GROUP BY (MEDIUM)                             ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = "SELECT department_id, COUNT(*) as count FROM employees GROUP BY department_id";
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 13: GROUP BY with AVG
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: GROUP BY with AVG (MEDIUM)                         ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = "SELECT department_id, AVG(salary) as avg_salary FROM employees GROUP BY department_id";
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 14: GROUP BY with multiple aggregates
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: GROUP BY with multiple aggregates (MEDIUM)        ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = r#"
        SELECT department_id, 
               COUNT(*) as count, 
               AVG(salary) as avg_salary, 
               MAX(salary) as max_salary, 
               MIN(salary) as min_salary
        FROM employees
        GROUP BY department_id
    "#;
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 15: WHERE with OR
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: WHERE with OR (MEDIUM)                              ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = "SELECT name, age, salary FROM employees WHERE age > 35 OR salary > 90000";
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 16: Complex WHERE with nested conditions
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: Complex WHERE with nested conditions (MEDIUM)       ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = r#"
        SELECT name, department_id, age, salary
        FROM employees
        WHERE (age > 30 AND salary > 75000) OR (age < 28 AND salary < 65000)
    "#;
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 17: JOIN with ORDER BY
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: JOIN with ORDER BY (MEDIUM)                         ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = r#"
        SELECT e.name, e.salary, d.name as department
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        ORDER BY e.salary DESC
    "#;
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Test 18: GROUP BY with ORDER BY
    test_num += 1;
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST {}: GROUP BY with ORDER BY (MEDIUM)                    ║", test_num);
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query = r#"
        SELECT department_id, AVG(salary) as avg_salary
        FROM employees
        GROUP BY department_id
        ORDER BY avg_salary DESC
    "#;
    match engine.execute_query(query) {
        Ok(result) => {
            println!("✅ PASSED: {} rows, {:.3} ms", result.row_count, result.execution_time_ms);
            passed += 1;
        }
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            failed += 1;
        }
    }
    println!();
    
    // Summary
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║              EASY TO MEDIUM SQL TEST SUMMARY                ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("Total Tests: {}", passed + failed);
    println!("✅ Passed: {}", passed);
    println!("❌ Failed: {}", failed);
    let success_rate = (passed as f64 / (passed + failed) as f64) * 100.0;
    println!("Success Rate: {:.1}%", success_rate);
    println!();
    
    if success_rate >= 90.0 {
        println!("🎉 EXCELLENT! Easy to medium SQL queries are working well!");
    } else if success_rate >= 70.0 {
        println!("✅ GOOD! Most easy to medium SQL queries are working.");
    } else {
        println!("⚠️  Some easy to medium SQL queries need attention.");
    }
    println!();
    
    Ok(())
}

