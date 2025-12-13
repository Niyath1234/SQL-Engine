use hypergraph_sql_engine::engine::HypergraphSQLEngine;

fn main() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║    Hypergraph SQL Engine - Extensive Query Test Suite      ║");
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
        (10, 'Jack', 3, 33, 72000.0, '2021-09-15', 3),
        (11, 'Karen', 1, 31, 78000.0, '2020-07-20', 3),
        (12, 'Liam', 2, 24, 55000.0, '2023-03-10', 1),
        (13, 'Mia', 1, 36, 88000.0, '2019-02-18', NULL),
        (14, 'Noah', 3, 29, 68000.0, '2021-11-25', 3),
        (15, 'Olivia', 2, 28, 64000.0, '2022-01-30', 1)
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
        (4, 'Project Delta', 3, 60000.0, 'Active'),
        (5, 'Project Epsilon', 1, 120000.0, 'Planning'),
        (6, 'Project Zeta', 2, 70000.0, 'Active')
    "#;
    let result = engine.execute_query(insert_projects)?;
    println!("        ✅ Inserted {} projects", result.row_count);
    println!();
    
    // Test Suite
    let mut passed = 0;
    let mut failed = 0;
    
    // Test 1: Multiple JOINs
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST 1: Multiple Table JOINs                                ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query1 = r#"
        SELECT e.name, d.name as dept, p.name as project, e.salary
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        JOIN projects p ON d.id = p.department_id
        WHERE e.salary > 70000 AND p.status = 'Active'
        ORDER BY e.salary DESC
    "#;
    match engine.execute_query(query1) {
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
    
    // Test 2: Multiple GROUP BY columns
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST 2: GROUP BY Multiple Columns                          ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query2 = r#"
        SELECT department_id, age, COUNT(*), AVG(salary), MAX(salary)
        FROM employees
        WHERE salary > 60000
        GROUP BY department_id, age
        HAVING COUNT(*) >= 1
        ORDER BY department_id, age
    "#;
    match engine.execute_query(query2) {
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
    
    // Test 3: Complex WHERE with multiple conditions
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST 3: Complex WHERE with Nested Conditions             ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query3 = r#"
        SELECT name, department_id, age, salary
        FROM employees
        WHERE (age > 30 AND salary > 75000) 
           OR (age < 28 AND salary < 65000)
           OR (department_id = 1 AND salary BETWEEN 70000 AND 90000)
        ORDER BY department_id, salary DESC
    "#;
    match engine.execute_query(query3) {
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
    
    // Test 4: Aggregation with HAVING
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST 4: GROUP BY with HAVING Clause                        ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query4 = r#"
        SELECT department_id, COUNT(*) as emp_count, AVG(salary) as avg_sal, SUM(salary) as total
        FROM employees
        GROUP BY department_id
        HAVING COUNT(*) > 3 AND AVG(salary) > 65000
        ORDER BY avg_sal DESC
    "#;
    match engine.execute_query(query4) {
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
    
    // Test 5: JOIN with Aggregation
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST 5: JOIN with GROUP BY Aggregation                      ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query5 = r#"
        SELECT d.name, COUNT(e.id) as employees, AVG(e.salary) as avg_salary, SUM(e.salary) as payroll
        FROM departments d
        LEFT JOIN employees e ON d.id = e.department_id
        GROUP BY d.id, d.name
        ORDER BY employees DESC
    "#;
    match engine.execute_query(query5) {
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
    
    // Test 6: Multiple ORDER BY columns
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST 6: Multiple ORDER BY Columns                           ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query6 = r#"
        SELECT name, department_id, age, salary
        FROM employees
        WHERE salary > 60000
        ORDER BY department_id ASC, salary DESC, name ASC
    "#;
    match engine.execute_query(query6) {
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
    
    // Test 7: Complex aggregation functions
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST 7: Multiple Aggregation Functions                      ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query7 = r#"
        SELECT 
            department_id,
            COUNT(*) as total,
            AVG(salary) as avg_salary,
            MIN(salary) as min_salary,
            MAX(salary) as max_salary,
            SUM(salary) as total_payroll
        FROM employees
        GROUP BY department_id
        ORDER BY avg_salary DESC
    "#;
    match engine.execute_query(query7) {
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
    
    // Test 8: Self-join (employees and managers)
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST 8: Self-JOIN (Employee-Manager)                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query8 = r#"
        SELECT e.name as employee, m.name as manager, e.salary, m.salary as manager_salary
        FROM employees e
        LEFT JOIN employees m ON e.manager_id = m.id
        WHERE e.salary > 65000
        ORDER BY e.salary DESC
    "#;
    match engine.execute_query(query8) {
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
    
    // Test 9: Complex filtering with IN
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST 9: Complex Filtering with Multiple Conditions         ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query9 = r#"
        SELECT e.name, e.age, e.salary, d.name as dept, d.location
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        WHERE e.age IN (28, 30, 32, 35)
          AND e.salary > 65000
          AND d.budget > 200000
          AND e.name LIKE '%a%'
        ORDER BY e.salary DESC
    "#;
    match engine.execute_query(query9) {
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
    
    // Test 10: Aggregation with JOIN and HAVING
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║ TEST 10: JOIN + GROUP BY + HAVING                            ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    let query10 = r#"
        SELECT d.name, COUNT(e.id) as emp_count, AVG(e.salary) as avg_sal, d.budget
        FROM departments d
        LEFT JOIN employees e ON d.id = e.department_id
        WHERE d.budget > 200000
        GROUP BY d.id, d.name, d.budget
        HAVING COUNT(e.id) > 4
        ORDER BY avg_sal DESC
    "#;
    match engine.execute_query(query10) {
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
    println!("║                    TEST SUMMARY                              ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("Total Tests: {}", passed + failed);
    println!("✅ Passed: {}", passed);
    println!("❌ Failed: {}", failed);
    println!("Success Rate: {:.1}%", (passed as f64 / (passed + failed) as f64) * 100.0);
    println!();
    
    if failed == 0 {
        println!("🎉 ALL TESTS PASSED! 🎉");
    } else {
        println!("⚠️  Some tests failed. Review errors above.");
    }
    println!();
    
    Ok(())
}

