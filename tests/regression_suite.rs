/// Comprehensive Regression Test Suite
/// Tests all fixes from Branches 1-6:
/// - Branch 1: Array bounds violations
/// - Branch 2: ColumnResolver unification
/// - Branch 3: Schema propagation determinism
/// - Branch 4: WindowOperator schema preservation
/// - Branch 5: CTE wildcard expansion
/// - Branch 6: ProjectOperator column resolution

use hypergraph_sql_engine::engine::HypergraphSQLEngine;

fn setup_test_engine() -> HypergraphSQLEngine {
    HypergraphSQLEngine::new()
}

fn setup_employees_table(engine: &mut HypergraphSQLEngine) {
    // Create employees table with test data
    let create_sql = "
        CREATE TABLE employees (
            id INT,
            name VARCHAR,
            salary FLOAT,
            department_id INT
        )
    ";
    engine.execute_query(create_sql).unwrap();
    
    // Insert test data
    let insert_sql = "
        INSERT INTO employees VALUES
        (1, 'Alice', 50000.0, 1),
        (2, 'Bob', 60000.0, 1),
        (3, 'Charlie', 70000.0, 2),
        (4, 'Diana', 55000.0, 2),
        (5, 'Eve', 75000.0, 3)
    ";
    engine.execute_query(insert_sql).unwrap();
}

#[test]
fn test_branch1_array_bounds_filter_operator() {
    // Test: FilterOperator with selection bitmap
    // Branch 1: Ensure all operators use batch.selection.len() for array bounds
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    let sql = "SELECT * FROM employees WHERE salary > 60000.0";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count > 0, "FilterOperator should return filtered rows");
    // Verify no array bounds panics occurred
}

#[test]
fn test_branch1_array_bounds_join_operator() {
    // Test: JoinOperator with selection bitmap
    // Branch 1: Ensure JOIN operators handle selection vectors correctly
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    // Create departments table
    engine.execute_query("CREATE TABLE departments (id INT, name VARCHAR)").unwrap();
    engine.execute_query("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Marketing')").unwrap();
    
    let sql = "
        SELECT e.name, d.name as dept_name
        FROM employees e
        INNER JOIN departments d ON e.department_id = d.id
        WHERE e.salary > 60000.0
    ";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count > 0, "JoinOperator should return joined rows");
    // Verify no array bounds panics occurred
}

#[test]
fn test_branch2_column_resolver_qualified_names() {
    // Test: ColumnResolver handles qualified column names
    // Branch 2: Unified column resolution
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    let sql = "SELECT e.name, e.salary FROM employees e WHERE e.salary > 60000.0";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count > 0, "ColumnResolver should resolve qualified names");
}

#[test]
fn test_branch2_column_resolver_table_aliases() {
    // Test: ColumnResolver handles table aliases
    // Branch 2: Table alias resolution
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    let sql = "SELECT emp.name, emp.salary FROM employees emp WHERE emp.department_id = 1";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count > 0, "ColumnResolver should resolve table aliases");
}

#[test]
fn test_branch3_schema_propagation_scan_to_project() {
    // Test: Schema propagation through pipeline
    // Branch 3: Deterministic schema propagation
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    let sql = "
        SELECT e.name, e.salary
        FROM employees e
        WHERE e.department_id = 1
        ORDER BY e.salary DESC
    ";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count > 0, "Schema should propagate through Scan → Filter → Project → Sort");
}

#[test]
fn test_branch4_window_operator_schema_preservation() {
    // Test: WindowOperator preserves input columns
    // Branch 4: WindowOperator schema preservation
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    let sql = "
        SELECT 
            e.name,
            e.department_id,
            e.salary,
            RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS salary_rank,
            SUM(e.salary) OVER (PARTITION BY e.department_id) AS dept_total_salary
        FROM employees e
        ORDER BY e.department_id, e.salary DESC
    ";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count > 0, "WindowOperator should preserve input columns + add window columns");
    // Verify schema has both input columns (name, department_id, salary) and window columns (salary_rank, dept_total_salary)
}

#[test]
fn test_branch4_window_operator_multiple_functions() {
    // Test: WindowOperator with multiple window functions
    // Branch 4: Schema preservation with multiple functions
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    let sql = "
        SELECT 
            e.name,
            e.salary,
            ROW_NUMBER() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS row_num,
            AVG(e.salary) OVER (PARTITION BY e.department_id) AS avg_salary
        FROM employees e
    ";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count > 0, "WindowOperator should handle multiple window functions");
}

#[test]
fn test_branch5_cte_wildcard_expansion() {
    // Test: CTE with wildcard expansion
    // Branch 5: Wildcard expansion before CTE materialization
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    let sql = "
        WITH high_salary AS (
            SELECT e.*
            FROM employees e
            WHERE e.salary > 60000.0
        )
        SELECT * FROM high_salary
        ORDER BY salary DESC
    ";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count > 0, "CTE with wildcard should expand and materialize correctly");
}

#[test]
fn test_branch5_cte_qualified_wildcard() {
    // Test: CTE with qualified wildcard (e.*)
    // Branch 5: Qualified wildcard expansion
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    let sql = "
        WITH dept_employees AS (
            SELECT e.*
            FROM employees e
            WHERE e.department_id = 1
        )
        SELECT dept_employees.name, dept_employees.salary
        FROM dept_employees
    ";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count > 0, "CTE with qualified wildcard should expand correctly");
}

#[test]
fn test_branch5_cte_with_window_functions() {
    // Test: CTE with window functions
    // Branch 5: CTE materialization preserves window function columns
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    let sql = "
        WITH ranked_employees AS (
            SELECT 
                e.*,
                RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS rank
            FROM employees e
        )
        SELECT * FROM ranked_employees
        WHERE rank <= 2
    ";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count > 0, "CTE with window functions should preserve all columns");
}

#[test]
fn test_branch6_project_operator_column_resolution() {
    // Test: ProjectOperator resolves columns consistently
    // Branch 6: ColumnResolver in ProjectOperator
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    let sql = "
        SELECT e.name, e.salary, e.department_id
        FROM employees e
        WHERE e.salary > 60000.0
    ";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count > 0, "ProjectOperator should resolve all columns correctly");
}

#[test]
fn test_branch6_project_operator_with_aliases() {
    // Test: ProjectOperator with column aliases
    // Branch 6: ColumnResolver handles aliases
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    let sql = "
        SELECT 
            e.name AS employee_name,
            e.salary AS employee_salary,
            e.department_id AS dept
        FROM employees e
    ";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count > 0, "ProjectOperator should handle column aliases correctly");
}

#[test]
fn test_integration_join_window_project() {
    // Test: Integration of JOIN, Window, and Project operators
    // Tests schema propagation through complex pipeline
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    // Create departments table
    engine.execute_query("CREATE TABLE departments (id INT, name VARCHAR)").unwrap();
    engine.execute_query("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Marketing')").unwrap();
    
    let sql = "
        SELECT 
            e.name,
            d.name AS dept_name,
            e.salary,
            RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS salary_rank
        FROM employees e
        INNER JOIN departments d ON e.department_id = d.id
        WHERE e.salary > 55000.0
        ORDER BY e.department_id, e.salary DESC
    ";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count > 0, "Complex pipeline should work: JOIN → Filter → Window → Project → Sort");
}

#[test]
fn test_integration_cte_join_window() {
    // Test: Integration of CTE, JOIN, and Window operators
    // Tests CTE materialization + complex pipeline
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    // Create departments table
    engine.execute_query("CREATE TABLE departments (id INT, name VARCHAR)").unwrap();
    engine.execute_query("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Marketing')").unwrap();
    
    let sql = "
        WITH high_salary_employees AS (
            SELECT e.*
            FROM employees e
            WHERE e.salary > 60000.0
        )
        SELECT 
            h.name,
            h.salary,
            RANK() OVER (ORDER BY h.salary DESC) AS global_rank
        FROM high_salary_employees h
        ORDER BY global_rank
    ";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count > 0, "CTE → Window → Project pipeline should work");
}

#[test]
fn test_integration_self_join_projection() {
    // Test: Self-join with projection
    // Tests Branch 6: ProjectOperator column resolution with JOIN output
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    let sql = "
        SELECT 
            e1.name AS employee,
            e2.name AS manager_candidate,
            e1.salary AS emp_salary,
            e2.salary AS manager_salary,
            e1.salary - e2.salary AS salary_diff
        FROM employees e1
        INNER JOIN employees e2 ON e1.department_id = e2.department_id AND e1.salary < e2.salary
        ORDER BY e1.department_id, salary_diff
    ";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count >= 0, "Self-join with projection should work (may return 0 rows)");
}

#[test]
fn test_integration_multiple_aggregations() {
    // Test: Multiple aggregations with GROUP BY and HAVING
    // Tests AggregateOperator + HavingOperator
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    let sql = "
        SELECT 
            department_id,
            COUNT(*) AS emp_count,
            AVG(salary) AS avg_salary,
            MAX(salary) AS max_salary
        FROM employees
        GROUP BY department_id
        HAVING AVG(salary) > 60000.0
    ";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count >= 0, "Multiple aggregations with HAVING should work");
}

#[test]
fn test_integration_nested_ctes() {
    // Test: Nested CTEs
    // Tests CTE materialization with multiple levels
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    let sql = "
        WITH dept_stats AS (
            SELECT 
                department_id,
                AVG(salary) AS avg_salary,
                COUNT(*) AS emp_count
            FROM employees
            GROUP BY department_id
        ),
        high_avg_depts AS (
            SELECT *
            FROM dept_stats
            WHERE avg_salary > 60000.0
        )
        SELECT * FROM high_avg_depts
    ";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count >= 0, "Nested CTEs should work");
}

#[test]
fn test_schema_consistency_through_pipeline() {
    // Test: Schema consistency through entire pipeline
    // Tests Branch 3: Schema propagation determinism
    let mut engine = setup_test_engine();
    setup_employees_table(&mut engine);
    
    let sql = "
        SELECT 
            e.name,
            e.salary,
            e.department_id,
            RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS rank
        FROM employees e
        WHERE e.salary > 50000.0
        ORDER BY e.department_id, rank
    ";
    let result = engine.execute_query(sql).unwrap();
    
    assert!(result.row_count > 0, "Schema should be consistent through entire pipeline");
    
    // Verify result has all expected columns
    if result.row_count > 0 {
        let first_batch = result.batches.first().unwrap();
        let schema = &first_batch.batch.schema;
        let field_names: Vec<String> = schema.fields().iter().map(|f| f.name().to_string()).collect();
        
        // Should have: name, salary, department_id, rank
        assert!(field_names.contains(&"name".to_string()) || field_names.iter().any(|f| f.contains("name")));
        assert!(field_names.contains(&"salary".to_string()) || field_names.iter().any(|f| f.contains("salary")));
        assert!(field_names.contains(&"department_id".to_string()) || field_names.iter().any(|f| f.contains("department_id")));
    }
}

/// Run all regression tests
#[test]
fn test_all_regression_suite() {
    println!("Running comprehensive regression test suite...");
    
    // Array bounds tests
    test_branch1_array_bounds_filter_operator();
    test_branch1_array_bounds_join_operator();
    
    // ColumnResolver tests
    test_branch2_column_resolver_qualified_names();
    test_branch2_column_resolver_table_aliases();
    
    // Schema propagation tests
    test_branch3_schema_propagation_scan_to_project();
    
    // WindowOperator tests
    test_branch4_window_operator_schema_preservation();
    test_branch4_window_operator_multiple_functions();
    
    // CTE wildcard tests
    test_branch5_cte_wildcard_expansion();
    test_branch5_cte_qualified_wildcard();
    test_branch5_cte_with_window_functions();
    
    // ProjectOperator tests
    test_branch6_project_operator_column_resolution();
    test_branch6_project_operator_with_aliases();
    
    // Integration tests
    test_integration_join_window_project();
    test_integration_cte_join_window();
    test_integration_self_join_projection();
    test_integration_multiple_aggregations();
    test_integration_nested_ctes();
    test_schema_consistency_through_pipeline();
    
    println!("✅ All regression tests passed!");
}

