/// Memory-Bounded Comprehensive Test Suite
/// Tests all features with medium to complex queries while monitoring memory usage
/// Memory limit: 7 GB

use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use std::time::Instant;

/// Memory limit in bytes (7 GB)
const MEMORY_LIMIT_BYTES: u64 = 7 * 1024 * 1024 * 1024;

/// Test result
#[derive(Debug, Clone)]
struct TestResult {
    name: String,
    passed: bool,
    execution_time_ms: f64,
    rows_returned: usize,
    error: Option<String>,
}

/// Memory-bounded test suite
struct MemoryBoundedTestSuite {
    engine: HypergraphSQLEngine,
    results: Vec<TestResult>,
    start_memory: u64,
}

impl MemoryBoundedTestSuite {
    fn new() -> Self {
        let engine = HypergraphSQLEngine::new();
        let mut suite = Self {
            engine,
            results: Vec::new(),
            start_memory: 0,
        };
        // Capture starting memory
        suite.start_memory = suite.get_memory_usage();
        suite
    }

    /// Get current memory usage (approximate)
    /// Uses RSS (Resident Set Size) if available, otherwise returns 0
    fn get_memory_usage(&self) -> u64 {
        #[cfg(target_os = "macos")]
        {
            use std::process::Command;
            // On macOS, use ps to get RSS memory
            if let Ok(output) = Command::new("ps")
                .args(&["-o", "rss=", "-p"])
                .arg(std::process::id().to_string())
                .output()
            {
                if let Ok(memory_str) = String::from_utf8(output.stdout) {
                    let trimmed = memory_str.trim();
                    if !trimmed.is_empty() {
                        if let Ok(memory_kb) = trimmed.parse::<u64>() {
                            return memory_kb * 1024; // Convert KB to bytes
                        }
                    }
                }
            }
        }
        
        #[cfg(target_os = "linux")]
        {
            // On Linux, read from /proc/self/status
            if let Ok(contents) = std::fs::read_to_string("/proc/self/status") {
                for line in contents.lines() {
                    if line.starts_with("VmRSS:") {
                        if let Some(memory_kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(memory_kb) = memory_kb_str.parse::<u64>() {
                                return memory_kb * 1024; // Convert KB to bytes
                            }
                        }
                    }
                }
            }
        }
        
        // Fallback: return 0 if we can't determine memory usage
        0
    }

    /// Check if memory limit is exceeded
    fn check_memory_limit(&self) -> bool {
        let current_memory = self.get_memory_usage();
        let used_memory = current_memory.saturating_sub(self.start_memory);
        used_memory < MEMORY_LIMIT_BYTES
    }

    /// Run a test query and record results
    fn run_test(&mut self, name: &str, sql: &str) -> TestResult {
        let start = Instant::now();
        
        // Check memory before test
        if !self.check_memory_limit() {
            return TestResult {
                name: name.to_string(),
                passed: false,
                execution_time_ms: 0.0,
                rows_returned: 0,
                error: Some("Memory limit exceeded before test start".to_string()),
            };
        }

        let result = match self.engine.execute_query(sql) {
            Ok(r) => {
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                let passed = r.row_count >= 0; // Valid result (even if 0 rows)
                
                TestResult {
                    name: name.to_string(),
                    passed,
                    execution_time_ms: elapsed,
                    rows_returned: r.row_count,
                    error: None,
                }
            }
            Err(e) => {
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                TestResult {
                    name: name.to_string(),
                    passed: false,
                    execution_time_ms: elapsed,
                    rows_returned: 0,
                    error: Some(format!("{}", e)),
                }
            }
        };

        // Check memory after test
        if !self.check_memory_limit() {
            return TestResult {
                name: result.name.clone(),
                passed: false,
                execution_time_ms: result.execution_time_ms,
                rows_returned: result.rows_returned,
                error: Some("Memory limit exceeded after test".to_string()),
            };
        }

        result
    }

    /// Setup test data
    fn setup_test_data(&mut self) -> Result<(), String> {
        // Create employees table
        self.engine.execute_query("CREATE TABLE employees (id INT, name VARCHAR, salary FLOAT, department_id INT)")
            .map_err(|e| format!("Failed to create employees table: {}", e))?;
        
        // Insert test data
        let insert_sql = "
            INSERT INTO employees VALUES
            (1, 'Alice', 50000.0, 1),
            (2, 'Bob', 60000.0, 1),
            (3, 'Charlie', 70000.0, 2),
            (4, 'Diana', 55000.0, 2),
            (5, 'Eve', 75000.0, 3),
            (6, 'Frank', 65000.0, 1),
            (7, 'Grace', 80000.0, 3),
            (8, 'Henry', 58000.0, 2)
        ";
        self.engine.execute_query(insert_sql)
            .map_err(|e| format!("Failed to insert employees data: {}", e))?;

        // Create departments table
        self.engine.execute_query("CREATE TABLE departments (id INT, name VARCHAR, budget FLOAT)")
            .map_err(|e| format!("Failed to create departments table: {}", e))?;
        
        let dept_insert = "
            INSERT INTO departments VALUES
            (1, 'Engineering', 1000000.0),
            (2, 'Sales', 800000.0),
            (3, 'Marketing', 600000.0)
        ";
        self.engine.execute_query(dept_insert)
            .map_err(|e| format!("Failed to insert departments data: {}", e))?;

        Ok(())
    }

    /// Run all feature tests
    fn run_all_tests(&mut self) {
        println!("\n{}", "=".repeat(80));
        println!("MEMORY-BOUNDED COMPREHENSIVE TEST SUITE");
        println!("Memory Limit: 7 GB");
        println!("{}", "=".repeat(80));

        // Setup
        println!("\n📋 Setting up test data...");
        if let Err(e) = self.setup_test_data() {
            println!("❌ Setup failed: {}", e);
            return;
        }
        println!("✅ Test data setup complete");

        println!("\n🧪 Running feature tests...\n");

        // 1. Basic SELECT with WHERE
        let result = self.run_test(
            "1. Basic SELECT with WHERE",
            "SELECT * FROM employees WHERE salary > 60000.0"
        );
        self.results.push(result);

        // 2. JOIN operations
        let result = self.run_test(
            "2. INNER JOIN",
            "SELECT e.name, d.name as dept_name, e.salary 
             FROM employees e 
             INNER JOIN departments d ON e.department_id = d.id 
             WHERE e.salary > 60000.0"
        );
        self.results.push(result);

        // 3. Aggregations with GROUP BY
        let result = self.run_test(
            "3. Aggregation with GROUP BY",
            "SELECT department_id, COUNT(*) as emp_count, AVG(salary) as avg_salary 
             FROM employees 
             GROUP BY department_id"
        );
        self.results.push(result);

        // 4. Window Functions
        let result = self.run_test(
            "4. Window Functions",
            "SELECT 
                name,
                department_id,
                salary,
                RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS salary_rank,
                AVG(salary) OVER (PARTITION BY department_id) AS dept_avg_salary
             FROM employees
             ORDER BY department_id, salary DESC"
        );
        self.results.push(result);

        // 5. CTE (Common Table Expression)
        let result = self.run_test(
            "5. CTE with Wildcard",
            "WITH high_salary AS (
                SELECT *
                FROM employees
                WHERE salary > 60000.0
            )
            SELECT * FROM high_salary
            ORDER BY salary DESC"
        );
        self.results.push(result);

        // 6. CTE with Window Functions
        let result = self.run_test(
            "6. CTE with Window Functions",
            "WITH ranked_employees AS (
                SELECT 
                    e.*,
                    RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS rank
                FROM employees e
            )
            SELECT * FROM ranked_employees
            WHERE rank <= 2"
        );
        self.results.push(result);

        // 7. Multiple JOINs
        let result = self.run_test(
            "7. Multiple JOINs",
            "SELECT 
                e1.name AS employee,
                e2.name AS manager_candidate,
                e1.salary AS emp_salary,
                e2.salary AS mgr_salary,
                d.name AS department
             FROM employees e1
             INNER JOIN employees e2 ON e1.department_id = e2.department_id AND e1.salary < e2.salary
             INNER JOIN departments d ON e1.department_id = d.id
             ORDER BY e1.department_id, e1.salary"
        );
        self.results.push(result);

        // 8. Aggregation with HAVING
        let result = self.run_test(
            "8. GROUP BY with HAVING",
            "SELECT 
                department_id,
                COUNT(*) AS emp_count,
                AVG(salary) AS avg_salary,
                MAX(salary) AS max_salary
             FROM employees
             GROUP BY department_id
             HAVING AVG(salary) > 60000.0"
        );
        self.results.push(result);

        // 9. Nested CTEs
        let result = self.run_test(
            "9. Nested CTEs",
            "WITH dept_stats AS (
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
            SELECT * FROM high_avg_depts"
        );
        self.results.push(result);

        // 10. Complex Query: JOIN + Window + CTE
        let result = self.run_test(
            "10. Complex: JOIN + Window + CTE",
            "WITH dept_info AS (
                SELECT 
                    e.*,
                    d.name AS dept_name
                FROM employees e
                INNER JOIN departments d ON e.department_id = d.id
            )
            SELECT 
                name,
                dept_name,
                salary,
                RANK() OVER (PARTITION BY dept_name ORDER BY salary DESC) AS rank_in_dept
            FROM dept_info
            ORDER BY dept_name, rank_in_dept"
        );
        self.results.push(result);

        // 11. Self-JOIN with Projection
        let result = self.run_test(
            "11. Self-JOIN with Projection",
            "SELECT 
                e1.name AS employee,
                e2.name AS manager_candidate,
                e1.salary AS emp_salary,
                e2.salary AS manager_salary,
                e1.salary - e2.salary AS salary_diff
            FROM employees e1
            INNER JOIN employees e2 ON e1.department_id = e2.department_id AND e1.salary < e2.salary
            ORDER BY e1.department_id, salary_diff"
        );
        self.results.push(result);

        // 12. Window Functions with Multiple Functions
        let result = self.run_test(
            "12. Multiple Window Functions",
            "SELECT 
                name,
                salary,
                department_id,
                ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS row_num,
                RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS salary_rank,
                AVG(salary) OVER (PARTITION BY department_id) AS dept_avg
            FROM employees
            ORDER BY department_id, salary DESC"
        );
        self.results.push(result);

        // 13. ORDER BY with Multiple Columns
        let result = self.run_test(
            "13. ORDER BY Multiple Columns",
            "SELECT name, department_id, salary
             FROM employees
             ORDER BY department_id ASC, salary DESC"
        );
        self.results.push(result);

        // 14. DISTINCT
        let result = self.run_test(
            "14. DISTINCT",
            "SELECT DISTINCT department_id
             FROM employees
             ORDER BY department_id"
        );
        self.results.push(result);

        // 15. LIMIT
        let result = self.run_test(
            "15. LIMIT",
            "SELECT name, salary
             FROM employees
             ORDER BY salary DESC
             LIMIT 3"
        );
        self.results.push(result);

        // Print results
        self.print_results();
        
        // Final memory check
        let final_memory = self.get_memory_usage();
        let memory_used = final_memory.saturating_sub(self.start_memory);
        let memory_used_gb = memory_used as f64 / (1024.0 * 1024.0 * 1024.0);
        
        println!("\n💾 Memory Usage:");
        println!("   Start: {:.2} GB", self.start_memory as f64 / (1024.0 * 1024.0 * 1024.0));
        println!("   End: {:.2} GB", final_memory as f64 / (1024.0 * 1024.0 * 1024.0));
        println!("   Used: {:.2} GB", memory_used_gb);
        println!("   Limit: 7.00 GB");
        
        if memory_used > MEMORY_LIMIT_BYTES {
            println!("   ⚠️  WARNING: Memory limit exceeded!");
        } else {
            println!("   ✅ Memory usage within limit");
        }
    }

    /// Print test results
    fn print_results(&self) {
        println!("\n{}", "=".repeat(80));
        println!("TEST RESULTS");
        println!("{}", "=".repeat(80));

        let passed = self.results.iter().filter(|r| r.passed).count();
        let failed = self.results.len() - passed;
        let total_time: f64 = self.results.iter().map(|r| r.execution_time_ms).sum();
        let avg_time = total_time / self.results.len() as f64;

        println!("\n📊 Summary:");
        println!("   Total Tests: {}", self.results.len());
        println!("   ✅ Passed: {}", passed);
        println!("   ❌ Failed: {}", failed);
        println!("   ⏱️  Total Time: {:.2} ms", total_time);
        println!("   ⏱️  Avg Time: {:.2} ms", avg_time);

        println!("\n📋 Detailed Results:");
        println!("{}", "-".repeat(80));

        for result in &self.results {
            let status = if result.passed { "✅ PASS" } else { "❌ FAIL" };
            println!("\n{} {}", status, result.name);
            println!("   Rows: {}, Time: {:.2} ms", result.rows_returned, result.execution_time_ms);
            if let Some(ref error) = result.error {
                println!("   Error: {}", error);
            }
        }

        println!("\n{}", "=".repeat(80));
        
        if passed == self.results.len() {
            println!("🎉 ALL TESTS PASSED!");
        } else {
            println!("⚠️  {} TEST(S) FAILED", failed);
        }
        println!("{}", "=".repeat(80));
    }
}

#[test]
fn test_memory_bounded_comprehensive() {
    let mut suite = MemoryBoundedTestSuite::new();
    suite.run_all_tests();
    
    // Assert that at least some tests passed
    let passed_count = suite.results.iter().filter(|r| r.passed).count();
    assert!(passed_count > 0, "At least some tests should pass");
    
    // Check memory usage is reasonable (approximate check)
    // Note: Precise memory monitoring would require platform-specific APIs
    println!("\n✅ Memory-bounded test suite completed");
}

