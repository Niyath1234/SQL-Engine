/// Memory-Efficient Test Suite
/// Tests 5 medium-to-complex queries with single table load
/// Designed to stay under 7 GB memory limit

use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use hypergraph_sql_engine::execution::engine::QueryResult;
use arrow::array::Array;

/// Get current memory usage in bytes
fn get_memory_usage() -> u64 {
    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
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
        if let Ok(contents) = std::fs::read_to_string("/proc/self/status") {
            for line in contents.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(memory_kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(memory_kb) = memory_kb_str.parse::<u64>() {
                            return memory_kb * 1024;
                        }
                    }
                }
            }
        }
    }
    
    0
}

fn format_memory(bytes: u64) -> String {
    let gb = bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    format!("{:.2} GB", gb)
}

fn print_query_result(query_num: usize, description: &str, result: &QueryResult) {
    println!("\n{}", "=".repeat(80));
    println!("Query {}: {}", query_num, description);
    println!("{}", "=".repeat(80));
    println!("Rows returned: {}", result.row_count);
    println!("Execution time: {:.2} ms", result.execution_time_ms);
    
    if result.row_count > 0 && !result.batches.is_empty() {
        let first_batch = &result.batches[0];
        let schema = &first_batch.batch.schema;
        
        println!("\nColumns: {:?}", 
            schema.fields().iter().map(|f| f.name().to_string()).collect::<Vec<_>>());
        
        // Print first 10 rows
        let rows_to_print = result.row_count.min(10);
        println!("\nFirst {} row(s):", rows_to_print);
        
        let mut printed = 0;
        for batch in &result.batches {
            if printed >= rows_to_print {
                break;
            }
            
            // Iterate through rows, checking selection bitmap
            for row_idx in 0..batch.batch.row_count {
                if printed >= rows_to_print {
                    break;
                }
                
                // Skip unselected rows
                if row_idx >= batch.selection.len() || !batch.selection[row_idx] {
                    continue;
                }
                
                let mut row_values = Vec::new();
                for col_idx in 0..batch.batch.columns.len() {
                    let col = batch.batch.column(col_idx).unwrap();
                    let field = schema.field(col_idx);
                    
                    let value = if col.is_null(row_idx) {
                        "NULL".to_string()
                    } else {
                        match col.data_type() {
                            arrow::datatypes::DataType::Int64 => {
                                let arr = col.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
                                if row_idx < arr.len() {
                                    arr.value(row_idx).to_string()
                                } else {
                                    "?".to_string()
                                }
                            }
                            arrow::datatypes::DataType::Float64 => {
                                let arr = col.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
                                if row_idx < arr.len() {
                                    arr.value(row_idx).to_string()
                                } else {
                                    "?".to_string()
                                }
                            }
                            arrow::datatypes::DataType::Utf8 => {
                                let arr = col.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                                if row_idx < arr.len() {
                                    format!("'{}'", arr.value(row_idx))
                                } else {
                                    "?".to_string()
                                }
                            }
                            _ => "?".to_string(),
                        }
                    };
                    row_values.push(format!("{}={}", field.name(), value));
                }
                println!("  Row {}: {}", printed + 1, row_values.join(", "));
                printed += 1;
            }
        }
        
        if result.row_count > rows_to_print {
            println!("  ... ({} more rows)", result.row_count - rows_to_print);
        }
    }
}

#[test]
fn test_5_queries_memory_efficient() {
    println!("\n{}", "=".repeat(80));
    println!("MEMORY-EFFICIENT TEST SUITE - 5 Queries");
    println!("Memory Limit: 7 GB");
    println!("{}", "=".repeat(80));

    let start_memory = get_memory_usage();
    println!("\n💾 Starting memory: {}", format_memory(start_memory));

    let mut engine = HypergraphSQLEngine::new();

    // ==========================================
    // STEP 1: Load tables ONCE
    // ==========================================
    println!("\n{}", "=".repeat(80));
    println!("STEP 1: Loading test data...");
    println!("{}", "=".repeat(80));

    // Create employees table
    println!("\n📋 Creating employees table...");
    match engine.execute_query("CREATE TABLE employees (id INT, name VARCHAR, salary FLOAT, department_id INT)") {
        Ok(_) => println!("✅ Employees table created"),
        Err(e) => {
            println!("❌ Failed to create employees table: {}", e);
            return;
        }
    }

    // Insert test data
    println!("\n📋 Inserting employees data...");
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
    match engine.execute_query(insert_sql) {
        Ok(_) => println!("✅ Employees data inserted (8 rows)"),
        Err(e) => {
            println!("❌ Failed to insert employees data: {}", e);
            return;
        }
    }

    // Create departments table
    println!("\n📋 Creating departments table...");
    match engine.execute_query("CREATE TABLE departments (id INT, name VARCHAR, budget FLOAT)") {
        Ok(_) => println!("✅ Departments table created"),
        Err(e) => {
            println!("❌ Failed to create departments table: {}", e);
            return;
        }
    }

    // Insert departments data
    println!("\n📋 Inserting departments data...");
    let dept_insert = "
        INSERT INTO departments VALUES
        (1, 'Engineering', 1000000.0),
        (2, 'Sales', 800000.0),
        (3, 'Marketing', 600000.0)
    ";
    match engine.execute_query(dept_insert) {
        Ok(_) => println!("✅ Departments data inserted (3 rows)"),
        Err(e) => {
            println!("❌ Failed to insert departments data: {}", e);
            return;
        }
    }

    let after_load_memory = get_memory_usage();
    let load_memory = after_load_memory.saturating_sub(start_memory);
    println!("\n💾 Memory after data load: {} (used: {})", 
        format_memory(after_load_memory), 
        format_memory(load_memory));

    // ==========================================
    // STEP 2: Run 5 Queries
    // ==========================================
    println!("\n{}", "=".repeat(80));
    println!("STEP 2: Running 5 Test Queries");
    println!("{}", "=".repeat(80));

    let queries = vec![
        (
            1,
            "Basic JOIN with WHERE",
            "SELECT e.name, d.name as dept_name, e.salary 
             FROM employees e 
             INNER JOIN departments d ON e.department_id = d.id 
             WHERE e.salary > 60000.0
             ORDER BY e.salary DESC"
        ),
        (
            2,
            "Window Functions with PARTITION BY",
            "SELECT 
                name,
                department_id,
                salary,
                RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS salary_rank,
                AVG(salary) OVER (PARTITION BY department_id) AS dept_avg_salary
             FROM employees
             ORDER BY department_id, salary DESC"
        ),
        (
            3,
            "CTE with Aggregation",
            "WITH dept_stats AS (
                SELECT 
                    department_id,
                    COUNT(*) AS emp_count,
                    AVG(salary) AS avg_salary,
                    MAX(salary) AS max_salary
                FROM employees
                GROUP BY department_id
            )
            SELECT ds.*, d.name AS dept_name
            FROM dept_stats ds
            INNER JOIN departments d ON ds.department_id = d.id
            ORDER BY ds.avg_salary DESC"
        ),
        (
            4,
            "Self-JOIN with Window Function",
            "SELECT 
                e1.name AS employee,
                e2.name AS manager_candidate,
                e1.salary AS emp_salary,
                e2.salary AS mgr_salary,
                ROW_NUMBER() OVER (PARTITION BY e1.department_id ORDER BY e2.salary DESC) AS manager_rank
            FROM employees e1
            INNER JOIN employees e2 ON e1.department_id = e2.department_id AND e1.salary < e2.salary
            ORDER BY e1.department_id, manager_rank"
        ),
        (
            5,
            "Complex: CTE + Window + JOIN",
            "WITH ranked_employees AS (
                SELECT 
                    e.*,
                    RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS rank
                FROM employees e
            )
            SELECT 
                re.name,
                d.name AS dept_name,
                re.salary,
                re.rank
            FROM ranked_employees re
            INNER JOIN departments d ON re.department_id = d.id
            WHERE re.rank <= 2
            ORDER BY re.department_id, re.rank"
        ),
    ];

    for (query_num, description, sql) in queries {
        let before_memory = get_memory_usage();
        
        println!("\n{}", "-".repeat(80));
        println!("Running Query {}: {}", query_num, description);
        println!("SQL: {}", sql.replace("\n", " ").trim());
        
        let query_start = std::time::Instant::now();
        match engine.execute_query(sql) {
            Ok(result) => {
                let query_time = query_start.elapsed().as_secs_f64() * 1000.0;
                let after_memory = get_memory_usage();
                let query_memory = after_memory.saturating_sub(before_memory);
                
                println!("\n✅ Query {} executed successfully!", query_num);
                println!("   Time: {:.2} ms", query_time);
                println!("   Memory delta: {}", format_memory(query_memory));
                println!("   Total memory: {}", format_memory(after_memory));
                
                print_query_result(query_num, description, &result);
            }
            Err(e) => {
                let query_time = query_start.elapsed().as_secs_f64() * 1000.0;
                println!("\n❌ Query {} failed!", query_num);
                println!("   Time: {:.2} ms", query_time);
                println!("   Error: {}", e);
            }
        }
    }

    // Final memory summary
    let final_memory = get_memory_usage();
    let total_memory = final_memory.saturating_sub(start_memory);
    
    println!("\n{}", "=".repeat(80));
    println!("MEMORY SUMMARY");
    println!("{}", "=".repeat(80));
    println!("Start memory: {}", format_memory(start_memory));
    println!("Final memory: {}", format_memory(final_memory));
    println!("Total used: {}", format_memory(total_memory));
    println!("Memory limit: 7.00 GB");
    
    if total_memory > 7 * 1024 * 1024 * 1024 {
        println!("\n⚠️  WARNING: Memory limit (7 GB) exceeded!");
        println!("   Used: {}", format_memory(total_memory));
    } else {
        println!("\n✅ Memory usage within limit");
    }
    
    println!("\n{}", "=".repeat(80));
    println!("✅ Test suite completed!");
    println!("{}", "=".repeat(80));
}

