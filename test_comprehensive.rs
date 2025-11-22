/// Comprehensive test suite for all SQL engine functionalities
/// Tests: Basic queries, Vector search, Window functions, CTEs, Aggregations, JOINs, etc.
use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use anyhow::Result;

/// Print query result in a formatted way
fn print_query_result(query: &str, result: &hypergraph_sql_engine::execution::engine::QueryResult) {
    println!("\n{}", "=".repeat(80));
    println!("Query: {}", query);
    println!("{}", "-".repeat(80));
    println!("Rows: {} | Execution time: {:.2} ms", result.row_count, result.execution_time_ms);
    
    if result.row_count > 0 && !result.batches.is_empty() {
        let batch = &result.batches[0].batch;
        let schema = &batch.schema;
        
        // Print header
        let headers: Vec<String> = schema.fields().iter().map(|f| f.name().to_string()).collect();
        println!("\nColumns: {}", headers.join(", "));
        println!("{}", "-".repeat(80));
        
        // Print rows (limit to 10 for display)
        let max_rows = result.row_count.min(10);
        for row_idx in 0..max_rows {
            let mut values = Vec::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                if col_idx < batch.columns.len() {
                    let col = &batch.columns[col_idx];
                    let val_str = if col.len() > row_idx {
                        if col.is_null(row_idx) {
                            "NULL".to_string()
                        } else {
                            match col.data_type() {
                                DataType::Int64 => {
                                    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                                    format!("{}", arr.value(row_idx))
                                }
                                DataType::Int32 => {
                                    let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                                    format!("{}", arr.value(row_idx))
                                }
                                DataType::Float64 => {
                                    let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                                    format!("{:.4}", arr.value(row_idx))
                                }
                                DataType::Float32 => {
                                    let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                                    format!("{:.4}", arr.value(row_idx))
                                }
                                DataType::Utf8 | DataType::LargeUtf8 => {
                                    let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                                    format!("\"{}\"", arr.value(row_idx))
                                }
                                DataType::Boolean => {
                                    let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                                    format!("{}", arr.value(row_idx))
                                }
                                _ => format!("{:?}", col),
                            }
                        }
                    } else {
                        "N/A".to_string()
                    };
                    values.push(format!("{}: {}", field.name(), val_str));
                }
            }
            println!("Row {}: {}", row_idx + 1, values.join(" | "));
        }
        
        if result.row_count > max_rows {
            println!("... ({} more rows)", result.row_count - max_rows);
        }
    } else {
        println!("(No rows returned)");
    }
    println!("{}", "=".repeat(80));
}

/// Create test data for employees table
fn create_employees_data() -> (Vec<String>, Vec<Arc<dyn Array>>) {
    let ids = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let names = vec![
        "Alice", "Bob", "Charlie", "David", "Eve",
        "Frank", "Grace", "Henry", "Ivy", "Jack"
    ];
    let departments = vec![
        "Engineering", "Engineering", "Sales", "Sales", "Engineering",
        "Marketing", "Marketing", "Engineering", "Sales", "Marketing"
    ];
    let salaries = vec![
        95000.0, 105000.0, 80000.0, 85000.0, 110000.0,
        75000.0, 90000.0, 100000.0, 82000.0, 88000.0
    ];
    let years = vec![2020, 2021, 2019, 2022, 2020, 2021, 2019, 2020, 2022, 2021];
    
    let id_array: Arc<dyn Array> = Arc::new(Int64Array::from(ids));
    let name_array: Arc<dyn Array> = Arc::new(StringArray::from(names));
    let dept_array: Arc<dyn Array> = Arc::new(StringArray::from(departments));
    let salary_array: Arc<dyn Array> = Arc::new(Float64Array::from(salaries));
    let year_array: Arc<dyn Array> = Arc::new(Int64Array::from(years));
    
    let column_names = vec![
        "id".to_string(),
        "name".to_string(),
        "department".to_string(),
        "salary".to_string(),
        "join_year".to_string(),
    ];
    
    let columns = vec![id_array, name_array, dept_array, salary_array, year_array];
    
    (column_names, columns)
}

/// Create test data for products table
fn create_products_data() -> (Vec<String>, Vec<Arc<dyn Array>>) {
    let ids = vec![1, 2, 3, 4, 5];
    let names = vec!["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"];
    let prices = vec![999.99, 29.99, 79.99, 299.99, 149.99];
    let categories = vec!["Electronics", "Electronics", "Electronics", "Electronics", "Electronics"];
    
    let id_array: Arc<dyn Array> = Arc::new(Int64Array::from(ids));
    let name_array: Arc<dyn Array> = Arc::new(StringArray::from(names));
    let price_array: Arc<dyn Array> = Arc::new(Float64Array::from(prices));
    let cat_array: Arc<dyn Array> = Arc::new(StringArray::from(categories));
    
    let column_names = vec![
        "id".to_string(),
        "name".to_string(),
        "price".to_string(),
        "category".to_string(),
    ];
    
    let columns = vec![id_array, name_array, price_array, cat_array];
    
    (column_names, columns)
}

/// Create vector test data
fn create_vector_data() -> (
    (Vec<String>, Vec<Arc<dyn Array>>),
    (Vec<String>, Vec<Arc<dyn Array>>),
) {
    // Documents table
    let doc_ids = vec!["doc1", "doc2", "doc3", "doc4", "doc5"];
    let doc_titles = vec![
        "Machine Learning Basics",
        "Deep Learning Tutorial",
        "Natural Language Processing",
        "Computer Vision Guide",
        "Neural Networks Explained",
    ];
    
    // Normalized embeddings (3D vectors)
    let embeddings: Vec<Vec<f32>> = vec![
        vec![0.8, 0.6, 0.0],
        vec![0.9, 0.8, 0.1],
        vec![0.6, 0.4, 0.8],
        vec![0.7, 0.5, 0.3],
        vec![0.85, 0.7, 0.2],
    ];
    
    let normalized_embeddings: Vec<Vec<f32>> = embeddings.iter()
        .map(|emb| {
            let norm: f32 = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                emb.iter().map(|x| x / norm).collect()
            } else {
                emb.clone()
            }
        })
        .collect();
    
    let id_array = StringArray::from(doc_ids.iter().map(|s| Some(*s)).collect::<Vec<_>>());
    let title_array = StringArray::from(doc_titles.iter().map(|s| Some(*s)).collect::<Vec<_>>());
    
    let dimension = 3;
    let mut embedding_values = Vec::new();
    for emb in &normalized_embeddings {
        for &val in emb {
            embedding_values.push(Some(val));
        }
    }
    
    let embedding_float_array = Float32Array::from(embedding_values);
    let list_field = Arc::new(Field::new("item", DataType::Float32, true));
    let embedding_array = FixedSizeListArray::try_new(
        list_field.clone(),
        dimension as i32,
        Arc::new(embedding_float_array),
        None,
    ).unwrap();
    
    let doc_columns = vec![
        Arc::new(id_array) as Arc<dyn Array>,
        Arc::new(title_array) as Arc<dyn Array>,
        Arc::new(embedding_array) as Arc<dyn Array>,
    ];
    let doc_column_names = vec!["id".to_string(), "title".to_string(), "embedding".to_string()];
    
    // Queries table
    let query_ids = vec!["q1", "q2"];
    let query_texts = vec!["What is machine learning?", "How does neural network work?"];
    let query_embeddings_raw: Vec<Vec<f32>> = vec![
        vec![0.85, 0.65, 0.05],
        vec![0.88, 0.72, 0.15],
    ];
    
    let normalized_query_embeddings: Vec<Vec<f32>> = query_embeddings_raw.iter()
        .map(|emb| {
            let norm: f32 = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                emb.iter().map(|x| x / norm).collect()
            } else {
                emb.clone()
            }
        })
        .collect();
    
    let query_id_array = StringArray::from(query_ids.iter().map(|s| Some(*s)).collect::<Vec<_>>());
    let query_text_array = StringArray::from(query_texts.iter().map(|s| Some(*s)).collect::<Vec<_>>());
    
    let mut query_embedding_values = Vec::new();
    for emb in &normalized_query_embeddings {
        for &val in emb {
            query_embedding_values.push(Some(val));
        }
    }
    
    let query_embedding_float_array = Float32Array::from(query_embedding_values);
    let query_list_field = Arc::new(Field::new("item", DataType::Float32, true));
    let query_embedding_array = FixedSizeListArray::try_new(
        query_list_field,
        dimension as i32,
        Arc::new(query_embedding_float_array),
        None,
    ).unwrap();
    
    let query_columns = vec![
        Arc::new(query_id_array) as Arc<dyn Array>,
        Arc::new(query_text_array) as Arc<dyn Array>,
        Arc::new(query_embedding_array) as Arc<dyn Array>,
    ];
    let query_column_names = vec!["id".to_string(), "text".to_string(), "embedding".to_string()];
    
    (
        (doc_column_names, doc_columns),
        (query_column_names, query_columns),
    )
}

/// Create fragments from column data
fn create_fragments(column_names: Vec<String>, columns: Vec<Arc<dyn Array>>) -> Vec<(String, hypergraph_sql_engine::storage::fragment::ColumnFragment)> {
    let mut fragments = Vec::new();
    
    for (name, array) in column_names.iter().zip(columns.iter()) {
        let row_count = array.len();
        let fragment = hypergraph_sql_engine::storage::fragment::ColumnFragment::new(
            array.clone(),
            hypergraph_sql_engine::storage::fragment::FragmentMetadata {
                row_count,
                min_value: None,
                max_value: None,
                cardinality: row_count,
                compression: hypergraph_sql_engine::storage::fragment::CompressionType::None,
                memory_size: 0,
            },
        );
        fragments.push((name.clone(), fragment));
    }
    
    fragments
}

fn main() -> Result<()> {
    println!("\n{}", "=".repeat(80));
    println!("COMPREHENSIVE SQL ENGINE TEST SUITE");
    println!("{}", "=".repeat(80));
    
    // Initialize engine
    println!("\n Initializing Hypergraph SQL Engine...");
    let mut engine = HypergraphSQLEngine::new();
    
    // Load test data
    println!("\n Loading test data...");
    
    // Load employees table
    let (emp_cols, emp_data) = create_employees_data();
    let emp_fragments = create_fragments(emp_cols, emp_data);
    engine.load_table("employees", emp_fragments)?;
    println!("    Loaded 'employees' table (10 rows)");
    
    // Load products table
    let (prod_cols, prod_data) = create_products_data();
    let prod_fragments = create_fragments(prod_cols, prod_data);
    engine.load_table("products", prod_fragments)?;
    println!("    Loaded 'products' table (5 rows)");
    
    // Load vector data
    let (doc_data, query_data) = create_vector_data();
    let doc_fragments = create_fragments(doc_data.0, doc_data.1);
    let query_fragments = create_fragments(query_data.0, query_data.1);
    engine.load_table("documents", doc_fragments)?;
    engine.load_table("queries", query_fragments)?;
    println!("    Loaded 'documents' table (5 rows with embeddings)");
    println!("    Loaded 'queries' table (2 rows with embeddings)");
    
    let mut test_count = 0;
    let mut pass_count = 0;
    
    // Test 1: Basic SELECT
    test_count += 1;
    println!("\n\n Test {}: Basic SELECT", test_count);
    match engine.execute_query("SELECT id, name, department FROM employees LIMIT 5") {
        Ok(result) => {
            print_query_result("SELECT id, name, department FROM employees LIMIT 5", &result);
            pass_count += 1;
        }
        Err(e) => {
            println!(" FAILED: {}", e);
        }
    }
    
    // Test 2: WHERE clause
    test_count += 1;
    println!("\n\n Test {}: WHERE clause", test_count);
    match engine.execute_query("SELECT name, salary FROM employees WHERE department = 'Engineering'") {
        Ok(result) => {
            print_query_result("SELECT name, salary FROM employees WHERE department = 'Engineering'", &result);
            pass_count += 1;
        }
        Err(e) => {
            println!(" FAILED: {}", e);
        }
    }
    
    // Test 3: ORDER BY
    test_count += 1;
    println!("\n\n Test {}: ORDER BY", test_count);
    match engine.execute_query("SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 5") {
        Ok(result) => {
            print_query_result("SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 5", &result);
            pass_count += 1;
        }
        Err(e) => {
            println!(" FAILED: {}", e);
        }
    }
    
    // Test 4: Aggregation with GROUP BY
    test_count += 1;
    println!("\n\n Test {}: GROUP BY aggregation", test_count);
    match engine.execute_query("SELECT department, COUNT(*) as count, AVG(salary) as avg_salary FROM employees GROUP BY department") {
        Ok(result) => {
            print_query_result("SELECT department, COUNT(*) as count, AVG(salary) as avg_salary FROM employees GROUP BY department", &result);
            pass_count += 1;
        }
        Err(e) => {
            println!(" FAILED: {}", e);
        }
    }
    
    // Test 5: HAVING clause
    test_count += 1;
    println!("\n\n Test {}: HAVING clause", test_count);
    match engine.execute_query("SELECT department, COUNT(*) as count FROM employees GROUP BY department HAVING COUNT(*) > 2") {
        Ok(result) => {
            print_query_result("SELECT department, COUNT(*) as count FROM employees GROUP BY department HAVING COUNT(*) > 2", &result);
            pass_count += 1;
        }
        Err(e) => {
            println!(" FAILED: {}", e);
        }
    }
    
    // Test 6: JOIN
    test_count += 1;
    println!("\n\n Test {}: JOIN", test_count);
    match engine.execute_query("SELECT e.name, e.department, p.name as product FROM employees e CROSS JOIN products p LIMIT 10") {
        Ok(result) => {
            print_query_result("SELECT e.name, e.department, p.name as product FROM employees e CROSS JOIN products p LIMIT 10", &result);
            pass_count += 1;
        }
        Err(e) => {
            println!(" FAILED: {}", e);
        }
    }
    
    // Test 7: Window Functions - ROW_NUMBER
    test_count += 1;
    println!("\n\n Test {}: Window Function - ROW_NUMBER", test_count);
    match engine.execute_query("SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank FROM employees") {
        Ok(result) => {
            print_query_result("SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank FROM employees", &result);
            pass_count += 1;
        }
        Err(e) => {
            println!(" FAILED: {}", e);
        }
    }
    
    // Test 8: Window Functions - RANK
    test_count += 1;
    println!("\n\n Test {}: Window Function - RANK", test_count);
    match engine.execute_query("SELECT name, salary, RANK() OVER (ORDER BY salary DESC) as rank FROM employees LIMIT 5") {
        Ok(result) => {
            print_query_result("SELECT name, salary, RANK() OVER (ORDER BY salary DESC) as rank FROM employees LIMIT 5", &result);
            pass_count += 1;
        }
        Err(e) => {
            println!(" FAILED: {}", e);
        }
    }
    
    // Test 9: Window Functions - SUM OVER
    test_count += 1;
    println!("\n\n Test {}: Window Function - SUM OVER", test_count);
    match engine.execute_query("SELECT name, department, salary, SUM(salary) OVER (PARTITION BY department) as dept_total FROM employees LIMIT 5") {
        Ok(result) => {
            print_query_result("SELECT name, department, salary, SUM(salary) OVER (PARTITION BY department) as dept_total FROM employees LIMIT 5", &result);
            pass_count += 1;
        }
        Err(e) => {
            println!(" FAILED: {}", e);
        }
    }
    
    // Test 10: CTE (Common Table Expression)
    test_count += 1;
    println!("\n\n Test {}: CTE (Common Table Expression)", test_count);
    match engine.execute_query("WITH high_salary AS (SELECT * FROM employees WHERE salary > 100000) SELECT name, salary FROM high_salary") {
        Ok(result) => {
            print_query_result("WITH high_salary AS (SELECT * FROM employees WHERE salary > 100000) SELECT name, salary FROM high_salary", &result);
            pass_count += 1;
        }
        Err(e) => {
            println!(" FAILED: {}", e);
        }
    }
    
    // Test 11: Vector Similarity Search
    test_count += 1;
    println!("\n\n Test {}: Vector Similarity Search", test_count);
    match engine.execute_query("SELECT d.id AS doc_id, d.title, q.id AS query_id, VECTOR_SIMILARITY(d.embedding, q.embedding) AS similarity FROM documents d CROSS JOIN queries q ORDER BY similarity DESC LIMIT 5") {
        Ok(result) => {
            print_query_result("SELECT d.id AS doc_id, d.title, q.id AS query_id, VECTOR_SIMILARITY(d.embedding, q.embedding) AS similarity FROM documents d CROSS JOIN queries q ORDER BY similarity DESC LIMIT 5", &result);
            pass_count += 1;
        }
        Err(e) => {
            println!(" FAILED: {}", e);
        }
    }
    
    // Test 12: Vector Distance
    test_count += 1;
    println!("\n\n Test {}: Vector Distance", test_count);
    match engine.execute_query("SELECT d.id AS doc_id, q.id AS query_id, VECTOR_DISTANCE(d.embedding, q.embedding) AS distance FROM documents d CROSS JOIN queries q WHERE q.id = 'q1' ORDER BY distance ASC LIMIT 3") {
        Ok(result) => {
            print_query_result("SELECT d.id AS doc_id, q.id AS query_id, VECTOR_DISTANCE(d.embedding, q.embedding) AS distance FROM documents d CROSS JOIN queries q WHERE q.id = 'q1' ORDER BY distance ASC LIMIT 3", &result);
            pass_count += 1;
        }
        Err(e) => {
            println!(" FAILED: {}", e);
        }
    }
    
    // Test 13: Complex query with multiple features
    test_count += 1;
    println!("\n\n Test {}: Complex query (CTE + Window + Aggregation)", test_count);
    match engine.execute_query("WITH dept_stats AS (SELECT department, AVG(salary) as avg_sal FROM employees GROUP BY department) SELECT e.name, e.department, e.salary, ds.avg_sal, RANK() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_rank FROM employees e JOIN dept_stats ds ON e.department = ds.department LIMIT 10") {
        Ok(result) => {
            print_query_result("Complex query (CTE + Window + Aggregation)", &result);
            pass_count += 1;
        }
        Err(e) => {
            println!(" FAILED: {}", e);
        }
    }
    
    // Summary
    println!("\n\n{}", "=".repeat(80));
    println!("TEST SUMMARY");
    println!("{}", "=".repeat(80));
    println!("Total tests: {}", test_count);
    println!("Passed: {}", pass_count);
    println!("Failed: {}", test_count - pass_count);
    println!("Success rate: {:.1}%", (pass_count as f64 / test_count as f64) * 100.0);
    
    if pass_count == test_count {
        println!("\n All tests passed!");
    } else {
        println!("\n  Some tests failed - see details above");
    }
    
    Ok(())
}

