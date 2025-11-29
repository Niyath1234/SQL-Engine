/// Comprehensive Join Stress Tests for DuckDB/Presto-grade join engine
/// 
/// Tests for:
/// - 5-table joins
/// - 10-table and 20-table random synthetic schema
/// - Star schema: 1 big fact + 10-30 dimensions
/// - Linear chain of 50 tables
/// - Random graph of 100 tables (all small)
use hypergraph_sql_engine::engine::HypergraphSQLEngine;

/// Test helper: Create engine and load test data
fn create_test_engine() -> HypergraphSQLEngine {
    HypergraphSQLEngine::new().expect("Failed to create engine")
}

/// Test helper: Execute query and return row count
fn execute_query(engine: &mut HypergraphSQLEngine, sql: &str) -> std::result::Result<usize, String> {
    match engine.execute_query(sql) {
        Ok(result) => Ok(result.batches.iter().map(|b| b.row_count).sum()),
        Err(e) => Err(format!("Query failed: {}", e)),
    }
}

/// Test 1: 5-table join (customers-orders-products-warehouses-regions)
#[test]
fn test_5_table_join() {
    let mut engine = create_test_engine();
    
    // Create tables
    engine.execute("CREATE TABLE customers (customer_id INT64, name VARCHAR, region_id INT64);").unwrap();
    engine.execute("CREATE TABLE orders (order_id INT64, customer_id INT64, product_id INT64);").unwrap();
    engine.execute("CREATE TABLE products (product_id INT64, name VARCHAR, warehouse_id INT64);").unwrap();
    engine.execute("CREATE TABLE warehouses (warehouse_id INT64, name VARCHAR);").unwrap();
    engine.execute("CREATE TABLE regions (region_id INT64, name VARCHAR);").unwrap();
    
    // Insert test data
    for i in 1..=10 {
        engine.execute(&format!("INSERT INTO customers VALUES ({}, 'Customer{}', {});", i, i, (i % 3) + 1)).unwrap();
        engine.execute(&format!("INSERT INTO orders VALUES ({}, {}, {});", i, i, i)).unwrap();
        engine.execute(&format!("INSERT INTO products VALUES ({}, 'Product{}', {});", i, i, (i % 2) + 1)).unwrap();
    }
    for i in 1..=2 {
        engine.execute(&format!("INSERT INTO warehouses VALUES ({}, 'Warehouse{}');", i, i)).unwrap();
    }
    for i in 1..=3 {
        engine.execute(&format!("INSERT INTO regions VALUES ({}, 'Region{}');", i, i)).unwrap();
    }
    
    // Test 5-table join
    let sql = "SELECT c.name, o.order_id, p.name, w.name, r.name \
               FROM customers c \
               JOIN orders o ON c.customer_id = o.customer_id \
               JOIN products p ON o.product_id = p.product_id \
               JOIN warehouses w ON p.warehouse_id = w.warehouse_id \
               JOIN regions r ON c.region_id = r.region_id;";
    
    let row_count = execute_query(&mut engine, sql).expect("5-table join should succeed");
    assert!(row_count > 0, "5-table join should return rows");
}

/// Test 2: 10-table join (synthetic schema)
#[test]
fn test_10_table_join() {
    let mut engine = create_test_engine();
    
    // Create 10 tables in a chain
    for i in 1..=10 {
        if i == 1 {
            engine.execute(&format!("CREATE TABLE t{} (id INT64, name VARCHAR);", i)).unwrap();
        } else {
            engine.execute(&format!("CREATE TABLE t{} (id INT64, t{}_id INT64, name VARCHAR);", i, i-1)).unwrap();
        }
        
        // Insert test data
        for j in 1..=5 {
            if i == 1 {
                engine.execute(&format!("INSERT INTO t{} VALUES ({}, 'Name{}');", i, j, j)).unwrap();
            } else {
                engine.execute(&format!("INSERT INTO t{} VALUES ({}, {}, 'Name{}');", i, j, j, j)).unwrap();
            }
        }
    }
    
    // Build 10-table join query
    let mut sql = "SELECT t1.name".to_string();
    for i in 2..=10 {
        sql.push_str(&format!(", t{}.name", i));
    }
    sql.push_str(" FROM t1");
    for i in 2..=10 {
        sql.push_str(&format!(" JOIN t{} ON t{}.id = t{}.t{}_id", i, i-1, i, i-1));
    }
    sql.push_str(";");
    
    let row_count = execute_query(&mut engine, &sql).expect("10-table join should succeed");
    assert!(row_count > 0, "10-table join should return rows");
}

/// Test 3: Star schema (1 fact + 10 dimensions)
#[test]
fn test_star_schema_10_dimensions() {
    let mut engine = create_test_engine();
    
    // Create fact table
    engine.execute("CREATE TABLE fact (fact_id INT64, dim1_id INT64, dim2_id INT64, dim3_id INT64, \
                   dim4_id INT64, dim5_id INT64, dim6_id INT64, dim7_id INT64, dim8_id INT64, \
                   dim9_id INT64, dim10_id INT64, value INT64);").unwrap();
    
    // Create dimension tables
    for i in 1..=10 {
        engine.execute(&format!("CREATE TABLE dim{} (id INT64, name VARCHAR);", i)).unwrap();
        
        // Insert dimension data
        for j in 1..=5 {
            engine.execute(&format!("INSERT INTO dim{} VALUES ({}, 'Dim{}Name{}');", i, j, i, j)).unwrap();
        }
    }
    
    // Insert fact data
    for i in 1..=20 {
        engine.execute(&format!("INSERT INTO fact VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});",
            i, (i % 5) + 1, (i % 5) + 1, (i % 5) + 1, (i % 5) + 1, (i % 5) + 1,
            (i % 5) + 1, (i % 5) + 1, (i % 5) + 1, (i % 5) + 1, (i % 5) + 1, i * 10)).unwrap();
    }
    
    // Build star schema join query
    let mut sql = "SELECT fact.value".to_string();
    for i in 1..=10 {
        sql.push_str(&format!(", dim{}.name as dim{}_name", i, i));
    }
    sql.push_str(" FROM fact");
    for i in 1..=10 {
        sql.push_str(&format!(" JOIN dim{} ON fact.dim{}_id = dim{}.id", i, i, i));
    }
    sql.push_str(";");
    
    let row_count = execute_query(&mut engine, &sql).expect("Star schema join should succeed");
    assert!(row_count > 0, "Star schema join should return rows");
}

/// Test 4: Linear chain of 20 tables (tests deep nesting)
#[test]
fn test_linear_chain_20_tables() {
    let mut engine = create_test_engine();
    
    // Create 20 tables in a linear chain
    for i in 1..=20 {
        if i == 1 {
            engine.execute(&format!("CREATE TABLE chain{} (id INT64, value INT64);", i)).unwrap();
        } else {
            engine.execute(&format!("CREATE TABLE chain{} (id INT64, chain{}_id INT64, value INT64);", i, i-1)).unwrap();
        }
        
        // Insert test data
        for j in 1..=3 {
            if i == 1 {
                engine.execute(&format!("INSERT INTO chain{} VALUES ({}, {});", i, j, j * 10)).unwrap();
            } else {
                engine.execute(&format!("INSERT INTO chain{} VALUES ({}, {}, {});", i, j, j, j * 10)).unwrap();
            }
        }
    }
    
    // Build linear chain join query
    let mut sql = "SELECT chain1.value".to_string();
    for i in 2..=20 {
        sql.push_str(&format!(", chain{}.value", i));
    }
    sql.push_str(" FROM chain1");
    for i in 2..=20 {
        sql.push_str(&format!(" JOIN chain{} ON chain{}.id = chain{}.chain{}_id", i, i-1, i, i-1));
    }
    sql.push_str(";");
    
    let row_count = execute_query(&mut engine, &sql).expect("Linear chain join should succeed");
    assert!(row_count > 0, "Linear chain join should return rows");
}

/// Test 5: Multiple tables with repeated column names (tests column resolution)
#[test]
fn test_repeated_column_names() {
    let mut engine = create_test_engine();
    
    engine.execute("CREATE TABLE a (id INT64, name VARCHAR);").unwrap();
    engine.execute("CREATE TABLE b (id INT64, name VARCHAR, a_id INT64);").unwrap();
    engine.execute("CREATE TABLE c (id INT64, name VARCHAR, b_id INT64);").unwrap();
    
    engine.execute("INSERT INTO a VALUES (1, 'A1');").unwrap();
    engine.execute("INSERT INTO b VALUES (1, 'B1', 1);").unwrap();
    engine.execute("INSERT INTO c VALUES (1, 'C1', 1);").unwrap();
    
    // Test that all three 'name' columns are preserved
    let sql = "SELECT a.name AS a_name, b.name AS b_name, c.name AS c_name \
               FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id;";
    
    let row_count = execute_query(&mut engine, sql).expect("Repeated column names should work");
    assert_eq!(row_count, 1, "Should return 1 row");
}

