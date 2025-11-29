/// Performance Benchmarks for Join Engine
/// 
/// Measures:
/// - Build-time latency per join
/// - Probe throughput (rows/sec)
/// - Total memory usage
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use hypergraph_sql_engine::engine::HypergraphSQLEngine;

fn setup_engine() -> HypergraphSQLEngine {
    let mut engine = HypergraphSQLEngine::new().expect("Failed to create engine");
    
    // Create test tables
    engine.execute("CREATE TABLE customers (customer_id INT64, name VARCHAR, region_id INT64);").unwrap();
    engine.execute("CREATE TABLE orders (order_id INT64, customer_id INT64, product_id INT64);").unwrap();
    engine.execute("CREATE TABLE products (product_id INT64, name VARCHAR, warehouse_id INT64);").unwrap();
    engine.execute("CREATE TABLE warehouses (warehouse_id INT64, name VARCHAR);").unwrap();
    engine.execute("CREATE TABLE regions (region_id INT64, name VARCHAR);").unwrap();
    
    // Insert test data
    for i in 1..=1000 {
        engine.execute(&format!("INSERT INTO customers VALUES ({}, 'Customer{}', {});", i, i, (i % 10) + 1)).unwrap();
        engine.execute(&format!("INSERT INTO orders VALUES ({}, {}, {});", i, i, i)).unwrap();
        engine.execute(&format!("INSERT INTO products VALUES ({}, 'Product{}', {});", i, i, (i % 5) + 1)).unwrap();
    }
    for i in 1..=5 {
        engine.execute(&format!("INSERT INTO warehouses VALUES ({}, 'Warehouse{}');", i, i)).unwrap();
    }
    for i in 1..=10 {
        engine.execute(&format!("INSERT INTO regions VALUES ({}, 'Region{}');", i, i)).unwrap();
    }
    
    engine
}

fn benchmark_2_table_join(c: &mut Criterion) {
    let mut engine = setup_engine();
    
    c.bench_function("2-table join", |b| {
        b.iter(|| {
            let sql = "SELECT c.name, o.order_id \
                      FROM customers c \
                      JOIN orders o ON c.customer_id = o.customer_id;";
            black_box(engine.execute(sql).unwrap());
        });
    });
}

fn benchmark_3_table_join(c: &mut Criterion) {
    let mut engine = setup_engine();
    
    c.bench_function("3-table join", |b| {
        b.iter(|| {
            let sql = "SELECT c.name, o.order_id, p.name \
                      FROM customers c \
                      JOIN orders o ON c.customer_id = o.customer_id \
                      JOIN products p ON o.product_id = p.product_id;";
            black_box(engine.execute(sql).unwrap());
        });
    });
}

fn benchmark_5_table_join(c: &mut Criterion) {
    let mut engine = setup_engine();
    
    c.bench_function("5-table join", |b| {
        b.iter(|| {
            let sql = "SELECT c.name, o.order_id, p.name, w.name, r.name \
                      FROM customers c \
                      JOIN orders o ON c.customer_id = o.customer_id \
                      JOIN products p ON o.product_id = p.product_id \
                      JOIN warehouses w ON p.warehouse_id = w.warehouse_id \
                      JOIN regions r ON c.region_id = r.region_id;";
            black_box(engine.execute(sql).unwrap());
        });
    });
}

criterion_group!(benches, benchmark_2_table_join, benchmark_3_table_join, benchmark_5_table_join);
criterion_main!(benches);

