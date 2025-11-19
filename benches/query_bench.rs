/// Benchmark suite for query performance
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use hypergraph_sql_engine::engine::HypergraphSQLEngine;

fn bench_query_parsing(c: &mut Criterion) {
    c.bench_function("parse_sql", |b| {
        let sql = "SELECT r.region_name, SUM(f.sales_amount) 
                   FROM fact_sales f 
                   JOIN dim_region r ON f.region_id = r.id 
                   WHERE f.sale_date BETWEEN '2025-01-01' AND '2025-01-31' 
                   GROUP BY r.region_name";
        
        b.iter(|| {
            let _ = hypergraph_sql_engine::query::parser::parse_sql(black_box(sql));
        });
    });
}

fn bench_query_planning(c: &mut Criterion) {
    c.bench_function("plan_query", |b| {
        let mut engine = HypergraphSQLEngine::new();
        let sql = "SELECT * FROM test_table WHERE id > 100";
        
        b.iter(|| {
            let _ = engine.execute_query(black_box(sql));
        });
    });
}

criterion_group!(benches, bench_query_parsing, bench_query_planning);
criterion_main!(benches);

