/// Micro-benchmarks comparing BAMJ vs hash join
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use hypergraph_sql_engine::storage::bitmap_index::BitmapIndex;
use hypergraph_sql_engine::storage::bitset::Bitset;
use hypergraph_sql_engine::storage::fragment::Value;
use std::collections::HashMap;

fn build_bitmap_index(size: usize) -> BitmapIndex {
    let mut index = BitmapIndex::new("table".to_string(), "id".to_string());
    
    // Create index with 'size' distinct values, each appearing 10 times
    for value in 0..size {
        for row in 0..10 {
            index.add_value(Value::Int64(value as i64), value * 10 + row);
        }
    }
    
    index
}

fn bitmap_join_benchmark(index1: &BitmapIndex, index2: &BitmapIndex) {
    let intersections = index1.intersect_with(index2);
    black_box(intersections);
}

fn hash_join_simulation(left: &[(i64, usize)], right: &[(i64, usize)]) -> Vec<(usize, usize)> {
    // Simulate hash join: build hash table, then probe
    let mut hash_table: HashMap<i64, Vec<usize>> = HashMap::new();
    
    for (value, row_id) in left {
        hash_table.entry(*value).or_default().push(*row_id);
    }
    
    let mut results = Vec::new();
    for (value, row_id) in right {
        if let Some(left_rows) = hash_table.get(value) {
            for &left_row in left_rows {
                results.push((left_row, *row_id));
            }
        }
    }
    
    results
}

fn bench_bitmap_vs_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_algorithms");
    
    for size in [100, 1000, 10000].iter() {
        // Build bitmap indexes
        let index1 = build_bitmap_index(*size);
        let index2 = build_bitmap_index(*size);
        
        // Build data for hash join simulation
        let mut left_data = Vec::new();
        let mut right_data = Vec::new();
        for value in 0..*size {
            for row in 0..10 {
                left_data.push((value as i64, value * 10 + row));
                right_data.push((value as i64, value * 10 + row));
            }
        }
        
        group.bench_with_input(
            BenchmarkId::new("bitmap_join", size),
            size,
            |b, _| {
                b.iter(|| bitmap_join_benchmark(&index1, &index2));
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("hash_join", size),
            size,
            |b, _| {
                b.iter(|| hash_join_simulation(black_box(&left_data), black_box(&right_data)));
            },
        );
    }
    
    group.finish();
}

fn bench_bitset_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("bitset_operations");
    
    for size in [1000, 10000, 100000].iter() {
        // Create two bitsets with some overlap
        let mut bs1 = Bitset::new();
        let mut bs2 = Bitset::new();
        
        for i in 0..*size {
            if i % 2 == 0 {
                bs1.set(i);
            }
            if i % 3 == 0 {
                bs2.set(i);
            }
        }
        
        group.bench_with_input(
            BenchmarkId::new("intersect", size),
            size,
            |b, _| {
                b.iter(|| black_box(bs1.intersect(&bs2)));
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("union", size),
            size,
            |b, _| {
                b.iter(|| black_box(bs1.union(&bs2)));
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("iter_set_bits", size),
            size,
            |b, _| {
                b.iter(|| {
                    let count: usize = bs1.iter_set_bits().count();
                    black_box(count);
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(benches, bench_bitmap_vs_hash, bench_bitset_operations);
criterion_main!(benches);

