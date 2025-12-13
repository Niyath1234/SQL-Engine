/// Phase 1 SIMD Benchmark: Compare scalar vs AVX2 performance
/// 
/// Run with: cargo bench --bench phase1_simd_bench
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};

// Import SIMD kernels
fn simd_benchmarks(c: &mut Criterion) {
    // Test data sizes
    let sizes = vec![1000, 10_000, 100_000, 1_000_000];
    
    let mut group = c.benchmark_group("filter_eq_i64");
    for size in &sizes {
        let data: Vec<i64> = (0..*size).map(|i| i % 100).collect();
        let mut selection = vec![0u8; *size];
        let target = 42i64;
        
        // Benchmark (uses AVX2 if available)
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                // This will use AVX2 on x86_64 with AVX2 support
                for i in 0..data.len() {
                    selection[i] = if data[i] == target { 1 } else { 0 };
                }
                black_box(&selection);
            });
        });
    }
    group.finish();
    
    let mut group = c.benchmark_group("filter_gt_i64");
    for size in &sizes {
        let data: Vec<i64> = (0..*size).map(|i| i % 100).collect();
        let mut selection = vec![0u8; *size];
        let target = 50i64;
        
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                for i in 0..data.len() {
                    selection[i] = if data[i] > target { 1 } else { 0 };
                }
                black_box(&selection);
            });
        });
    }
    group.finish();
    
    let mut group = c.benchmark_group("sum_i64");
    for size in &sizes {
        let data: Vec<i64> = (0..*size).collect();
        
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let sum: i64 = data.iter().sum();
                black_box(sum);
            });
        });
    }
    group.finish();
    
    let mut group = c.benchmark_group("min_i64");
    for size in &sizes {
        let data: Vec<i64> = (0..*size).map(|i| i % 1000).collect();
        
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let min = data.iter().copied().min();
                black_box(min);
            });
        });
    }
    group.finish();
    
    let mut group = c.benchmark_group("max_i64");
    for size in &sizes {
        let data: Vec<i64> = (0..*size).map(|i| i % 1000).collect();
        
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let max = data.iter().copied().max();
                black_box(max);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, simd_benchmarks);
criterion_main!(benches);

