/// Batch 1 Integration Tests: Validate SIMD and fused operators
/// 
/// Run with: cargo test --test batch1_integration
use hypergraph_sql_engine::execution::simd_avx2;
use hypergraph_sql_engine::execution::fused_scan::{FusedScanOperator, ProjectionExpr, ProjectionExprType};

#[test]
fn test_simd_filter_correctness() {
    // Test all SIMD filters produce correct results
    
    // Int64 equality
    let data = vec![1, 5, 3, 5, 2, 5, 7];
    let mut selection = vec![0u8; data.len()];
    simd_avx2::filter_eq_i64(&data, 5, &mut selection);
    assert_eq!(selection, vec![0, 1, 0, 1, 0, 1, 0]);
    
    // Int64 greater-than
    let data = vec![1, 5, 3, 8, 2, 10, 7];
    let mut selection = vec![0u8; data.len()];
    simd_avx2::filter_gt_i64(&data, 5, &mut selection);
    assert_eq!(selection, vec![0, 0, 0, 1, 0, 1, 1]);
    
    // Int64 less-than
    let data = vec![1, 5, 3, 8, 2, 10, 7];
    let mut selection = vec![0u8; data.len()];
    simd_avx2::filter_lt_i64(&data, 5, &mut selection);
    assert_eq!(selection, vec![1, 0, 1, 0, 1, 0, 0]);
    
    // Float64 greater-than
    let data = vec![1.5, 5.5, 3.3, 8.1, 2.2, 10.0, 7.7];
    let mut selection = vec![0u8; data.len()];
    simd_avx2::filter_gt_f64(&data, 5.0, &mut selection);
    assert_eq!(selection, vec![0, 1, 0, 1, 0, 1, 1]);
    
    // Float64 less-than
    let data = vec![1.5, 5.5, 3.3, 8.1, 2.2, 10.0, 7.7];
    let mut selection = vec![0u8; data.len()];
    simd_avx2::filter_lt_f64(&data, 5.0, &mut selection);
    assert_eq!(selection, vec![1, 0, 1, 0, 1, 0, 0]);
}

#[test]
fn test_simd_aggregation_correctness() {
    // Test all SIMD aggregations produce correct results
    
    // Int64 sum
    let data = vec![1, 2, 3, 4, 5];
    assert_eq!(simd_avx2::sum_i64(&data), 15);
    
    // Int64 min
    let data = vec![5, 2, 8, 1, 9, 3];
    assert_eq!(simd_avx2::min_i64(&data), Some(1));
    
    // Int64 max
    let data = vec![5, 2, 8, 1, 9, 3];
    assert_eq!(simd_avx2::max_i64(&data), Some(9));
    
    // Float64 sum
    let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    assert!((simd_avx2::sum_f64(&data) - 15.0).abs() < 1e-10);
    
    // Float64 min
    let data = vec![5.5, 2.2, 8.8, 1.1, 9.9, 3.3];
    assert!((simd_avx2::min_f64(&data).unwrap() - 1.1).abs() < 1e-10);
    
    // Float64 max
    let data = vec![5.5, 2.2, 8.8, 1.1, 9.9, 3.3];
    assert!((simd_avx2::max_f64(&data).unwrap() - 9.9).abs() < 1e-10);
}

#[test]
fn test_simd_edge_cases() {
    // Test edge cases: empty, single element, odd sizes
    
    // Empty array
    let data: Vec<i64> = vec![];
    assert_eq!(simd_avx2::sum_i64(&data), 0);
    assert_eq!(simd_avx2::min_i64(&data), None);
    assert_eq!(simd_avx2::max_i64(&data), None);
    
    // Single element
    let data = vec![42];
    assert_eq!(simd_avx2::sum_i64(&data), 42);
    assert_eq!(simd_avx2::min_i64(&data), Some(42));
    assert_eq!(simd_avx2::max_i64(&data), Some(42));
    
    let mut selection = vec![0u8; 1];
    simd_avx2::filter_eq_i64(&data, 42, &mut selection);
    assert_eq!(selection, vec![1]);
    
    // Odd sizes (not multiple of 4 - tests remainder handling)
    let data = vec![1, 2, 3, 4, 5, 6, 7];  // 7 elements
    assert_eq!(simd_avx2::sum_i64(&data), 28);
    
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];  // 9 elements
    assert_eq!(simd_avx2::sum_i64(&data), 45);
    
    // Large array (tests chunked processing)
    let data: Vec<i64> = (0..10_000).collect();
    let expected_sum: i64 = (0..10_000).sum();
    assert_eq!(simd_avx2::sum_i64(&data), expected_sum);
}

#[test]
fn test_simd_performance_characteristics() {
    // Smoke test that SIMD is actually being used
    // (We expect AVX2 if available, or scalar fallback)
    
    println!("AVX2 available: {}", simd_avx2::is_avx2_available());
    println!("AVX-512 available: {}", simd_avx2::is_avx512_available());
    
    // Large dataset
    let size = 1_000_000;
    let data: Vec<i64> = (0..size).map(|i| i % 100).collect();
    let mut selection = vec![0u8; size];
    
    // This should use SIMD if available
    let start = std::time::Instant::now();
    simd_avx2::filter_eq_i64(&data, 42, &mut selection);
    let elapsed = start.elapsed();
    
    println!("Filtered {} elements in {:?}", size, elapsed);
    println!("Throughput: {:.2} M elements/sec", 
        size as f64 / elapsed.as_secs_f64() / 1_000_000.0);
    
    // Verify correctness
    let matches: usize = selection.iter().map(|&x| x as usize).sum();
    let expected_matches = data.iter().filter(|&&x| x == 42).count();
    assert_eq!(matches, expected_matches);
}

#[test]
fn test_simd_vs_scalar_equivalence() {
    // Verify SIMD and scalar produce identical results
    use rand::Rng;
    
    let mut rng = rand::thread_rng();
    let size = 1_000;
    let data: Vec<i64> = (0..size).map(|_| rng.gen_range(0..100)).collect();
    
    let target = 50i64;
    
    // SIMD result
    let mut simd_selection = vec![0u8; size];
    simd_avx2::filter_eq_i64(&data, target, &mut simd_selection);
    
    // Scalar result
    let mut scalar_selection = vec![0u8; size];
    for i in 0..size {
        scalar_selection[i] = if data[i] == target { 1 } else { 0 };
    }
    
    // Should be identical
    assert_eq!(simd_selection, scalar_selection);
}

#[test]
fn test_simd_all_zeros() {
    // Test with data that produces all zeros (no matches)
    let data = vec![1, 2, 3, 4, 5];
    let mut selection = vec![0u8; data.len()];
    simd_avx2::filter_eq_i64(&data, 10, &mut selection);
    assert_eq!(selection, vec![0, 0, 0, 0, 0]);
}

#[test]
fn test_simd_all_ones() {
    // Test with data that produces all ones (all match)
    let data = vec![5, 5, 5, 5, 5];
    let mut selection = vec![0u8; data.len()];
    simd_avx2::filter_eq_i64(&data, 5, &mut selection);
    assert_eq!(selection, vec![1, 1, 1, 1, 1]);
}

#[test]
fn test_simd_negative_numbers() {
    // Test with negative integers
    let data = vec![-5, -2, 0, 3, -1, 10, -8];
    let mut selection = vec![0u8; data.len()];
    
    // Greater than 0
    simd_avx2::filter_gt_i64(&data, 0, &mut selection);
    assert_eq!(selection, vec![0, 0, 0, 1, 0, 1, 0]);
    
    // Less than 0
    simd_avx2::filter_lt_i64(&data, 0, &mut selection);
    assert_eq!(selection, vec![1, 1, 0, 0, 1, 0, 1]);
}

#[test]
fn test_aggregation_with_negatives() {
    let data = vec![-5, -2, 0, 3, -1, 10, -8];
    assert_eq!(simd_avx2::sum_i64(&data), -3);
    assert_eq!(simd_avx2::min_i64(&data), Some(-8));
    assert_eq!(simd_avx2::max_i64(&data), Some(10));
}

// Note: Fused scan tests would go here, but they require a full hypergraph setup
// For now, we focus on SIMD kernel validation

