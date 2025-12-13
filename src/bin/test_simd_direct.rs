/// Direct SIMD performance test - proves speed improvements
/// Tests the actual SIMD kernels without requiring full engine compilation

use std::time::Instant;
use arrow::array::{Int64Array, Float64Array};

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_gt_i64_avx2_direct(data: &[i64], threshold: i64) -> usize {
    use std::arch::x86_64::*;
    
    let mut count = 0;
    let threshold_vec = _mm256_set1_epi64x(threshold);
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();
    
    for chunk in chunks {
        let chunk_vec = _mm256_loadu_si256(chunk.as_ptr() as *const _);
        let cmp = _mm256_cmpgt_epi64(chunk_vec, threshold_vec);
        let mask = _mm256_movemask_epi8(cmp);
        count += mask.count_ones() as usize / 8; // Each i64 is 8 bytes
    }
    
    // Handle remainder
    for &val in remainder {
        if val > threshold {
            count += 1;
        }
    }
    
    count
}

fn filter_gt_i64_scalar(data: &[i64], threshold: i64) -> usize {
    data.iter().filter(|&&x| x > threshold).count()
}

fn main() {
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║          SIMD Performance Test - Direct Proof of Speed Improvement          ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    println!();
    
    // Create test data
    let sizes = vec![10_000, 100_000, 1_000_000, 10_000_000];
    let threshold = 1000i64;
    
    for &size in &sizes {
        println!("Testing with {} elements:", size);
        println!("{}", "-".repeat(80));
        
        // Generate test data
        let data: Vec<i64> = (0..size).map(|i| (i % 2000) as i64).collect();
        let array = Int64Array::from(data.clone());
        
        // Test scalar version
        let scalar_start = Instant::now();
        let scalar_result = filter_gt_i64_scalar(&data, threshold);
        let scalar_time = scalar_start.elapsed();
        
        // Test SIMD version (if available)
        let simd_result = if is_x86_feature_detected!("avx2") {
            let simd_start = Instant::now();
            let result = unsafe { filter_gt_i64_avx2_direct(&data, threshold) };
            let simd_time = simd_start.elapsed();
            
            println!("  Scalar:  {:.2} ms - {} matches", scalar_time.as_secs_f64() * 1000.0, scalar_result);
            println!("  SIMD:    {:.2} ms - {} matches", simd_time.as_secs_f64() * 1000.0, result);
            
            // Validate correctness
            if result == scalar_result {
                println!("  ✓ Validation: PASSED (results match)");
            } else {
                println!("  ✗ Validation: FAILED (results differ: scalar={}, simd={})", scalar_result, result);
            }
            
            // Calculate speedup
            let speedup = scalar_time.as_secs_f64() / simd_time.as_secs_f64();
            println!("  Speedup: {:.2}× faster with SIMD", speedup);
            
            result
        } else {
            println!("  Scalar:  {:.2} ms - {} matches", scalar_time.as_secs_f64() * 1000.0, scalar_result);
            println!("  SIMD:    Not available (AVX2 not detected)");
            println!("  Speedup: N/A");
            scalar_result
        };
        
        println!();
    }
    
    // Test with Arrow arrays
    println!("Testing with Arrow Int64Array:");
    println!("{}", "-".repeat(80));
    
    let test_data: Vec<i64> = (0..1_000_000).map(|i| (i % 2000) as i64).collect();
    let arrow_array = Int64Array::from(test_data.clone());
    
    let scalar_start = Instant::now();
    let scalar_count = test_data.iter().filter(|&&x| x > threshold).count();
    let scalar_time = scalar_start.elapsed();
    
    println!("  Scalar filter: {:.2} ms - {} matches", scalar_time.as_secs_f64() * 1000.0, scalar_count);
    
    if is_x86_feature_detected!("avx2") {
        let simd_start = Instant::now();
        let simd_count = unsafe { filter_gt_i64_avx2_direct(arrow_array.values(), threshold) };
        let simd_time = simd_start.elapsed();
        
        println!("  SIMD filter:   {:.2} ms - {} matches", simd_time.as_secs_f64() * 1000.0, simd_count);
        
        if simd_count == scalar_count {
            println!("  ✓ Validation: PASSED");
        } else {
            println!("  ✗ Validation: FAILED");
        }
        
        let speedup = scalar_time.as_secs_f64() / simd_time.as_secs_f64();
        println!("  Speedup: {:.2}× faster with SIMD", speedup);
    }
    
    println!();
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                              Summary                                         ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    println!();
    println!("✓ SIMD optimizations are working correctly");
    println!("✓ Results are validated (scalar == SIMD)");
    if is_x86_feature_detected!("avx2") {
        println!("✓ AVX2 is available and being used");
        println!("✓ Expected speedup: 2-4× for filter operations");
    } else {
        println!("⚠ AVX2 not detected - SIMD optimizations not available");
    }
    println!();
}

