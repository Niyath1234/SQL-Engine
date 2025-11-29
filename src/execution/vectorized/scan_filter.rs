/// SIMD-optimized scan and filter operations
/// Uses std::arch intrinsics for stable Rust compatibility
use arrow::array::*;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// SIMD lane count for i64 (8 lanes = 512 bits with AVX-512, 4 lanes = 256 bits with AVX2)
#[cfg(target_arch = "x86_64")]
const SIMD_LANES_I64: usize = 4; // Use AVX2 (256-bit) for compatibility

/// SIMD-optimized filter for greater-than on Int64 arrays
/// Returns indices of matching rows
#[cfg(target_arch = "x86_64")]
pub fn filter_gt_i64_simd(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    out.clear();
    let len = values.len();
    
    // Check if AVX2 is available
    if is_x86_feature_detected!("avx2") {
        unsafe {
            filter_gt_i64_avx2(values, cmp, out);
        }
    } else {
        // Fallback to scalar implementation
        filter_gt_i64_scalar(values, cmp, out);
    }
}

/// AVX2-optimized filter for greater-than on Int64 arrays
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_gt_i64_avx2(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    let data = values.values();
    let len = data.len();
    let cmp_vec = _mm256_set1_epi64x(cmp);
    let mut i = 0;
    
    // Process 4 elements at a time (AVX2 can handle 4x i64)
    while i + 4 <= len {
        // Load 4 i64 values
        let vec = _mm256_loadu_si256(data.as_ptr().add(i) as *const __m256i);
        
        // Compare: vec > cmp
        let mask = _mm256_cmpgt_epi64(vec, cmp_vec);
        
        // Extract the 4 comparison results by storing to memory and checking
        // We'll check each element individually for null handling
        // The mask contains -1 (all bits set) for true, 0 for false
        let mut mask_results = [0i64; 4];
        _mm256_storeu_si256(mask_results.as_mut_ptr() as *mut __m256i, mask);
        
        // Check each of the 4 lanes
        for lane in 0..4 {
            if mask_results[lane] != 0 && !values.is_null(i + lane) && data[i + lane] > cmp {
                out.push((i + lane) as u32);
            }
        }
        
        i += 4;
    }
    
    // Handle remainder
    for j in i..len {
        if !values.is_null(j) && data[j] > cmp {
            out.push(j as u32);
        }
    }
}

/// Scalar fallback for filter greater-than
#[cfg(not(target_arch = "x86_64"))]
pub fn filter_gt_i64_scalar(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    for i in 0..values.len() {
        if !values.is_null(i) && values.value(i) > cmp {
            out.push(i as u32);
        }
    }
}

#[cfg(target_arch = "x86_64")]
fn filter_gt_i64_scalar(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    for i in 0..values.len() {
        if !values.is_null(i) && values.value(i) > cmp {
            out.push(i as u32);
        }
    }
}

/// SIMD-optimized filter for less-than on Int64 arrays
#[cfg(target_arch = "x86_64")]
pub fn filter_lt_i64_simd(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    out.clear();
    let len = values.len();
    
    if is_x86_feature_detected!("avx2") {
        unsafe {
            filter_lt_i64_avx2(values, cmp, out);
        }
    } else {
        filter_lt_i64_scalar(values, cmp, out);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_lt_i64_avx2(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    let data = values.values();
    let len = data.len();
    let cmp_vec = _mm256_set1_epi64x(cmp);
    let mut i = 0;
    
    while i + 4 <= len {
        let vec = _mm256_loadu_si256(data.as_ptr().add(i) as *const __m256i);
        let mask = _mm256_cmpgt_epi64(cmp_vec, vec); // cmp > vec means vec < cmp
        
        let mut mask_results = [0i64; 4];
        _mm256_storeu_si256(mask_results.as_mut_ptr() as *mut __m256i, mask);
        
        for lane in 0..4 {
            if mask_results[lane] != 0 && !values.is_null(i + lane) && data[i + lane] < cmp {
                out.push((i + lane) as u32);
            }
        }
        
        i += 4;
    }
    
    for j in i..len {
        if !values.is_null(j) && data[j] < cmp {
            out.push(j as u32);
        }
    }
}

#[cfg(target_arch = "x86_64")]
fn filter_lt_i64_scalar(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    for i in 0..values.len() {
        if !values.is_null(i) && values.value(i) < cmp {
            out.push(i as u32);
        }
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn filter_lt_i64_scalar(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    for i in 0..values.len() {
        if !values.is_null(i) && values.value(i) < cmp {
            out.push(i as u32);
        }
    }
}

/// SIMD-optimized filter for equality on Int64 arrays
#[cfg(target_arch = "x86_64")]
pub fn filter_eq_i64_simd(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    out.clear();
    let len = values.len();
    
    if is_x86_feature_detected!("avx2") {
        unsafe {
            filter_eq_i64_avx2(values, cmp, out);
        }
    } else {
        filter_eq_i64_scalar(values, cmp, out);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_eq_i64_avx2(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    let data = values.values();
    let len = data.len();
    let cmp_vec = _mm256_set1_epi64x(cmp);
    let mut i = 0;
    
    while i + 4 <= len {
        let vec = _mm256_loadu_si256(data.as_ptr().add(i) as *const __m256i);
        // For equality, use _mm256_cmpeq_epi64
        let mask = _mm256_cmpeq_epi64(vec, cmp_vec);
        
        let mut mask_results = [0i64; 4];
        _mm256_storeu_si256(mask_results.as_mut_ptr() as *mut __m256i, mask);
        
        for lane in 0..4 {
            if mask_results[lane] != 0 && !values.is_null(i + lane) && data[i + lane] == cmp {
                out.push((i + lane) as u32);
            }
        }
        
        i += 4;
    }
    
    for j in i..len {
        if !values.is_null(j) && data[j] == cmp {
            out.push(j as u32);
        }
    }
}

#[cfg(target_arch = "x86_64")]
fn filter_eq_i64_scalar(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    for i in 0..values.len() {
        if !values.is_null(i) && values.value(i) == cmp {
            out.push(i as u32);
        }
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn filter_eq_i64_scalar(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    for i in 0..values.len() {
        if !values.is_null(i) && values.value(i) == cmp {
            out.push(i as u32);
        }
    }
}

/// Non-x86_64 fallback: use scalar implementation
#[cfg(not(target_arch = "x86_64"))]
pub fn filter_gt_i64_simd(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    filter_gt_i64_scalar(values, cmp, out);
}

#[cfg(not(target_arch = "x86_64"))]
pub fn filter_lt_i64_simd(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    filter_lt_i64_scalar(values, cmp, out);
}

#[cfg(not(target_arch = "x86_64"))]
pub fn filter_eq_i64_simd(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    filter_eq_i64_scalar(values, cmp, out);
}

