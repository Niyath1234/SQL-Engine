/// Manual AVX2/AVX-512 SIMD kernels for hot-path operations
/// Phase 1: Core Single-Node Speed optimization
/// 
/// These kernels provide explicit SIMD intrinsics for critical operations:
/// - Int64/Float64 filters (=, <, >, <=, >=)
/// - Int64/Float64 aggregations (SUM, MIN, MAX, COUNT)
/// - Simple arithmetic projections

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// Check if AVX2 is available at runtime
#[inline]
pub fn is_avx2_available() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        is_x86_feature_detected!("avx2")
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

/// Check if AVX-512 is available at runtime
#[inline]
pub fn is_avx512_available() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        is_x86_feature_detected!("avx512f")
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

// ============================================================================
// INT64 FILTERS (AVX2: 4 lanes, AVX-512: 8 lanes)
// ============================================================================

/// SIMD equality filter for Int64 arrays (AVX2)
/// Returns a selection vector (1 = match, 0 = no match)
#[target_feature(enable = "avx2")]
#[cfg(target_arch = "x86_64")]
unsafe fn simd_filter_eq_i64_avx2(data: &[i64], target: i64, selection: &mut [u8]) {
    const LANES: usize = 4; // AVX2 processes 4 x i64 at once
    let len = data.len();
    let chunks = len / LANES;
    
    // Broadcast target to all lanes
    let target_vec = _mm256_set1_epi64x(target);
    
    // Process 4 i64s at a time
    for i in 0..chunks {
        let idx = i * LANES;
        
        // Load 4 i64 values
        let data_vec = _mm256_loadu_si256(data.as_ptr().add(idx) as *const __m256i);
        
        // Compare for equality
        let cmp = _mm256_cmpeq_epi64(data_vec, target_vec);
        
        // Extract comparison results
        let mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp));
        
        // Write selection bits
        selection[idx] = if mask & 0b0001 != 0 { 1 } else { 0 };
        selection[idx + 1] = if mask & 0b0010 != 0 { 1 } else { 0 };
        selection[idx + 2] = if mask & 0b0100 != 0 { 1 } else { 0 };
        selection[idx + 3] = if mask & 0b1000 != 0 { 1 } else { 0 };
    }
    
    // Handle remainder scalar
    for i in (chunks * LANES)..len {
        selection[i] = if data[i] == target { 1 } else { 0 };
    }
}

/// SIMD greater-than filter for Int64 arrays (AVX2)
#[target_feature(enable = "avx2")]
#[cfg(target_arch = "x86_64")]
unsafe fn simd_filter_gt_i64_avx2(data: &[i64], target: i64, selection: &mut [u8]) {
    const LANES: usize = 4;
    let len = data.len();
    let chunks = len / LANES;
    
    let target_vec = _mm256_set1_epi64x(target);
    
    for i in 0..chunks {
        let idx = i * LANES;
        let data_vec = _mm256_loadu_si256(data.as_ptr().add(idx) as *const __m256i);
        
        // Compare for greater-than
        let cmp = _mm256_cmpgt_epi64(data_vec, target_vec);
        let mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp));
        
        selection[idx] = if mask & 0b0001 != 0 { 1 } else { 0 };
        selection[idx + 1] = if mask & 0b0010 != 0 { 1 } else { 0 };
        selection[idx + 2] = if mask & 0b0100 != 0 { 1 } else { 0 };
        selection[idx + 3] = if mask & 0b1000 != 0 { 1 } else { 0 };
    }
    
    for i in (chunks * LANES)..len {
        selection[i] = if data[i] > target { 1 } else { 0 };
    }
}

/// SIMD less-than filter for Int64 arrays (AVX2)
#[target_feature(enable = "avx2")]
#[cfg(target_arch = "x86_64")]
unsafe fn simd_filter_lt_i64_avx2(data: &[i64], target: i64, selection: &mut [u8]) {
    const LANES: usize = 4;
    let len = data.len();
    let chunks = len / LANES;
    
    let target_vec = _mm256_set1_epi64x(target);
    
    for i in 0..chunks {
        let idx = i * LANES;
        let data_vec = _mm256_loadu_si256(data.as_ptr().add(idx) as *const __m256i);
        
        // Less-than = target > data
        let cmp = _mm256_cmpgt_epi64(target_vec, data_vec);
        let mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp));
        
        selection[idx] = if mask & 0b0001 != 0 { 1 } else { 0 };
        selection[idx + 1] = if mask & 0b0010 != 0 { 1 } else { 0 };
        selection[idx + 2] = if mask & 0b0100 != 0 { 1 } else { 0 };
        selection[idx + 3] = if mask & 0b1000 != 0 { 1 } else { 0 };
    }
    
    for i in (chunks * LANES)..len {
        selection[i] = if data[i] < target { 1 } else { 0 };
    }
}

// ============================================================================
// FLOAT64 FILTERS (AVX2: 4 lanes)
// ============================================================================

/// SIMD greater-than filter for Float64 arrays (AVX2)
#[target_feature(enable = "avx2")]
#[cfg(target_arch = "x86_64")]
unsafe fn simd_filter_gt_f64_avx2(data: &[f64], target: f64, selection: &mut [u8]) {
    const LANES: usize = 4;
    let len = data.len();
    let chunks = len / LANES;
    
    let target_vec = _mm256_set1_pd(target);
    
    for i in 0..chunks {
        let idx = i * LANES;
        let data_vec = _mm256_loadu_pd(data.as_ptr().add(idx));
        
        // Compare for greater-than
        let cmp = _mm256_cmp_pd::<_CMP_GT_OQ>(data_vec, target_vec);
        let mask = _mm256_movemask_pd(cmp);
        
        selection[idx] = if mask & 0b0001 != 0 { 1 } else { 0 };
        selection[idx + 1] = if mask & 0b0010 != 0 { 1 } else { 0 };
        selection[idx + 2] = if mask & 0b0100 != 0 { 1 } else { 0 };
        selection[idx + 3] = if mask & 0b1000 != 0 { 1 } else { 0 };
    }
    
    for i in (chunks * LANES)..len {
        selection[i] = if data[i] > target { 1 } else { 0 };
    }
}

/// SIMD less-than filter for Float64 arrays (AVX2)
#[target_feature(enable = "avx2")]
#[cfg(target_arch = "x86_64")]
unsafe fn simd_filter_lt_f64_avx2(data: &[f64], target: f64, selection: &mut [u8]) {
    const LANES: usize = 4;
    let len = data.len();
    let chunks = len / LANES;
    
    let target_vec = _mm256_set1_pd(target);
    
    for i in 0..chunks {
        let idx = i * LANES;
        let data_vec = _mm256_loadu_pd(data.as_ptr().add(idx));
        
        let cmp = _mm256_cmp_pd::<_CMP_LT_OQ>(data_vec, target_vec);
        let mask = _mm256_movemask_pd(cmp);
        
        selection[idx] = if mask & 0b0001 != 0 { 1 } else { 0 };
        selection[idx + 1] = if mask & 0b0010 != 0 { 1 } else { 0 };
        selection[idx + 2] = if mask & 0b0100 != 0 { 1 } else { 0 };
        selection[idx + 3] = if mask & 0b1000 != 0 { 1 } else { 0 };
    }
    
    for i in (chunks * LANES)..len {
        selection[i] = if data[i] < target { 1 } else { 0 };
    }
}

// ============================================================================
// INT64 AGGREGATIONS (AVX2)
// ============================================================================

/// SIMD sum for Int64 arrays (AVX2)
#[target_feature(enable = "avx2")]
#[cfg(target_arch = "x86_64")]
unsafe fn simd_sum_i64_avx2(data: &[i64]) -> i64 {
    const LANES: usize = 4;
    let len = data.len();
    let chunks = len / LANES;
    
    // Accumulator vector (4 partial sums)
    let mut acc = _mm256_setzero_si256();
    
    for i in 0..chunks {
        let idx = i * LANES;
        let data_vec = _mm256_loadu_si256(data.as_ptr().add(idx) as *const __m256i);
        acc = _mm256_add_epi64(acc, data_vec);
    }
    
    // Horizontal sum of 4 lanes
    let mut result = [0i64; 4];
    _mm256_storeu_si256(result.as_mut_ptr() as *mut __m256i, acc);
    let mut sum = result[0] + result[1] + result[2] + result[3];
    
    // Add remainder
    for i in (chunks * LANES)..len {
        sum += data[i];
    }
    
    sum
}

/// SIMD min for Int64 arrays (AVX2)
#[target_feature(enable = "avx2")]
#[cfg(target_arch = "x86_64")]
unsafe fn simd_min_i64_avx2(data: &[i64]) -> Option<i64> {
    if data.is_empty() {
        return None;
    }
    
    const LANES: usize = 4;
    let len = data.len();
    let chunks = len / LANES;
    
    // Initialize with first value
    let mut min_vec = _mm256_set1_epi64x(data[0]);
    
    for i in 0..chunks {
        let idx = i * LANES;
        let data_vec = _mm256_loadu_si256(data.as_ptr().add(idx) as *const __m256i);
        
        // AVX2 doesn't have direct _mm256_min_epi64, use comparison
        let cmp = _mm256_cmpgt_epi64(min_vec, data_vec);
        min_vec = _mm256_blendv_epi8(min_vec, data_vec, cmp);
    }
    
    // Horizontal min of 4 lanes
    let mut result = [0i64; 4];
    _mm256_storeu_si256(result.as_mut_ptr() as *mut __m256i, min_vec);
    let mut min = result[0].min(result[1]).min(result[2]).min(result[3]);
    
    // Check remainder
    for i in (chunks * LANES)..len {
        min = min.min(data[i]);
    }
    
    Some(min)
}

/// SIMD max for Int64 arrays (AVX2)
#[target_feature(enable = "avx2")]
#[cfg(target_arch = "x86_64")]
unsafe fn simd_max_i64_avx2(data: &[i64]) -> Option<i64> {
    if data.is_empty() {
        return None;
    }
    
    const LANES: usize = 4;
    let len = data.len();
    let chunks = len / LANES;
    
    let mut max_vec = _mm256_set1_epi64x(data[0]);
    
    for i in 0..chunks {
        let idx = i * LANES;
        let data_vec = _mm256_loadu_si256(data.as_ptr().add(idx) as *const __m256i);
        
        let cmp = _mm256_cmpgt_epi64(data_vec, max_vec);
        max_vec = _mm256_blendv_epi8(max_vec, data_vec, cmp);
    }
    
    let mut result = [0i64; 4];
    _mm256_storeu_si256(result.as_mut_ptr() as *mut __m256i, max_vec);
    let mut max = result[0].max(result[1]).max(result[2]).max(result[3]);
    
    for i in (chunks * LANES)..len {
        max = max.max(data[i]);
    }
    
    Some(max)
}

// ============================================================================
// FLOAT64 AGGREGATIONS (AVX2)
// ============================================================================

/// SIMD sum for Float64 arrays (AVX2)
#[target_feature(enable = "avx2")]
#[cfg(target_arch = "x86_64")]
unsafe fn simd_sum_f64_avx2(data: &[f64]) -> f64 {
    const LANES: usize = 4;
    let len = data.len();
    let chunks = len / LANES;
    
    let mut acc = _mm256_setzero_pd();
    
    for i in 0..chunks {
        let idx = i * LANES;
        let data_vec = _mm256_loadu_pd(data.as_ptr().add(idx));
        acc = _mm256_add_pd(acc, data_vec);
    }
    
    // Horizontal sum
    let mut result = [0.0; 4];
    _mm256_storeu_pd(result.as_mut_ptr(), acc);
    let mut sum = result[0] + result[1] + result[2] + result[3];
    
    for i in (chunks * LANES)..len {
        sum += data[i];
    }
    
    sum
}

/// SIMD min for Float64 arrays (AVX2)
#[target_feature(enable = "avx2")]
#[cfg(target_arch = "x86_64")]
unsafe fn simd_min_f64_avx2(data: &[f64]) -> Option<f64> {
    if data.is_empty() {
        return None;
    }
    
    const LANES: usize = 4;
    let len = data.len();
    let chunks = len / LANES;
    
    let mut min_vec = _mm256_set1_pd(data[0]);
    
    for i in 0..chunks {
        let idx = i * LANES;
        let data_vec = _mm256_loadu_pd(data.as_ptr().add(idx));
        min_vec = _mm256_min_pd(min_vec, data_vec);
    }
    
    let mut result = [0.0; 4];
    _mm256_storeu_pd(result.as_mut_ptr(), min_vec);
    let mut min = result[0].min(result[1]).min(result[2]).min(result[3]);
    
    for i in (chunks * LANES)..len {
        min = min.min(data[i]);
    }
    
    Some(min)
}

/// SIMD max for Float64 arrays (AVX2)
#[target_feature(enable = "avx2")]
#[cfg(target_arch = "x86_64")]
unsafe fn simd_max_f64_avx2(data: &[f64]) -> Option<f64> {
    if data.is_empty() {
        return None;
    }
    
    const LANES: usize = 4;
    let len = data.len();
    let chunks = len / LANES;
    
    let mut max_vec = _mm256_set1_pd(data[0]);
    
    for i in 0..chunks {
        let idx = i * LANES;
        let data_vec = _mm256_loadu_pd(data.as_ptr().add(idx));
        max_vec = _mm256_max_pd(max_vec, data_vec);
    }
    
    let mut result = [0.0; 4];
    _mm256_storeu_pd(result.as_mut_ptr(), max_vec);
    let mut max = result[0].max(result[1]).max(result[2]).max(result[3]);
    
    for i in (chunks * LANES)..len {
        max = max.max(data[i]);
    }
    
    Some(max)
}

// ============================================================================
// PUBLIC API (with runtime CPU feature detection)
// ============================================================================

/// Filter Int64 array for equality (auto-selects SIMD or scalar)
pub fn filter_eq_i64(data: &[i64], target: i64, selection: &mut [u8]) {
    assert_eq!(data.len(), selection.len());
    
    #[cfg(target_arch = "x86_64")]
    {
        if is_avx2_available() {
            unsafe { simd_filter_eq_i64_avx2(data, target, selection) };
            return;
        }
    }
    
    // Scalar fallback
    for i in 0..data.len() {
        selection[i] = if data[i] == target { 1 } else { 0 };
    }
}

/// Filter Int64 array for greater-than
pub fn filter_gt_i64(data: &[i64], target: i64, selection: &mut [u8]) {
    assert_eq!(data.len(), selection.len());
    
    #[cfg(target_arch = "x86_64")]
    {
        if is_avx2_available() {
            unsafe { simd_filter_gt_i64_avx2(data, target, selection) };
            return;
        }
    }
    
    for i in 0..data.len() {
        selection[i] = if data[i] > target { 1 } else { 0 };
    }
}

/// Filter Int64 array for less-than
pub fn filter_lt_i64(data: &[i64], target: i64, selection: &mut [u8]) {
    assert_eq!(data.len(), selection.len());
    
    #[cfg(target_arch = "x86_64")]
    {
        if is_avx2_available() {
            unsafe { simd_filter_lt_i64_avx2(data, target, selection) };
            return;
        }
    }
    
    for i in 0..data.len() {
        selection[i] = if data[i] < target { 1 } else { 0 };
    }
}

/// Filter Float64 array for greater-than
pub fn filter_gt_f64(data: &[f64], target: f64, selection: &mut [u8]) {
    assert_eq!(data.len(), selection.len());
    
    #[cfg(target_arch = "x86_64")]
    {
        if is_avx2_available() {
            unsafe { simd_filter_gt_f64_avx2(data, target, selection) };
            return;
        }
    }
    
    for i in 0..data.len() {
        selection[i] = if data[i] > target { 1 } else { 0 };
    }
}

/// Filter Float64 array for less-than
pub fn filter_lt_f64(data: &[f64], target: f64, selection: &mut [u8]) {
    assert_eq!(data.len(), selection.len());
    
    #[cfg(target_arch = "x86_64")]
    {
        if is_avx2_available() {
            unsafe { simd_filter_lt_f64_avx2(data, target, selection) };
            return;
        }
    }
    
    for i in 0..data.len() {
        selection[i] = if data[i] < target { 1 } else { 0 };
    }
}

/// Sum Int64 array
pub fn sum_i64(data: &[i64]) -> i64 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_avx2_available() {
            return unsafe { simd_sum_i64_avx2(data) };
        }
    }
    
    data.iter().sum()
}

/// Min Int64 array
pub fn min_i64(data: &[i64]) -> Option<i64> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_avx2_available() {
            return unsafe { simd_min_i64_avx2(data) };
        }
    }
    
    data.iter().copied().min()
}

/// Max Int64 array
pub fn max_i64(data: &[i64]) -> Option<i64> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_avx2_available() {
            return unsafe { simd_max_i64_avx2(data) };
        }
    }
    
    data.iter().copied().max()
}

/// Sum Float64 array
pub fn sum_f64(data: &[f64]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_avx2_available() {
            return unsafe { simd_sum_f64_avx2(data) };
        }
    }
    
    data.iter().sum()
}

/// Min Float64 array
pub fn min_f64(data: &[f64]) -> Option<f64> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_avx2_available() {
            return unsafe { simd_min_f64_avx2(data) };
        }
    }
    
    data.iter().copied().fold(None, |acc, x| {
        Some(acc.map_or(x, |a| a.min(x)))
    })
}

/// Max Float64 array
pub fn max_f64(data: &[f64]) -> Option<f64> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_avx2_available() {
            return unsafe { simd_max_f64_avx2(data) };
        }
    }
    
    data.iter().copied().fold(None, |acc, x| {
        Some(acc.map_or(x, |a| a.max(x)))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_filter_eq_i64() {
        let data = vec![1, 2, 3, 4, 5, 2, 7, 8];
        let mut selection = vec![0u8; data.len()];
        filter_eq_i64(&data, 2, &mut selection);
        assert_eq!(selection, vec![0, 1, 0, 0, 0, 1, 0, 0]);
    }
    
    #[test]
    fn test_filter_gt_i64() {
        let data = vec![1, 2, 3, 4, 5];
        let mut selection = vec![0u8; data.len()];
        filter_gt_i64(&data, 3, &mut selection);
        assert_eq!(selection, vec![0, 0, 0, 1, 1]);
    }
    
    #[test]
    fn test_sum_i64() {
        let data = vec![1, 2, 3, 4, 5];
        assert_eq!(sum_i64(&data), 15);
    }
    
    #[test]
    fn test_min_max_i64() {
        let data = vec![5, 2, 8, 1, 9];
        assert_eq!(min_i64(&data), Some(1));
        assert_eq!(max_i64(&data), Some(9));
    }
    
    #[test]
    fn test_sum_f64() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((sum_f64(&data) - 15.0).abs() < 1e-10);
    }
}

