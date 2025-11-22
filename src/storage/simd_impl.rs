/// SIMD-optimized vector similarity calculations
/// Uses SIMD instructions for faster cosine similarity and L2 distance
use std::arch::x86_64::*;

/// SIMD-optimized cosine similarity calculation
/// Uses AVX/FMA instructions when available, falls back to scalar
pub fn cosine_similarity_simd(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
            unsafe {
                return cosine_similarity_avx(a, b);
            }
        }
    }
    
    // Fallback to optimized scalar version
    cosine_similarity_scalar(a, b)
}

/// SIMD-optimized L2 distance calculation
pub fn l2_distance_simd(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
            unsafe {
                return l2_distance_avx(a, b);
            }
        }
    }
    
    // Fallback to optimized scalar version
    l2_distance_scalar(a, b)
}

/// AVX-optimized cosine similarity
/// Uses 8 f32 values at once (256-bit AVX registers)
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx,fma")]
unsafe fn cosine_similarity_avx(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len().min(b.len());
    if len == 0 {
        return 0.0;
    }
    
    let mut dot_sum = _mm256_setzero_ps();
    let mut norm_a_sum = _mm256_setzero_ps();
    let mut norm_b_sum = _mm256_setzero_ps();
    
    let chunks = len / 8;
    let remainder = len % 8;
    
    // Process 8 floats at a time
    for i in 0..chunks {
        let offset = i * 8;
        let a_vec = _mm256_loadu_ps(a.as_ptr().add(offset));
        let b_vec = _mm256_loadu_ps(b.as_ptr().add(offset));
        
        // Dot product: a * b
        dot_sum = _mm256_fmadd_ps(a_vec, b_vec, dot_sum);
        
        // Norm a: a * a
        norm_a_sum = _mm256_fmadd_ps(a_vec, a_vec, norm_a_sum);
        
        // Norm b: b * b
        norm_b_sum = _mm256_fmadd_ps(b_vec, b_vec, norm_b_sum);
    }
    
    // Horizontal sum of dot_product
    let dot_array = std::mem::transmute::<__m256, [f32; 8]>(dot_sum);
    let mut dot_product = dot_array[0] + dot_array[1] + dot_array[2] + dot_array[3] +
                         dot_array[4] + dot_array[5] + dot_array[6] + dot_array[7];
    
    // Horizontal sum of norm_a
    let norm_a_array = std::mem::transmute::<__m256, [f32; 8]>(norm_a_sum);
    let mut norm_a = norm_a_array[0] + norm_a_array[1] + norm_a_array[2] + norm_a_array[3] +
                    norm_a_array[4] + norm_a_array[5] + norm_a_array[6] + norm_a_array[7];
    
    // Horizontal sum of norm_b
    let norm_b_array = std::mem::transmute::<__m256, [f32; 8]>(norm_b_sum);
    let mut norm_b = norm_b_array[0] + norm_b_array[1] + norm_b_array[2] + norm_b_array[3] +
                    norm_b_array[4] + norm_b_array[5] + norm_b_array[6] + norm_b_array[7];
    
    // Handle remainder
    for i in (chunks * 8)..len {
        dot_product += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    
    let norm_a = norm_a.sqrt();
    let norm_b = norm_b.sqrt();
    
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    
    dot_product / (norm_a * norm_b)
}

/// AVX-optimized L2 distance
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx,fma")]
unsafe fn l2_distance_avx(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len().min(b.len());
    if len == 0 {
        return 0.0;
    }
    
    let mut diff_sum = _mm256_setzero_ps();
    
    let chunks = len / 8;
    let remainder = len % 8;
    
    // Process 8 floats at a time: (a - b)^2
    for i in 0..chunks {
        let offset = i * 8;
        let a_vec = _mm256_loadu_ps(a.as_ptr().add(offset));
        let b_vec = _mm256_loadu_ps(b.as_ptr().add(offset));
        
        let diff = _mm256_sub_ps(a_vec, b_vec);
        diff_sum = _mm256_fmadd_ps(diff, diff, diff_sum);
    }
    
    // Horizontal sum
    let diff_array = std::mem::transmute::<__m256, [f32; 8]>(diff_sum);
    let mut distance_sq = diff_array[0] + diff_array[1] + diff_array[2] + diff_array[3] +
                         diff_array[4] + diff_array[5] + diff_array[6] + diff_array[7];
    
    // Handle remainder
    for i in (chunks * 8)..len {
        let diff = a[i] - b[i];
        distance_sq += diff * diff;
    }
    
    distance_sq.sqrt()
}

/// Optimized scalar cosine similarity (with better compiler hints)
#[inline(always)]
fn cosine_similarity_scalar(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len().min(b.len());
    if len == 0 {
        return 0.0;
    }
    
    // Use separate accumulators for better CPU pipelining
    let mut dot_product = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    
    // Process in chunks for better cache locality
    const CHUNK_SIZE: usize = 32;
    let chunks = len / CHUNK_SIZE;
    let remainder = len % CHUNK_SIZE;
    
    for chunk_idx in 0..chunks {
        let start = chunk_idx * CHUNK_SIZE;
        let end = start + CHUNK_SIZE;
        
        for i in start..end {
            let ai = a[i];
            let bi = b[i];
            dot_product += ai * bi;
            norm_a += ai * ai;
            norm_b += bi * bi;
        }
    }
    
    // Handle remainder
    for i in (chunks * CHUNK_SIZE)..len {
        let ai = a[i];
        let bi = b[i];
        dot_product += ai * bi;
        norm_a += ai * ai;
        norm_b += bi * bi;
    }
    
    let norm_a = norm_a.sqrt();
    let norm_b = norm_b.sqrt();
    
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    
    dot_product / (norm_a * norm_b)
}

/// Optimized scalar L2 distance
#[inline(always)]
fn l2_distance_scalar(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len().min(b.len());
    if len == 0 {
        return 0.0;
    }
    
    let mut distance_sq = 0.0f32;
    
    // Process in chunks for better cache locality
    const CHUNK_SIZE: usize = 32;
    let chunks = len / CHUNK_SIZE;
    let remainder = len % CHUNK_SIZE;
    
    for chunk_idx in 0..chunks {
        let start = chunk_idx * CHUNK_SIZE;
        let end = start + CHUNK_SIZE;
        
        for i in start..end {
            let diff = a[i] - b[i];
            distance_sq += diff * diff;
        }
    }
    
    // Handle remainder
    for i in (chunks * CHUNK_SIZE)..len {
        let diff = a[i] - b[i];
        distance_sq += diff * diff;
    }
    
    distance_sq.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![1.0, 2.0, 3.0];
        
        // Identical vectors should have similarity 1.0
        let similarity = cosine_similarity_simd(&a, &b);
        assert!((similarity - 1.0).abs() < 0.0001);
    }
    
    #[test]
    fn test_l2_distance() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![3.0, 4.0, 0.0];
        
        // Distance should be 5.0 (3-4-5 triangle)
        let distance = l2_distance_simd(&a, &b);
        assert!((distance - 5.0).abs() < 0.0001);
    }
}

