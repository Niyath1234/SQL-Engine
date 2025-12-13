/// Vector Indexes for Similarity Search
/// Supports HNSW (Hierarchical Navigable Small World) and other ANN algorithms
/// Phase 2: Optimized with SIMD and early termination
use crate::storage::fragment::ColumnFragment;
use std::sync::Arc;
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use anyhow::Result;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

// SIMD-optimized functions - inline implementation
pub mod simd_ops {
    
    pub fn cosine_similarity_simd(a: &[f32], b: &[f32]) -> f32 {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
                unsafe {
                    return cosine_similarity_avx_impl(a, b);
                }
            }
        }
        cosine_similarity_scalar_impl(a, b)
    }

    pub fn l2_distance_simd(a: &[f32], b: &[f32]) -> f32 {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
                unsafe {
                    return l2_distance_avx_impl(a, b);
                }
            }
        }
        l2_distance_scalar_impl(a, b)
    }
    
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx,fma")]
    unsafe fn cosine_similarity_avx_impl(a: &[f32], b: &[f32]) -> f32 {
        use std::arch::x86_64::*;
        let len = a.len().min(b.len());
        if len == 0 { return 0.0; }
        
        let mut dot_sum = _mm256_setzero_ps();
        let mut norm_a_sum = _mm256_setzero_ps();
        let mut norm_b_sum = _mm256_setzero_ps();
        let chunks = len / 8;
        
        for i in 0..chunks {
            let offset = i * 8;
            let a_vec = _mm256_loadu_ps(a.as_ptr().add(offset));
            let b_vec = _mm256_loadu_ps(b.as_ptr().add(offset));
            dot_sum = _mm256_fmadd_ps(a_vec, b_vec, dot_sum);
            norm_a_sum = _mm256_fmadd_ps(a_vec, a_vec, norm_a_sum);
            norm_b_sum = _mm256_fmadd_ps(b_vec, b_vec, norm_b_sum);
        }
        
        let dot_array: [f32; 8] = std::mem::transmute(dot_sum);
        let mut dot_product: f32 = dot_array.iter().sum();
        let norm_a_array: [f32; 8] = std::mem::transmute(norm_a_sum);
        let mut norm_a: f32 = norm_a_array.iter().sum();
        let norm_b_array: [f32; 8] = std::mem::transmute(norm_b_sum);
        let mut norm_b: f32 = norm_b_array.iter().sum();
        
        for i in (chunks * 8)..len {
            dot_product += a[i] * b[i];
            norm_a += a[i] * a[i];
            norm_b += b[i] * b[i];
        }
        
        let norm_a = norm_a.sqrt();
        let norm_b = norm_b.sqrt();
        if norm_a == 0.0 || norm_b == 0.0 { return 0.0; }
        dot_product / (norm_a * norm_b)
    }
    
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx,fma")]
    unsafe fn l2_distance_avx_impl(a: &[f32], b: &[f32]) -> f32 {
        use std::arch::x86_64::*;
        let len = a.len().min(b.len());
        if len == 0 { return 0.0; }
        
        let mut diff_sum = _mm256_setzero_ps();
        let chunks = len / 8;
        
        for i in 0..chunks {
            let offset = i * 8;
            let a_vec = _mm256_loadu_ps(a.as_ptr().add(offset));
            let b_vec = _mm256_loadu_ps(b.as_ptr().add(offset));
            let diff = _mm256_sub_ps(a_vec, b_vec);
            diff_sum = _mm256_fmadd_ps(diff, diff, diff_sum);
        }
        
        let diff_array: [f32; 8] = std::mem::transmute(diff_sum);
        let mut distance_sq: f32 = diff_array.iter().sum();
        
        for i in (chunks * 8)..len {
            let diff = a[i] - b[i];
            distance_sq += diff * diff;
        }
        
        distance_sq.sqrt()
    }
    
    fn cosine_similarity_scalar_impl(a: &[f32], b: &[f32]) -> f32 {
        let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm_a == 0.0 || norm_b == 0.0 { return 0.0; }
        dot_product / (norm_a * norm_b)
    }
    
    fn l2_distance_scalar_impl(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum::<f32>().sqrt()
    }
}

use simd_ops::{cosine_similarity_simd, l2_distance_simd};

/// Vector metric/distance function
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum VectorMetric {
    /// Cosine similarity (1 - cosine_distance)
    Cosine,
    /// L2 (Euclidean) distance
    L2,
    /// Inner product (dot product)
    InnerProduct,
}

/// Vector index trait for similarity search
pub trait VectorIndexTrait: Send + Sync {
    /// Search for the k nearest neighbors
    /// Returns vector of (row_index, similarity_score) pairs, sorted by similarity (descending)
    fn search(&self, query_vector: &[f32], k: usize, metric: VectorMetric) -> Vec<(usize, f32)>;
    
    /// Get the dimension of vectors in this index
    fn dimension(&self) -> usize;
    
    /// Get the number of vectors in this index
    fn size(&self) -> usize;
}

#[path = "hnsw_graph.rs"]
mod hnsw_graph;
use hnsw_graph::HNSWGraph;

/// HNSW (Hierarchical Navigable Small World) index
/// Fast approximate nearest neighbor search with high recall
/// Phase 3: Proper HNSW implementation with graph-based search
#[derive(Clone, Debug)]
pub struct HNSWIndex {
    /// Vector dimension
    dimension: usize,
    /// Vectors stored in the index (row_index -> vector)
    vectors: Vec<Vec<f32>>,
    /// HNSW graph structure for fast approximate search
    graph: Option<Arc<HNSWGraph>>,
    /// HNSW parameters
    m: usize,  // Maximum connections per node
    m_max0: usize,  // Maximum connections for layer 0
    ef_construction: usize,  // Candidate list size during construction
    ef_search: usize,  // Candidate list size during search
}

impl HNSWIndex {
    /// Create a new HNSW index from vectors
    /// Phase 3: Builds proper HNSW graph structure
    pub fn new(vectors: Vec<Vec<f32>>) -> Result<Self> {
        Self::new_with_params(vectors, 16, 32, 200, 50)
    }
    
    /// Create a new HNSW index with custom parameters
    pub fn new_with_params(
        vectors: Vec<Vec<f32>>,
        m: usize,
        m_max0: usize,
        ef_construction: usize,
        ef_search: usize,
    ) -> Result<Self> {
        if vectors.is_empty() {
            return Ok(Self {
                dimension: 0,
                vectors: vec![],
                graph: None,
                m,
                m_max0,
                ef_construction,
                ef_search,
            });
        }
        
        let dimension = vectors[0].len();
        // Validate all vectors have the same dimension
        for (idx, vec) in vectors.iter().enumerate() {
            if vec.len() != dimension {
                anyhow::bail!("Vector {} has dimension {} but expected {}", idx, vec.len(), dimension);
            }
        }
        
        // Build HNSW graph
        let graph = Self::build_hnsw_graph(&vectors, m, m_max0, ef_construction)?;
        
        Ok(Self {
            dimension,
            vectors,
            graph: Some(Arc::new(graph)),
            m,
            m_max0,
            ef_construction,
            ef_search,
        })
    }
    
    /// Build HNSW graph from vectors
    fn build_hnsw_graph(
        vectors: &[Vec<f32>],
        m: usize,
        m_max0: usize,
        _ef_construction: usize,  // Used in advanced implementation
    ) -> Result<HNSWGraph> {
        if vectors.is_empty() {
            return Ok(HNSWGraph::new(m, m_max0));
        }
        
        let dimension = vectors[0].len();
        let mut graph = HNSWGraph::new(m, m_max0);
        
        // Insert all vectors into the graph
        for (idx, vector) in vectors.iter().enumerate() {
            graph.insert(idx, vector, dimension, vectors)?;
        }
        
        Ok(graph)
    }
    
    /// Build index from a ColumnFragment containing vectors
    pub fn from_fragment(fragment: &ColumnFragment) -> Result<Self> {
        let array = fragment.get_array()
            .ok_or_else(|| anyhow::anyhow!("Fragment array not available"))?;
        
        // Extract vectors from Arrow array
        // For now, we'll need to convert from Arrow array to Vec<Vec<f32>>
        // This depends on how vectors are stored in Arrow
        // Option 1: Fixed-size list of floats
        // Option 2: Custom extension type
        // For Phase 1, we'll use a simple approach
        
        let mut vectors = Vec::new();
        
        // Check if this is a FixedSizeListArray with Float32/Float64 elements
            if let Some(list_array) = array.as_any().downcast_ref::<arrow::array::FixedSizeListArray>() {
                let value_array = list_array.values();
                let dimension = list_array.value_length() as usize;
                let row_count = array.len();
                
                // Check element type
                if let Some(float_array) = value_array.as_any().downcast_ref::<arrow::array::Float32Array>() {
                    for i in 0..row_count {
                        let start = i * dimension;
                        let end = start + dimension;
                        let mut vec = Vec::with_capacity(dimension);
                        for j in start..end {
                            vec.push(float_array.value(j));
                        }
                        vectors.push(vec);
                    }
                } else if let Some(double_array) = value_array.as_any().downcast_ref::<arrow::array::Float64Array>() {
                    for i in 0..row_count {
                        let start = i * dimension;
                        let end = start + dimension;
                        let mut vec = Vec::with_capacity(dimension);
                        for j in start..end {
                            vec.push(double_array.value(j) as f32);
                        }
                        vectors.push(vec);
                    }
                } else {
                    anyhow::bail!("Vector array must contain Float32 or Float64 elements");
                }
            } else {
                anyhow::bail!("Vector fragment must be a FixedSizeListArray");
            }
        
        Self::new(vectors)
    }
}

impl VectorIndexTrait for HNSWIndex {
    fn search(&self, query_vector: &[f32], k: usize, metric: VectorMetric) -> Vec<(usize, f32)> {
        if query_vector.len() != self.dimension {
            return vec![]; // Dimension mismatch
        }
        
        if self.vectors.is_empty() {
            return vec![];
        }
        
        // Phase 3: Use proper HNSW graph for approximate nearest neighbor search
        // O(log n) complexity instead of O(n)
        
        if let Some(ref graph) = self.graph {
            // Use HNSW graph for fast search
            let ef = self.ef_search.max(k);
            let results = graph.search(query_vector, &self.vectors, k, ef, metric);
            return results;
        }
        
        // Fallback to optimized linear scan if graph not built
        // Phase 2: Optimized search with early termination and SIMD
        self.fallback_search_linear(query_vector, k, metric)
    }
    
    fn dimension(&self) -> usize {
        self.dimension
    }
    
    fn size(&self) -> usize {
        self.vectors.len()
    }
}

impl HNSWIndex {
    /// Fallback linear search (Phase 2 optimized)
    fn fallback_search_linear(&self, query_vector: &[f32], k: usize, metric: VectorMetric) -> Vec<(usize, f32)> {
        use ordered_float::OrderedFloat;
        
        #[derive(Eq, PartialEq)]
        struct SimilarityItem {
            idx: usize,
            similarity: OrderedFloat<f32>,
        }
        
        impl Ord for SimilarityItem {
            fn cmp(&self, other: &Self) -> Ordering {
                other.similarity.cmp(&self.similarity)
            }
        }
        
        impl PartialOrd for SimilarityItem {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }
        
        let mut heap: BinaryHeap<SimilarityItem> = BinaryHeap::with_capacity(k + 1);
        
        for (idx, vec) in self.vectors.iter().enumerate() {
            let similarity = match metric {
                VectorMetric::Cosine => cosine_similarity_simd(query_vector, vec),
                VectorMetric::L2 => {
                    let distance = l2_distance_simd(query_vector, vec);
                    1.0 / (1.0 + distance)
                }
                VectorMetric::InnerProduct => inner_product_simd(query_vector, vec),
            };
            
            heap.push(SimilarityItem {
                idx,
                similarity: OrderedFloat(similarity),
            });
            
            if heap.len() > k {
                heap.pop();
            }
        }
        
        let mut results: Vec<(usize, f32)> = heap.into_iter()
            .map(|item| (item.idx, item.similarity.into_inner()))
            .collect();
        
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        results
    }
}

/// Public function for cosine similarity (used by expression evaluator)
/// Uses SIMD-optimized version when available
pub fn cosine_similarity_impl(a: &[f32], b: &[f32]) -> f32 {
    cosine_similarity_simd(a, b)
}

/// Public function for L2 distance (used by expression evaluator)
/// Uses SIMD-optimized version when available
pub fn l2_distance_impl(a: &[f32], b: &[f32]) -> f32 {
    l2_distance_simd(a, b)
}

/// SIMD-optimized inner product
fn inner_product_simd(a: &[f32], b: &[f32]) -> f32 {
    // For inner product, we can use SIMD dot product
    // Reuse cosine similarity dot product calculation without normalization
    let len = a.len().min(b.len());
    if len == 0 {
        return 0.0;
    }
    
    #[cfg(target_arch = "x86_64")]
    {
        #[cfg(target_feature = "avx")]
        if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
            unsafe {
                return inner_product_avx(a, b);
            }
        }
    }
    
    // Fallback to optimized scalar
    inner_product_scalar(a, b)
}

/// AVX-optimized inner product
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx,fma")]
unsafe fn inner_product_avx(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;
    
    let len = a.len().min(b.len());
    if len == 0 {
        return 0.0;
    }
    
    let mut sum = _mm256_setzero_ps();
    let chunks = len / 8;
    let remainder = len % 8;
    
    // Process 8 floats at a time
    for i in 0..chunks {
        let offset = i * 8;
        let a_vec = _mm256_loadu_ps(a.as_ptr().add(offset));
        let b_vec = _mm256_loadu_ps(b.as_ptr().add(offset));
        sum = _mm256_fmadd_ps(a_vec, b_vec, sum);
    }
    
    // Horizontal sum
    let sum_array = std::mem::transmute::<__m256, [f32; 8]>(sum);
    let mut result = sum_array[0] + sum_array[1] + sum_array[2] + sum_array[3] +
                    sum_array[4] + sum_array[5] + sum_array[6] + sum_array[7];
    
    // Handle remainder
    for i in (chunks * 8)..len {
        result += a[i] * b[i];
    }
    
    result
}

/// Optimized scalar inner product
#[inline(always)]
fn inner_product_scalar(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len().min(b.len());
    if len == 0 {
        return 0.0;
    }
    
    const CHUNK_SIZE: usize = 32;
    let chunks = len / CHUNK_SIZE;
    let remainder = len % CHUNK_SIZE;
    
    let mut result = 0.0f32;
    
    for chunk_idx in 0..chunks {
        let start = chunk_idx * CHUNK_SIZE;
        let end = start + CHUNK_SIZE;
        for i in start..end {
            result += a[i] * b[i];
        }
    }
    
    for i in (chunks * CHUNK_SIZE)..len {
        result += a[i] * b[i];
    }
    
    result
}

/// Enum wrapper for different vector index types
#[derive(Clone, Debug)]
pub enum VectorIndexEnum {
    HNSW(Arc<HNSWIndex>),
}

impl VectorIndexTrait for VectorIndexEnum {
    fn search(&self, query_vector: &[f32], k: usize, metric: VectorMetric) -> Vec<(usize, f32)> {
        match self {
            VectorIndexEnum::HNSW(idx) => idx.search(query_vector, k, metric),
        }
    }
    
    fn dimension(&self) -> usize {
        match self {
            VectorIndexEnum::HNSW(idx) => idx.dimension(),
        }
    }
    
    fn size(&self) -> usize {
        match self {
            VectorIndexEnum::HNSW(idx) => idx.size(),
        }
    }
}

/// Type alias for the actual vector index type used
pub type VectorIndex = VectorIndexEnum;


/// Build a vector index from a ColumnFragment
pub fn build_vector_index(fragment: &ColumnFragment) -> Result<VectorIndexEnum> {
    let hnsw = HNSWIndex::from_fragment(fragment)?;
    Ok(VectorIndexEnum::HNSW(Arc::new(hnsw)))
}

