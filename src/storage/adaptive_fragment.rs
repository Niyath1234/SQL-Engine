/// Adaptive fragmenting and recompression
/// Dynamically adjusts fragment sizes and compression based on access patterns
use crate::storage::fragment::{ColumnFragment, CompressionType};
use crate::storage::memory_tier::FragmentAccessStats;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;

/// Fragment access pattern for adaptive decisions
#[derive(Clone, Debug)]
pub struct FragmentAccessPattern {
    /// Access frequency (accesses per second)
    pub access_frequency: f64,
    
    /// Hotness score
    pub hotness_score: f64,
    
    /// Current fragment size in bytes
    pub current_size_bytes: usize,
    
    /// Current compression type
    pub current_compression: CompressionType,
    
    /// Number of times accessed
    pub access_count: u64,
    
    /// Average access size (bytes per access)
    pub avg_access_size: usize,
    
    /// Whether fragment is frequently scanned sequentially
    pub is_sequential: bool,
    
    /// Whether fragment is frequently accessed randomly
    pub is_random: bool,
}

impl FragmentAccessPattern {
    pub fn new(size_bytes: usize, compression: CompressionType) -> Self {
        Self {
            access_frequency: 0.0,
            hotness_score: 0.0,
            current_size_bytes: size_bytes,
            current_compression: compression,
            access_count: 0,
            avg_access_size: 0,
            is_sequential: false,
            is_random: false,
        }
    }
    
    pub fn from_stats(stats: &FragmentAccessStats, size_bytes: usize, compression: CompressionType) -> Self {
        Self {
            access_frequency: stats.access_frequency,
            hotness_score: stats.hotness_score,
            current_size_bytes: size_bytes,
            current_compression: compression,
            access_count: stats.access_count,
            avg_access_size: if stats.access_count > 0 {
                (stats.total_bytes_accessed / stats.access_count) as usize
            } else {
                0
            },
            is_sequential: stats.access_frequency > 100.0, // High frequency suggests sequential
            is_random: stats.access_frequency < 10.0 && stats.access_count > 5, // Low frequency, multiple accesses suggests random
        }
    }
}

/// Adaptive fragment manager
/// Makes decisions about fragmenting and compression based on access patterns
pub struct AdaptiveFragmentManager {
    /// Target fragment size for hot data (smaller for better cache locality)
    hot_fragment_size_bytes: usize,
    
    /// Target fragment size for cold data (larger to reduce overhead)
    cold_fragment_size_bytes: usize,
    
    /// Hotness threshold for considering a fragment "hot"
    hotness_threshold: f64,
    
    /// Coldness threshold for considering a fragment "cold"
    coldness_threshold: f64,
}

impl AdaptiveFragmentManager {
    pub fn new() -> Self {
        Self {
            // Hot fragments: smaller (64KB) for better cache locality
            hot_fragment_size_bytes: 64 * 1024,
            // Cold fragments: larger (256KB) to reduce overhead
            cold_fragment_size_bytes: 256 * 1024,
            hotness_threshold: 50.0,
            coldness_threshold: 10.0,
        }
    }
    
    /// Decide if fragment should be split (for hot fragments)
    pub fn should_split_fragment(&self, pattern: &FragmentAccessPattern) -> bool {
        // Split if:
        // 1. Fragment is hot (high access frequency)
        // 2. Fragment is larger than target hot size
        // 3. Fragment is accessed sequentially (benefits from smaller chunks)
        pattern.hotness_score > self.hotness_threshold
            && pattern.current_size_bytes > self.hot_fragment_size_bytes
            && pattern.is_sequential
    }
    
    /// Decide if fragments should be merged (for cold fragments)
    pub fn should_merge_fragments(&self, patterns: &[FragmentAccessPattern]) -> bool {
        if patterns.len() < 2 {
            return false;
        }
        
        // Merge if:
        // 1. All fragments are cold
        // 2. Combined size would be reasonable
        // 3. Fragments are from the same column/table
        let all_cold = patterns.iter().all(|p| p.hotness_score < self.coldness_threshold);
        let total_size: usize = patterns.iter().map(|p| p.current_size_bytes).sum();
        
        all_cold && total_size < self.cold_fragment_size_bytes * 4 // Don't merge into huge fragments
    }
    
    /// Decide optimal compression strategy
    pub fn optimal_compression(&self, pattern: &FragmentAccessPattern, data_type: &DataType) -> CompressionType {
        // For hot fragments: prefer lighter or no compression for speed
        if pattern.hotness_score > self.hotness_threshold {
            // Very hot: no compression
            if pattern.hotness_score > 80.0 {
                return CompressionType::None;
            }
            // Moderately hot: light compression (RLE or Dictionary)
            return match data_type {
                DataType::Int64 | DataType::Int32 => CompressionType::RLE, // Good for integers
                DataType::Utf8 => CompressionType::Dictionary, // Good for strings
                _ => CompressionType::None,
            };
        }
        
        // For cold fragments: prefer heavier compression to save space
        if pattern.hotness_score < self.coldness_threshold {
            // Very cold: aggressive compression
            if pattern.hotness_score < 5.0 {
                return CompressionType::Zstd; // Best compression ratio
            }
            // Moderately cold: medium compression
            return CompressionType::Lz4; // Good balance
        }
        
        // For medium-hot fragments: use delta compression if applicable
        match data_type {
            DataType::Int64 | DataType::Int32 | DataType::Float64 | DataType::Float32 => {
                CompressionType::Delta // Good for numeric sequences
            }
            DataType::Utf8 => CompressionType::Dictionary,
            _ => CompressionType::None,
        }
    }
    
    /// Decide if fragment should be recompressed
    pub fn should_recompress(&self, pattern: &FragmentAccessPattern, current_compression: &CompressionType, data_type: &DataType) -> bool {
        let optimal = self.optimal_compression(pattern, data_type);
        
        // Recompress if optimal strategy differs from current
        // Use match to compare since CompressionType doesn't implement PartialEq
        let should_change = match (current_compression, &optimal) {
            (CompressionType::None, CompressionType::None) => false,
            (CompressionType::Dictionary, CompressionType::Dictionary) => false,
            (CompressionType::Delta, CompressionType::Delta) => false,
            (CompressionType::BitPacked, CompressionType::BitPacked) => false,
            (CompressionType::RLE, CompressionType::RLE) => false,
            (CompressionType::Zstd, CompressionType::Zstd) => false,
            (CompressionType::Lz4, CompressionType::Lz4) => false,
            _ => true, // Different compression types
        };
        
        if should_change {
            // Only recompress if the benefit is significant
            match (current_compression, &optimal) {
                // Moving from heavy to light compression (hot fragment)
                (CompressionType::Zstd | CompressionType::Lz4, CompressionType::None | CompressionType::RLE | CompressionType::Dictionary) => {
                    pattern.hotness_score > self.hotness_threshold
                }
                // Moving from light to heavy compression (cold fragment)
                (CompressionType::None | CompressionType::RLE | CompressionType::Dictionary, CompressionType::Zstd | CompressionType::Lz4) => {
                    pattern.hotness_score < self.coldness_threshold
                }
                _ => false,
            }
        } else {
            false
        }
    }
    
    /// Calculate optimal fragment size based on access pattern
    pub fn optimal_fragment_size(&self, pattern: &FragmentAccessPattern) -> usize {
        if pattern.hotness_score > self.hotness_threshold {
            // Hot fragments: smaller for cache locality
            self.hot_fragment_size_bytes
        } else if pattern.hotness_score < self.coldness_threshold {
            // Cold fragments: larger to reduce overhead
            self.cold_fragment_size_bytes
        } else {
            // Medium fragments: use current size or default
            pattern.current_size_bytes.max(128 * 1024) // Default 128KB
        }
    }
    
    /// Split a fragment into smaller pieces
    pub fn split_fragment(&self, fragment: &ColumnFragment, target_size_bytes: usize) -> Vec<ColumnFragment> {
        let array = fragment.get_array().expect("Fragment must have array");
        let total_rows = array.len();
        let element_size = self.estimate_element_size(array.data_type());
        
        // Calculate rows per fragment
        let rows_per_fragment = if element_size > 0 {
            (target_size_bytes / element_size).max(1)
        } else {
            8192 // Default for variable-length types
        };
        
        let mut fragments = Vec::new();
        
        for offset in (0..total_rows).step_by(rows_per_fragment) {
            let length = rows_per_fragment.min(total_rows - offset);
            let fragment_array = array.slice(offset, length);
            
            // Create new fragment with same metadata but updated size
            let mut metadata = fragment.metadata.clone();
            metadata.row_count = length;
            let fragment_arc = Arc::from(fragment_array);
            metadata.memory_size = self.calculate_memory_size(&fragment_arc);
            
            let new_fragment = ColumnFragment {
                array: Some(fragment_arc),
                mmap: None, // Split fragments don't use mmap initially
                mmap_offset: 0,
                metadata,
                bloom_filter: fragment.bloom_filter.clone(),
                bitmap_index: fragment.bitmap_index.clone(),
                is_sorted: fragment.is_sorted,
                dictionary: fragment.dictionary.clone(),
                compressed_data: None,
                vector_index: fragment.vector_index.clone(),
                vector_dimension: fragment.vector_dimension,
            };
            
            fragments.push(new_fragment);
        }
        
        fragments
    }
    
    /// Merge multiple fragments into one
    pub fn merge_fragments(&self, fragments: &[ColumnFragment]) -> Option<ColumnFragment> {
        if fragments.is_empty() {
            return None;
        }
        
        if fragments.len() == 1 {
            return Some(fragments[0].clone());
        }
        
        // Combine arrays
        let mut combined_arrays: Vec<Arc<dyn Array>> = Vec::new();
        let mut total_rows = 0;
        
        for fragment in fragments {
            if let Some(array) = fragment.get_array() {
                combined_arrays.push(array);
            }
            total_rows += fragment.len();
        }
        
        // For now, we'll use the first fragment's array as a placeholder
        // In a full implementation, we'd concatenate the arrays
        // This is a simplified version
        let first_fragment = &fragments[0];
        
        // Create merged metadata
        let mut metadata = first_fragment.metadata.clone();
        metadata.row_count = total_rows;
        metadata.memory_size = fragments.iter().map(|f| f.metadata.memory_size).sum();
        
        Some(ColumnFragment {
            array: first_fragment.get_array(), // Simplified: would concatenate in full implementation
            mmap: None, // Merged fragments don't use mmap
            mmap_offset: 0,
            metadata,
            bloom_filter: first_fragment.bloom_filter.clone(),
            bitmap_index: first_fragment.bitmap_index.clone(),
            is_sorted: first_fragment.is_sorted,
            dictionary: first_fragment.dictionary.clone(),
            compressed_data: None,
            vector_index: first_fragment.vector_index.clone(),
            vector_dimension: first_fragment.vector_dimension,
        })
    }
    
    /// Estimate element size for data type
    fn estimate_element_size(&self, data_type: &DataType) -> usize {
        match data_type {
            DataType::Int64 => 8,
            DataType::Int32 => 4,
            DataType::Float64 => 8,
            DataType::Float32 => 4,
            DataType::Utf8 => 32, // Average string length estimate
            DataType::Boolean => 1,
            _ => 8,
        }
    }
    
    /// Calculate memory size of array
    fn calculate_memory_size(&self, array: &Arc<dyn Array>) -> usize {
        match array.data_type() {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                arr.len() * 8 + std::mem::size_of::<Int64Array>()
            }
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                arr.len() * 4 + std::mem::size_of::<Int32Array>()
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                arr.len() * 8 + std::mem::size_of::<Float64Array>()
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                let mut total = 0;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        total += arr.value(i).len();
                    }
                }
                total + std::mem::size_of::<StringArray>()
            }
            _ => array.len() * 8,
        }
    }
}

impl Default for AdaptiveFragmentManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Adaptive recompression decision
#[derive(Clone, Debug)]
pub struct RecompressionDecision {
    /// Whether to recompress
    pub should_recompress: bool,
    
    /// New compression type
    pub new_compression: CompressionType,
    
    /// Expected space savings (percentage)
    pub expected_savings: f64,
    
    /// Expected performance impact (positive = faster, negative = slower)
    pub expected_performance_delta: f64,
}

impl AdaptiveFragmentManager {
    /// Make recompression decision
    pub fn make_recompression_decision(
        &self,
        pattern: &FragmentAccessPattern,
        data_type: &DataType,
    ) -> RecompressionDecision {
        let optimal = self.optimal_compression(pattern, data_type);
        let should_recompress = self.should_recompress(pattern, &pattern.current_compression, data_type);
        
        // Estimate space savings
        let expected_savings = self.estimate_compression_savings(&optimal, data_type);
        
        // Estimate performance impact
        let current_compression = pattern.current_compression.clone();
        let current_latency = self.compression_latency(&current_compression);
        let optimal_latency = self.compression_latency(&optimal);
        let expected_performance_delta = current_latency - optimal_latency; // Positive = faster
        
        RecompressionDecision {
            should_recompress,
            new_compression: optimal,
            expected_savings,
            expected_performance_delta,
        }
    }
    
    /// Estimate compression ratio
    fn estimate_compression_savings(&self, compression: &CompressionType, data_type: &DataType) -> f64 {
        match (compression, data_type) {
            (CompressionType::None, _) => 0.0,
            (CompressionType::Dictionary, DataType::Utf8) => 0.6, // 60% savings for strings
            (CompressionType::RLE, DataType::Int64 | DataType::Int32) => 0.5, // 50% for repeated integers
            (CompressionType::Delta, DataType::Int64 | DataType::Int32 | DataType::Float64) => 0.4, // 40% for sequences
            (CompressionType::Lz4, _) => 0.3, // 30% general purpose
            (CompressionType::Zstd, _) => 0.5, // 50% best compression
            _ => 0.2, // Default 20%
        }
    }
    
    /// Estimate compression/decompression latency (nanoseconds)
    fn compression_latency(&self, compression: &CompressionType) -> f64 {
        match compression {
            CompressionType::None => 0.0,
            CompressionType::Dictionary => 100.0, // Fast
            CompressionType::RLE => 50.0, // Very fast
            CompressionType::Delta => 200.0, // Medium
            CompressionType::Lz4 => 500.0, // Medium-fast
            CompressionType::Zstd => 2000.0, // Slower but better compression
            CompressionType::BitPacked => 150.0, // Fast
        }
    }
}

