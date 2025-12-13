/// Bitset Engine V3: Unified Bitset System
/// 
/// Foundational bitset primitives with SIMD acceleration
/// Supports multiple internal formats (Sparse, Dense, Compressed)
/// Adaptive representation based on density

use bitvec::prelude::*;
use bitvec::order::Lsb0;
use std::sync::Arc;

/// Bitset format variants
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BitsetFormat {
    /// Sparse: Only stores set bits (good for <1% density)
    Sparse,
    
    /// Dense: Full bitmap (good for >10% density)
    Dense,
    
    /// Compressed: RLE or other compression (good for medium density)
    Compressed,
}

/// Block-level metadata for hierarchical skipping
#[derive(Clone, Debug)]
pub struct BlockSkipMetadata {
    /// Whether this block has any set bits
    pub has_any_bits: bool,
    
    /// Population count (number of set bits)
    pub popcount: usize,
    
    /// First set bit position (for fast skipping)
    pub first_set_bit: Option<usize>,
    
    /// Last set bit position
    pub last_set_bit: Option<usize>,
}

/// Bitset block for hierarchical representation
#[derive(Clone, Debug)]
pub struct BitsetBlock {
    /// Level in hierarchy (0 = superblock, 1 = block, 2 = row)
    pub level: u8,
    
    /// Bitmap data (Vec<u64> for dense, or sparse representation)
    pub bitmap: Vec<u64>,
    
    /// Skip metadata for fast pruning
    pub skip_info: BlockSkipMetadata,
}

/// Unified Bitset type with adaptive representation
#[derive(Clone, Debug)]
pub struct Bitset {
    /// Current format
    pub format: BitsetFormat,
    
    /// Cardinality (number of set bits)
    pub cardinality: usize,
    
    /// Blocks (for hierarchical representation)
    pub blocks: Vec<BitsetBlock>,
    
    /// Total capacity (number of bits)
    pub capacity: usize,
    
    /// Sparse representation (only for Sparse format)
    sparse_bits: Option<Vec<usize>>,
    
    /// Dense representation (only for Dense format)
    dense_bits: Option<BitVec>,
}

impl Bitset {
    /// Create new empty bitset
    pub fn new(capacity: usize) -> Self {
        Self {
            format: BitsetFormat::Dense,
            cardinality: 0,
            blocks: Vec::new(),
            capacity,
            sparse_bits: None,
            dense_bits: Some(bitvec![0; capacity]),
        }
    }
    
    /// Create sparse bitset
    pub fn new_sparse(capacity: usize) -> Self {
        Self {
            format: BitsetFormat::Sparse,
            cardinality: 0,
            blocks: Vec::new(),
            capacity,
            sparse_bits: Some(Vec::new()),
            dense_bits: None,
        }
    }
    
    /// Set a bit
    pub fn set(&mut self, pos: usize) {
        if pos >= self.capacity {
            return;
        }
        
        match &mut self.format {
            BitsetFormat::Sparse => {
                if let Some(ref mut sparse) = self.sparse_bits {
                    if !sparse.contains(&pos) {
                        sparse.push(pos);
                        sparse.sort();
                        self.cardinality += 1;
                    }
                }
            }
            BitsetFormat::Dense => {
                if let Some(ref mut dense) = self.dense_bits {
                    if !dense[pos] {
                        dense.set(pos, true);
                        self.cardinality += 1;
                    }
                }
            }
            BitsetFormat::Compressed => {
                // For compressed, convert to dense temporarily
                self.to_dense();
                self.set(pos);
            }
        }
    }
    
    /// Get a bit
    pub fn get(&self, pos: usize) -> bool {
        if pos >= self.capacity {
            return false;
        }
        
        match &self.format {
            BitsetFormat::Sparse => {
                self.sparse_bits.as_ref()
                    .map(|s| s.binary_search(&pos).is_ok())
                    .unwrap_or(false)
            }
            BitsetFormat::Dense => {
                self.dense_bits.as_ref()
                    .map(|d| d[pos])
                    .unwrap_or(false)
            }
            BitsetFormat::Compressed => {
                // Decompress on-demand
                false // TODO: Implement compressed get
            }
        }
    }
    
    /// Convert to sparse format
    pub fn to_sparse(&mut self) {
        if matches!(self.format, BitsetFormat::Sparse) {
            return;
        }
        
        if let Some(dense) = &self.dense_bits {
            let mut sparse = Vec::new();
            for (i, bit) in dense.iter().enumerate() {
                if *bit {
                    sparse.push(i);
                }
            }
            self.sparse_bits = Some(sparse);
            self.dense_bits = None;
            self.format = BitsetFormat::Sparse;
        }
    }
    
    /// Convert to dense format
    pub fn to_dense(&mut self) {
        if matches!(self.format, BitsetFormat::Dense) {
            return;
        }
        
        let mut dense = bitvec![0; self.capacity];
        
        match &self.format {
            BitsetFormat::Sparse => {
                if let Some(sparse) = &self.sparse_bits {
                    for &pos in sparse {
                        if pos < self.capacity {
                            dense.set(pos, true);
                        }
                    }
                }
            }
            BitsetFormat::Compressed => {
                // TODO: Decompress from blocks
            }
            _ => {}
        }
        
        self.dense_bits = Some(dense);
        self.sparse_bits = None;
        self.format = BitsetFormat::Dense;
    }
    
    /// Convert to compressed format
    pub fn to_compressed(&mut self) {
        if matches!(self.format, BitsetFormat::Compressed) {
            return;
        }
        
        // TODO: Implement compression
        // For now, just convert to dense
        self.to_dense();
    }
    
    /// Auto-optimize representation based on density
    pub fn auto_optimize_representation(&mut self) {
        let density = if self.capacity > 0 {
            self.cardinality as f64 / self.capacity as f64
        } else {
            0.0
        };
        
        if density < 0.01 {
            // Very sparse: use sparse format
            self.to_sparse();
        } else if density > 0.1 {
            // Dense: use dense format
            self.to_dense();
        } else {
            // Medium: use compressed
            self.to_compressed();
        }
    }
    
    /// Intersect with another bitset (SIMD-accelerated)
    pub fn intersect(&self, other: &Self) -> Self {
        if self.capacity != other.capacity {
            return Bitset::new(0);
        }
        
        // Convert both to dense for SIMD operations
        let mut self_dense = self.clone();
        self_dense.to_dense();
        let mut other_dense = other.clone();
        other_dense.to_dense();
        
        if let (Some(self_bits), Some(other_bits)) = (self_dense.dense_bits, other_dense.dense_bits) {
            let result = self_bits & other_bits;
            let cardinality = result.count_ones();
            
            let mut result_bitset = Bitset {
                format: BitsetFormat::Dense,
                cardinality,
                blocks: Vec::new(),
                capacity: self.capacity,
                sparse_bits: None,
                dense_bits: Some(result),
            };
            
            result_bitset.auto_optimize_representation();
            result_bitset
        } else {
            Bitset::new(self.capacity)
        }
    }
    
    /// Union with another bitset (SIMD-accelerated)
    pub fn union(&self, other: &Self) -> Self {
        if self.capacity != other.capacity {
            return Bitset::new(0);
        }
        
        // Convert both to dense for SIMD operations
        let mut self_dense = self.clone();
        self_dense.to_dense();
        let mut other_dense = other.clone();
        other_dense.to_dense();
        
        if let (Some(self_bits), Some(other_bits)) = (self_dense.dense_bits, other_dense.dense_bits) {
            let result = self_bits | other_bits;
            let cardinality = result.count_ones();
            
            let mut result_bitset = Bitset {
                format: BitsetFormat::Dense,
                cardinality,
                blocks: Vec::new(),
                capacity: self.capacity,
                sparse_bits: None,
                dense_bits: Some(result),
            };
            
            result_bitset.auto_optimize_representation();
            result_bitset
        } else {
            Bitset::new(self.capacity)
        }
    }
    
    /// Get cardinality
    pub fn cardinality(&self) -> usize {
        self.cardinality
    }
    
    /// Get capacity (total number of bits)
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    
    /// Iterate over set bits
    pub fn iter_set_bits(&self) -> SetBitsIterator {
        match &self.format {
            BitsetFormat::Sparse => {
                let sparse_vec: Vec<usize> = self.sparse_bits.as_ref()
                    .map(|s| s.clone())
                    .unwrap_or_default();
                SetBitsIterator {
                    sparse: Some(sparse_vec.into_iter()),
                    dense: None,
                }
            }
            BitsetFormat::Dense => {
                // For dense, we'll collect into a vector
                let bits = self.get_set_bits();
                SetBitsIterator {
                    sparse: Some(bits.into_iter()),
                    dense: None,
                }
            }
            BitsetFormat::Compressed => {
                // TODO: Implement compressed iteration
                SetBitsIterator {
                    sparse: None,
                    dense: None,
                }
            }
        }
    }
    
    /// Get all set bit positions as a vector
    pub fn get_set_bits(&self) -> Vec<usize> {
        match &self.format {
            BitsetFormat::Sparse => {
                self.sparse_bits.as_ref().cloned().unwrap_or_default()
            }
            BitsetFormat::Dense => {
                if let Some(ref dense) = self.dense_bits {
                    dense.iter_ones().collect()
                } else {
                    Vec::new()
                }
            }
            BitsetFormat::Compressed => {
                // TODO: Decompress and get bits
                Vec::new()
            }
        }
    }
}

/// Iterator over set bits
pub struct SetBitsIterator {
    sparse: Option<std::vec::IntoIter<usize>>,
    dense: Option<std::vec::IntoIter<usize>>, // Dense also uses vec iterator
}

impl Iterator for SetBitsIterator {
    type Item = usize;
    
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut sparse_iter) = self.sparse {
            sparse_iter.next()
        } else if let Some(ref mut dense_iter) = self.dense {
            dense_iter.next()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_bitset_basic() {
        let mut bitset = Bitset::new(100);
        bitset.set(5);
        bitset.set(10);
        bitset.set(20);
        
        assert!(bitset.get(5));
        assert!(bitset.get(10));
        assert!(bitset.get(20));
        assert!(!bitset.get(15));
        assert_eq!(bitset.cardinality(), 3);
    }
    
    #[test]
    fn test_bitset_intersect() {
        let mut bitset1 = Bitset::new(100);
        bitset1.set(5);
        bitset1.set(10);
        bitset1.set(20);
        
        let mut bitset2 = Bitset::new(100);
        bitset2.set(10);
        bitset2.set(20);
        bitset2.set(30);
        
        let result = bitset1.intersect(&bitset2);
        assert_eq!(result.cardinality(), 2);
        assert!(result.get(10));
        assert!(result.get(20));
    }
    
    #[test]
    fn test_bitset_union() {
        let mut bitset1 = Bitset::new(100);
        bitset1.set(5);
        bitset1.set(10);
        
        let mut bitset2 = Bitset::new(100);
        bitset2.set(10);
        bitset2.set(20);
        
        let result = bitset1.union(&bitset2);
        assert_eq!(result.cardinality(), 3);
        assert!(result.get(5));
        assert!(result.get(10));
        assert!(result.get(20));
    }
    
    #[test]
    fn test_bitset_format_conversion() {
        let mut bitset = Bitset::new(1000);
        bitset.set(5);
        bitset.set(100);
        bitset.set(500);
        
        bitset.to_dense();
        assert!(matches!(bitset.format, BitsetFormat::Dense));
        assert!(bitset.get(5));
        assert!(bitset.get(100));
        assert!(bitset.get(500));
    }
    
    #[test]
    fn test_bitset_get_set_bits() {
        let mut bitset = Bitset::new(100);
        bitset.set(5);
        bitset.set(10);
        bitset.set(20);
        
        let set_bits = bitset.get_set_bits();
        assert_eq!(set_bits.len(), 3);
        assert!(set_bits.contains(&5));
        assert!(set_bits.contains(&10));
        assert!(set_bits.contains(&20));
    }
    
    #[test]
    fn test_bitset_large_capacity() {
        let mut bitset = Bitset::new(1_000_000);
        bitset.set(0);
        bitset.set(500_000);
        bitset.set(999_999);
        
        assert!(bitset.get(0));
        assert!(bitset.get(500_000));
        assert!(bitset.get(999_999));
        assert_eq!(bitset.cardinality(), 3);
    }
}

