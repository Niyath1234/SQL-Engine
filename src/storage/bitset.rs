/// Minimal compressed bitset container.
/// Internally uses Vec<u64> blocks and skip-empty-block strategy.
/// Optimized for SIMD operations and fast intersection.
use serde::{Deserialize, Serialize};
use std::ops::{BitAnd, BitOr};

/// Compressed bitset container optimized for sparse sets
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bitset {
    /// Blocks of 64 bits each (u64)
    pub blocks: Vec<u64>,
    /// Cached population count (number of set bits)
    #[serde(skip)]
    pub popcount: Option<usize>,
}

impl Bitset {
    /// Create a new empty bitset
    pub fn new() -> Self {
        Self {
            blocks: Vec::new(),
            popcount: None,
        }
    }

    /// Create a bitset with pre-allocated capacity
    pub fn with_capacity(num_blocks: usize) -> Self {
        Self {
            blocks: vec![0; num_blocks],
            popcount: Some(0),
        }
    }

    /// Set a bit at the given position
    pub fn set(&mut self, pos: usize) {
        let block = pos / 64;
        let bit = pos % 64;
        if block >= self.blocks.len() {
            self.blocks.resize(block + 1, 0);
            self.popcount = None; // Invalidate cache
        }
        let old_value = self.blocks[block];
        self.blocks[block] |= 1u64 << bit;
        // Update popcount if cached
        if let Some(ref mut count) = self.popcount {
            if old_value != self.blocks[block] {
                *count += 1;
            }
        }
    }

    /// Clear a bit at the given position
    pub fn clear(&mut self, pos: usize) {
        let block = pos / 64;
        let bit = pos % 64;
        if block < self.blocks.len() {
            let old_value = self.blocks[block];
            self.blocks[block] &= !(1u64 << bit);
            // Update popcount if cached
            if let Some(ref mut count) = self.popcount {
                if old_value != self.blocks[block] {
                    *count = count.saturating_sub(1);
                }
            }
        }
    }

    /// Check if a bit is set
    pub fn get(&self, pos: usize) -> bool {
        let block = pos / 64;
        let bit = pos % 64;
        if block >= self.blocks.len() {
            return false;
        }
        (self.blocks[block] >> bit) & 1 == 1
    }

    /// Intersect two bitsets (returns new bitset)
    pub fn intersect(&self, other: &Self) -> Self {
        let len = self.blocks.len().min(other.blocks.len());
        let mut out = Vec::with_capacity(len);
        let mut popcount = 0;
        
        for i in 0..len {
            let result = self.blocks[i] & other.blocks[i];
            out.push(result);
            popcount += result.count_ones() as usize;
        }
        
        Self {
            blocks: out,
            popcount: Some(popcount),
        }
    }

    /// Union two bitsets (returns new bitset)
    pub fn union(&self, other: &Self) -> Self {
        let len = self.blocks.len().max(other.blocks.len());
        let mut out = Vec::with_capacity(len);
        
        for i in 0..len {
            let left = self.blocks.get(i).copied().unwrap_or(0);
            let right = other.blocks.get(i).copied().unwrap_or(0);
            out.push(left | right);
        }
        
        Self {
            blocks: out,
            popcount: None, // Recalculate if needed
        }
    }

    /// Check if bitset has any set bits
    pub fn any(&self) -> bool {
        self.blocks.iter().any(|b| *b != 0)
    }

    /// Check if bitset is empty
    pub fn is_empty(&self) -> bool {
        !self.any()
    }

    /// Get population count (number of set bits)
    pub fn popcount(&self) -> usize {
        if let Some(count) = self.popcount {
            count
        } else {
            let count = self.blocks.iter().map(|b| b.count_ones() as usize).sum();
            // Cache would require &mut, so we just return
            count
        }
    }

    /// Iterate over all set bit positions
    pub fn iter_set_bits(&self) -> impl Iterator<Item = usize> + '_ {
        self.blocks.iter().enumerate().flat_map(|(block_idx, &bits)| {
            (0..64).filter_map(move |bit_idx| {
                if (bits >> bit_idx) & 1 == 1 {
                    Some(block_idx * 64 + bit_idx)
                } else {
                    None
                }
            })
        })
    }

    /// Get cardinality (number of set bits) - alias for popcount
    pub fn cardinality(&self) -> usize {
        self.popcount()
    }

    /// Check if this bitset is a subset of another
    pub fn is_subset(&self, other: &Self) -> bool {
        for i in 0..self.blocks.len() {
            let self_block = self.blocks[i];
            let other_block = other.blocks.get(i).copied().unwrap_or(0);
            if (self_block & !other_block) != 0 {
                return false;
            }
        }
        true
    }

    /// In-place intersection (modifies self)
    pub fn intersect_inplace(&mut self, other: &Self) {
        let len = self.blocks.len().min(other.blocks.len());
        self.blocks.truncate(len);
        for i in 0..len {
            self.blocks[i] &= other.blocks[i];
        }
        self.popcount = None; // Invalidate cache
    }

    /// In-place union (modifies self)
    pub fn union_inplace(&mut self, other: &Self) {
        let other_len = other.blocks.len();
        if other_len > self.blocks.len() {
            self.blocks.resize(other_len, 0);
        }
        for i in 0..other_len {
            self.blocks[i] |= other.blocks[i];
        }
        self.popcount = None; // Invalidate cache
    }
}

impl BitAnd<&Bitset> for &Bitset {
    type Output = Bitset;

    fn bitand(self, rhs: &Bitset) -> Self::Output {
        self.intersect(rhs)
    }
}

impl BitOr<&Bitset> for &Bitset {
    type Output = Bitset;

    fn bitor(self, rhs: &Bitset) -> Self::Output {
        self.union(rhs)
    }
}

impl Default for Bitset {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitset_basic() {
        let mut bs = Bitset::new();
        assert!(!bs.get(0));
        assert!(!bs.get(100));
        
        bs.set(0);
        assert!(bs.get(0));
        assert!(!bs.get(1));
        
        bs.set(100);
        assert!(bs.get(100));
    }

    #[test]
    fn test_bitset_intersect() {
        let mut bs1 = Bitset::new();
        let mut bs2 = Bitset::new();
        
        bs1.set(0);
        bs1.set(1);
        bs1.set(2);
        
        bs2.set(1);
        bs2.set(2);
        bs2.set(3);
        
        let inter = bs1.intersect(&bs2);
        assert!(inter.get(1));
        assert!(inter.get(2));
        assert!(!inter.get(0));
        assert!(!inter.get(3));
        assert_eq!(inter.popcount(), 2);
    }

    #[test]
    fn test_bitset_iter() {
        let mut bs = Bitset::new();
        bs.set(0);
        bs.set(5);
        bs.set(100);
        
        let bits: Vec<usize> = bs.iter_set_bits().collect();
        assert_eq!(bits, vec![0, 5, 100]);
    }
}

