/// Bitset Engine V3: Hierarchical Bitsets
/// 
/// Three-level bitset representation enabling 100× skipping on large fact tables
/// Level 0: 1 bit = 4096 rows (Superblock)
/// Level 1: 1 bit = 256 rows (Block)
/// Level 2: Dense or Sparse row bitset

use crate::storage::bitset_v3::Bitset;
use crate::storage::fragment::ColumnFragment;
use anyhow::Result;

const SUPERBLOCK_SIZE: usize = 4096; // Level 0: 1 bit = 4096 rows
const BLOCK_SIZE: usize = 256;      // Level 1: 1 bit = 256 rows

/// Hierarchical bitset for large fact tables
pub struct HierarchicalBitset {
    /// Level 0: Superblock bitset (1 bit = 4096 rows)
    level_0: Bitset,
    
    /// Level 1: Block bitsets (1 bit = 256 rows)
    /// Only loaded for superblocks that have set bits
    level_1: Vec<Option<Bitset>>,
    
    /// Level 2: Row-level bitsets
    /// Only loaded for blocks that have set bits
    level_2: Vec<Vec<Option<Bitset>>>,
    
    /// Total number of rows
    total_rows: usize,
}

impl HierarchicalBitset {
    /// Build hierarchical bitset from fact table
    pub fn build_from_fact_table(
        fact_table_size: usize,
        matching_row_ids: &[usize],
    ) -> Result<Self> {
        let num_superblocks = (fact_table_size + SUPERBLOCK_SIZE - 1) / SUPERBLOCK_SIZE;
        let mut level_0 = Bitset::new(num_superblocks);
        
        // Track which superblocks and blocks have set bits
        let mut superblock_has_bits = vec![false; num_superblocks];
        let mut block_has_bits: Vec<Vec<bool>> = vec![vec![false; SUPERBLOCK_SIZE / BLOCK_SIZE]; num_superblocks];
        
        // Process matching rows
        for &row_id in matching_row_ids {
            if row_id >= fact_table_size {
                continue;
            }
            
            let superblock_id = row_id / SUPERBLOCK_SIZE;
            let block_id = (row_id % SUPERBLOCK_SIZE) / BLOCK_SIZE;
            
            superblock_has_bits[superblock_id] = true;
            block_has_bits[superblock_id][block_id] = true;
            level_0.set(superblock_id);
        }
        
        // Build level 1 (blocks) - only for superblocks with bits
        let mut level_1 = Vec::with_capacity(num_superblocks);
        for superblock_id in 0..num_superblocks {
            if superblock_has_bits[superblock_id] {
                let num_blocks = SUPERBLOCK_SIZE / BLOCK_SIZE;
                let mut block_bitset = Bitset::new(num_blocks);
                
                for block_id in 0..num_blocks {
                    if block_has_bits[superblock_id][block_id] {
                        block_bitset.set(block_id);
                    }
                }
                
                level_1.push(Some(block_bitset));
            } else {
                level_1.push(None);
            }
        }
        
        // Build level 2 (rows) - only for blocks with bits
        let mut level_2 = Vec::with_capacity(num_superblocks);
        for superblock_id in 0..num_superblocks {
            if !superblock_has_bits[superblock_id] {
                level_2.push(Vec::new());
                continue;
            }
            
            let num_blocks = SUPERBLOCK_SIZE / BLOCK_SIZE;
            let mut superblock_level_2 = Vec::with_capacity(num_blocks);
            
            for block_id in 0..num_blocks {
                if block_has_bits[superblock_id][block_id] {
                    // Build row-level bitset for this block
                    let mut row_bitset = Bitset::new(BLOCK_SIZE);
                    let block_start = superblock_id * SUPERBLOCK_SIZE + block_id * BLOCK_SIZE;
                    
                    for &row_id in matching_row_ids {
                        if row_id >= block_start && row_id < block_start + BLOCK_SIZE {
                            let local_row = row_id - block_start;
                            row_bitset.set(local_row);
                        }
                    }
                    
                    superblock_level_2.push(Some(row_bitset));
                } else {
                    superblock_level_2.push(None);
                }
            }
            
            level_2.push(superblock_level_2);
        }
        
        Ok(Self {
            level_0,
            level_1,
            level_2,
            total_rows: fact_table_size,
        })
    }
    
    /// Skip superblock if empty
    pub fn skip_superblock_if_empty(&self, superblock_id: usize) -> bool {
        !self.level_0.get(superblock_id)
    }
    
    /// Skip block if empty
    pub fn skip_block_if_empty(&self, superblock_id: usize, block_id: usize) -> bool {
        if let Some(ref level_1_bitset) = self.level_1.get(superblock_id).and_then(|b| b.as_ref()) {
            !level_1_bitset.get(block_id)
        } else {
            true // Superblock is empty, so block is empty
        }
    }
    
    /// Lazy load lower levels (returns row bitset for a block)
    pub fn lazy_load_block_rows(&self, superblock_id: usize, block_id: usize) -> Option<&Bitset> {
        self.level_2
            .get(superblock_id)?
            .get(block_id)?
            .as_ref()
    }
    
    /// Get all matching row IDs (materialization)
    pub fn get_matching_rows(&self) -> Vec<usize> {
        let mut rows = Vec::new();
        
        let num_superblocks = self.level_0.capacity();
        for superblock_id in 0..num_superblocks {
            if self.skip_superblock_if_empty(superblock_id) {
                continue; // Skip entire superblock
            }
            
            let num_blocks = SUPERBLOCK_SIZE / BLOCK_SIZE;
            for block_id in 0..num_blocks {
                if self.skip_block_if_empty(superblock_id, block_id) {
                    continue; // Skip block
                }
                
                // Get row-level bitset
                if let Some(row_bitset) = self.lazy_load_block_rows(superblock_id, block_id) {
                    let block_start = superblock_id * SUPERBLOCK_SIZE + block_id * BLOCK_SIZE;
                    for row_offset in row_bitset.get_set_bits() {
                        let row_id = block_start + row_offset;
                        if row_id < self.total_rows {
                            rows.push(row_id);
                        }
                    }
                }
            }
        }
        
        rows
    }
    
    /// Intersect with another hierarchical bitset
    pub fn intersect(&self, other: &Self) -> Result<Self> {
        if self.total_rows != other.total_rows {
            anyhow::bail!("Cannot intersect bitsets of different sizes");
        }
        
        // Intersect level 0
        let level_0_intersect = self.level_0.intersect(&other.level_0);
        
        // Build new hierarchical structure
        let num_superblocks = level_0_intersect.capacity();
        let mut level_1 = Vec::with_capacity(num_superblocks);
        let mut level_2 = Vec::with_capacity(num_superblocks);
        
        for superblock_id in 0..num_superblocks {
            if !level_0_intersect.get(superblock_id) {
                level_1.push(None);
                level_2.push(Vec::new());
                continue;
            }
            
            // Intersect level 1 for this superblock
            let self_level_1 = self.level_1.get(superblock_id).and_then(|b| b.as_ref());
            let other_level_1 = other.level_1.get(superblock_id).and_then(|b| b.as_ref());
            
            if let (Some(self_bits), Some(other_bits)) = (self_level_1, other_level_1) {
                let level_1_intersect = self_bits.intersect(other_bits);
                level_1.push(Some(level_1_intersect));
                
                // Intersect level 2
                let num_blocks = SUPERBLOCK_SIZE / BLOCK_SIZE;
                let mut superblock_level_2 = Vec::with_capacity(num_blocks);
                
                for block_id in 0..num_blocks {
                    let self_level_2 = self.level_2.get(superblock_id)
                        .and_then(|b| b.get(block_id))
                        .and_then(|b| b.as_ref());
                    let other_level_2 = other.level_2.get(superblock_id)
                        .and_then(|b| b.get(block_id))
                        .and_then(|b| b.as_ref());
                    
                    if let (Some(self_rows), Some(other_rows)) = (self_level_2, other_level_2) {
                        let row_intersect = self_rows.intersect(other_rows);
                        superblock_level_2.push(Some(row_intersect));
                    } else {
                        superblock_level_2.push(None);
                    }
                }
                
                level_2.push(superblock_level_2);
            } else {
                level_1.push(None);
                level_2.push(Vec::new());
            }
        }
        
        Ok(Self {
            level_0: level_0_intersect,
            level_1,
            level_2,
            total_rows: self.total_rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_hierarchical_bitset() {
        let fact_size = 10000;
        let matching_rows = vec![100, 200, 300, 5000, 6000];
        
        let hbitset = HierarchicalBitset::build_from_fact_table(fact_size, &matching_rows).unwrap();
        
        // Should skip empty superblocks
        assert!(hbitset.skip_superblock_if_empty(2)); // Superblock 2 should be empty
        
        // Should find matching rows
        let rows = hbitset.get_matching_rows();
        assert_eq!(rows.len(), matching_rows.len());
    }
}

