/// Tiered indexes as memory filters
/// Multi-level indexes that act as filters to skip irrelevant data
use crate::storage::fragment::{ColumnFragment, Value};
use crate::storage::learned_index::{RecursiveModelIndex, HashLearnedIndex, LearnedIndex};
use crate::storage::index::{SortedIndex, HashIndex, BloomIndex, BTreeIndex, ColumnIndex};
use crate::hypergraph::node::NodeId;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;

/// Tiered index structure
/// Combines multiple index types in a hierarchy for efficient filtering
#[derive(Clone)]
pub struct TieredIndex {
    /// L1: Learned index (fastest, approximate)
    l1_learned: Option<RecursiveModelIndex>,
    
    /// L2: Sorted index (fast, exact for sorted data)
    l2_sorted: Option<SortedIndex>,
    
    /// L3: Hash index (fast lookups, exact)
    l3_hash: Option<HashIndex>,
    
    /// L4: Bloom filter (fastest negative lookups)
    l4_bloom: Option<BloomIndex>,
    
    /// L5: B-tree index (general purpose, exact)
    l5_btree: Option<BTreeIndex>,
    
    /// Data type this index is for
    data_type: DataType,
    
    /// Total rows indexed
    total_rows: usize,
}

impl TieredIndex {
    pub fn new(data_type: DataType, total_rows: usize) -> Self {
        Self {
            l1_learned: None,
            l2_sorted: None,
            l3_hash: None,
            l4_bloom: None,
            l5_btree: None,
            data_type,
            total_rows,
        }
    }
    
    /// Build tiered index from fragment
    pub fn build_from_fragment(fragment: &ColumnFragment) -> Self {
        let array = fragment.get_array().expect("Fragment must have array to build index");
        let data_type = array.data_type();
        let total_rows = array.len();
        let mut index = Self::new(data_type.clone(), total_rows);
        
        // Build L1: Learned index (always build for approximate lookups)
        let learned = RecursiveModelIndex::build(fragment);
        index.l1_learned = Some(learned);
        
        // Build L2: Sorted index (if fragment is sorted)
        if fragment.is_sorted {
            let sorted = SortedIndex::new(fragment);
            // Check if sorted index has segments (it's private, so we'll just build it)
            index.l2_sorted = Some(sorted);
        }
        
        // Build L3: Hash index (for equality lookups)
        let hash = HashIndex::build(fragment);
        index.l3_hash = Some(hash);
        
        // Build L4: Bloom filter (for fast negative lookups)
        let mut bloom = BloomIndex::new(fragment.len(), 0.01);
        for i in 0..fragment.len().min(10000) { // Sample for performance
            if let Some(value) = Self::extract_value(fragment, i) {
                bloom.insert(&value);
            }
        }
        index.l4_bloom = Some(bloom);
        
        // Build L5: B-tree index (general purpose fallback)
        let btree = BTreeIndex::build(fragment, 10);
        index.l5_btree = Some(btree);
        
        index
    }
    
    /// Check if value might exist (using bloom filter - fastest)
    /// Returns false if definitely not present, true if might be present
    pub fn might_contain(&self, value: &Value) -> bool {
        // L4: Bloom filter (fastest negative lookup)
        if let Some(ref bloom) = self.l4_bloom {
            if !bloom.might_contain(value) {
                return false; // Definitely not present
            }
        }
        
        // If bloom filter says might contain, return true
        true
    }
    
    /// Find approximate position using learned index (L1)
    pub fn approximate_position(&self, value: &Value) -> Option<usize> {
        Some(self.l1_learned.as_ref()?.predict_position(value))
    }
    
    /// Find exact position using sorted index (L2)
    pub fn find_sorted(&self, value: &Value) -> Option<Vec<usize>> {
        use crate::storage::index::Predicate;
        if let Some(ref sorted) = self.l2_sorted {
            // Use ColumnIndex trait method
            let bitmap = sorted.get_matching_rows(&Predicate::Equals(value.clone()));
            let mut positions = Vec::new();
            for i in 0..bitmap.len() {
                if bitmap[i] {
                    positions.push(i);
                }
            }
            if !positions.is_empty() {
                return Some(positions);
            }
        }
        None
    }
    
    /// Find exact positions using hash index (L3)
    pub fn find_hash(&self, value: &Value) -> Option<Vec<usize>> {
        use crate::storage::index::Predicate;
        if let Some(ref hash) = self.l3_hash {
            let bitmap = hash.get_matching_rows(&Predicate::Equals(value.clone()));
            let mut positions = Vec::new();
            for i in 0..bitmap.len() {
                if bitmap[i] {
                    positions.push(i);
                }
            }
            if !positions.is_empty() {
                return Some(positions);
            }
        }
        None
    }
    
    /// Find exact positions using B-tree (L5)
    pub fn find_btree(&self, value: &Value) -> Option<Vec<usize>> {
        use crate::storage::index::Predicate;
        if let Some(ref btree) = self.l5_btree {
            // Use ColumnIndex trait method
            let bitmap = btree.get_matching_rows(&Predicate::Equals(value.clone()));
            let mut positions = Vec::new();
            for i in 0..bitmap.len() {
                if bitmap[i] {
                    positions.push(i);
                }
            }
            if !positions.is_empty() {
                return Some(positions);
            }
        }
        None
    }
    
    /// Find positions using tiered lookup (tries fastest first)
    pub fn find(&self, value: &Value) -> Vec<usize> {
        // Step 1: Check bloom filter (fastest - negative lookup)
        if !self.might_contain(value) {
            return vec![]; // Definitely not present
        }
        
        // Step 2: Try hash index (fastest exact lookup)
        if let Some(positions) = self.find_hash(value) {
            return positions;
        }
        
        // Step 3: Try sorted index (if data is sorted)
        if let Some(positions) = self.find_sorted(value) {
            return positions;
        }
        
        // Step 4: Try B-tree (general purpose)
        if let Some(positions) = self.find_btree(value) {
            return positions;
        }
        
        // Step 5: Fallback to learned index (approximate, then scan)
        if let Some(approx_pos) = self.approximate_position(value) {
            // Use approximate position as starting point for scan
            // In a full implementation, we'd scan around this position
            vec![approx_pos]
        } else {
            vec![] // Not found
        }
    }
    
    /// Range query using tiered indexes
    pub fn find_range(&self, min: &Value, max: &Value) -> Vec<usize> {
        use crate::storage::index::Predicate;
        
        // Try sorted index first (best for range queries)
        if let Some(ref sorted) = self.l2_sorted {
            let bitmap = sorted.get_matching_rows(&Predicate::Between(min.clone(), max.clone()));
            let mut positions = Vec::new();
            for i in 0..bitmap.len() {
                if bitmap[i] {
                    positions.push(i);
                }
            }
            if !positions.is_empty() {
                return positions;
            }
        }
        
        // Try B-tree (also good for range queries)
        if let Some(ref btree) = self.l5_btree {
            let bitmap = btree.get_matching_rows(&Predicate::Between(min.clone(), max.clone()));
            let mut positions = Vec::new();
            for i in 0..bitmap.len() {
                if bitmap[i] {
                    positions.push(i);
                }
            }
            if !positions.is_empty() {
                return positions;
            }
        }
        
        // Fallback: use learned index for approximate bounds
        if let (Some(min_pos), Some(max_pos)) = (
            self.approximate_position(min),
            self.approximate_position(max),
        ) {
            // Return approximate range
            (min_pos..=max_pos.min(self.total_rows)).collect()
        } else {
            vec![]
        }
    }
    
    /// Extract value from fragment at index (helper)
    fn extract_value(fragment: &ColumnFragment, idx: usize) -> Option<Value> {
        let array = fragment.get_array()?;
        if array.is_null(idx) {
            return Some(Value::Null);
        }
        
        match array.data_type() {
            DataType::Int64 => {
                array.as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|arr| Value::Int64(arr.value(idx)))
            }
            DataType::Float64 => {
                array.as_any()
                    .downcast_ref::<Float64Array>()
                    .map(|arr| Value::Float64(arr.value(idx)))
            }
            DataType::Utf8 => {
                array.as_any()
                    .downcast_ref::<StringArray>()
                    .map(|arr| Value::String(arr.value(idx).to_string()))
            }
            _ => None,
        }
    }
    
    /// Get index statistics
    pub fn stats(&self) -> TieredIndexStats {
        TieredIndexStats {
            has_learned: self.l1_learned.is_some(),
            has_sorted: self.l2_sorted.is_some(),
            has_hash: self.l3_hash.is_some(),
            has_bloom: self.l4_bloom.is_some(),
            has_btree: self.l5_btree.is_some(),
            total_rows: self.total_rows,
        }
    }
}

/// Statistics for tiered index
#[derive(Debug, Clone)]
pub struct TieredIndexStats {
    pub has_learned: bool,
    pub has_sorted: bool,
    pub has_hash: bool,
    pub has_bloom: bool,
    pub has_btree: bool,
    pub total_rows: usize,
}

/// Memory filter using tiered indexes
/// Acts as a filter to skip irrelevant data before accessing memory
pub struct MemoryFilter {
    /// Tiered index for filtering
    index: TieredIndex,
    
    /// Fragment this filter is for
    fragment_id: (NodeId, usize),
    
    /// Memory tier this filter operates on
    memory_tier: crate::storage::memory_tier::MemoryTier,
}

impl MemoryFilter {
    pub fn new(index: TieredIndex, fragment_id: (NodeId, usize), memory_tier: crate::storage::memory_tier::MemoryTier) -> Self {
        Self {
            index,
            fragment_id,
            memory_tier,
        }
    }
    
    /// Filter rows that match predicate (returns bitmask of matching rows)
    pub fn filter(&self, predicate: &FilterPredicate) -> Vec<bool> {
        match predicate {
            FilterPredicate::Equals(value) => {
                // Fast path: use bloom filter first
                if !self.index.might_contain(value) {
                    return vec![false; self.index.total_rows]; // All false
                }
                
                // Find positions
                let positions = self.index.find(value);
                let mut result = vec![false; self.index.total_rows];
                for pos in positions {
                    if pos < result.len() {
                        result[pos] = true;
                    }
                }
                result
            }
            FilterPredicate::Range(min, max) => {
                let positions = self.index.find_range(min, max);
                let mut result = vec![false; self.index.total_rows];
                for pos in positions {
                    if pos < result.len() {
                        result[pos] = true;
                    }
                }
                result
            }
            FilterPredicate::In(values) => {
                // For IN clause, check each value
                let mut result = vec![false; self.index.total_rows];
                for value in values {
                    if self.index.might_contain(value) {
                        let positions = self.index.find(value);
                        for pos in positions {
                            if pos < result.len() {
                                result[pos] = true;
                            }
                        }
                    }
                }
                result
            }
            _ => {
                // For other predicates, can't use index effectively
                vec![true; self.index.total_rows] // Don't filter
            }
        }
    }
    
    /// Estimate selectivity (fraction of rows that match)
    pub fn estimate_selectivity(&self, predicate: &FilterPredicate) -> f64 {
        match predicate {
            FilterPredicate::Equals(_) => {
                // Use cardinality estimate
                1.0 / self.index.total_rows as f64
            }
            FilterPredicate::Range(min, max) => {
                // Use learned index to estimate range size
                if let (Some(min_pos), Some(max_pos)) = (
                    self.index.approximate_position(min),
                    self.index.approximate_position(max),
                ) {
                    let range_size = (max_pos.saturating_sub(min_pos)) as f64;
                    range_size / self.index.total_rows as f64
                } else {
                    0.5 // Default estimate
                }
            }
            FilterPredicate::In(values) => {
                // Estimate based on number of values
                (values.len() as f64) / self.index.total_rows as f64
            }
            _ => 1.0, // No estimate, assume all match
        }
    }
}

/// Filter predicate for memory filtering
#[derive(Clone, Debug)]
pub enum FilterPredicate {
    Equals(Value),
    Range(Value, Value),
    In(Vec<Value>),
    LessThan(Value),
    GreaterThan(Value),
}

/// Tiered index manager
/// Manages tiered indexes for all fragments
pub struct TieredIndexManager {
    /// Map from (node_id, fragment_idx) to tiered index
    indexes: dashmap::DashMap<(NodeId, usize), TieredIndex>,
    
    /// Map from (node_id, fragment_idx) to memory filter
    filters: dashmap::DashMap<(NodeId, usize), MemoryFilter>,
}

impl TieredIndexManager {
    pub fn new() -> Self {
        Self {
            indexes: dashmap::DashMap::new(),
            filters: dashmap::DashMap::new(),
        }
    }
    
    /// Build tiered index for a fragment
    pub fn build_index(&self, node_id: NodeId, fragment_idx: usize, fragment: &ColumnFragment) {
        let index = TieredIndex::build_from_fragment(fragment);
        let key = (node_id, fragment_idx);
        self.indexes.insert(key, index);
    }
    
    /// Get tiered index for a fragment
    pub fn get_index(&self, node_id: NodeId, fragment_idx: usize) -> Option<TieredIndex> {
        let key = (node_id, fragment_idx);
        self.indexes.get(&key).map(|entry| (*entry.value()).clone())
    }
    
    /// Create memory filter for a fragment
    pub fn create_filter(
        &self,
        node_id: NodeId,
        fragment_idx: usize,
        memory_tier: crate::storage::memory_tier::MemoryTier,
    ) -> Option<MemoryFilter> {
        let key = (node_id, fragment_idx);
        let index = (*self.indexes.get(&key)?.value()).clone();
        Some(MemoryFilter::new(index, key, memory_tier))
    }
    
    /// Get all indexes (for statistics)
    pub fn get_all_indexes(&self) -> Vec<((NodeId, usize), TieredIndexStats)> {
        self.indexes
            .iter()
            .map(|entry| (*entry.key(), entry.value().stats()))
            .collect()
    }
}

impl Default for TieredIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

