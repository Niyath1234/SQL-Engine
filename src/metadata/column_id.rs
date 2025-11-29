/// Stable ColumnId for DuckDB/Presto-grade join engine
/// 
/// ColumnId provides stable, immutable column identity throughout the query pipeline.
/// It is assigned once at ScanOperator and preserved through all operators, even when:
/// - JOINs reorder columns
/// - Hypergraph optimizer changes join order
/// - Aliases are applied
/// - Schema transformations occur
/// 
/// Design: ColumnId = {table_id, column_ordinal}
/// - table_id: Unique identifier for the source table (u32)
/// - column_ordinal: Index of column within that table's schema (u32)
/// 
/// This ensures stable, deterministic column identity throughout the pipeline.
use std::hash::{Hash, Hasher};

/// Column ordinal (index within table schema)
pub type ColumnOrdinal = u32;

/// Stable Column Identifier
/// 
/// ColumnId is an immutable struct representing {table_id, column_ordinal}.
/// It implements Hash, Eq, Clone, Debug for use in hash maps and sets.
/// 
/// # Invariants
/// - ColumnId is NEVER modified after creation
/// - Same table_id + column_ordinal always produces same ColumnId
/// - ColumnIds are globally unique (no collisions)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ColumnId {
    /// Unique identifier for the source table
    pub table_id: u32,
    /// Index of column within that table's schema
    pub column_ordinal: u32,
}

impl ColumnId {
    /// Create a new ColumnId from table_id and column_ordinal
    pub fn new(table_id: u32, column_ordinal: u32) -> Self {
        ColumnId { table_id, column_ordinal }
    }
    
    /// Get table_id
    pub fn table_id(&self) -> u32 {
        self.table_id
    }
    
    /// Get column_ordinal
    pub fn column_ordinal(&self) -> u32 {
        self.column_ordinal
    }
    
    /// Convert to u64 for efficient hashing and storage
    /// Uses encoding: (table_id << 32) | column_ordinal
    pub fn to_u64(&self) -> u64 {
        ((self.table_id as u64) << 32) | (self.column_ordinal as u64)
    }
    
    /// Create from u64 (for efficient storage/deserialization)
    pub fn from_u64(value: u64) -> Self {
        ColumnId {
            table_id: (value >> 32) as u32,
            column_ordinal: (value & 0xFFFFFFFF) as u32,
        }
    }
    
    /// Convert to u32 for backward compatibility (if needed)
    /// Uses encoding: (table_id << 16) | column_ordinal (limited to 16-bit table_id)
    /// WARNING: Only use if table_id < 65536
    pub fn to_u32(&self) -> Option<u32> {
        if self.table_id < 65536 {
            Some((self.table_id << 16) | self.column_ordinal)
        } else {
            None
        }
    }
    
    /// Create from u32 (for backward compatibility)
    /// WARNING: Only works if table_id < 65536
    pub fn from_u32(value: u32) -> Self {
        ColumnId {
            table_id: value >> 16,
            column_ordinal: value & 0xFFFF,
        }
    }
}

impl Hash for ColumnId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use efficient hashing for ColumnId
        state.write_u64(self.to_u64());
    }
}

/// Fast hash function for ColumnId
impl ColumnId {
    pub fn fast_hash(&self) -> u64 {
        // Use efficient u64-based hashing
        self.to_u64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_column_id_creation() {
        let id1 = ColumnId::new(1, 0);
        let id2 = ColumnId::new(1, 0);
        assert_eq!(id1, id2);
        
        let id3 = ColumnId::new(1, 1);
        assert_ne!(id1, id3);
    }
    
    #[test]
    fn test_column_id_u64_conversion() {
        let id = ColumnId::new(12345, 67);
        let encoded = id.to_u64();
        let decoded = ColumnId::from_u64(encoded);
        assert_eq!(id, decoded);
    }
    
    #[test]
    fn test_column_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ColumnId::new(1, 0));
        set.insert(ColumnId::new(1, 1));
        assert_eq!(set.len(), 2);
        
        // Same ID should not be inserted twice
        set.insert(ColumnId::new(1, 0));
        assert_eq!(set.len(), 2);
    }
}

