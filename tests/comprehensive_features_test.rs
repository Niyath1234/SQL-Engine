/// Comprehensive test suite for all implemented performance features
use hypergraph_sql_engine::storage::bitset::Bitset;
use hypergraph_sql_engine::storage::bitmap_index::{BitmapIndex, InvertedList};
use hypergraph_sql_engine::storage::btree_index::BTreeIndex;
use hypergraph_sql_engine::storage::partitioning::{PartitionManager, PartitionKey, PartitionFunction, Partition, PartitionId, PartitionStatistics};
use hypergraph_sql_engine::storage::fragment::Value;
use std::collections::HashMap;

// ============================================================================
// BITSET TESTS
// ============================================================================

#[test]
fn test_bitset_basic_operations() {
    let mut bs = Bitset::new();
    
    // Test set/get
    bs.set(0);
    bs.set(5);
    bs.set(100);
    assert!(bs.get(0));
    assert!(bs.get(5));
    assert!(bs.get(100));
    assert!(!bs.get(1));
    assert!(!bs.get(50));
    
    // Test clear
    bs.clear(5);
    assert!(!bs.get(5));
    assert!(bs.get(0));
    assert!(bs.get(100));
}

#[test]
fn test_bitset_intersection() {
    let mut bs1 = Bitset::new();
    let mut bs2 = Bitset::new();
    
    bs1.set(0);
    bs1.set(1);
    bs1.set(2);
    bs1.set(5);
    
    bs2.set(1);
    bs2.set(2);
    bs2.set(3);
    bs2.set(5);
    
    let inter = bs1.intersect(&bs2);
    assert!(inter.get(1));
    assert!(inter.get(2));
    assert!(inter.get(5));
    assert!(!inter.get(0));
    assert!(!inter.get(3));
    assert_eq!(inter.popcount(), 3);
}

#[test]
fn test_bitset_union() {
    let mut bs1 = Bitset::new();
    let mut bs2 = Bitset::new();
    
    bs1.set(0);
    bs1.set(1);
    
    bs2.set(1);
    bs2.set(2);
    
    let union = bs1.union(&bs2);
    assert!(union.get(0));
    assert!(union.get(1));
    assert!(union.get(2));
    assert_eq!(union.popcount(), 3);
}

#[test]
fn test_bitset_iteration() {
    let mut bs = Bitset::new();
    bs.set(0);
    bs.set(5);
    bs.set(100);
    bs.set(200);
    
    let bits: Vec<usize> = bs.iter_set_bits().collect();
    assert_eq!(bits, vec![0, 5, 100, 200]);
}

#[test]
fn test_bitset_popcount() {
    let mut bs = Bitset::new();
    for i in 0..100 {
        if i % 2 == 0 {
            bs.set(i);
        }
    }
    assert_eq!(bs.popcount(), 50);
}

#[test]
fn test_bitset_is_subset() {
    let mut bs1 = Bitset::new();
    let mut bs2 = Bitset::new();
    
    bs1.set(1);
    bs1.set(2);
    
    bs2.set(1);
    bs2.set(2);
    bs2.set(3);
    
    assert!(bs1.is_subset(&bs2));
    assert!(!bs2.is_subset(&bs1));
}

// ============================================================================
// BITMAP INDEX TESTS
// ============================================================================

#[test]
fn test_bitmap_index_creation() {
    let mut index = BitmapIndex::new("table".to_string(), "id".to_string());
    
    index.add_value(Value::Int64(100), 0);
    index.add_value(Value::Int64(100), 1);
    index.add_value(Value::Int64(200), 2);
    
    assert_eq!(index.distinct_count(), 2);
    assert_eq!(index.row_count, 3);
}

#[test]
fn test_bitmap_index_lookup() {
    let mut index = BitmapIndex::new("table".to_string(), "id".to_string());
    
    index.add_value(Value::Int64(100), 0);
    index.add_value(Value::Int64(100), 1);
    index.add_value(Value::Int64(200), 2);
    
    let bs = index.get_bitset(&Value::Int64(100)).unwrap();
    assert!(bs.get(0));
    assert!(bs.get(1));
    assert!(!bs.get(2));
    
    let bs2 = index.get_bitset(&Value::Int64(200)).unwrap();
    assert!(!bs2.get(0));
    assert!(!bs2.get(1));
    assert!(bs2.get(2));
}

#[test]
fn test_bitmap_index_intersection() {
    let mut left = BitmapIndex::new("t1".to_string(), "id".to_string());
    let mut right = BitmapIndex::new("t2".to_string(), "id".to_string());
    
    // left: value 100 at rows 0,1; value 200 at row 2
    left.add_value(Value::Int64(100), 0);
    left.add_value(Value::Int64(100), 1);
    left.add_value(Value::Int64(200), 2);
    
    // right: value 100 at row 0; value 200 at row 1
    right.add_value(Value::Int64(100), 0);
    right.add_value(Value::Int64(200), 1);
    
    let intersections = left.intersect_with(&right);
    
    // The intersection only includes values where row IDs overlap
    // For value 100: left has rows 0,1; right has row 0 -> intersection has row 0 ✓
    // For value 200: left has row 2; right has row 1 -> intersection is empty ✗
    
    // So we should have exactly 1 result (value 100, since row 0 matches)
    assert_eq!(intersections.len(), 1);
    assert!(intersections.contains_key(&Value::Int64(100)));
    assert!(!intersections.contains_key(&Value::Int64(200))); // Value 200 has no overlapping row IDs
    
    // Verify value 100 intersection
    let inter_100 = intersections.get(&Value::Int64(100)).unwrap();
    assert!(inter_100.get(0)); // Row 0 is in both indexes
    assert!(!inter_100.get(1)); // Row 1 is only in left index
}

#[test]
fn test_bitmap_index_selectivity() {
    let mut index = BitmapIndex::new("table".to_string(), "status".to_string());
    
    for i in 0..100 {
        if i < 20 {
            index.add_value(Value::String("active".to_string()), i);
        } else {
            index.add_value(Value::String("inactive".to_string()), i);
        }
    }
    
    let sel_active = index.estimate_selectivity(&Value::String("active".to_string()));
    assert!((sel_active - 0.2).abs() < 0.01);
    
    let sel_inactive = index.estimate_selectivity(&Value::String("inactive".to_string()));
    assert!((sel_inactive - 0.8).abs() < 0.01);
}

#[test]
fn test_inverted_list() {
    let mut list = InvertedList::new("table".to_string(), "id".to_string());
    
    list.add(Value::Int64(100), 0);
    list.add(Value::Int64(100), 1);
    list.add(Value::Int64(200), 2);
    
    let rows = list.get(&Value::Int64(100)).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], 0);
    assert_eq!(rows[1], 1);
}

// ============================================================================
// B-TREE INDEX TESTS
// ============================================================================

#[test]
fn test_btree_index_creation() {
    let mut index = BTreeIndex::new("table".to_string(), "id".to_string(), false);
    
    index.insert(Value::Int64(100), 0);
    index.insert(Value::Int64(200), 1);
    index.insert(Value::Int64(100), 2);
    
    assert_eq!(index.entry_count, 3);
}

#[test]
fn test_btree_index_lookup() {
    let mut index = BTreeIndex::new("table".to_string(), "id".to_string(), false);
    
    index.insert(Value::Int64(100), 0);
    index.insert(Value::Int64(200), 1);
    index.insert(Value::Int64(100), 2);
    
    let rows = index.lookup(&Value::Int64(100)).unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&0));
    assert!(rows.contains(&2));
}

#[test]
fn test_btree_index_range_scan() {
    let mut index = BTreeIndex::new("table".to_string(), "id".to_string(), false);
    
    for i in 0..100 {
        index.insert(Value::Int64(i as i64), i);
    }
    
    let rows = index.range_scan(
        Some(&Value::Int64(10)),
        Some(&Value::Int64(20)),
        true,
        true,
    );
    
    assert_eq!(rows.len(), 11); // 10 through 20 inclusive
    assert!(rows.contains(&10));
    assert!(rows.contains(&20));
    assert!(!rows.contains(&9));
    assert!(!rows.contains(&21));
}

#[test]
fn test_btree_index_less_than() {
    let mut index = BTreeIndex::new("table".to_string(), "id".to_string(), false);
    
    for i in 0..50 {
        index.insert(Value::Int64(i as i64), i);
    }
    
    // Test less than with inclusive=false (should exclude 20)
    let rows = index.less_than(&Value::Int64(20), false);
    assert!(rows.len() >= 20);
    assert!(rows.contains(&19));
    assert!(!rows.contains(&20)); // 20 should be excluded when inclusive=false
    
    // Test less than with inclusive=true (should include 20)
    let rows_inclusive = index.less_than(&Value::Int64(20), true);
    assert!(rows_inclusive.len() >= 21); // Should include 0-20
    assert!(rows_inclusive.contains(&19));
    assert!(rows_inclusive.contains(&20)); // 20 should be included when inclusive=true
}

#[test]
fn test_btree_index_greater_than() {
    let mut index = BTreeIndex::new("table".to_string(), "id".to_string(), false);
    
    for i in 0..50 {
        index.insert(Value::Int64(i as i64), i);
    }
    
    let rows = index.greater_than(&Value::Int64(20), true);
    assert!(rows.len() >= 29); // 20 through 49
    assert!(rows.contains(&20));
    assert!(rows.contains(&49));
    assert!(!rows.contains(&19));
}

#[test]
fn test_btree_index_selectivity() {
    let mut index = BTreeIndex::new("table".to_string(), "id".to_string(), false);
    
    for i in 0..100 {
        index.insert(Value::Int64(i as i64), i);
    }
    
    let selectivity = index.estimate_selectivity(
        Some(&Value::Int64(10)),
        Some(&Value::Int64(20)),
    );
    
    // Should be approximately 11/100 = 0.11
    assert!(selectivity > 0.1 && selectivity < 0.12);
}

// ============================================================================
// PARTITIONING TESTS
// ============================================================================

#[test]
fn test_partition_manager_creation() {
    let manager = PartitionManager::new();
    assert!(manager.partition_key.is_none());
    assert!(manager.partitions.is_empty());
}

#[test]
fn test_partition_manager_with_key() {
    let key = PartitionKey {
        columns: vec!["date".to_string()],
        function: PartitionFunction::Range { ranges: vec![] },
    };
    let manager = PartitionManager::with_partition_key(key);
    assert!(manager.partition_key.is_some());
}

#[test]
fn test_partition_creation() {
    let mut manager = PartitionManager::new();
    
    let partition = Partition {
        id: PartitionId(0),
        key_values: vec![Value::String("2024-01-01".to_string())],
        column_fragments: HashMap::new(),
        stats: PartitionStatistics {
            row_count: 1000,
            size_bytes: 10000,
            min_values: HashMap::new(),
            max_values: HashMap::new(),
        },
        node_ids: vec![],
    };
    
    manager.add_partition(partition);
    assert_eq!(manager.partitions.len(), 1);
}

#[test]
fn test_partition_pruning_equals() {
    use hypergraph_sql_engine::storage::partitioning::PartitionPredicate;
    
    let key = PartitionKey {
        columns: vec!["date".to_string()],
        function: PartitionFunction::Range { ranges: vec![] },
    };
    let mut manager = PartitionManager::with_partition_key(key);
    
    // Add partition for 2024-01-01
    let mut partition = Partition {
        id: PartitionId(0),
        key_values: vec![Value::String("2024-01-01".to_string())],
        column_fragments: HashMap::new(),
        stats: PartitionStatistics {
            row_count: 1000,
            size_bytes: 10000,
            min_values: HashMap::new(),
            max_values: HashMap::new(),
        },
        node_ids: vec![],
    };
    partition.stats.min_values.insert("date".to_string(), Value::String("2024-01-01".to_string()));
    partition.stats.max_values.insert("date".to_string(), Value::String("2024-01-01".to_string()));
    manager.add_partition(partition);
    
    // Add partition for 2024-01-02
    let mut partition2 = Partition {
        id: PartitionId(1),
        key_values: vec![Value::String("2024-01-02".to_string())],
        column_fragments: HashMap::new(),
        stats: PartitionStatistics {
            row_count: 1000,
            size_bytes: 10000,
            min_values: HashMap::new(),
            max_values: HashMap::new(),
        },
        node_ids: vec![],
    };
    partition2.stats.min_values.insert("date".to_string(), Value::String("2024-01-02".to_string()));
    partition2.stats.max_values.insert("date".to_string(), Value::String("2024-01-02".to_string()));
    manager.add_partition(partition2);
    
    // Prune with equals predicate
    let predicates = vec![
        PartitionPredicate::Equals {
            column: "date".to_string(),
            value: Value::String("2024-01-01".to_string()),
        },
    ];
    
    let matching = manager.prune_partitions(&predicates);
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0], PartitionId(0));
}

#[test]
fn test_partition_pruning_range() {
    use hypergraph_sql_engine::storage::partitioning::PartitionPredicate;
    
    let key = PartitionKey {
        columns: vec!["date".to_string()],
        function: PartitionFunction::Range { ranges: vec![] },
    };
    let mut manager = PartitionManager::with_partition_key(key);
    
    // Add partitions
    for i in 1..=3 {
        let mut partition = Partition {
            id: PartitionId(i as u64 - 1),
            key_values: vec![Value::String(format!("2024-01-{:02}", i))],
            column_fragments: HashMap::new(),
            stats: PartitionStatistics {
                row_count: 1000,
                size_bytes: 10000,
                min_values: HashMap::new(),
                max_values: HashMap::new(),
            },
            node_ids: vec![],
        };
        partition.stats.min_values.insert("date".to_string(), Value::String(format!("2024-01-{:02}", i)));
        partition.stats.max_values.insert("date".to_string(), Value::String(format!("2024-01-{:02}", i)));
        manager.add_partition(partition);
    }
    
    // Prune with range predicate (2024-01-01 to 2024-01-02)
    let predicates = vec![
        PartitionPredicate::Range {
            column: "date".to_string(),
            min: Some(Value::String("2024-01-01".to_string())),
            max: Some(Value::String("2024-01-02".to_string())),
            min_inclusive: true,
            max_inclusive: true,
        },
    ];
    
    let matching = manager.prune_partitions(&predicates);
    assert_eq!(matching.len(), 2); // Should match partitions 0 and 1
}

// ============================================================================
// INTEGRATION TESTS
// ============================================================================

#[test]
fn test_bitset_with_bitmap_index() {
    // Test that bitset operations work correctly with bitmap indexes
    let mut index = BitmapIndex::new("table".to_string(), "id".to_string());
    
    for i in 0..100 {
        index.add_value(Value::Int64((i % 10) as i64), i);
    }
    
    let bs = index.get_bitset(&Value::Int64(5)).unwrap();
    assert_eq!(bs.popcount(), 10); // Should have 10 rows with value 5
    
    let bits: Vec<usize> = bs.iter_set_bits().collect();
    assert_eq!(bits.len(), 10);
}

#[test]
fn test_multi_index_operations() {
    // Test using multiple index types together
    let mut bitmap_index = BitmapIndex::new("table".to_string(), "status".to_string());
    let mut btree_index = BTreeIndex::new("table".to_string(), "id".to_string(), false);
    
    for i in 0..100 {
        bitmap_index.add_value(Value::String("active".to_string()), i);
        btree_index.insert(Value::Int64(i as i64), i);
    }
    
    // Both indexes should work independently
    assert!(bitmap_index.get_bitset(&Value::String("active".to_string())).is_some());
    assert!(btree_index.lookup(&Value::Int64(50)).is_some());
}

#[test]
fn test_performance_features_summary() {
    // Summary test to verify all features compile and basic functionality works
    let mut bs = Bitset::new();
    bs.set(0);
    assert!(bs.get(0));
    
    let mut bitmap_idx = BitmapIndex::new("t".to_string(), "c".to_string());
    bitmap_idx.add_value(Value::Int64(1), 0);
    assert_eq!(bitmap_idx.distinct_count(), 1);
    
    let mut btree_idx = BTreeIndex::new("t".to_string(), "c".to_string(), false);
    btree_idx.insert(Value::Int64(1), 0);
    assert_eq!(btree_idx.entry_count, 1);
    
    let manager = PartitionManager::new();
    assert!(manager.partitions.is_empty());
    
    // All features work!
    assert!(true);
}

