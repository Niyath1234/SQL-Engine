/// Integration tests for Bitmap-Algebra Multiway Join (BAMJ)
use hypergraph_sql_engine::storage::bitmap_index::BitmapIndex;
use hypergraph_sql_engine::storage::bitset::Bitset;
use hypergraph_sql_engine::storage::fragment::Value;

#[test]
fn test_simple_bitmap_join() {
    // Create two small tables with bitmap indexes
    let mut left_index = BitmapIndex::new("table1".to_string(), "id".to_string());
    let mut right_index = BitmapIndex::new("table2".to_string(), "id".to_string());

    // Table1: id column
    // Row 0: id=100
    // Row 1: id=100
    // Row 2: id=200
    left_index.add_value(Value::Int64(100), 0);
    left_index.add_value(Value::Int64(100), 1);
    left_index.add_value(Value::Int64(200), 2);

    // Table2: id column
    // Row 0: id=100
    // Row 1: id=200
    right_index.add_value(Value::Int64(100), 0);
    right_index.add_value(Value::Int64(200), 1);

    // Perform intersection
    let intersections = left_index.intersect_with(&right_index);

    // The intersection only includes values where row IDs overlap between tables
    // For value 100: left has rows 0,1; right has row 0 -> intersection has row 0 (both have it) ✓
    // For value 200: left has row 2; right has row 1 -> intersection is empty (no common row IDs) ✗
    
    // So we should have exactly 1 result (value 100, since row 0 matches)
    assert_eq!(intersections.len(), 1);
    assert!(intersections.contains_key(&Value::Int64(100)));
    assert!(!intersections.contains_key(&Value::Int64(200))); // Value 200 has no overlapping row IDs

    // Check intersection for value 100
    // The intersection bitset represents rows where both tables have the same row ID set
    // For value 100: left has rows 0,1; right has row 0 -> intersection has row 0
    let inter_100 = intersections.get(&Value::Int64(100)).unwrap();
    assert!(inter_100.any()); // Should have matches
    assert!(inter_100.get(0)); // Row 0 is in both indexes
    assert!(!inter_100.get(1)); // Row 1 is only in left index
    
    // Verify the left bitset has the expected rows
    let left_bs_100 = left_index.get_bitset(&Value::Int64(100)).unwrap();
    assert!(left_bs_100.get(0)); // Row 0 from left has value 100
    assert!(left_bs_100.get(1)); // Row 1 from left has value 100
    assert_eq!(left_bs_100.popcount(), 2); // Two rows in left table with value 100
    
    // Verify the right bitset has the expected rows
    let right_bs_100 = right_index.get_bitset(&Value::Int64(100)).unwrap();
    assert!(right_bs_100.get(0)); // Row 0 from right has value 100
    assert_eq!(right_bs_100.popcount(), 1); // One row in right table with value 100
    
    // Verify value 200 is not in intersections (no overlapping row IDs)
    // But we can still verify the individual bitsets are correct
    let left_bs_200 = left_index.get_bitset(&Value::Int64(200)).unwrap();
    assert!(left_bs_200.get(2)); // Row 2 from left has value 200
    assert_eq!(left_bs_200.popcount(), 1); // One row in left table with value 200
    
    let right_bs_200 = right_index.get_bitset(&Value::Int64(200)).unwrap();
    assert!(right_bs_200.get(1)); // Row 1 from right has value 200
    assert_eq!(right_bs_200.popcount(), 1); // One row in right table with value 200
}

#[test]
fn test_bitset_intersection() {
    let mut bs1 = Bitset::new();
    let mut bs2 = Bitset::new();

    // bs1: bits 0, 1, 2 set
    bs1.set(0);
    bs1.set(1);
    bs1.set(2);

    // bs2: bits 1, 2, 3 set
    bs2.set(1);
    bs2.set(2);
    bs2.set(3);

    // Intersection should have bits 1, 2
    let inter = bs1.intersect(&bs2);
    assert!(inter.get(1));
    assert!(inter.get(2));
    assert!(!inter.get(0));
    assert!(!inter.get(3));
    assert_eq!(inter.popcount(), 2);
}

#[test]
fn test_bitset_iteration() {
    let mut bs = Bitset::new();
    bs.set(0);
    bs.set(5);
    bs.set(100);

    let bits: Vec<usize> = bs.iter_set_bits().collect();
    assert_eq!(bits, vec![0, 5, 100]);
}

#[test]
fn test_multiway_bitmap_join() {
    // Create 3 tables for star schema join
    let mut index1 = BitmapIndex::new("fact".to_string(), "dim1_id".to_string());
    let mut index2 = BitmapIndex::new("fact".to_string(), "dim2_id".to_string());
    let mut index3 = BitmapIndex::new("fact".to_string(), "dim3_id".to_string());

    // Fact table: 3 rows
    // Row 0: dim1_id=10, dim2_id=20, dim3_id=30
    // Row 1: dim1_id=10, dim2_id=21, dim3_id=30
    // Row 2: dim1_id=11, dim2_id=20, dim3_id=31

    index1.add_value(Value::Int64(10), 0);
    index1.add_value(Value::Int64(10), 1);
    index1.add_value(Value::Int64(11), 2);

    index2.add_value(Value::Int64(20), 0);
    index2.add_value(Value::Int64(21), 1);
    index2.add_value(Value::Int64(20), 2);

    index3.add_value(Value::Int64(30), 0);
    index3.add_value(Value::Int64(30), 1);
    index3.add_value(Value::Int64(31), 2);

    // Check that all indexes are built correctly
    assert_eq!(index1.distinct_count(), 2);
    assert_eq!(index2.distinct_count(), 2);
    assert_eq!(index3.distinct_count(), 2);

    // Verify bitsets
    let bs1_10 = index1.get_bitset(&Value::Int64(10)).unwrap();
    assert_eq!(bs1_10.popcount(), 2); // Rows 0 and 1

    let bs2_20 = index2.get_bitset(&Value::Int64(20)).unwrap();
    assert_eq!(bs2_20.popcount(), 2); // Rows 0 and 2
}

#[test]
fn test_bitmap_index_selectivity() {
    let mut index = BitmapIndex::new("table".to_string(), "status".to_string());
    
    // 100 rows total
    for i in 0..100 {
        if i < 10 {
            index.add_value(Value::String("active".to_string()), i);
        } else {
            index.add_value(Value::String("inactive".to_string()), i);
        }
    }

    // Selectivity for "active" should be 0.1 (10/100)
    let selectivity = index.estimate_selectivity(&Value::String("active".to_string()));
    assert!((selectivity - 0.1).abs() < 0.01);

    // Selectivity for "inactive" should be 0.9 (90/100)
    let selectivity = index.estimate_selectivity(&Value::String("inactive".to_string()));
    assert!((selectivity - 0.9).abs() < 0.01);
}

#[test]
fn test_bitmap_index_should_use() {
    let mut index = BitmapIndex::new("table".to_string(), "id".to_string());
    
    // Add 1000 distinct values
    for i in 0..1000 {
        index.add_value(Value::Int64(i as i64), i);
    }

    // Should use for joins (domain < 1M)
    assert!(index.should_use_for_join(1_000_000));
    
    // Should not use if threshold is too low
    assert!(!index.should_use_for_join(500));
}

