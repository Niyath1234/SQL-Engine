/// Phase 6: Integration Tests for Bitset Engine V3
/// 
/// Tests end-to-end query execution with bitset joins

use hypergraph_sql_engine::storage::bitset_v3::Bitset;
use hypergraph_sql_engine::storage::bitset_hierarchy::HierarchicalBitset;
use hypergraph_sql_engine::storage::range_bitmap_index::RangeBitmapIndex;
use hypergraph_sql_engine::storage::fragment::Value;
use hypergraph_sql_engine::execution::bitset_join_v3::BitsetJoinOperatorV3;
use hypergraph_sql_engine::execution::bitset_filter_v3::BitsetFilterExecutor;
use hypergraph_sql_engine::execution::operators::bitset_filter::BitsetFilterExecutor as FilterExecutor;
use hypergraph_sql_engine::query::bitset_cost_model::BitsetCostModel;
use hypergraph_sql_engine::hypergraph::graph::HyperGraph;
use hypergraph_sql_engine::hypergraph::node::NodeId;
use std::sync::Arc;
use anyhow::Result;

fn main() -> Result<()> {
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║              Phase 6: Bitset Engine V3 Integration Tests                    ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    println!();
    
    // Test 1: Bitset Operations
    println!("🧪 Test 1: Bitset Operations");
    test_bitset_operations()?;
    println!("  ✅ Passed\n");
    
    // Test 2: Hierarchical Skipping
    println!("🧪 Test 2: Hierarchical Skipping");
    test_hierarchical_skipping()?;
    println!("  ✅ Passed\n");
    
    // Test 3: Range Queries
    println!("🧪 Test 3: Range Queries");
    test_range_queries()?;
    println!("  ✅ Passed\n");
    
    // Test 4: Filter Conversion
    println!("🧪 Test 4: Filter Conversion");
    test_filter_conversion()?;
    println!("  ✅ Passed\n");
    
    // Test 5: Cost Model
    println!("🧪 Test 5: Cost Model");
    test_cost_model()?;
    println!("  ✅ Passed\n");
    
    println!("✅ All integration tests passed!");
    Ok(())
}

fn test_bitset_operations() -> Result<()> {
    let mut bitset1 = Bitset::new(1000);
    bitset1.set(10);
    bitset1.set(20);
    bitset1.set(30);
    
    let mut bitset2 = Bitset::new(1000);
    bitset2.set(20);
    bitset2.set(30);
    bitset2.set(40);
    
    let intersection = bitset1.intersect(&bitset2);
    assert_eq!(intersection.cardinality(), 2);
    assert!(intersection.get(20));
    assert!(intersection.get(30));
    
    let union = bitset1.union(&bitset2);
    assert_eq!(union.cardinality(), 4);
    
    Ok(())
}

fn test_hierarchical_skipping() -> Result<()> {
    let mut hierarchy = HierarchicalBitset::new(1_000_000);
    
    // Set sparse rows
    hierarchy.set(100);
    hierarchy.set(5000);
    hierarchy.set(500_000);
    
    let matching = hierarchy.get_matching_rows();
    assert_eq!(matching.len(), 3);
    assert!(matching.contains(&100));
    assert!(matching.contains(&5000));
    assert!(matching.contains(&500_000));
    
    Ok(())
}

fn test_range_queries() -> Result<()> {
    let values: Vec<Value> = (0..1000)
        .map(|i| Value::Int64(i))
        .collect();
    
    let index = RangeBitmapIndex::build_from_column("col1".to_string(), &values)?;
    
    let result = index.query_range(&Value::Int64(100), &Value::Int64(200));
    assert_eq!(result.cardinality(), 101); // 100 to 200 inclusive
    
    Ok(())
}

fn test_filter_conversion() -> Result<()> {
    use hypergraph_sql_engine::query::plan::PredicateOperator;
    
    // Test equality filter
    let bitset = FilterExecutor::convert_filter_to_bitset(
        "col1",
        &PredicateOperator::Equals,
        &Value::Int64(42),
        None,
        None,
        1000,
    )?;
    
    assert_eq!(bitset.capacity(), 1000);
    
    Ok(())
}

fn test_cost_model() -> Result<()> {
    // Low cardinality, low selectivity → should use bitset join
    let should_use = BitsetCostModel::should_use_bitset_join(
        1000,      // distinct_count
        0.1,       // filter_selectivity
        1_000_000, // fact_table_size
        10_000,    // dimension_size
        0.05,      // bitmap_density
        false,     // is_star_schema
    );
    
    assert!(should_use, "Should use bitset join for low cardinality");
    
    // High cardinality → should use hash join
    let should_not_use = BitsetCostModel::should_use_bitset_join(
        2_000_000, // distinct_count (high)
        0.1,
        1_000_000,
        10_000,
        0.05,
        false,
    );
    
    assert!(!should_not_use, "Should not use bitset join for high cardinality");
    
    Ok(())
}

