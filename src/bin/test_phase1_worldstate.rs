//! Phase 1 Test - Verify WorldState integration

use hypergraph_sql_engine::HypergraphSQLEngine;

fn main() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║         Phase 1 Test - WorldState Integration               ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    
    // Test 1: Create engine and check WorldState loads
    println!("Test 1: Creating engine and loading WorldState...");
    let mut engine = HypergraphSQLEngine::new();
    {
        let world_state = engine.world_state();
        println!("✅ WorldState loaded - Version: {}, Last Updated: {}", 
                 world_state.version, world_state.last_updated);
    }
    println!();
    
    // Test 2: Get world hash
    println!("Test 2: Computing world hash...");
    let hash = engine.world_hash_global();
    println!("✅ Global world hash: {}", hash);
    println!();
    
    // Test 3: Export metadata pack
    println!("Test 3: Exporting metadata pack...");
    let pack = engine.export_metadata_pack(None);
    println!("✅ Metadata pack exported:");
    println!("   - Tables: {}", pack.tables.len());
    println!("   - Join rules: {}", pack.join_rules.len());
    println!("   - Stats: {}", pack.stats.len());
    println!("   - World hash: {}", pack.world_hash);
    println!();
    
    // Test 4: Modify WorldState and check hash changes
    println!("Test 4: Modifying WorldState and checking hash change...");
    let hash_before = engine.world_hash_global();
    
    {
        let mut ws = engine.world_state_mut();
        ws.bump_version();
    }
    
    let hash_after = engine.world_hash_global();
    println!("✅ Hash before: {}", hash_before);
    println!("✅ Hash after: {}", hash_after);
    assert_ne!(hash_before, hash_after, "Hash should change after version bump");
    println!();
    
    // Test 5: Save WorldState
    println!("Test 5: Saving WorldState to disk...");
    engine.save_world_state()?;
    println!("✅ WorldState saved to .da_cursor_worldstate.json");
    println!();
    
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║              All Phase 1 Tests Passed! ✅                    ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    
    Ok(())
}

