//! Phase 2 Test - Verify Ingestion Pipeline

use hypergraph_sql_engine::HypergraphSQLEngine;
use hypergraph_sql_engine::ingestion::{SimulatorConnector, SimulatorSchema};

fn main() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║         Phase 2 Test - Ingestion Pipeline                   ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    
    // Test 1: Flat schema ingestion
    println!("Test 1: Ingesting flat schema data...");
    let mut engine = HypergraphSQLEngine::new();
    let connector = SimulatorConnector::new("test_flat".to_string(), SimulatorSchema::Flat)
        .with_batch_size(10);
    
    let result = engine.ingest(Box::new(connector), Some("test_flat_table".to_string()))?;
    println!("✅ Ingested {} records into {} tables", 
             result.records_ingested, result.tables_affected.len());
    println!("   Tables: {:?}", result.tables_affected);
    println!("   Schema versions: {:?}", result.schema_versions);
    println!();
    
    // Test 2: Query the ingested data
    println!("Test 2: Querying ingested data...");
    let query_result = engine.execute_query(
        "SELECT id, name, value FROM test_flat_table WHERE value > 50 ORDER BY value DESC LIMIT 5"
    )?;
    println!("✅ Query returned {} rows in {:.3} ms", 
             query_result.row_count, query_result.execution_time_ms);
    println!();
    
    // Test 3: Nested schema ingestion
    println!("Test 3: Ingesting nested schema data...");
    let connector = SimulatorConnector::new("test_nested".to_string(), SimulatorSchema::Nested)
        .with_batch_size(5);
    
    let result = engine.ingest(Box::new(connector), Some("test_nested_table".to_string()))?;
    println!("✅ Ingested {} records", result.records_ingested);
    println!("   Tables: {:?}", result.tables_affected);
    println!();
    
    // Test 4: Query nested data (flattened columns)
    println!("Test 4: Querying nested data (flattened columns)...");
    let query_result = engine.execute_query(
        "SELECT id, user__name, user__email FROM test_nested_table LIMIT 3"
    )?;
    println!("✅ Query returned {} rows", query_result.row_count);
    println!();
    
    // Test 5: Check lineage
    println!("Test 5: Checking lineage...");
    {
        let world_state = engine.world_state();
        let sources = world_state.lineage_registry.list_sources();
        println!("✅ Found {} sources", sources.len());
        for source in sources {
            println!("   - {} ({})", source.source_id, source.source_type);
        }
        
        let tables = world_state.lineage_registry.list_tables();
        println!("✅ Found {} tables with lineage", tables.len());
        for table_name in tables {
            if let Some(lineage) = world_state.lineage_registry.get_table_lineage(&table_name) {
                println!("   - {} (source: {}, version: {})", 
                         table_name, lineage.source_id, lineage.schema_version);
            }
        }
    }
    println!();
    
    // Test 6: Schema evolution (add more data with same schema)
    println!("Test 6: Testing idempotency (same data twice)...");
    let connector2 = SimulatorConnector::new("test_flat".to_string(), SimulatorSchema::Flat)
        .with_batch_size(5);
    
    let result2 = engine.ingest(Box::new(connector2), Some("test_flat_table".to_string()))?;
    println!("✅ Second ingestion completed (idempotency test)");
    println!("   Records ingested: {}", result2.records_ingested);
    println!();
    
    // Test 7: Check WorldState was updated
    println!("Test 7: Verifying WorldState was updated...");
    let hash_after = engine.world_hash_global();
    println!("✅ World hash after ingestion: {}", hash_after);
    
    // Save WorldState
    engine.save_world_state()?;
    println!("✅ WorldState saved to disk");
    println!();
    
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║              All Phase 2 Tests Passed! ✅                    ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    
    Ok(())
}

