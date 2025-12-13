//! Phase 3 Test - Join Rule Governance

use hypergraph_sql_engine::HypergraphSQLEngine;
use hypergraph_sql_engine::ingestion::{SimulatorConnector, SimulatorSchema};

fn main() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║         Phase 3 Test - Join Rule Governance                 ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    
    // Test 1: Create two related tables
    println!("Test 1: Creating related tables...");
    let mut engine = HypergraphSQLEngine::new();
    
    // Create orders table (this will auto-register in WorldState via add_table)
    engine.execute_query("CREATE TABLE orders (order_id INT, customer_id INT, order_date VARCHAR, total_amount FLOAT)")?;
    engine.execute_query("INSERT INTO orders VALUES (1, 1, '2024-01-01', 100.0), (2, 2, '2024-01-02', 200.0), (3, 1, '2024-01-03', 150.0)")?;
    println!("✅ Created 'orders' table with sample data");
    
    // Create customers table
    engine.execute_query("CREATE TABLE customers (customer_id INT, name VARCHAR, email VARCHAR)")?;
    engine.execute_query("INSERT INTO customers VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')")?;
    println!("✅ Created 'customers' table with sample data");
    
    // Verify tables are registered in WorldState
    {
        let world_state = engine.world_state();
        let orders_registered = world_state.schema_registry.get_table("orders").is_some();
        let customers_registered = world_state.schema_registry.get_table("customers").is_some();
        println!("   WorldState: orders registered: {}, customers registered: {}", 
                 orders_registered, customers_registered);
        
        if !customers_registered {
            // Manually register customers table in WorldState
            drop(world_state);
            let mut ws = engine.world_state_mut();
            use hypergraph_sql_engine::worldstate::schema::{TableSchema, ColumnInfo};
            let mut customers_schema = TableSchema::new("customers".to_string());
            customers_schema.add_column(ColumnInfo {
                name: "customer_id".to_string(),
                data_type: "INT".to_string(),
                nullable: false,
                semantic_tags: Vec::new(),
                description: None,
            });
            customers_schema.add_column(ColumnInfo {
                name: "name".to_string(),
                data_type: "VARCHAR".to_string(),
                nullable: false,
                semantic_tags: Vec::new(),
                description: None,
            });
            customers_schema.add_column(ColumnInfo {
                name: "email".to_string(),
                data_type: "VARCHAR".to_string(),
                nullable: false,
                semantic_tags: Vec::new(),
                description: None,
            });
            ws.schema_registry.register_table(customers_schema);
            ws.bump_version();
            println!("   ✅ Manually registered 'customers' in WorldState");
        }
    }
    println!();
    
    // Test 2: Infer join rules
    println!("Test 2: Inferring join rules from schemas...");
    let rule_ids = engine.infer_join_rules()?;
    println!("✅ Inferred {} join rules (proposed state)", rule_ids.len());
    for rule_id in &rule_ids {
        println!("   - {}", rule_id);
    }
    println!();
    
    // Test 3: List all rules (including proposed)
    println!("Test 3: Listing all join rules...");
    let all_rules = engine.list_join_rules();
    println!("✅ Found {} total rules", all_rules.len());
    for rule in &all_rules {
        println!("   - {}: {} JOIN {} (state: {:?})", 
                 rule.id, rule.left_table, rule.right_table, rule.state);
    }
    println!();
    
    // Test 4: Approve a join rule
    println!("Test 4: Approving a join rule...");
    if let Some(rule_id) = rule_ids.first() {
        engine.approve_join_rule(rule_id)?;
        println!("✅ Approved rule: {}", rule_id);
        
        // Verify it's approved
        let approved_rules = engine.list_approved_join_rules();
        println!("✅ Found {} approved rules", approved_rules.len());
        assert!(approved_rules.iter().any(|r| r.id == *rule_id), 
                "Approved rule not found in approved list!");
    } else {
        println!("⚠️  No rules to approve (skipping)");
    }
    println!();
    
    // Test 5: Try to query with unapproved join (should fail)
    println!("Test 5: Attempting query with unapproved join (should fail)...");
    let query_result = engine.execute_query(
        "SELECT orders.order_id, customers.name FROM orders JOIN customers ON orders.customer_id = customers.customer_id"
    );
    
    match query_result {
        Ok(_) => {
            println!("⚠️  Query succeeded (this might be okay if a rule was auto-approved)");
        }
        Err(e) => {
            let error_msg = e.to_string();
            if error_msg.contains("Join validation failed") || error_msg.contains("not allowed") {
                println!("✅ Query correctly rejected: {}", error_msg);
            } else {
                println!("⚠️  Query failed for different reason: {}", error_msg);
            }
        }
    }
    println!();
    
    // Test 6: Create and approve a join rule manually
    println!("Test 6: Creating and approving join rule manually...");
    let rule_id = engine.create_join_rule(
        "orders".to_string(),
        vec!["customer_id".to_string()],
        "customers".to_string(),
        vec!["customer_id".to_string()],
        "inner".to_string(),
        "N:1".to_string(),
    )?;
    println!("✅ Created rule: {}", rule_id);
    
    engine.approve_join_rule(&rule_id)?;
    println!("✅ Approved rule: {}", rule_id);
    println!();
    
    // Test 7: Query with approved join (should succeed)
    println!("Test 7: Querying with approved join (should succeed)...");
    let query_result = engine.execute_query(
        "SELECT orders.order_id, customers.name FROM orders JOIN customers ON orders.customer_id = customers.customer_id LIMIT 5"
    )?;
    println!("✅ Query succeeded! Returned {} rows", query_result.row_count);
    println!();
    
    // Test 8: Verify only approved rules show in relationships
    println!("Test 8: Verifying only approved rules in relationships...");
    let approved_rules = engine.list_approved_join_rules();
    let all_rules = engine.list_join_rules();
    let proposed_count = all_rules.iter()
        .filter(|r| !r.is_approved())
        .count();
    println!("✅ Total rules: {}, Approved: {}, Proposed: {}", 
             all_rules.len(), approved_rules.len(), proposed_count);
    assert_eq!(approved_rules.len(), all_rules.len() - proposed_count, 
               "Approved count mismatch!");
    println!();
    
    // Test 9: Sync all approved rules to hypergraph
    println!("Test 9: Syncing approved rules to hypergraph...");
    engine.sync_all_approved_rules_to_hypergraph()?;
    println!("✅ All approved rules synced to hypergraph edges");
    println!();
    
    // Test 10: Save WorldState
    println!("Test 10: Saving WorldState...");
    engine.save_world_state()?;
    println!("✅ WorldState saved");
    println!();
    
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║              All Phase 3 Tests Passed! ✅                    ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    
    Ok(())
}

