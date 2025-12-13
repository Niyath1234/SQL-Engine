//! Full Pipeline Test - Simulates real-world scenario:
//! 1. Product teams share dummy data structures
//! 2. API responses are fetched (simulated)
//! 3. Tables are created from API data
//! 4. Business rules (join rules) are established
//! 5. LLM cortex processes natural language queries

use hypergraph_sql_engine::HypergraphSQLEngine;
use hypergraph_sql_engine::worldstate::QueryPolicy;
use hypergraph_sql_engine::ingestion::{SimulatorConnector, SimulatorSchema};
use hypergraph_sql_engine::llm::{PlanGenerator, SQLGenerator, PlanValidator as LLMPlanValidator, AuditLog};
use std::sync::Arc;
use tokio;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     Full Pipeline Test: Product Team → API → Tables → LLM  ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // Clear any existing state for clean testing
    println!("🧹 Clearing previous state for clean test run...");
    let _ = std::fs::remove_file(".da_cursor_worldstate.json");
    println!("✅ Cleared WorldState cache\n");

    // Initialize engine
    let mut engine = HypergraphSQLEngine::new();
    
    println!("Step 1: Product teams share data structures...");
    println!("  📋 Products API: id, name, price, category_id, created_at");
    println!("  📋 Categories API: id, name, description");
    println!("  📋 Orders API: id, customer_id, order_date, status, total_amount");
    println!("  📋 Customers API: id, name, email, created_at");
    println!("  📋 OrderItems API: order_id, product_id, quantity, price\n");

    // Step 2: Simulate API responses and create tables
    println!("Step 2: Fetching API responses and creating tables...\n");
    
    // Products API - Create with correct schema
    println!("  🔄 Fetching Products API...");
    engine.execute_query("CREATE TABLE products (id INT, name VARCHAR, price FLOAT, category_id INT, created_at VARCHAR)")?;
    for i in 1..=100 {
        engine.execute_query(&format!(
            "INSERT INTO products VALUES ({}, 'Product_{}', {}, {}, '2024-01-{:02}')",
            i, i, (i as f64) * 10.5, (i % 10) + 1, (i % 28) + 1
        ))?;
    }
    println!("  ✅ Created 'products' table (100 records)\n");

    // Categories API - Create with correct schema
    println!("  🔄 Fetching Categories API...");
    engine.execute_query("CREATE TABLE categories (id INT, name VARCHAR, description VARCHAR)")?;
    for i in 1..=10 {
        engine.execute_query(&format!(
            "INSERT INTO categories VALUES ({}, 'Category_{}', 'Description for category {}')",
            i, i, i
        ))?;
    }
    println!("  ✅ Created 'categories' table (10 records)\n");

    // Customers API - Create with correct schema
    println!("  🔄 Fetching Customers API...");
    engine.execute_query("CREATE TABLE customers (id INT, name VARCHAR, email VARCHAR, created_at VARCHAR)")?;
    for i in 1..=100 {
        engine.execute_query(&format!(
            "INSERT INTO customers VALUES ({}, 'Customer_{}', 'customer{}@example.com', '2024-01-{:02}')",
            i, i, i, (i % 28) + 1
        ))?;
    }
    println!("  ✅ Created 'customers' table (100 records)\n");

    // Orders API - Create with correct schema
    println!("  🔄 Fetching Orders API...");
    engine.execute_query("CREATE TABLE orders (id INT, customer_id INT, order_date VARCHAR, status VARCHAR, total_amount FLOAT)")?;
    for i in 1..=100 {
        engine.execute_query(&format!(
            "INSERT INTO orders VALUES ({}, {}, '2024-01-{:02}', 'completed', {})",
            i, (i % 100) + 1, (i % 28) + 1, (i as f64) * 25.99
        ))?;
    }
    println!("  ✅ Created 'orders' table (100 records)\n");

    // OrderItems API - Create with correct schema
    println!("  🔄 Fetching OrderItems API...");
    engine.execute_query("CREATE TABLE order_items (order_id INT, product_id INT, quantity INT, price FLOAT)")?;
    for i in 1..=300 {
        engine.execute_query(&format!(
            "INSERT INTO order_items VALUES ({}, {}, {}, {})",
            (i % 100) + 1, (i % 100) + 1, (i % 5) + 1, (i as f64) * 10.5
        ))?;
    }
    println!("  ✅ Created 'order_items' table (300 records)\n");

    // Step 3: Product teams establish business rules (join rules)
    println!("Step 3: Product teams establish business rules...\n");
    
    // Rule 1: products.category_id → categories.id
    println!("  📐 Rule 1: products.category_id → categories.id (N:1)");
    let rule_id1 = engine.create_join_rule(
        "products".to_string(),
        vec!["category_id".to_string()],
        "categories".to_string(),
        vec!["id".to_string()],
        "inner".to_string(),
        "Product belongs to a category".to_string(),
    )?;
    engine.approve_join_rule(&rule_id1)?;
    println!("  ✅ Approved\n");

    // Rule 2: orders.customer_id → customers.id
    println!("  📐 Rule 2: orders.customer_id → customers.id (N:1)");
    let rule_id2 = engine.create_join_rule(
        "orders".to_string(),
        vec!["customer_id".to_string()],
        "customers".to_string(),
        vec!["id".to_string()],
        "inner".to_string(),
        "Order belongs to a customer".to_string(),
    )?;
    engine.approve_join_rule(&rule_id2)?;
    println!("  ✅ Approved\n");

    // Rule 3: order_items.order_id → orders.id
    println!("  📐 Rule 3: order_items.order_id → orders.id (N:1)");
    let rule_id3 = engine.create_join_rule(
        "order_items".to_string(),
        vec!["order_id".to_string()],
        "orders".to_string(),
        vec!["id".to_string()],
        "inner".to_string(),
        "Order item belongs to an order".to_string(),
    )?;
    engine.approve_join_rule(&rule_id3)?;
    println!("  ✅ Approved\n");

    // Rule 4: order_items.product_id → products.id
    println!("  📐 Rule 4: order_items.product_id → products.id (N:1)");
    let rule_id4 = engine.create_join_rule(
        "order_items".to_string(),
        vec!["product_id".to_string()],
        "products".to_string(),
        vec!["id".to_string()],
        "inner".to_string(),
        "Order item references a product".to_string(),
    )?;
    engine.approve_join_rule(&rule_id4)?;
    println!("  ✅ Approved\n");

    // Sync all approved rules to hypergraph
    engine.sync_all_approved_rules_to_hypergraph()?;
    println!("  ✅ All rules synced to hypergraph\n");

    // Step 4: Test LLM Cortex with natural language queries
    println!("Step 4: Testing LLM Cortex with natural language queries...\n");

    let plan_generator = PlanGenerator::default();
    let sql_generator = SQLGenerator::default();
    let plan_validator = LLMPlanValidator::default();
    let audit_log = AuditLog::new(1000);

    // Test Query 1: Simple product listing
    println!("  🔍 Query 1: 'Show me all products with their categories'");
    let (metadata_pack, policy) = {
        let metadata_pack = engine.export_metadata_pack(None);
        let policy = {
            let world_state = engine.world_state();
            world_state.policy_registry.get_query_policy(None).clone()
        };
        (metadata_pack, policy)
    };
    
    match plan_generator.generate_plan(
        "Show me all products with their categories",
        &metadata_pack,
        &policy,
    ).await {
        Ok(plan) => {
            println!("  ✅ Plan generated");
            match plan_validator.validate(&plan, &metadata_pack, &policy) {
                hypergraph_sql_engine::llm::ValidationResult::Valid => {
                    println!("  ✅ Plan validated");
                    let sql = sql_generator.generate_sql(&plan, &policy);
                    println!("  📝 Generated SQL: {}", sql);
                    
                    // Execute query
                    match engine.execute_query(&sql) {
                        Ok(result) => {
                            println!("  ✅ Query executed successfully");
                            println!("  📊 Rows returned: {}", result.row_count);
                            let entry_id = audit_log.log(
                                "Show me all products with their categories".to_string(),
                                plan.clone(),
                                sql,
                                engine.world_hash_global(),
                                0, // query_hash - simplified
                            );
                            audit_log.update_execution_stats(
                                &entry_id,
                                result.row_count as u64,
                                result.execution_time_ms as f64,
                                false,
                            );
                        }
                        Err(e) => {
                            println!("  ⚠️  Query execution failed: {}", e);
                        }
                    }
                }
                hypergraph_sql_engine::llm::ValidationResult::Invalid { reason, suggestions } => {
                    println!("  ⚠️  Plan validation failed: {}", reason);
                    if !suggestions.is_empty() {
                        println!("     Suggestions: {:?}", suggestions);
                    }
                }
            }
        }
        Err(e) => {
            println!("  ⚠️  Plan generation failed: {}", e);
        }
    }
    println!();

    // Test Query 2: Aggregation query
    println!("  🔍 Query 2: 'What are the total sales by category?'");
    let (metadata_pack, policy) = {
        let metadata_pack = engine.export_metadata_pack(None);
        let policy = {
            let world_state = engine.world_state();
            world_state.policy_registry.get_query_policy(None).clone()
        };
        (metadata_pack, policy)
    };
    
    match plan_generator.generate_plan(
        "What are the total sales by category?",
        &metadata_pack,
        &policy,
    ).await {
        Ok(plan) => {
            println!("  ✅ Plan generated");
            match plan_validator.validate(&plan, &metadata_pack, &policy) {
                hypergraph_sql_engine::llm::ValidationResult::Valid => {
                    println!("  ✅ Plan validated");
                    let sql = sql_generator.generate_sql(&plan, &policy);
                    println!("  📝 Generated SQL: {}", sql);
                    
                    // Execute query
                    match engine.execute_query(&sql) {
                        Ok(result) => {
                            println!("  ✅ Query executed successfully");
                            println!("  📊 Rows returned: {}", result.row_count);
                            audit_log.log(
                                "What are the total sales by category?".to_string(),
                                plan.clone(),
                                sql,
                                engine.world_hash_global(),
                                0,
                            );
                        }
                        Err(e) => {
                            println!("  ⚠️  Query execution failed: {}", e);
                        }
                    }
                }
                hypergraph_sql_engine::llm::ValidationResult::Invalid { reason, suggestions } => {
                    println!("  ⚠️  Plan validation failed: {}", reason);
                    if !suggestions.is_empty() {
                        println!("     Suggestions: {:?}", suggestions);
                    }
                }
            }
        }
        Err(e) => {
            println!("  ⚠️  Plan generation failed: {}", e);
        }
    }
    println!();

    // Test Query 3: Multi-table join
    println!("  🔍 Query 3: 'Show me customer names with their order totals'");
    let (metadata_pack, policy) = {
        let metadata_pack = engine.export_metadata_pack(None);
        let policy = {
            let world_state = engine.world_state();
            world_state.policy_registry.get_query_policy(None).clone()
        };
        (metadata_pack, policy)
    };
    
    match plan_generator.generate_plan(
        "Show me customer names with their order totals",
        &metadata_pack,
        &policy,
    ).await {
        Ok(plan) => {
            println!("  ✅ Plan generated");
            match plan_validator.validate(&plan, &metadata_pack, &policy) {
                hypergraph_sql_engine::llm::ValidationResult::Valid => {
                    println!("  ✅ Plan validated");
                    let sql = sql_generator.generate_sql(&plan, &policy);
                    println!("  📝 Generated SQL: {}", sql);
                    
                    // Execute query
                    match engine.execute_query(&sql) {
                        Ok(result) => {
                            println!("  ✅ Query executed successfully");
                            println!("  📊 Rows returned: {}", result.row_count);
                            audit_log.log(
                                "Show me customer names with their order totals".to_string(),
                                plan.clone(),
                                sql,
                                engine.world_hash_global(),
                                0,
                            );
                        }
                        Err(e) => {
                            println!("  ⚠️  Query execution failed: {}", e);
                        }
                    }
                }
                hypergraph_sql_engine::llm::ValidationResult::Invalid { reason, suggestions } => {
                    println!("  ⚠️  Plan validation failed: {}", reason);
                    if !suggestions.is_empty() {
                        println!("     Suggestions: {:?}", suggestions);
                    }
                }
            }
        }
        Err(e) => {
            println!("  ⚠️  Plan generation failed: {}", e);
        }
    }
    println!();

    // Step 5: Show audit log
    println!("Step 5: Audit Log Summary...\n");
    let entries = audit_log.get_entries();
    println!("  📋 Total queries processed: {}", entries.len());
    for (i, entry) in entries.iter().enumerate() {
        println!("  {}. {} - {}", i + 1, entry.timestamp, entry.user_intent);
        if let Some(stats) = &entry.execution_stats {
            println!("     ✅ Success - {} rows, {:.2}ms", 
                stats.row_count,
                stats.execution_time_ms);
        } else {
            println!("     ⏳ Not executed");
        }
    }

    // Clear caches and state after test
    println!("\n🧹 Cleaning up test state...");
    // Note: Cache clearing would require public access to internal fields
    // For now, just clear the WorldState file
    let _ = std::fs::remove_file(".da_cursor_worldstate.json");
    println!("✅ Cleared WorldState cache\n");

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║              Full Pipeline Test Complete! ✅                ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    Ok(())
}

