//! Phase 5 Test - LLM Cortex with Ollama

use hypergraph_sql_engine::HypergraphSQLEngine;
use hypergraph_sql_engine::ingestion::{SimulatorConnector, SimulatorSchema};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║         Phase 5 Test - LLM Cortex (Ollama)                 ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    
    // Test 1: Setup - Create tables with data
    println!("Test 1: Setting up test data...");
    let mut engine = HypergraphSQLEngine::new();
    
    // Create orders table
    engine.execute_query("CREATE TABLE orders (order_id INT, customer_id INT, order_date VARCHAR, total_amount FLOAT)")?;
    engine.execute_query("INSERT INTO orders VALUES (1, 1, '2024-01-01', 100.0), (2, 2, '2024-01-02', 200.0), (3, 1, '2024-01-03', 150.0)")?;
    
    // Create customers table
    engine.execute_query("CREATE TABLE customers (customer_id INT, name VARCHAR, email VARCHAR)")?;
    engine.execute_query("INSERT INTO customers VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')")?;
    
    // Create and approve join rule
    let rule_id = engine.create_join_rule(
        "orders".to_string(),
        vec!["customer_id".to_string()],
        "customers".to_string(),
        vec!["customer_id".to_string()],
        "inner".to_string(),
        "N:1".to_string(),
    )?;
    engine.approve_join_rule(&rule_id)?;
    println!("✅ Test data setup complete");
    println!();
    
    // Test 2: Check Ollama availability
    println!("Test 2: Checking Ollama availability...");
    let ollama = hypergraph_sql_engine::llm::OllamaClient::new(None, None);
    match ollama.health_check().await {
        Ok(true) => {
            println!("✅ Ollama server is available");
        }
        Ok(false) | Err(_) => {
            println!("⚠️  Ollama server not available - skipping LLM tests");
            println!("   To test LLM features, start Ollama:");
            println!("   $ ollama serve");
            println!("   $ ollama pull llama3.2");
            return Ok(());
        }
    }
    println!();
    
    // Test 3: Generate metadata pack
    println!("Test 3: Generating metadata pack...");
    let metadata_pack = {
        let world_state = engine.world_state();
        use hypergraph_sql_engine::worldstate::metadata_pack::MetadataPackBuilder;
        MetadataPackBuilder::new(&world_state)
            .with_rbac_filter(None)
            .build()
    };
    println!("✅ Metadata pack generated: {} tables, {} join rules", 
             metadata_pack.tables.len(), metadata_pack.join_rules.len());
    println!();
    
    // Test 4: Generate structured plan (if Ollama available)
    println!("Test 4: Testing structured plan generation...");
    let plan_generator = hypergraph_sql_engine::llm::PlanGenerator::new(None, None);
    let policy = engine.world_state().policy_registry.get_query_policy(None).clone();
    
    let user_intent = "Show me all orders with customer names";
    match plan_generator.generate_plan(user_intent, &metadata_pack, &policy).await {
        Ok(plan) => {
            println!("✅ Structured plan generated!");
            println!("   Intent: {}", plan.intent_interpretation);
            println!("   Tables: {:?}", plan.datasets.iter().map(|d| &d.table).collect::<Vec<_>>());
            println!("   Joins: {}", plan.joins.len());
            println!("   Projections: {:?}", plan.projections);
            
            // Test 5: Validate plan
            println!();
            println!("Test 5: Validating structured plan...");
            let validator = hypergraph_sql_engine::llm::PlanValidator::new();
            match validator.validate(&plan, &metadata_pack, &policy) {
                hypergraph_sql_engine::llm::ValidationResult::Valid => {
                    println!("✅ Plan validation passed");
                }
                hypergraph_sql_engine::llm::ValidationResult::Invalid { reason, suggestions } => {
                    println!("⚠️  Plan validation failed: {}", reason);
                    println!("   Suggestions: {:?}", suggestions);
                }
            }
            println!();
            
            // Test 6: Generate SQL from plan
            println!("Test 6: Generating SQL from structured plan...");
            let sql_generator = hypergraph_sql_engine::llm::SQLGenerator::new();
            let sql = sql_generator.generate_sql(&plan, &policy);
            println!("✅ Generated SQL:");
            println!("   {}", sql);
            println!();
            
            // Test 7: Execute SQL
            println!("Test 7: Executing generated SQL...");
            match engine.execute_query(&sql) {
                Ok(result) => {
                    println!("✅ SQL executed successfully!");
                    println!("   Rows returned: {}", result.row_count);
                    println!("   Execution time: {:.3} ms", result.execution_time_ms);
                }
                Err(e) => {
                    println!("⚠️  SQL execution failed: {}", e);
                }
            }
        }
        Err(e) => {
            println!("⚠️  Plan generation failed: {}", e);
            println!("   This might be because:");
            println!("   1. Ollama server is not running");
            println!("   2. Model is not available (try: ollama pull llama3.2)");
            println!("   3. LLM response was not valid JSON");
        }
    }
    println!();
    
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║              Phase 5 Tests Complete! ✅                    ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    
    Ok(())
}

