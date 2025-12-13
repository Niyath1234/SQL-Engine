//! Structured Plan Generator - LLM produces structured plans (not raw SQL)

use crate::llm::ollama_client::OllamaClient;
use crate::worldstate::MetadataPack;
use serde::{Deserialize, Serialize, Deserializer};
use serde::de;
use anyhow::Result;

/// Structured analytical plan (output from LLM)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StructuredPlan {
    /// Restatement of user intent
    pub intent_interpretation: String,
    
    /// Datasets to query
    pub datasets: Vec<DatasetInfo>,
    
    /// Join relationships
    pub joins: Vec<JoinInfo>,
    
    /// Aggregations
    pub aggregations: Vec<AggregationInfo>,
    
    /// Group by columns
    pub group_by: Vec<String>,
    
    /// Projections (output columns)
    pub projections: Vec<String>,
    
    /// Order by (optional)
    pub order_by: Option<Vec<OrderByInfo>>,
    
    /// Output schema
    pub output_schema: Vec<ColumnSchema>,
    
    /// Estimated cost
    pub estimated_cost: f64,
    
    /// Warnings
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatasetInfo {
    pub table: String,
    pub filters: Vec<String>, // SQL filter expressions
    pub time_constraints: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinInfo {
    pub left_table: String,
    #[serde(deserialize_with = "deserialize_join_key")]
    pub left_key: String,
    pub right_table: String,
    #[serde(deserialize_with = "deserialize_join_key")]
    pub right_key: String,
    pub join_type: String, // "inner", "left", etc.
    pub justification: String,
}

/// Custom deserializer for join keys - handles both strings and arrays (takes first element)
fn deserialize_join_key<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Visitor};
    use std::fmt;

    struct JoinKeyVisitor;

    impl<'de> Visitor<'de> for JoinKeyVisitor {
        type Value = String;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string or array of strings (will use first element)")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value.to_string())
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            if let Some(first) = seq.next_element::<String>()? {
                // Warn but don't fail - take first element
                eprintln!("WARNING: LLM returned array for join key, using first element: {:?}", first);
                Ok(first)
            } else {
                Err(de::Error::invalid_length(0, &self))
            }
        }
    }

    deserializer.deserialize_any(JoinKeyVisitor)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregationInfo {
    pub expr: String, // SQL aggregate expression
    pub alias: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderByInfo {
    pub column: String,
    pub direction: String, // "ASC", "DESC"
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub column: String,
    pub r#type: String,
}

/// Plan Generator - Converts user intent to structured plan using LLM
/// 
/// **DEPRECATED**: This is replaced by `IntentSpecGenerator` + `IntentCompiler` pipeline.
/// Kept only for backward compatibility with old test files.
/// New code should use `IntentSpecGenerator` instead.
#[deprecated(note = "Use IntentSpecGenerator + IntentCompiler instead")]
pub struct PlanGenerator {
    ollama: OllamaClient,
}

impl PlanGenerator {
    pub fn new(ollama_url: Option<String>, model: Option<String>) -> Self {
        Self {
            ollama: OllamaClient::new(ollama_url, model),
        }
    }
    
    /// Generate structured plan from user intent
    pub async fn generate_plan(
        &self,
        user_intent: &str,
        metadata_pack: &MetadataPack,
        policies: &crate::worldstate::policies::QueryPolicy,
    ) -> Result<StructuredPlan> {
        // Build prompt for LLM
        let prompt = self.build_prompt(user_intent, metadata_pack, policies);
        
        // Generate structured plan
        let plan: StructuredPlan = self.ollama.generate_json(&prompt).await?;
        
        Ok(plan)
    }
    
    /// Build prompt for LLM
    fn build_prompt(
        &self,
        user_intent: &str,
        metadata_pack: &MetadataPack,
        policies: &crate::worldstate::policies::QueryPolicy,
    ) -> String {
        // Example MUST be valid JSON and MUST match real columns to avoid training the LLM into mistakes.
        let schema_example = r#"{
  "intent_interpretation": "Total sales by customer (sum of orders.total_amount grouped by customer)",
  "datasets": [
    {
      "table": "orders",
      "filters": [],
      "time_constraints": null
    }
  ],
  "joins": [
    {
      "left_table": "orders",
      "left_key": "customer_id",
      "right_table": "customers",
      "right_key": "id",
      "join_type": "inner",
      "justification": "Using approved join rule: orders.customer_id = customers.id"
    }
  ],
  "aggregations": [
    {
      "expr": "SUM(orders.total_amount)",
      "alias": "total_sales"
    }
  ],
  "group_by": ["customers.name"],
  "projections": ["customers.name", "total_sales"],
  "order_by": [
    {
      "column": "total_sales",
      "direction": "DESC"
    }
  ],
  "output_schema": [
    { "column": "name", "type": "string" },
    { "column": "total_sales", "type": "float" }
  ],
  "estimated_cost": 200.0,
  "warnings": []
}"#;
        
        // Build column reference guide (execution-truth)
        let column_guide = metadata_pack.tables.iter()
            .map(|t| {
                let cols: Vec<String> = t.columns.iter()
                    .map(|c| format!("  - {} ({})", c.name, c.data_type))
                    .collect();
                format!("Table '{}':\n{}", t.name, cols.join("\n"))
            })
            .collect::<Vec<_>>()
            .join("\n\n");

        let common_pitfalls = r#"COMMON PITFALLS (DO NOT DO THESE):
- orders primary key is orders.id (NOT orders.order_id)
- products primary key is products.id (NOT products.product_id)
- orders table does NOT have a name column (use customers.name)
"#;
        
        // Format join rules more clearly for LLM
        let join_rules_formatted = if metadata_pack.join_rules.is_empty() {
            "No approved join rules available.".to_string()
        } else {
            metadata_pack.join_rules.iter()
                .map(|rule| {
                    format!(
                        "  - {} JOIN {} ON {}.{} = {}.{} (type: {}, cardinality: {})",
                        rule.left_table,
                        rule.right_table,
                        rule.left_table,
                        rule.left_key.first().unwrap_or(&"".to_string()),
                        rule.right_table,
                        rule.right_key.first().unwrap_or(&"".to_string()),
                        rule.join_type,
                        rule.cardinality
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        };
        
        // Format hypergraph structure for LLM (graph traversal guidance)
        let hypergraph_structure = if metadata_pack.hypergraph_edges.is_empty() {
            "No hypergraph edges available.".to_string()
        } else {
            // Group edges by source table for easier navigation
            use std::collections::HashMap;
            let mut edges_by_table: HashMap<String, Vec<String>> = HashMap::new();
            for edge in &metadata_pack.hypergraph_edges {
                edges_by_table
                    .entry(edge.from_table.clone())
                    .or_insert_with(Vec::new)
                    .push(format!("  → {} (via {})", edge.to_table, edge.via_column));
            }
            
            let mut result = String::from("HYPERGRAPH STRUCTURE (think in terms of graph traversal):\n");
            for (table, edges) in edges_by_table {
                result.push_str(&format!("  {} can reach:\n", table));
                for edge in edges {
                    result.push_str(&format!("{}\n", edge));
                }
            }
            result.push_str("\n💡 STRATEGY: Start at the table with your data (e.g., orders for total_amount), follow the shortest path to tables you need (e.g., customers for name). Only join tables you actually reference in projections/group_by/filters.\n");
            result
        };
        
        format!(r#"You are a data analyst assistant. Given user intent and metadata, produce a structured analytical plan.

USER INTENT: {}
MAX ROWS: {}
MAX TIME (ms): {}

AVAILABLE TABLES AND COLUMNS:
{}

{}

HYPERGRAPH STRUCTURE:
{}
Think of this as a graph: start at the table with your data, follow edges to reach other tables you need. Only traverse the minimal path needed.

APPROVED JOIN RULES (USE THESE EXACTLY - DO NOT CHANGE THE KEYS):
{}
⚠️ CRITICAL: Use the EXACT left_key and right_key from the join rules above. Do NOT use different columns like 'id' when the rule says 'category_id'.

STATISTICS:
{}

CRITICAL RULES:
1. Use ONLY tables and columns listed above - DO NOT invent any
2. Use ONLY approved join rules - DO NOT create new joins
   - Find the matching join rule in the APPROVED JOIN RULES section above
   - Use the EXACT left_key and right_key from that rule
   - Example: If rule says "products JOIN categories ON products.category_id = categories.id", 
     then use left_key: "category_id" and right_key: "id" (NOT "id" and "id")
3. **HYPERGRAPH TRAVERSAL**: Think in terms of graph traversal - follow the shortest path
   - Start at the table that has your main data (e.g., orders for total_amount)
   - Follow hypergraph edges to reach tables you need (e.g., orders → customers for name)
   - ONLY traverse to tables you actually reference in: projections, group_by, filters, or order_by
   - Example: "total sales by customer" needs orders (total_amount) and customers (name)
     → Start at orders, follow edge to customers, DONE. No need for order_items/products/categories.
   - DO NOT join tables just because they exist - only follow edges you actually need
   - Use the HYPERGRAPH STRUCTURE section above to see which tables are directly connected
4. **FILTERS**: Only add filters if the user EXPLICITLY requests them in their question
   - If user says "cancelled orders" or "active products", then add filters
   - If user just asks "total sales by customer", use empty filters [] - DO NOT add status != 'cancelled' or other assumptions
   - Only use business/product rule filters if they are explicitly mentioned in the metadata pack
   - For filters: Prefer fully-qualified names when multiple tables are involved
5. For projections/group_by/order_by:
   - If the query touches multiple tables, use fully-qualified names: "table.column"
   - Aggregation aliases (like "total_sales") are allowed in projections and order_by
6. For aggregations: Use SQL expressions like "SUM(price)" or "COUNT(*)"
7. For group_by: Use column names WITHOUT table prefixes
8. For order_by: Use column names WITHOUT table prefixes
9. If no filters needed, use empty array [] NOT null
10. If no aggregations needed, use empty array [] NOT null
11. If no group_by needed, use empty array [] NOT null
12. If no order_by needed, use null (not empty array)
13. If no warnings, use empty array [] NOT null
14. **IMPORTANT**: If your query needs data from multiple tables, ALL tables must be connected via JOINs in the 'joins' array

REQUIRED JSON SCHEMA:
{}

FIELD REQUIREMENTS:
- intent_interpretation: REQUIRED string (restate user intent)
- datasets: REQUIRED array (can be empty [])
  - table: REQUIRED string (must match table name from metadata)
  - filters: REQUIRED array of strings (use [] if none, NOT null)
  - time_constraints: optional string or null
- joins: REQUIRED array (can be empty [])
  - left_table, right_table: REQUIRED strings
  - left_key: REQUIRED string (single column name like "customer_id", NOT an array like ["customer_id"])
  - right_key: REQUIRED string (single column name like "id", NOT an array like ["id"])
  - join_type: REQUIRED string ("inner" or "left")
  - justification: REQUIRED string
  - ⚠️ CRITICAL: left_key and right_key must be STRINGS, never arrays!
- aggregations: REQUIRED array (use [] if none, NOT null)
  - expr: REQUIRED string (SQL expression like "SUM(price)")
  - alias: REQUIRED string
- group_by: REQUIRED array of strings (use [] if none, NOT null)
- projections: REQUIRED array of strings (column names WITHOUT table prefixes)
- order_by: optional array or null (use null if none)
  - column: REQUIRED string (column name WITHOUT table prefix)
  - direction: REQUIRED string ("ASC" or "DESC")
- output_schema: REQUIRED array
  - column: REQUIRED string (column name WITHOUT table prefix)
  - type: REQUIRED string (e.g., "integer", "string", "float", "date")
- estimated_cost: REQUIRED number (float)
- warnings: REQUIRED array of strings (use [] if none, NOT null)

OUTPUT FORMAT:
- Return ONLY valid JSON
- Start with {{ and end with }}
- NO markdown code blocks
- NO explanations outside JSON
- Use [] for empty arrays, null only for optional fields
- All required fields must be present

Generate the plan as JSON:
"#,
            user_intent,
            policies.max_rows.unwrap_or(10000),
            policies.max_time_ms.unwrap_or(30000),
            column_guide,
            common_pitfalls,
            hypergraph_structure,
            join_rules_formatted,
            serde_json::to_string_pretty(&metadata_pack.stats).unwrap_or_default(),
            schema_example,
        )
    }
}

impl Default for PlanGenerator {
    fn default() -> Self {
        Self::new(None, None)
    }
}

