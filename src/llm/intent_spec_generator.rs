/// IntentSpecGenerator: LLM generates schema-agnostic IntentSpec (bullets only)
/// 
/// This replaces PlanGenerator for the new pipeline. The LLM outputs generic
/// "what to compute" bullets, NOT table/column/join details.
use anyhow::{Context, Result};
use crate::llm::ollama_client::OllamaClient;
use crate::llm::intent_spec::IntentSpec;
use crate::worldstate::MetadataPack;

pub struct IntentSpecGenerator {
    client: OllamaClient,
}

impl IntentSpecGenerator {
    pub fn new(ollama_url: Option<String>, model: Option<String>) -> Self {
        Self {
            client: OllamaClient::new(ollama_url, model),
        }
    }

    pub fn get_ollama_client(&self) -> Option<&OllamaClient> {
        Some(&self.client)
    }

    /// Generate IntentSpec from natural language intent
    pub async fn generate_intent_spec(
        &self,
        user_intent: &str,
        metadata_pack: &MetadataPack,
    ) -> Result<IntentSpec> {
        let prompt = self.build_prompt(user_intent, metadata_pack);
        
        eprintln!("DEBUG IntentSpecGenerator: Sending prompt to LLM...");
        let response = self.client.generate(&prompt, false).await
            .context("Failed to call Ollama API")?;
        
        eprintln!("DEBUG IntentSpecGenerator: LLM response length: {} chars", response.len());
        eprintln!("DEBUG IntentSpecGenerator: LLM response (first 500 chars): {}", 
            response.chars().take(500).collect::<String>());
        
        // Parse JSON from response (strip markdown code blocks if present)
        let cleaned = self.clean_json_response(&response);
        eprintln!("DEBUG IntentSpecGenerator: Cleaned response (first 500 chars): {}", 
            cleaned.chars().take(500).collect::<String>());
        
        let intent_spec: IntentSpec = serde_json::from_str(&cleaned)
            .with_context(|| format!("Failed to parse IntentSpec JSON from LLM response. Cleaned response: {}", 
                cleaned.chars().take(1000).collect::<String>()))?;
        
        // Validate no schema references
        let schema_errors = intent_spec.validate_no_schema_references();
        if !schema_errors.is_empty() {
            eprintln!("WARNING IntentSpecGenerator: Schema references found in IntentSpec:");
            for err in &schema_errors {
                eprintln!("  - {}", err);
            }
            // For MVP: log warning but continue (compiler will reject in strict mode)
        }
        
        eprintln!("DEBUG IntentSpecGenerator: ✅ Generated IntentSpec - task: {}, metrics: {}, grain: {}, confidence: {:.2}",
            intent_spec.task,
            intent_spec.metrics.len(),
            intent_spec.grain.len(),
            intent_spec.confidence);
        
        Ok(intent_spec)
    }

    fn build_prompt(&self, user_intent: &str, metadata_pack: &MetadataPack) -> String {
        // Extract entity hints from hypergraph (table names → entity labels)
        let entity_hints: Vec<String> = metadata_pack.tables.iter()
            .map(|t| format!("  - {}", t.name))
            .collect();
        
        format!(r#"You are a natural language parser. Your SOLE task is to parse the user's query and extract ALL information into a structured JSON format.

**YOUR ONLY JOB: Parse and Extract**
- Extract column name hints (what the user is referring to, e.g., "price", "status", "name")
- Extract table name hints (if user explicitly mentions a table, capture it in "table_hints")
- Extract filtering conditions (operations like ">", "=", "in", and their values)
- Extract aggregations and math operations (sum, avg, count, min, max, etc.)
- Extract any functions used (distinct, count, etc.)
- Extract grouping/dimension information
- Extract sorting, limits, time windows

**CRITICAL RULES:**
1. Capture EVERYTHING the user mentions - be comprehensive
2. If user explicitly mentions a table name (e.g., "from products", "in orders table"), add it to "table_hints"
3. Use the EXACT column/field names the user mentions (e.g., if user says "price", use "price" as field_hint)
4. Use semantic hints (e.g., "money", "count", "date") to help the deterministic compiler
5. If you're unsure about something, add it to "ambiguities" array
6. Output ONLY valid JSON (no markdown, no explanations)

**User Intent:**
{}

**Available Entities (from hypergraph):**
{}

**IMPORTANT: Use the EXACT table names listed above. If you see "products", use "products" (plural), not "product" (singular). Match the entity name to the table name exactly.**

**Allowed Operations:**
  - sum: Sum numeric values
  - count: Count rows
  - avg: Average numeric values
  - min: Minimum value
  - max: Maximum value
  - distinct_count: Count distinct values

**Allowed Filter Operations:**
  - eq: Equals
  - ne: Not equals
  - gt: Greater than
  - gte: Greater than or equal
  - lt: Less than
  - lte: Less than or equal
  - in: In list
  - like: Pattern match
  - between: Between two values

**Business Rules (Default Filters):**
These filters are automatically applied to queries unless you explicitly mention a different value:
{}

**Output Format (JSON):**
{{
  "task": "aggregate|list|count|exists|compare",
  "metrics": [
    {{
      "name": "metric_name",
      "op": "sum|count|avg|min|max|distinct_count",
      "semantic": "money|count|percentage|date|text",
      "expression_hint": "optional field hint (e.g., 'amount', 'price')"
    }}
  ],
  "grain": [
    {{
      "entity": "customer|product|day|category",
      "attribute_hint": "optional (e.g., 'name', 'id')"
    }}
  ],
  "filters": [
    {{
      "field_hint": "status|date|amount",
      "op": "eq|ne|gt|gte|lt|lte|in|like|between",
      "value": <JSON value>,
      "semantic": "status|date|money|text"
    }}
  ],
  "table_hints": ["table_name"] | null (if user explicitly mentions table names, e.g., "from products", "in orders table"),
  "time": {{
    "type": "relative|absolute",
    "value": "last_7_days|last_month|last_year|this_month|this_year|ISO8601_range"
  }},
  "sort": [
    {{
      "by": "metric_name or entity",
      "dir": "asc|desc"
    }}
  ],
  "limit": 100,
  "ambiguities": [
    {{
      "what": "what was ambiguous",
      "interpretations": ["option1", "option2"],
      "reason": "why it's ambiguous"
    }}
  ],
  "confidence": 0.0-1.0
}}

**Examples:**

Example 1: "What are the total sales by customer?"
{{
  "task": "aggregate",
  "metrics": [
    {{
      "name": "total_sales",
      "op": "sum",
      "semantic": "money",
      "expression_hint": "amount"
    }}
  ],
  "grain": [
    {{
      "entity": "customer",
      "attribute_hint": "name"
    }}
  ],
  "filters": [],
  "table_hints": null,
  "time": null,
  "sort": [{{"by": "total_sales", "dir": "desc"}}],
  "limit": 100,
  "ambiguities": [],
  "confidence": 0.95
}}

Example 2: "List all customers"
{{
  "task": "list",
  "metrics": [],
  "grain": [
    {{
      "entity": "customer",
      "attribute_hint": "name"
    }}
  ],
  "filters": [],
  "table_hints": null,
  "time": null,
  "sort": [],
  "limit": 100,
  "ambiguities": [],
  "confidence": 0.9
}}

**CRITICAL: You MUST output COMPLETE, VALID JSON. The JSON must be properly closed with all braces and brackets. Do not truncate the response.**

**Now generate the IntentSpec for the user intent above. Output ONLY the JSON, no markdown, no explanations. Make sure the JSON is complete and valid.**
"#,
            user_intent,
            entity_hints.join("\n"),
            self.format_business_rules(metadata_pack)
        )
    }

    fn clean_json_response(&self, response: &str) -> String {
        let mut cleaned = response.trim().to_string();
        
        // Remove markdown code blocks (handle various formats)
        if cleaned.starts_with("```json") {
            cleaned = cleaned.strip_prefix("```json").unwrap_or(&cleaned).to_string();
        } else if cleaned.starts_with("```") {
            cleaned = cleaned.strip_prefix("```").unwrap_or(&cleaned).to_string();
        }
        
        if cleaned.ends_with("```") {
            cleaned = cleaned.strip_suffix("```").unwrap_or(&cleaned).to_string();
        }
        
        cleaned = cleaned.trim().to_string();
        
        // Try to extract JSON if it's embedded in text
        // Look for first { and last }
        if let Some(start) = cleaned.find('{') {
            if let Some(end) = cleaned.rfind('}') {
                if end > start {
                    cleaned = cleaned[start..=end].to_string();
                }
            } else {
                // No closing brace found - try to repair incomplete JSON
                cleaned = self.repair_incomplete_json(&cleaned[start..]);
            }
        }
        
        // Remove any leading/trailing whitespace or newlines
        cleaned.trim().to_string()
    }
    
    fn repair_incomplete_json(&self, json: &str) -> String {
        let mut repaired = json.trim().to_string();
        
        // If it doesn't start with {, try to find the JSON object
        if !repaired.starts_with('{') {
            if let Some(start) = repaired.find('{') {
                repaired = repaired[start..].to_string();
            }
        }
        
        // Remove trailing commas and whitespace
        repaired = repaired.trim_end().trim_end_matches(',').to_string();
        
        // Check if this looks like an IntentSpec (has "task", "metrics", "grain")
        let is_intent_spec = repaired.contains("\"task\"") || repaired.contains("task");
        
        // Count opening and closing braces/brackets
        let open_braces = repaired.matches('{').count();
        let close_braces = repaired.matches('}').count();
        let open_brackets = repaired.matches('[').count();
        let close_brackets = repaired.matches(']').count();
        
        // Handle incomplete values at the end
        let trimmed = repaired.trim_end();
        if trimmed.ends_with(':') {
            // Incomplete key-value pair, add null
            repaired = trimmed.to_string();
            repaired.push_str(" null");
        } else if trimmed.ends_with("\"") && !trimmed.ends_with("\\\"") {
            // String value is complete
        } else if !trimmed.ends_with(']') && !trimmed.ends_with('}') && !trimmed.ends_with('"') 
            && !trimmed.ends_with("null") && !trimmed.ends_with("true") && !trimmed.ends_with("false")
            && !trimmed.chars().last().map(|c| c.is_ascii_digit()).unwrap_or(false) {
            // Looks incomplete, might need to close a string or add a value
            if trimmed.ends_with("attribute_hint") || trimmed.contains("attribute_hint:") {
                // Complete the attribute_hint field
                if !trimmed.ends_with("null") {
                    repaired.push_str(" null");
                }
            }
        }
        
        // Remove trailing comma again after value completion
        repaired = repaired.trim_end().trim_end_matches(',').to_string();
        
        // Add missing closing brackets first (inner structures like arrays)
        for _ in 0..(open_brackets - close_brackets) {
            repaired.push_str("\n  ]");
        }
        
        // If this is an IntentSpec and we're missing required fields, add them
        if is_intent_spec && open_braces > close_braces {
            // Check what fields are missing
            let has_filters = repaired.contains("\"filters\"") || repaired.contains("filters");
            let has_table_hints = repaired.contains("\"table_hints\"") || repaired.contains("table_hints");
            let has_time = repaired.contains("\"time\"") || repaired.contains("time");
            let has_sort = repaired.contains("\"sort\"") || repaired.contains("sort");
            let has_limit = repaired.contains("\"limit\"") || repaired.contains("limit");
            let has_ambiguities = repaired.contains("\"ambiguities\"") || repaired.contains("ambiguities");
            let has_confidence = repaired.contains("\"confidence\"") || repaired.contains("confidence");
            
            // Check if we're in the middle of a filters array
            let in_filters_array = repaired.contains("\"filters\"") && !repaired.contains("\"filters\": []") && !repaired.contains("\"filters\":[");
            if in_filters_array {
                // Complete the current filter object if incomplete
                let trimmed = repaired.trim_end();
                // Check if we're missing closing braces for filter object
                let filter_braces_open = repaired.matches('{').count() - repaired.matches('}').count();
                let needs_completion = trimmed.ends_with(',') || trimmed.ends_with(':') || 
                    (!trimmed.ends_with('}') && !trimmed.ends_with(']') && !trimmed.ends_with('"') && 
                     !trimmed.ends_with("null") && !trimmed.ends_with("true") && !trimmed.ends_with("false") &&
                     !trimmed.chars().last().map(|c| c.is_ascii_digit()).unwrap_or(false)) ||
                    (filter_braces_open > 1); // More than root object brace
                
                if needs_completion {
                    // Check what field we're in
                    if trimmed.ends_with("field_hint") || (trimmed.contains("\"field_hint\"") && !trimmed.contains("\"field_hint\":")) {
                        // Complete field_hint - try to infer from context
                        if repaired.contains("status") {
                            repaired.push_str("\"status\"");
                        } else if repaired.contains("price") || repaired.contains("cost") {
                            repaired.push_str("\"price\"");
                        } else if repaired.contains("amount") {
                            repaired.push_str("\"amount\"");
                        } else {
                            repaired.push_str("\"\"");
                        }
                    } else if trimmed.ends_with("op") || (trimmed.contains("\"op\"") && !trimmed.contains("\"op\":")) {
                        // Complete op field
                        if repaired.contains("eq") || repaired.contains("=") {
                            repaired.push_str("\"eq\"");
                        } else if repaired.contains("gt") || repaired.contains(">") {
                            repaired.push_str("\"gt\"");
                        } else {
                            repaired.push_str("\"eq\"");
                        }
                    } else if trimmed.ends_with("semantic") || (trimmed.contains("\"semantic\"") && !trimmed.contains("\"semantic\":")) {
                        // Complete semantic field
                        if !trimmed.ends_with('"') && !trimmed.ends_with("null") {
                            if repaired.contains("status") {
                                repaired.push_str("\"status\"");
                            } else if repaired.contains("price") || repaired.contains("cost") || repaired.contains("amount") {
                                repaired.push_str("\"money\"");
                            } else {
                                repaired.push_str("\"text\"");
                            }
                        }
                    } else if trimmed.ends_with("value") || (trimmed.contains("\"value\"") && !trimmed.contains("\"value\":")) {
                        // Complete value field
                        if !trimmed.ends_with('"') && !trimmed.ends_with("null") && !trimmed.chars().last().map(|c| c.is_ascii_digit()).unwrap_or(false) {
                            // Try to infer value from context
                            if repaired.contains("completed") {
                                repaired.push_str("\"completed\"");
                            } else if repaired.contains("pending") {
                                repaired.push_str("\"pending\"");
                            } else if repaired.contains("100") || repaired.contains("> 100") {
                                // Numeric value
                                if !trimmed.ends_with("100") {
                                    repaired.push_str("100");
                                }
                            } else {
                                repaired.push_str("\"\"");
                            }
                        }
                    } else {
                        // Unknown field, try to close it
                        if trimmed.ends_with(':') {
                            repaired.push_str(" null");
                        }
                    }
                    
                    // Close the filter object if needed - ensure all filter objects are closed
                    let current_open = repaired.matches('{').count();
                    let current_close = repaired.matches('}').count();
                    if current_open > current_close {
                        // Count how many filter objects we have (each has "op" field)
                        let filter_objects = repaired.matches("\"op\"").count();
                        let closed_objects = repaired.matches("}").count() - 1; // -1 for root object
                        // Close all incomplete filter objects
                        for _ in 0..(filter_objects - closed_objects) {
                            repaired.push_str("\n    }");
                        }
                    }
                } else {
                    // Even if needs_completion is false, check if filter object needs closing
                    let filter_objects = repaired.matches("\"op\"").count();
                    let closed_objects = repaired.matches("}").count() - 1; // -1 for root object
                    if filter_objects > closed_objects {
                        for _ in 0..(filter_objects - closed_objects) {
                            repaired.push_str("\n    }");
                        }
                    }
                }
                
                // Close filters array if needed
                if open_brackets > close_brackets {
                    repaired.push_str("\n  ]");
                }
            }
            
            // Add missing required fields before closing
            if !repaired.trim_end().ends_with('}') && !repaired.trim_end().ends_with(']') && !repaired.trim_end().ends_with(',') {
                repaired.push_str(",");
            }
            
            if !has_filters && !in_filters_array {
                repaired.push_str("\n  \"filters\": []");
            }
            if !has_table_hints {
                repaired.push_str(",\n  \"table_hints\": null");
            }
            if !has_time {
                repaired.push_str(",\n  \"time\": null");
            }
            if !has_sort {
                repaired.push_str(",\n  \"sort\": []");
            }
            if !has_limit {
                repaired.push_str(",\n  \"limit\": 100");
            }
            if !has_ambiguities {
                repaired.push_str(",\n  \"ambiguities\": []");
            }
            if !has_confidence {
                repaired.push_str(",\n  \"confidence\": 0.9");
            }
        }
        
        // Add missing closing braces
        for _ in 0..(open_braces - close_braces) {
            repaired.push_str("\n}");
        }
        
        repaired
    }
    
    fn format_business_rules(&self, metadata_pack: &MetadataPack) -> String {
        if metadata_pack.filter_rules.is_empty() {
            return "  (No business rules defined)".to_string();
        }
        
        let mut rules_text = Vec::new();
        for rule in &metadata_pack.filter_rules {
            let value_str = match rule.value {
                serde_json::Value::String(ref s) => format!("'{}'", s),
                serde_json::Value::Number(ref n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                _ => rule.value.to_string(),
            };
            rules_text.push(format!(
                "  - {} table: {} {} {} (mandatory: {})",
                rule.table_name, rule.column, rule.operator, value_str, rule.mandatory
            ));
        }
        rules_text.join("\n")
    }
}

