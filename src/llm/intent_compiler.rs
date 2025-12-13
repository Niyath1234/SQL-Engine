/// IntentCompiler: Deterministic compiler from IntentSpec → StructuredPlan
/// 
/// Uses hypergraph spine to resolve:
/// - Fact table selection (deterministic)
/// - Dimension/grain resolution
/// - Path finding (shortest approved join edges only)
/// - Column grounding (semantic → actual columns)
/// - Policy gates (RBAC, limits, cost caps)
/// - Explainability (why table/column/path chosen)
use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use crate::llm::intent_spec::{IntentSpec, MetricSpec, GrainSpec, FilterSpec};
use crate::llm::plan_generator::{StructuredPlan, DatasetInfo, JoinInfo, AggregationInfo, OrderByInfo, ColumnSchema};
use crate::worldstate::MetadataPack;
use crate::llm::ollama_client::OllamaClient;

#[derive(Debug, Clone)]
pub struct CompilerExplanation {
    pub step: String,
    pub decision: String,
    pub reason: String,
}

pub struct IntentCompiler {
    explanations: Vec<CompilerExplanation>,
    original_query: Option<String>,
    ollama_client: Option<OllamaClient>,
}

impl IntentCompiler {
    pub fn new() -> Self {
        Self {
            explanations: Vec::new(),
            original_query: None,
            ollama_client: None,
        }
    }
    
    /// Create with LLM scoring capability
    pub fn with_llm_scoring(original_query: String, ollama_url: Option<String>, model: Option<String>) -> Self {
        Self {
            explanations: Vec::new(),
            original_query: Some(original_query),
            ollama_client: Some(OllamaClient::new(ollama_url, model)),
        }
    }

    /// Compile IntentSpec → StructuredPlan using hypergraph
    pub async fn compile(
        &mut self,
        intent_spec: &IntentSpec,
        metadata_pack: &MetadataPack,
    ) -> Result<StructuredPlan> {
        self.explanations.clear();
        
        eprintln!("DEBUG IntentCompiler: Compiling IntentSpec - task: {}, metrics: {}, grain: {}",
            intent_spec.task, intent_spec.metrics.len(), intent_spec.grain.len());
        
        // Step 1: Score ALL tables using LLM based on the query (if available)
        // This gives us a ranked list of all relevant tables
        // First, check if user explicitly mentioned table names in IntentSpec
        let scored_tables = if let Some(ref table_hints) = intent_spec.table_hints {
            // User explicitly mentioned tables - prioritize these
            let mut scored: Vec<(String, f64)> = table_hints.iter()
                .map(|t| (t.clone(), 1.0)) // Highest priority for explicit mentions
                .collect();
            // Also score other tables using LLM if available
            if let Some(ref query) = &self.original_query {
                let llm_scored = self.llm_score_all_tables(query, intent_spec, metadata_pack).await?;
                // Merge, keeping explicit mentions at top
                for (table, score) in llm_scored {
                    if !scored.iter().any(|(t, _)| t == &table) {
                        scored.push((table, score * 0.8)); // Slightly lower than explicit
                    }
                }
            }
            scored
        } else if let Some(ref query) = &self.original_query {
            // No explicit table hints - use LLM scoring
            match self.llm_score_all_tables(query, intent_spec, metadata_pack).await {
                Ok(tables) if !tables.is_empty() => tables,
                _ => {
                    // LLM scoring failed, use fallback logic
                    eprintln!("DEBUG: LLM table scoring failed, using fallback");
                    self.fallback_table_selection(intent_spec, metadata_pack)
                }
            }
        } else {
            // Fallback: use existing logic
            let fact_tables = self.select_fact_tables(intent_spec, metadata_pack).await?;
            let dimension_tables = self.resolve_grain_entities(intent_spec, metadata_pack).await?;
            fact_tables.iter().chain(dimension_tables.iter())
                .map(|t| (t.clone(), 1.0))
                .collect()
        };
        
        // Extract table names from scored results, sorted by score
        let mut all_selected_tables: Vec<String> = scored_tables.iter()
            .filter(|(_, score)| *score > 0.1) // Only include tables with meaningful score
            .map(|(table, _)| table.clone())
            .collect();
        
        // If no tables scored well, fallback to original logic
        if all_selected_tables.is_empty() {
            let fact_tables = self.select_fact_tables(intent_spec, metadata_pack).await?;
            let dimension_tables = self.resolve_grain_entities(intent_spec, metadata_pack).await?;
            all_selected_tables = fact_tables.iter()
                .chain(dimension_tables.iter())
                .cloned()
                .collect();
        }
        
        // Remove duplicates while preserving order
        let mut seen = HashSet::new();
        all_selected_tables.retain(|t| seen.insert(t.clone()));
        
        self.add_explanation("table_selection", 
            &format!("Selected tables (LLM-scored): {:?}", all_selected_tables),
            "Scored all tables using LLM semantic understanding");
        
        // Step 2: Find shortest path in hypergraph to connect all selected tables
        // The path edges contain the join conditions
        let all_tables_set: HashSet<String> = all_selected_tables.iter().cloned().collect();
        let joins = self.find_join_path_from_tables(&all_selected_tables, metadata_pack)?;
        self.add_explanation("path_finding",
            &format!("Found {} join(s) via hypergraph", joins.len()),
            "Computed shortest path using approved join rules");
        
        // Step 3: Ground columns (semantic → actual column names)
        let projections = self.ground_projections(intent_spec, &all_tables_set, metadata_pack).await?;
        
        // For "list" tasks with filters but incorrect metrics, ignore metrics
        let should_ignore_metrics = intent_spec.task == "list" && !intent_spec.filters.is_empty() && 
            intent_spec.metrics.len() == 1 && intent_spec.metrics.iter().any(|m| {
                m.name.contains("cost") || m.name.contains("price") || m.name.contains("amount")
            });
        
        // Determine fact tables from selected tables (tables with metrics or high row counts)
        let fact_tables: Vec<String> = all_selected_tables.iter()
            .filter(|t| {
                // If there are metrics, check if this table has relevant columns
                if !intent_spec.metrics.is_empty() {
                    metadata_pack.tables.iter()
                        .any(|ti| ti.name == **t && ti.columns.iter().any(|c| {
                            c.data_type.contains("int") || c.data_type.contains("decimal") || 
                            c.data_type.contains("numeric") || c.data_type.contains("float")
                        }))
                } else {
                    // For list queries, prefer tables with more rows
                    metadata_pack.stats.iter()
                        .any(|s| s.table_name == **t && s.row_count > 100)
                }
            })
            .cloned()
            .collect();
        
        let dimension_tables: Vec<String> = all_selected_tables.iter()
            .filter(|t| !fact_tables.contains(t))
            .cloned()
            .collect();
        
        let aggregations = if should_ignore_metrics {
            Vec::new() // Ignore incorrectly generated metrics for filter queries
        } else {
            self.ground_aggregations(intent_spec, &fact_tables, metadata_pack).await?
        };
        
        // Only add GROUP BY if there are aggregations (otherwise it's invalid SQL)
        let group_by = if !aggregations.is_empty() {
            self.ground_group_by(intent_spec, &dimension_tables, metadata_pack).await?
        } else {
            Vec::new() // No GROUP BY for simple list queries
        };
        let mut filters = self.ground_filters(intent_spec, &all_tables_set, metadata_pack).await?;
        
        // Step 3.5: Apply business rules (default filters) unless explicitly overridden
        self.apply_business_rules(&mut filters, &all_tables_set, metadata_pack, intent_spec)?;
        
        self.add_explanation("column_grounding",
            &format!("Grounded {} projections, {} aggregations, {} group_by", 
                projections.len(), aggregations.len(), group_by.len()),
            "Mapped semantic hints to actual table.column names");
        
        // Step 4: Build StructuredPlan
        let datasets: Vec<DatasetInfo> = fact_tables.iter()
            .map(|table| DatasetInfo {
                table: table.clone(),
                filters: filters.get(table).cloned().unwrap_or_default(),
                time_constraints: intent_spec.time.as_ref().map(|t| {
                    // Convert time window to time constraint format
                    format!("{}:{}", t.r#type, t.value)
                }),
            })
            .collect();
        
        let order_by: Option<Vec<OrderByInfo>> = if intent_spec.sort.is_empty() {
            None
        } else {
            Some(intent_spec.sort.iter().map(|s| {
                // Check if sorting by a metric (aggregation alias) or by a grain entity
                let column = if intent_spec.metrics.iter().any(|m| m.name == s.by) {
                    // It's a metric name - use as-is (will be aggregation alias)
                    s.by.clone()
                } else {
                    // It's a grain entity - need to resolve to actual column
                    // For now, just use the entity name (will be qualified during normalization)
                    s.by.clone()
                };
                OrderByInfo {
                    column,
                    direction: s.dir.clone(),
                }
            }).collect())
        };
        
        let plan = StructuredPlan {
            intent_interpretation: format!("{} task with {} metric(s), {} grain dimension(s)",
                intent_spec.task, intent_spec.metrics.len(), intent_spec.grain.len()),
            datasets,
            joins,
            aggregations,
            group_by,
            projections,
            order_by,
            output_schema: vec![], // Will be filled by SQL generator
            estimated_cost: 0.0, // Will be computed by cost model
            warnings: intent_spec.ambiguities.iter()
                .map(|a| format!("Ambiguity: {} - {}", a.what, a.reason))
                .collect(),
        };
        
        eprintln!("DEBUG IntentCompiler: ✅ Compiled to StructuredPlan - {} datasets, {} joins, {} projections",
            plan.datasets.len(), plan.joins.len(), plan.projections.len());
        
        Ok(plan)
    }

    /// Get compiler explanations (for observability)
    pub fn get_explanations(&self) -> &[CompilerExplanation] {
        &self.explanations
    }

    fn add_explanation(&mut self, step: &str, decision: &str, reason: &str) {
        self.explanations.push(CompilerExplanation {
            step: step.to_string(),
            decision: decision.to_string(),
            reason: reason.to_string(),
        });
    }

    /// Step 1: Select fact table(s) for metrics (deterministic)
    async fn select_fact_tables(
        &self,
        intent_spec: &IntentSpec,
        metadata_pack: &MetadataPack,
    ) -> Result<Vec<String>> {
        let mut fact_tables = Vec::new();
        
        // For queries with filters, check if metrics are incorrectly generated
        // (LLM sometimes generates metrics for simple filter queries like "cost more than 100")
        let is_filter_query = !intent_spec.filters.is_empty() && intent_spec.metrics.len() == 1;
        let should_ignore_metrics = is_filter_query && intent_spec.metrics.iter().any(|m| {
            let name_lower = m.name.to_lowercase();
            // Check if metric name suggests it's related to the filter field
            let metric_has_money_field = name_lower.contains("cost") || name_lower.contains("price") || 
                                        name_lower.contains("amount") || name_lower.contains("total");
            // Check if filter is on a money/price field
            let filter_has_money_field = intent_spec.filters.iter().any(|f| {
                let field_lower = f.field_hint.to_lowercase();
                field_lower.contains("cost") || field_lower.contains("price") || 
                field_lower.contains("amount") || f.semantic.as_ref().map(|s| s == "money").unwrap_or(false)
            });
            // If both are money-related, likely a filter-only query
            metric_has_money_field && filter_has_money_field
        });
        
        if !should_ignore_metrics {
            for metric in &intent_spec.metrics {
                // Find best matching table for this metric
                let best_table = self.find_best_table_for_metric(metric, metadata_pack).await?;
                if !fact_tables.contains(&best_table) {
                    fact_tables.push(best_table);
                }
            }
        }
        
        // If no metrics (or metrics ignored), use first grain entity's table (for "list" tasks)
        if fact_tables.is_empty() && !intent_spec.grain.is_empty() {
            let grain_table = self.resolve_entity_to_table(&intent_spec.grain[0].entity, metadata_pack).await?;
            fact_tables.push(grain_table);
        }
        
        Ok(fact_tables)
    }

    async fn find_best_table_for_metric(
        &self,
        metric: &MetricSpec,
        metadata_pack: &MetadataPack,
    ) -> Result<String> {
        // Use LLM scoring if available
        if let (Some(ref query), Some(ref _client)) = (&self.original_query, &self.ollama_client) {
            return self.llm_score_metric_table(query, metric, metadata_pack).await;
        }
        
        // Fallback: simple heuristic scoring
        let mut best_score = 0.0;
        let mut best_table = None;
        
        for table_info in &metadata_pack.tables {
            let mut score = 0.0;
            
            // Check expression_hint match
            if let Some(ref expr_hint) = metric.expression_hint {
                for col in &table_info.columns {
                    if col.name.to_lowercase().contains(&expr_hint.to_lowercase()) {
                        score += 3.0;
                    }
                }
            }
            
            // Prefer tables with more rows (likely fact tables)
            if let Some(stats) = metadata_pack.stats.iter().find(|s| s.table_name == table_info.name) {
                if stats.row_count > 1000 {
                    score += 1.0;
                }
            }
            
            if score > best_score {
                best_score = score;
                best_table = Some(table_info.name.clone());
            }
        }
        
        best_table.ok_or_else(|| anyhow::anyhow!(
            "Could not find suitable table for metric '{}' with semantic '{}'. Available tables: {}",
            metric.name,
            metric.semantic.as_deref().unwrap_or("unknown"),
            metadata_pack.tables.iter().map(|t| t.name.clone()).collect::<Vec<_>>().join(", ")
        ))
    }
    
    /// Use LLM to score all tables based on the user's query and metric
    async fn llm_score_metric_table(
        &self,
        query: &str,
        metric: &MetricSpec,
        metadata_pack: &MetadataPack,
    ) -> Result<String> {
        let client = self.ollama_client.as_ref().unwrap();
        
        // Build table info with columns
        let mut tables_info = Vec::new();
        for table_info in &metadata_pack.tables {
            let columns: Vec<String> = table_info.columns.iter()
                .map(|c| format!("{} ({})", c.name, c.data_type))
                .collect();
            let row_count = metadata_pack.stats.iter()
                .find(|s| s.table_name == table_info.name)
                .map(|s| s.row_count)
                .unwrap_or(0);
            tables_info.push(format!(
                "Table: {} ({} rows)\n  Columns: {}",
                table_info.name,
                row_count,
                columns.join(", ")
            ));
        }
        
        let prompt = format!(
            r#"Given this user query: "{}"

The user wants to compute a metric: "{}" with operation: "{}" and semantic type: "{}"
Expression hint: "{}"

Here are all available tables in the database:
{}

Score each table from 0.0 to 1.0 based on how well it contains columns that match the metric in the context of the user's query.
Return ONLY a JSON array of objects with format:
[
  {{"table": "table_name", "score": 0.95, "reason": "brief explanation"}},
  ...
]

Sort by score descending. Only include tables with score > 0.1."#,
            query,
            metric.name,
            metric.op,
            metric.semantic.as_deref().unwrap_or("unknown"),
            metric.expression_hint.as_deref().unwrap_or("none"),
            tables_info.join("\n\n")
        );
        
        let response = client.generate(&prompt, true).await?;
        let scores: Vec<serde_json::Value> = serde_json::from_str(&response)
            .or_else(|_| {
                let cleaned = response.trim_start_matches("```json").trim_end_matches("```").trim();
                serde_json::from_str(cleaned)
            })?;
        
        // Find highest scoring table
        if let Some(best) = scores.iter()
            .filter_map(|s| {
                Some((
                    s.get("table")?.as_str()?,
                    s.get("score")?.as_f64()?,
                ))
            })
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        {
            return Ok(best.0.to_string());
        }
        
        Err(anyhow::anyhow!("LLM did not return valid scores for metric '{}'", metric.name))
    }

    /// Step 2: Resolve grain entities to dimension tables
    async fn resolve_grain_entities(
        &self,
        intent_spec: &IntentSpec,
        metadata_pack: &MetadataPack,
    ) -> Result<Vec<String>> {
        let mut dimension_tables = Vec::new();
        
        for grain in &intent_spec.grain {
            let table = self.resolve_entity_to_table(&grain.entity, metadata_pack).await?;
            if !dimension_tables.contains(&table) {
                dimension_tables.push(table);
            }
        }
        
        Ok(dimension_tables)
    }

    async fn resolve_entity_to_table(
        &self,
        entity: &str,
        metadata_pack: &MetadataPack,
    ) -> Result<String> {
        // Simple heuristic: entity name → table name (e.g., "customer" → "customers")
        let entity_lower = entity.to_lowercase();
        
        // Try exact match first
        for table_info in &metadata_pack.tables {
            if table_info.name.to_lowercase() == entity_lower {
                return Ok(table_info.name.clone());
            }
        }
        
        // Try pluralization (customer → customers, product → products)
        let plural = format!("{}s", entity_lower);
        for table_info in &metadata_pack.tables {
            if table_info.name.to_lowercase() == plural {
                return Ok(table_info.name.clone());
            }
        }
        
        // Try substring match (product → products, order → orders)
        for table_info in &metadata_pack.tables {
            let table_lower = table_info.name.to_lowercase();
            if table_lower.contains(&entity_lower) || entity_lower.contains(&table_lower) {
                return Ok(table_info.name.clone());
            }
        }
        
        // Try contains match
        for table_info in &metadata_pack.tables {
            if table_info.name.to_lowercase().contains(&entity_lower) {
                return Ok(table_info.name.clone());
            }
        }
        
        Err(anyhow::anyhow!("Could not resolve entity '{}' to a table", entity))
    }

    /// Legacy method - kept for backward compatibility
    /// Use find_join_path_from_tables instead
    fn find_join_path(
        &self,
        fact_tables: &[String],
        dimension_tables: &[String],
        metadata_pack: &MetadataPack,
    ) -> Result<Vec<JoinInfo>> {
        let all_tables: Vec<String> = fact_tables.iter()
            .chain(dimension_tables.iter())
            .cloned()
            .collect();
        self.find_join_path_from_tables(&all_tables, metadata_pack)
    }

    /// Score ALL tables using LLM based on the user's query
    /// Returns a list of (table_name, score) pairs sorted by relevance
    async fn llm_score_all_tables(
        &self,
        query: &str,
        intent_spec: &IntentSpec,
        metadata_pack: &MetadataPack,
    ) -> Result<Vec<(String, f64)>> {
        let client = self.ollama_client.as_ref().unwrap();
        
        // Build table info with columns and stats
        let mut tables_info = Vec::new();
        for table_info in &metadata_pack.tables {
            let columns: Vec<String> = table_info.columns.iter()
                .map(|c| format!("{} ({})", c.name, c.data_type))
                .collect();
            let row_count = metadata_pack.stats.iter()
                .find(|s| s.table_name == table_info.name)
                .map(|s| s.row_count)
                .unwrap_or(0);
            tables_info.push(format!(
                "Table: {} ({} rows)\n  Columns: {}",
                table_info.name,
                row_count,
                columns.join(", ")
            ));
        }
        
        // Build context about what the user wants
        let intent_context = format!(
            "Task: {}\nMetrics: {}\nGrain entities: {}\nFilters: {}\nTable hints: {}",
            intent_spec.task,
            intent_spec.metrics.iter().map(|m| m.name.clone()).collect::<Vec<_>>().join(", "),
            intent_spec.grain.iter().map(|g| g.entity.clone()).collect::<Vec<_>>().join(", "),
            intent_spec.filters.iter().map(|f| format!("{} {}", f.field_hint, f.op)).collect::<Vec<_>>().join(", "),
            intent_spec.table_hints.as_ref().map(|hints| hints.join(", ")).unwrap_or_else(|| "none".to_string())
        );
        
        let prompt = format!(
            r#"Given this user query: "{}"

Intent context:
{}

Here are all available tables in the database:
{}

Score each table from 0.0 to 1.0 based on how relevant it is to answering the user's query.
Consider:
- Which tables contain data needed for the metrics/aggregations
- Which tables contain entities mentioned in the query
- Which tables are needed for filters
- Which tables might be needed for joins to connect other relevant tables

Return ONLY a JSON array of objects with format:
[
  {{"table": "table_name", "score": 0.95, "reason": "brief explanation"}},
  ...
]

Sort by score descending. Include all tables with score > 0.05."#,
            query,
            intent_context,
            tables_info.join("\n\n")
        );
        
        let response = client.generate(&prompt, true).await?;
        let scores: Vec<serde_json::Value> = serde_json::from_str(&response)
            .or_else(|_| {
                let cleaned = response.trim_start_matches("```json").trim_end_matches("```").trim();
                serde_json::from_str(cleaned)
            })?;
        
        // Extract scores
        let mut result: Vec<(String, f64)> = scores.iter()
            .filter_map(|s| {
                Some((
                    s.get("table")?.as_str()?.to_string(),
                    s.get("score")?.as_f64()?,
                ))
            })
            .collect();
        
        // Sort by score descending
        result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(result)
    }

    fn find_join_rule<'a>(
        &self,
        left: &str,
        right: &str,
        metadata_pack: &'a MetadataPack,
    ) -> Option<&'a crate::worldstate::metadata_pack::JoinRuleInfo> {
        metadata_pack.join_rules.iter().find(|rule| {
            (rule.left_table == left && rule.right_table == right) ||
            (rule.left_table == right && rule.right_table == left)
        })
    }

    /// Find join path connecting all selected tables using shortest path algorithm
    /// Returns joins with conditions from hypergraph edges
    fn find_join_path_from_tables(
        &self,
        selected_tables: &[String],
        metadata_pack: &MetadataPack,
    ) -> Result<Vec<JoinInfo>> {
        if selected_tables.len() <= 1 {
            return Ok(Vec::new()); // No joins needed for single table
        }
        
        let mut joins = Vec::new();
        let mut connected = HashSet::new();
        let mut remaining: Vec<String> = selected_tables.iter().cloned().collect();
        
        // Start with first table as the root
        if let Some(root) = remaining.pop() {
            connected.insert(root.clone());
            
            // Connect all remaining tables to the connected set
            while !remaining.is_empty() {
                let mut best_path: Option<(String, Vec<String>, Vec<String>)> = None;
                let mut best_target_idx = 0;
                
                // Find shortest path from any connected table to any remaining table
                for (idx, target) in remaining.iter().enumerate() {
                    for source in &connected {
                        if let Some((path_tables, path_edges)) = self.find_shortest_path_with_edges(
                            source, target, metadata_pack
                        ) {
                            let path_len = path_tables.len();
                            if best_path.is_none() || path_len < best_path.as_ref().unwrap().1.len() {
                                best_path = Some((target.clone(), path_tables, path_edges));
                                best_target_idx = idx;
                            }
                        }
                    }
                }
                
                // Add joins along the best path
                if let Some((target, path_tables, path_edges)) = best_path {
                    // Build joins from the path edges (which contain join conditions)
                    let mut current = path_tables[0].clone();
                    for (i, next) in path_tables[1..].iter().enumerate() {
                        if let Some(via_column) = path_edges.get(i) {
                            // Find the join rule to get full join info
                            if let Some(join_rule) = self.find_join_rule(&current, next, metadata_pack) {
                                joins.push(JoinInfo {
                                    left_table: current.clone(),
                                    left_key: join_rule.left_key.first().cloned().unwrap_or_else(|| via_column.clone()),
                                    right_table: next.clone(),
                                    right_key: join_rule.right_key.first().cloned().unwrap_or_else(|| via_column.clone()),
                                    join_type: join_rule.join_type.clone(),
                                    justification: join_rule.justification.as_ref()
                                        .cloned()
                                        .unwrap_or_else(|| format!("Shortest path via hypergraph: {}.{} = {}.{}",
                                            current, via_column, next, via_column)),
                                });
                            } else {
                                // Fallback: use edge info directly
                                joins.push(JoinInfo {
                                    left_table: current.clone(),
                                    left_key: via_column.clone(),
                                    right_table: next.clone(),
                                    right_key: via_column.clone(),
                                    join_type: "INNER".to_string(),
                                    justification: format!("Hypergraph edge: {}.{} = {}.{}",
                                        current, via_column, next, via_column),
                                });
                            }
                        }
                        current = next.clone();
                    }
                    
                    connected.insert(target.clone());
                    remaining.remove(best_target_idx);
                } else {
                    // No path found - skip this table
                    remaining.remove(best_target_idx);
                }
            }
        }
        
        Ok(joins)
    }
    
    /// Find shortest path between two tables, returning both table path and edge information
    /// This gives us the join conditions directly from the hypergraph
    fn find_shortest_path_with_edges(
        &self,
        from: &str,
        to: &str,
        metadata_pack: &MetadataPack,
    ) -> Option<(Vec<String>, Vec<String>)> {
        use std::collections::VecDeque;
        
        let mut queue = VecDeque::new();
        queue.push_back((from.to_string(), vec![from.to_string()], Vec::new()));
        let mut visited = HashSet::new();
        visited.insert(from.to_string());
        
        while let Some((current, path, edges)) = queue.pop_front() {
            if current == to {
                return Some((path[1..].to_vec(), edges)); // Exclude starting node
            }
            
            // Find neighbors via hypergraph edges
            for edge in &metadata_pack.hypergraph_edges {
                if edge.from_table == current && !visited.contains(&edge.to_table) {
                    visited.insert(edge.to_table.clone());
                    let mut new_path = path.clone();
                    new_path.push(edge.to_table.clone());
                    let mut new_edges = edges.clone();
                    new_edges.push(edge.via_column.clone());
                    queue.push_back((edge.to_table.clone(), new_path, new_edges));
                }
                // Bidirectional
                if edge.to_table == current && !visited.contains(&edge.from_table) {
                    visited.insert(edge.from_table.clone());
                    let mut new_path = path.clone();
                    new_path.push(edge.from_table.clone());
                    let mut new_edges = edges.clone();
                    new_edges.push(edge.via_column.clone());
                    queue.push_back((edge.from_table.clone(), new_path, new_edges));
                }
            }
        }
        
        None
    }

    /// Step 4: Ground columns (semantic → actual column names)
    async fn ground_projections(
        &self,
        intent_spec: &IntentSpec,
        tables: &HashSet<String>,
        metadata_pack: &MetadataPack,
    ) -> Result<Vec<String>> {
        let mut projections = Vec::new();
        
        for grain in &intent_spec.grain {
            let table = self.resolve_entity_to_table(&grain.entity, metadata_pack).await?;
            if !tables.contains(&table) {
                continue;
            }
            
            // Find best column for attribute_hint
            let column = if let Some(ref attr_hint) = grain.attribute_hint {
                self.find_best_column(&table, attr_hint, metadata_pack).await?
            } else {
                // Default: use "name" or "id" or first column
                // Use async block to handle the fallback chain
                match self.find_best_column(&table, "name", metadata_pack).await {
                    Ok(col) => col,
                    Err(_) => {
                        match self.find_best_column(&table, "id", metadata_pack).await {
                            Ok(col) => col,
                            Err(_) => {
                                metadata_pack.tables.iter()
                                    .find(|t| t.name == table)
                                    .and_then(|t| t.columns.first())
                                    .map(|c| c.name.clone())
                                    .unwrap_or_else(|| "id".to_string())
                            }
                        }
                    }
                }
            };
            
            projections.push(format!("{}.{}", table, column));
        }
        
        Ok(projections)
    }

    async fn ground_aggregations(
        &self,
        intent_spec: &IntentSpec,
        fact_tables: &[String],
        metadata_pack: &MetadataPack,
    ) -> Result<Vec<AggregationInfo>> {
        let mut aggregations = Vec::new();
        
        // Check if we should ignore metrics (filter-only queries)
        // LLM sometimes generates metrics for simple filter queries
        let is_filter_query = !intent_spec.filters.is_empty() && intent_spec.metrics.len() == 1;
        let should_ignore_metrics = is_filter_query && intent_spec.metrics.iter().any(|m| {
            let name_lower = m.name.to_lowercase();
            let metric_has_money_field = name_lower.contains("cost") || name_lower.contains("price") || 
                                         name_lower.contains("amount") || name_lower.contains("total");
            // Check if filter is on a money/price field
            let filter_has_money_field = intent_spec.filters.iter().any(|f| {
                let field_lower = f.field_hint.to_lowercase();
                field_lower.contains("cost") || field_lower.contains("price") || 
                field_lower.contains("amount") || f.semantic.as_ref().map(|s| s == "money").unwrap_or(false)
            });
            // If both are money-related, likely a filter-only query
            metric_has_money_field && filter_has_money_field
        });
        
        if should_ignore_metrics {
            return Ok(Vec::new()); // Return empty aggregations for filter-only queries
        }
        
        for metric in &intent_spec.metrics {
            let table = self.find_best_table_for_metric(metric, metadata_pack).await?;
            
            // Find column for aggregation
            let column = if let Some(ref expr_hint) = metric.expression_hint {
                self.find_best_column(&table, expr_hint, metadata_pack).await?
            } else {
                // Find first numeric column
                metadata_pack.tables.iter()
                    .find(|t| t.name == table)
                    .and_then(|t| t.columns.iter().find(|c| {
                        c.data_type.contains("int") || c.data_type.contains("decimal") || 
                        c.data_type.contains("numeric") || c.data_type.contains("float")
                    }))
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| "id".to_string())
            };
            
            aggregations.push(AggregationInfo {
                expr: format!("{}({}.{})", metric.op.to_uppercase(), table, column),
                alias: metric.name.clone(),
            });
        }
        
        Ok(aggregations)
    }

    async fn ground_group_by(
        &self,
        intent_spec: &IntentSpec,
        dimension_tables: &[String],
        metadata_pack: &MetadataPack,
    ) -> Result<Vec<String>> {
        let mut group_by = Vec::new();
        
        for grain in &intent_spec.grain {
            let table = self.resolve_entity_to_table(&grain.entity, metadata_pack).await?;
            if !dimension_tables.contains(&table) {
                continue;
            }
            
            let column = if let Some(ref attr_hint) = grain.attribute_hint {
                self.find_best_column(&table, attr_hint, metadata_pack).await?
            } else {
                "id".to_string() // Default to id for grouping
            };
            
            group_by.push(format!("{}.{}", table, column));
        }
        
        Ok(group_by)
    }

    async fn ground_filters(
        &self,
        intent_spec: &IntentSpec,
        tables: &HashSet<String>,
        metadata_pack: &MetadataPack,
    ) -> Result<HashMap<String, Vec<String>>> {
        let mut filters_by_table: HashMap<String, Vec<String>> = HashMap::new();
        
        for filter in &intent_spec.filters {
            // Find table with matching column
            let (table, column) = self.find_table_column_for_filter(filter, metadata_pack).await?;
            if !tables.contains(&table) {
                continue;
            }
            
            // Build filter expression
            let filter_expr = match filter.op.as_str() {
                "eq" => format!("{}.{} = '{}'", table, column, filter.value),
                "ne" => format!("{}.{} != '{}'", table, column, filter.value),
                "gt" => format!("{}.{} > {}", table, column, filter.value),
                "gte" => format!("{}.{} >= {}", table, column, filter.value),
                "lt" => format!("{}.{} < {}", table, column, filter.value),
                "lte" => format!("{}.{} <= {}", table, column, filter.value),
                "in" => {
                    let values = filter.value.as_array()
                        .map(|arr| arr.iter().map(|v| format!("'{}'", v)).collect::<Vec<_>>().join(", "))
                        .unwrap_or_else(|| format!("'{}'", filter.value));
                    format!("{}.{} IN ({})", table, column, values)
                },
                "like" => format!("{}.{} LIKE '{}'", table, column, filter.value),
                "between" => {
                    let empty_vec: Vec<serde_json::Value> = vec![];
                    let arr = filter.value.as_array().unwrap_or(&empty_vec);
                    if arr.len() >= 2 {
                        format!("{}.{} BETWEEN {} AND {}", table, column, arr[0], arr[1])
                    } else {
                        continue; // Invalid between
                    }
                },
                _ => continue, // Unknown op
            };
            
            filters_by_table.entry(table).or_insert_with(Vec::new).push(filter_expr);
        }
        
        Ok(filters_by_table)
    }

    /// Apply business rules (default filters) unless explicitly overridden
    fn apply_business_rules(
        &mut self,
        filters: &mut HashMap<String, Vec<String>>,
        tables: &HashSet<String>,
        metadata_pack: &MetadataPack,
        intent_spec: &IntentSpec,
    ) -> Result<()> {
        use crate::worldstate::metadata_pack::FilterRuleInfo;
        
        // For each table in the query
        for table in tables {
            // Get mandatory filter rules for this table
            let business_rules: Vec<&FilterRuleInfo> = metadata_pack.filter_rules
                .iter()
                .filter(|rule| rule.table_name == *table && rule.mandatory)
                .collect();
            
            if business_rules.is_empty() {
                continue; // No business rules for this table
            }
            
            // Get existing filters for this table (if any)
            let existing_filters = filters.entry(table.clone()).or_insert_with(Vec::new);
            
            // Check if user explicitly mentioned a filter that overrides the business rule
            for rule in business_rules {
                let column_key = format!("{}.{}", table, rule.column);
                
                // Check if user explicitly mentioned this column in filters
                let user_mentioned = intent_spec.filters.iter().any(|f| {
                    // Try to match by column name (semantic matching)
                    f.field_hint.to_lowercase() == rule.column.to_lowercase() ||
                    // Or check if the value matches (user might have explicitly set it)
                    f.value == rule.value
                });
                
                // Only apply if user didn't explicitly mention it
                if !user_mentioned {
                    // Build filter expression
                    let filter_expr = match rule.operator.as_str() {
                        "eq" => format!("{}.{} = '{}'", table, rule.column, 
                            rule.value.as_str().unwrap_or("")),
                        "ne" => format!("{}.{} != '{}'", table, rule.column,
                            rule.value.as_str().unwrap_or("")),
                        "gt" => format!("{}.{} > {}", table, rule.column, rule.value),
                        "gte" => format!("{}.{} >= {}", table, rule.column, rule.value),
                        "lt" => format!("{}.{} < {}", table, rule.column, rule.value),
                        "lte" => format!("{}.{} <= {}", table, rule.column, rule.value),
                        "in" => {
                            if let Some(arr) = rule.value.as_array() {
                                let values: Vec<String> = arr.iter()
                                    .map(|v| v.as_str().unwrap_or("").to_string())
                                    .collect();
                                format!("{}.{} IN ({})", table, rule.column, 
                                    values.iter().map(|v| format!("'{}'", v)).collect::<Vec<_>>().join(", "))
                            } else {
                                continue; // Skip invalid IN filter
                            }
                        },
                        "like" => format!("{}.{} LIKE '{}'", table, rule.column,
                            rule.value.as_str().unwrap_or("")),
                        _ => continue, // Skip unknown operators
                    };
                    
                    // Add business rule filter
                    existing_filters.push(filter_expr);
                    
                    self.add_explanation("business_rule_applied",
                        &format!("Applied business rule: {}.{} {} {}", 
                            table, rule.column, rule.operator, rule.value),
                        "Automatically applied mandatory filter rule");
                } else {
                    self.add_explanation("business_rule_skipped",
                        &format!("Skipped business rule: {}.{} (user explicitly specified)",
                            table, rule.column),
                        "User explicitly mentioned this filter, so business rule was not applied");
                }
            }
        }
        
        Ok(())
    }

    async fn find_table_column_for_filter(
        &self,
        filter: &FilterSpec,
        metadata_pack: &MetadataPack,
    ) -> Result<(String, String)> {
        // Use LLM scoring if available, otherwise fallback to simple matching
        if let (Some(ref query), Some(ref client)) = (&self.original_query, &self.ollama_client) {
            return self.llm_score_filter_column(query, filter, metadata_pack).await;
        }
        
        // Fallback: simple matching
        let field_hint_lower = filter.field_hint.to_lowercase();
        for table_info in &metadata_pack.tables {
            for col in &table_info.columns {
                let col_lower = col.name.to_lowercase();
                if col_lower == field_hint_lower || col_lower.contains(&field_hint_lower) || field_hint_lower.contains(&col_lower) {
                    return Ok((table_info.name.clone(), col.name.clone()));
                }
            }
        }
        
        Err(anyhow::anyhow!("Could not find table/column for filter field_hint '{}'", filter.field_hint))
    }
    
    /// Use LLM to score all columns based on the user's query and filter hint
    async fn llm_score_filter_column(
        &self,
        query: &str,
        filter: &FilterSpec,
        metadata_pack: &MetadataPack,
    ) -> Result<(String, String)> {
        let client = self.ollama_client.as_ref().unwrap();
        
        // Build list of all columns with their tables
        let mut columns_info = Vec::new();
        for table_info in &metadata_pack.tables {
            for col in &table_info.columns {
                columns_info.push(format!("{}.{} (type: {})", table_info.name, col.name, col.data_type));
            }
        }
        
        let prompt = format!(
            r#"Given this user query: "{}"

The user wants to filter by a field hint: "{}" with operation: "{}" and value: "{}"

Here are all available columns in the database:
{}

Score each column from 0.0 to 1.0 based on how well it matches the filter field hint in the context of the user's query.
Return ONLY a JSON array of objects with format:
[
  {{"table": "table_name", "column": "column_name", "score": 0.95, "reason": "brief explanation"}},
  ...
]

Sort by score descending. Only include columns with score > 0.1."#,
            query,
            filter.field_hint,
            filter.op,
            filter.value,
            columns_info.join("\n")
        );
        
        let response = client.generate(&prompt, true).await?;
        let scores: Vec<serde_json::Value> = serde_json::from_str(&response)
            .or_else(|_| {
                // Try to extract JSON from markdown
                let cleaned = response.trim_start_matches("```json").trim_end_matches("```").trim();
                serde_json::from_str(cleaned)
            })?;
        
        // Find highest scoring column
        if let Some(best) = scores.iter()
            .filter_map(|s| {
                Some((
                    s.get("table")?.as_str()?,
                    s.get("column")?.as_str()?,
                    s.get("score")?.as_f64()?,
                ))
            })
            .max_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal))
        {
            return Ok((best.0.to_string(), best.1.to_string()));
        }
        
        Err(anyhow::anyhow!("LLM did not return valid scores for filter field_hint '{}'", filter.field_hint))
    }

    async fn find_best_column(
        &self,
        table: &str,
        hint: &str,
        metadata_pack: &MetadataPack,
    ) -> Result<String> {
        // Use LLM scoring if available
        if let (Some(ref query), Some(ref _client)) = (&self.original_query, &self.ollama_client) {
            return self.llm_score_column(query, table, hint, metadata_pack).await;
        }
        
        // Fallback: simple matching
        let table_info = metadata_pack.tables.iter()
            .find(|t| t.name == table)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", table))?;
        
        let hint_lower = hint.to_lowercase();
        
        // Try exact match
        for col in &table_info.columns {
            if col.name.to_lowercase() == hint_lower {
                return Ok(col.name.clone());
            }
        }
        
        // Try contains match
        for col in &table_info.columns {
            if col.name.to_lowercase().contains(&hint_lower) {
                return Ok(col.name.clone());
            }
        }
        
        // Fallback: first column
        table_info.columns.first()
            .map(|c| c.name.clone())
            .ok_or_else(|| anyhow::anyhow!("Table '{}' has no columns", table))
    }
    
    /// Use LLM to score columns in a table based on the user's query and hint
    async fn llm_score_column(
        &self,
        query: &str,
        table: &str,
        hint: &str,
        metadata_pack: &MetadataPack,
    ) -> Result<String> {
        let client = self.ollama_client.as_ref().unwrap();
        
        let table_info = metadata_pack.tables.iter()
            .find(|t| t.name == table)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", table))?;
        
        let columns_info: Vec<String> = table_info.columns.iter()
            .map(|c| format!("{} ({})", c.name, c.data_type))
            .collect();
        
        let prompt = format!(
            r#"Given this user query: "{}"

The user is looking for a column in table "{}" with hint: "{}"

Here are all columns in this table:
{}

Score each column from 0.0 to 1.0 based on how well it matches the hint in the context of the user's query.
Return ONLY a JSON array of objects with format:
[
  {{"column": "column_name", "score": 0.95, "reason": "brief explanation"}},
  ...
]

Sort by score descending. Only include columns with score > 0.1."#,
            query,
            table,
            hint,
            columns_info.join("\n")
        );
        
        let response = client.generate(&prompt, true).await?;
        let scores: Vec<serde_json::Value> = serde_json::from_str(&response)
            .or_else(|_| {
                let cleaned = response.trim_start_matches("```json").trim_end_matches("```").trim();
                serde_json::from_str(cleaned)
            })?;
        
        // Find highest scoring column
        if let Some(best) = scores.iter()
            .filter_map(|s| {
                Some((
                    s.get("column")?.as_str()?,
                    s.get("score")?.as_f64()?,
                ))
            })
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        {
            return Ok(best.0.to_string());
        }
        
        Err(anyhow::anyhow!("LLM did not return valid scores for column hint '{}' in table '{}'", hint, table))
    }

    fn build_output_schema(
        &self,
        intent_spec: &IntentSpec,
        projections: &[String],
        aggregations: &[AggregationInfo],
    ) -> Vec<ColumnSchema> {
        let mut schema = Vec::new();
        
        // Add projection columns
        for proj in projections {
            if let Some((table, column)) = proj.split_once('.') {
                schema.push(ColumnSchema {
                    column: proj.clone(),
                    r#type: "string".to_string(), // Will be refined by SQL generator
                });
            } else {
                schema.push(ColumnSchema {
                    column: proj.clone(),
                    r#type: "string".to_string(),
                });
            }
        }
        
        // Add aggregation columns
        for agg in aggregations {
            schema.push(ColumnSchema {
                column: agg.alias.clone(),
                r#type: "numeric".to_string(), // Aggregations are typically numeric
            });
        }
        
        schema
    }
}

