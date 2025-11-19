use crate::query::plan::*;
use crate::query::parser::ParsedQuery;
use crate::query::cte::CTEContext;
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::NodeId;
use crate::hypergraph::edge::EdgeId;
use anyhow::Result;
use std::sync::Arc;

/// Query planner - converts parsed query to execution plan
#[derive(Clone)]
pub struct QueryPlanner {
    graph: Arc<HyperGraph>,
    cte_context: Option<CTEContext>,
}

impl QueryPlanner {
    pub fn new(graph: HyperGraph) -> Self {
        Self { 
            graph: Arc::new(graph),
            cte_context: None,
        }
    }
    
    pub fn from_arc(graph: Arc<HyperGraph>) -> Self {
        Self { 
            graph,
            cte_context: None,
        }
    }
    
    pub fn with_cte_context(mut self, cte_context: CTEContext) -> Self {
        self.cte_context = Some(cte_context);
        self
    }
    
    /// Plan a query
    pub fn plan(&self, parsed: &ParsedQuery) -> Result<QueryPlan> {
        // 0. Handle CTEs - check if any tables are actually CTEs
        let mut actual_tables = parsed.tables.clone();
        if let Some(ref cte_ctx) = self.cte_context {
            // Filter out CTEs from table list (they'll be handled separately)
            actual_tables.retain(|t| !cte_ctx.contains(t));
        }
        
        // 1. Find nodes for all tables (excluding CTEs)
        let table_nodes = self.find_table_nodes(&actual_tables)?;
        
        // 2. Find join paths between tables
        let join_path = self.find_join_path(&parsed.joins, &table_nodes)?;
        
        // 3. Build plan tree
        let root = self.build_plan_tree(parsed, &table_nodes, &join_path)?;
        
        // 4. Create plan
        let mut plan = QueryPlan::new(root);
        
        // 5. Optimize plan
        plan.optimize();
        
        Ok(plan)
    }
    
    fn find_table_nodes(&self, tables: &[String]) -> Result<Vec<NodeId>> {
        let mut nodes = vec![];
        
        if tables.is_empty() {
            anyhow::bail!("No tables in query");
        }
        
        // OPTIMIZATION: Use O(1) table lookup instead of iterating all nodes
        for table in tables {
            // Normalize table name (remove quotes, handle case)
            let normalized_table = table.trim_matches('"').trim_matches('\'').to_lowercase();
            tracing::debug!("Normalized table name: '{}'", normalized_table);
            
            // Use O(1) lookup via table_index
            if let Some(table_node) = self.graph.get_table_node(&normalized_table) {
                nodes.push(table_node.id);
                tracing::debug!("Found table node: {:?}", table_node.id);
            } else {
                // Fallback: try case-insensitive lookup
                let mut found = false;
                for entry in self.graph.table_index.iter() {
                    let (table_name, node_id) = (entry.key(), entry.value());
                    if table_name.to_lowercase() == normalized_table {
                        if let Some(node) = self.graph.get_node(*node_id) {
                            nodes.push(node.id);
                            found = true;
                            tracing::debug!("Found table node (case-insensitive): {:?}", node.id);
                            break;
                        }
                    }
                }
                
                if !found {
                    // Collect available tables for error message
                    let mut all_table_names = Vec::new();
                    for entry in self.graph.table_index.iter() {
                        all_table_names.push(entry.key().clone());
                    }
                    anyhow::bail!("Table '{}' not found in hypergraph. Available tables: {:?}", table, all_table_names);
                }
            }
        }
        
        Ok(nodes)
    }
    
    fn find_join_path(&self, joins: &[crate::query::parser::JoinInfo], table_nodes: &[NodeId]) -> Result<Vec<EdgeId>> {
        let mut edges = vec![];
        
        for join in joins {
            // Find edge matching this join
            // TODO: Implement edge lookup
        }
        
        Ok(edges)
    }
    
    fn build_plan_tree(
        &self,
        parsed: &ParsedQuery,
        table_nodes: &[NodeId],
        join_path: &[EdgeId],
    ) -> Result<PlanOperator> {
        // Push LIMIT/OFFSET down to scan for early termination optimization
        // This allows ScanOperator to stop processing fragments early
        let scan_limit = parsed.limit;
        let scan_offset = parsed.offset;
        
        // Start with scan operators
        let mut current_op = if let Some(node_id) = table_nodes.first() {
            PlanOperator::Scan {
                node_id: *node_id,
                table: parsed.tables[0].clone(),
                columns: parsed.columns.clone(),
                limit: scan_limit,  // Push LIMIT down to scan
                offset: scan_offset, // Push OFFSET down to scan
            }
        } else {
            anyhow::bail!("No tables in query");
        };
        
        // Add joins
        for (i, edge_id) in join_path.iter().enumerate() {
            if i + 1 < table_nodes.len() {
                let right_op = PlanOperator::Scan {
                    node_id: table_nodes[i + 1],
                    table: parsed.tables[i + 1].clone(),
                    columns: parsed.columns.clone(),
                    limit: None,  // Joins don't push LIMIT to right side (would be incorrect)
                    offset: None,
                };
                
                // Extract join predicate and type from parsed query
                let join_info = parsed.joins.get(i).cloned();
                let (join_type, join_predicate) = if let Some(join) = join_info {
                    let jt = match join.join_type {
                        crate::query::parser::JoinType::Inner => JoinType::Inner,
                        crate::query::parser::JoinType::Left => JoinType::Left,
                        crate::query::parser::JoinType::Right => JoinType::Right,
                        crate::query::parser::JoinType::Full => JoinType::Full,
                    };
                    let pred = JoinPredicate {
                        left: (join.left_table, join.left_column),
                        right: (join.right_table, join.right_column),
                        operator: PredicateOperator::Equals,
                    };
                    (jt, pred)
                } else {
                    // Default to inner join if not specified
                    let pred = JoinPredicate {
                        left: ("".to_string(), "".to_string()),
                        right: ("".to_string(), "".to_string()),
                        operator: PredicateOperator::Equals,
                    };
                    (JoinType::Inner, pred)
                };
                
                current_op = PlanOperator::Join {
                    left: Box::new(current_op),
                    right: Box::new(right_op),
                    edge_id: *edge_id,
                    join_type,
                    predicate: join_predicate,
                };
            }
        }
        
        // Add filters
        if !parsed.filters.is_empty() {
            let predicates = parsed.filters.iter().map(|f| {
                // Convert FilterInfo to FilterPredicate
                let column = if f.table.is_empty() {
                    f.column.clone()
                } else {
                    format!("{}.{}", f.table, f.column)
                };
                
                let operator = match f.operator {
                    crate::query::parser::FilterOperator::Equals => PredicateOperator::Equals,
                    crate::query::parser::FilterOperator::NotEquals => PredicateOperator::NotEquals,
                    crate::query::parser::FilterOperator::LessThan => PredicateOperator::LessThan,
                    crate::query::parser::FilterOperator::LessThanOrEqual => PredicateOperator::LessThanOrEqual,
                    crate::query::parser::FilterOperator::GreaterThan => PredicateOperator::GreaterThan,
                    crate::query::parser::FilterOperator::GreaterThanOrEqual => PredicateOperator::GreaterThanOrEqual,
                    crate::query::parser::FilterOperator::Like => PredicateOperator::Like,
                    crate::query::parser::FilterOperator::NotLike => PredicateOperator::NotLike,
                    crate::query::parser::FilterOperator::In => PredicateOperator::In,
                    crate::query::parser::FilterOperator::NotIn => PredicateOperator::NotIn,
                    _ => PredicateOperator::Equals, // Default for Between, etc.
                };
                
                // Convert value string to Value enum
                let value = if !f.value.is_empty() {
                    // Try to parse as number first
                    if let Ok(i) = f.value.parse::<i64>() {
                        crate::storage::fragment::Value::Int64(i)
                    } else if let Ok(fl) = f.value.parse::<f64>() {
                        crate::storage::fragment::Value::Float64(fl)
                    } else {
                        crate::storage::fragment::Value::String(f.value.clone())
                    }
                } else {
                    crate::storage::fragment::Value::String(String::new())
                };
                
                // Convert in_values from Vec<String> to Vec<Value>
                let in_values = f.in_values.as_ref().map(|vals| {
                    vals.iter().map(|v| {
                        if let Ok(i) = v.parse::<i64>() {
                            crate::storage::fragment::Value::Int64(i)
                        } else if let Ok(fl) = v.parse::<f64>() {
                            crate::storage::fragment::Value::Float64(fl)
                        } else {
                            crate::storage::fragment::Value::String(v.clone())
                        }
                    }).collect()
                });
                
                FilterPredicate {
                    column,
                    operator,
                    value,
                    pattern: f.pattern.clone(),
                    in_values,
                }
            }).collect();
            
            current_op = PlanOperator::Filter {
                input: Box::new(current_op),
                predicates,
            };
        }
        
        // Add aggregates
        if !parsed.aggregates.is_empty() || !parsed.group_by.is_empty() {
            let aggregates = parsed.aggregates.iter().map(|a| {
                AggregateExpr {
                    function: match a.function {
                        crate::query::parser::AggregateFunction::Sum => AggregateFunction::Sum,
                        crate::query::parser::AggregateFunction::Count => AggregateFunction::Count,
                        crate::query::parser::AggregateFunction::Avg => AggregateFunction::Avg,
                        crate::query::parser::AggregateFunction::Min => AggregateFunction::Min,
                        crate::query::parser::AggregateFunction::Max => AggregateFunction::Max,
                        crate::query::parser::AggregateFunction::CountDistinct => AggregateFunction::CountDistinct,
                    },
                    column: a.column.clone(),
                    alias: a.alias.clone(),
                }
            }).collect();
            
            current_op = PlanOperator::Aggregate {
                input: Box::new(current_op),
                group_by: parsed.group_by.clone(),
                aggregates,
                having: parsed.having.clone(),
            };
            
            // Add HAVING clause as a filter after aggregation (if present)
            if let Some(having_pred) = &parsed.having {
                // Use HavingOperator to filter groups
                // For now, we'll add it as a separate filter operator
                // TODO: Parse HAVING predicate into FilterPredicate
            }
        }
        
        // Add projection
        if !parsed.columns.is_empty() {
            current_op = PlanOperator::Project {
                input: Box::new(current_op),
                columns: parsed.columns.clone(),
            };
        }
        
        // Add sort
        if !parsed.order_by.is_empty() {
            let order_by = parsed.order_by.iter().map(|o| {
                OrderByExpr {
                    column: o.column.clone(),
                    ascending: o.ascending,
                }
            }).collect();
            
            current_op = PlanOperator::Sort {
                input: Box::new(current_op),
                order_by,
                limit: parsed.limit,
                offset: parsed.offset,
            };
        }
        
        // Add limit
        if let Some(limit) = parsed.limit {
            current_op = PlanOperator::Limit {
                input: Box::new(current_op),
                limit,
                offset: parsed.offset.unwrap_or(0),
            };
        }
        
        Ok(current_op)
    }
}

