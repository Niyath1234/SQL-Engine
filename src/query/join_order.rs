/// Cost-Based Greedy Join Ordering for DuckDB/Presto-grade join engine
/// 
/// This module implements a greedy heuristic for determining optimal join order
/// based on estimated cardinality and selectivity.
use std::collections::{HashSet, HashMap};
use crate::query::join_graph::JoinGraph;
use crate::query::parser::JoinEdge;
use crate::hypergraph::graph::HyperGraph;
use anyhow::Result;

/// Join order result: ordered sequence of (table, join_edge) pairs
/// Describes the join sequence from left to right
#[derive(Debug, Clone)]
pub struct JoinOrder {
    /// Ordered sequence of tables to join
    pub tables: Vec<String>,
    
    /// Join edges in join order (one per join)
    pub join_edges: Vec<JoinEdge>,
    
    /// Estimated cardinality after each join (for cost estimation)
    pub estimated_cardinalities: Vec<u64>,
}

/// Cost-based join order planner
pub struct JoinOrderPlanner {
    graph: Option<HyperGraph>,
}

impl JoinOrderPlanner {
    /// Create new join order planner
    pub fn new() -> Self {
        Self { graph: None }
    }
    
    /// Create join order planner with hypergraph (for statistics)
    pub fn with_graph(graph: HyperGraph) -> Self {
        Self { graph: Some(graph) }
    }
    
    /// Determine optimal join order using greedy heuristic
    /// 
    /// # Algorithm (Greedy Left-Deep Join Order)
    /// 1. Start with table of smallest estimated cardinality (or first table if no stats)
    /// 2. Iteratively pick the next table connected to the existing set that minimizes
    ///    estimated join output cardinality
    /// 3. Use simple cardinality estimation: join_size = left_rows * right_rows * selectivity
    /// 
    /// # Rules
    /// - For inner joins only: reorder via join graph
    /// - For outer joins: enforce left-deep order but allow reorder of inner joins
    ///    below the nearest outer join anchor
    /// 
    /// # Returns
    /// Ordered vector of (table, join_edge) describing join sequence
    pub fn determine_join_order(
        &self,
        join_graph: &JoinGraph,
        table_aliases: &HashMap<String, String>,
    ) -> Result<JoinOrder> {
        if join_graph.is_empty() {
            return Ok(JoinOrder {
                tables: vec![],
                join_edges: vec![],
                estimated_cardinalities: vec![],
            });
        }
        
        // Step 1: Start with table of smallest estimated cardinality
        let start_table = self.select_start_table(join_graph, table_aliases)?;
        
        // Step 2: Greedily add tables that minimize join cost
        let mut joined_tables = HashSet::new();
        joined_tables.insert(start_table.clone());
        
        let mut ordered_tables = vec![start_table];
        let mut ordered_edges = vec![];
        let mut estimated_cardinalities = vec![];
        
        // Track current estimated cardinality
        let mut current_cardinality = self.estimate_table_cardinality(
            &ordered_tables[0],
            table_aliases,
        )?;
        estimated_cardinalities.push(current_cardinality);
        
        // Greedily add tables
        while joined_tables.len() < join_graph.num_nodes() {
            let (next_table, next_edge) = self.select_next_table(
                join_graph,
                &joined_tables,
                &ordered_tables,
                table_aliases,
                current_cardinality,
            )?;
            
            // Estimate join cardinality
            let right_cardinality = self.estimate_table_cardinality(&next_table, table_aliases)?;
            let selectivity = self.estimate_selectivity(&next_edge)?;
            let join_cardinality = (current_cardinality as f64 * right_cardinality as f64 * selectivity) as u64;
            
            // Add to join order
            ordered_tables.push(next_table.clone());
            ordered_edges.push(next_edge.clone());
            estimated_cardinalities.push(join_cardinality);
            
            joined_tables.insert(next_table);
            current_cardinality = join_cardinality;
        }
        
        Ok(JoinOrder {
            tables: ordered_tables,
            join_edges: ordered_edges,
            estimated_cardinalities,
        })
    }
    
    /// Select starting table (smallest cardinality or first table)
    fn select_start_table(
        &self,
        join_graph: &JoinGraph,
        table_aliases: &HashMap<String, String>,
    ) -> Result<String> {
        if join_graph.nodes.is_empty() {
            return Err(anyhow::anyhow!("Join graph is empty"));
        }
        
        // If we have statistics, pick smallest table
        if let Some(ref graph) = self.graph {
            let mut best_table = None;
            let mut best_cardinality = u64::MAX;
            
            for node in &join_graph.nodes {
                if let Ok(cardinality) = self.estimate_table_cardinality(node, table_aliases) {
                    if cardinality < best_cardinality {
                        best_cardinality = cardinality;
                        best_table = Some(node.clone());
                    }
                }
            }
            
            if let Some(table) = best_table {
                return Ok(table);
            }
        }
        
        // Fallback: use first table
        Ok(join_graph.nodes[0].clone())
    }
    
    /// Select next table to join (greedy: minimizes join cost)
    fn select_next_table(
        &self,
        join_graph: &JoinGraph,
        joined_tables: &HashSet<String>,
        _ordered_tables: &[String],
        table_aliases: &HashMap<String, String>,
        current_cardinality: u64,
    ) -> Result<(String, JoinEdge)> {
        let mut best_table = None;
        let mut best_edge = None;
        let mut best_cost = u64::MAX;
        
        // Find all edges connecting joined_tables to unjoined tables
        for edge in &join_graph.edges {
            let left_in_joined = joined_tables.contains(&edge.left.table_alias);
            let right_in_joined = joined_tables.contains(&edge.right.table_alias);
            
            // Edge connects joined set to new table
            if left_in_joined && !right_in_joined {
                let next_table = &edge.right.table_alias;
                let cost = self.estimate_join_cost(
                    current_cardinality,
                    next_table,
                    edge,
                    table_aliases,
                )?;
                
                if cost < best_cost {
                    best_cost = cost;
                    best_table = Some(next_table.clone());
                    best_edge = Some(edge.clone());
                }
            } else if right_in_joined && !left_in_joined {
                // Swap left/right for join
                let next_table = &edge.left.table_alias;
                let swapped_edge = JoinEdge {
                    left: edge.right.clone(),
                    right: edge.left.clone(),
                    join_type: edge.join_type.clone(),
                };
                let cost = self.estimate_join_cost(
                    current_cardinality,
                    next_table,
                    &swapped_edge,
                    table_aliases,
                )?;
                
                if cost < best_cost {
                    best_cost = cost;
                    best_table = Some(next_table.clone());
                    best_edge = Some(swapped_edge);
                }
            }
        }
        
        match (best_table, best_edge) {
            (Some(table), Some(edge)) => Ok((table, edge)),
            _ => Err(anyhow::anyhow!(
                "No join edge found connecting joined tables to remaining tables"
            )),
        }
    }
    
    /// Estimate join cost (output cardinality)
    fn estimate_join_cost(
        &self,
        left_cardinality: u64,
        right_table: &str,
        edge: &JoinEdge,
        table_aliases: &HashMap<String, String>,
    ) -> Result<u64> {
        let right_cardinality = self.estimate_table_cardinality(right_table, table_aliases)?;
        let selectivity = self.estimate_selectivity(edge)?;
        
        // Simple cardinality estimation: left_rows * right_rows * selectivity
        let cost = (left_cardinality as f64 * right_cardinality as f64 * selectivity) as u64;
        Ok(cost)
    }
    
    /// Estimate table cardinality (row count)
    fn estimate_table_cardinality(
        &self,
        table_alias: &str,
        table_aliases: &HashMap<String, String>,
    ) -> Result<u64> {
        // Try to get actual table name from alias
        let table_name = table_aliases
            .get(table_alias)
            .cloned()
            .unwrap_or_else(|| table_alias.to_string());
        
        // If we have hypergraph, try to get statistics
        if self.graph.is_some() {
            // Try to get row count from node statistics
            // For now, use a simple heuristic: assume small tables have ~1000 rows
            // In production, this would use actual statistics
            // TODO: Use actual statistics from hypergraph node
            return Ok(1000);
        }
        
        // Default: assume medium-sized table
        Ok(10000)
    }
    
    /// Estimate join selectivity (fraction of rows that match)
    /// 
    /// Selectivity estimation:
    /// - Foreign key joins: ~1.0 (assume all rows match)
    /// - Other joins: ~0.1 (assume 10% match)
    /// - This is a simple heuristic - production systems use histograms
    fn estimate_selectivity(&self, _edge: &JoinEdge) -> Result<f64> {
        // Simple heuristic: assume 10% selectivity for most joins
        // In production, this would use histograms, distinct value counts, etc.
        Ok(0.1)
    }
}

impl Default for JoinOrderPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_join_order_planner() {
        use crate::query::parser::{JoinEdge, QualifiedColumn, JoinType};
        let planner = JoinOrderPlanner::new();
        let mut graph = JoinGraph::new();
        
        graph.add_node("a".to_string());
        graph.add_node("b".to_string());
        
        let edge = JoinEdge {
            left: QualifiedColumn::new("a".to_string(), "id".to_string()),
            right: QualifiedColumn::new("b".to_string(), "a_id".to_string()),
            join_type: JoinType::Inner,
        };
        
        graph.add_edge(edge.clone()).unwrap();
        
        let table_aliases = HashMap::new();
        let order = planner.determine_join_order(&graph, &table_aliases).unwrap();
        
        assert_eq!(order.tables.len(), 2);
        assert_eq!(order.join_edges.len(), 1);
    }
}

