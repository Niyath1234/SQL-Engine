/// Bitset Engine V3: Planner Integration
/// 
/// Planner expansion rules to rewrite joins into bitset-driven form

use crate::query::plan::{QueryPlan, PlanOperator};
use crate::query::bitset_cost_model::BitsetCostModel;
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::edge::EdgeId;
use std::sync::Arc;
use anyhow::Result;

/// Bitset join planner rule
pub struct BitsetJoinPlannerRule;

impl BitsetJoinPlannerRule {
    /// Apply bitset join rule to query plan
    pub fn apply(plan: &mut QueryPlan, graph: &Arc<HyperGraph>) -> Result<()> {
        Self::rewrite_joins_recursive(&mut plan.root, graph)?;
        Ok(())
    }
    
    /// Recursively rewrite joins
    fn rewrite_joins_recursive(
        op: &mut PlanOperator,
        graph: &Arc<HyperGraph>,
    ) -> Result<()> {
        // Check if this is a Join that should be rewritten
        let should_rewrite = if let PlanOperator::Join { left, right, predicate, .. } = op {
            Self::should_rewrite_to_bitset(left, right, predicate, graph)?
        } else {
            false
        };
        
        // If we need to rewrite, extract values and replace
        if should_rewrite {
            if let PlanOperator::Join { mut left, mut right, predicate, .. } = std::mem::replace(op, PlanOperator::Scan {
                node_id: crate::hypergraph::node::NodeId(0),
                table: "".to_string(),
                columns: vec![],
                limit: None,
                offset: None,
            }) {
                // Recursively process children first
                Self::rewrite_joins_recursive(&mut *left, graph)?;
                Self::rewrite_joins_recursive(&mut *right, graph)?;
                
                // Now rewrite to bitset join
                *op = PlanOperator::BitsetJoin {
                    left,
                    right,
                    edge_id: EdgeId(0), // Default edge_id when rewriting
                    join_type: crate::query::plan::JoinType::Inner,
                    predicate: predicate.clone(),
                    use_bitmap: true,
                };
                return Ok(());
            }
        }
        
        // Otherwise, process normally
        match op {
            PlanOperator::Join { left, right, .. } => {
                // Recursively process children
                Self::rewrite_joins_recursive(left, graph)?;
                Self::rewrite_joins_recursive(right, graph)?;
            }
            PlanOperator::BitsetJoin { left, right, .. } => {
                // Already a bitset join, just recurse
                Self::rewrite_joins_recursive(left, graph)?;
                Self::rewrite_joins_recursive(right, graph)?;
            }
            _ => {
                // Recurse into children
                for child in op.children_mut() {
                    Self::rewrite_joins_recursive(child, graph)?;
                }
            }
        }
        Ok(())
    }
    
    /// Check if join should be rewritten to bitset join
    fn should_rewrite_to_bitset(
        left: &PlanOperator,
        right: &PlanOperator,
        predicate: &crate::query::plan::JoinPredicate,
        graph: &Arc<HyperGraph>,
    ) -> Result<bool> {
        // Detect star schema
        if Self::detect_star_schema(left, right, graph)? {
            return Ok(true);
        }
        
        // Detect selective dimensions
        if Self::detect_selective_dimensions(left, right, predicate, graph)? {
            return Ok(true);
        }
        
        // Check hierarchical pruning opportunities
        if Self::detect_hierarchical_pruning_opportunities(left, right, graph)? {
            return Ok(true);
        }
        
        Ok(false)
    }
    
    /// Detect star schema pattern
    fn detect_star_schema(
        left: &PlanOperator,
        right: &PlanOperator,
        _graph: &Arc<HyperGraph>,
    ) -> Result<bool> {
        // Simple heuristic: if one side is much larger, it's likely a fact table
        // In a real implementation, we'd check table statistics
        Ok(false) // TODO: Implement proper star schema detection
    }
    
    /// Detect selective dimensions
    fn detect_selective_dimensions(
        _left: &PlanOperator,
        _right: &PlanOperator,
        _predicate: &crate::query::plan::JoinPredicate,
        _graph: &Arc<HyperGraph>,
    ) -> Result<bool> {
        // Check if filters on dimensions are selective
        Ok(false) // TODO: Implement selectivity detection
    }
    
    /// Detect hierarchical pruning opportunities
    fn detect_hierarchical_pruning_opportunities(
        _left: &PlanOperator,
        _right: &PlanOperator,
        _graph: &Arc<HyperGraph>,
    ) -> Result<bool> {
        // Check if fact table is large enough for hierarchical skipping
        Ok(false) // TODO: Check fact table size > 10M rows
    }
}

/// Extension trait for PlanOperator to get mutable children
trait PlanOperatorChildren {
    fn children_mut(&mut self) -> Vec<&mut PlanOperator>;
}

impl PlanOperatorChildren for PlanOperator {
    fn children_mut(&mut self) -> Vec<&mut PlanOperator> {
        match self {
            PlanOperator::Scan { .. } => vec![],
            PlanOperator::Filter { input, .. } => vec![input],
            PlanOperator::Project { input, .. } => vec![input],
            PlanOperator::Join { left, right, .. } => vec![left, right],
            PlanOperator::BitsetJoin { left, right, .. } => vec![left, right],
            PlanOperator::Aggregate { input, .. } => vec![input],
            PlanOperator::Sort { input, .. } => vec![input],
            PlanOperator::Limit { input, .. } => vec![input],
            _ => vec![],
        }
    }
}

