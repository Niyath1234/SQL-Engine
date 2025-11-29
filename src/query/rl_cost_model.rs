/// RL-Based Cost Model (Lightweight Policy Network)
/// Reinforcement learning policy for join order, join type, and plan shape
use crate::query::plan::{QueryPlan, PlanOperator};
use std::collections::HashMap;

/// Feature vector for RL policy
#[derive(Clone, Debug)]
pub struct RLFeatureVector {
    pub num_tables: usize,
    pub join_graph_size: usize,
    pub avg_selectivity: f64,
    pub scan_row_counts: Vec<usize>,
    pub predicate_complexity: usize,
    pub batch_size: usize,
    pub vector_width: usize,
}

/// RL Policy agent
pub struct RLPolicy {
    /// Policy weights (simple linear model)
    weights: Vec<f32>,
    /// Learning rate
    learning_rate: f32,
    /// Number of features
    num_features: usize,
}

impl RLPolicy {
    pub fn new(num_features: usize) -> Self {
        // Initialize weights to small random values
        // Using a simple deterministic initialization for now
        let weights = (0..num_features)
            .map(|i| ((i as f32 * 0.1) % 0.2 - 0.1))
            .collect();
        
        Self {
            weights,
            learning_rate: 0.01,
            num_features,
        }
    }
    
    /// Compute cost adjustment based on feature vector
    pub fn compute_cost_adjustment(&self, features: &RLFeatureVector) -> f64 {
        // Convert features to a flat vector
        let feature_vec = self.features_to_vector(features);
        
        // Simple linear model: dot product of weights and features
        let adjustment: f32 = self.weights
            .iter()
            .zip(feature_vec.iter())
            .map(|(w, f)| w * f)
            .sum();
        
        adjustment as f64
    }
    
    /// Update policy weights based on reward
    pub fn update(&mut self, features: &RLFeatureVector, reward: f64) {
        let feature_vec = self.features_to_vector(features);
        let prediction = self.compute_cost_adjustment(features);
        
        // Simple gradient descent update
        let error = reward - prediction;
        
        for (weight, feature) in self.weights.iter_mut().zip(feature_vec.iter()) {
            *weight += self.learning_rate * error as f32 * feature;
        }
        
        tracing::debug!("RL policy updated: error={:.2}, reward={:.2}", error, reward);
    }
    
    /// Convert feature vector to flat array
    fn features_to_vector(&self, features: &RLFeatureVector) -> Vec<f32> {
        let mut vec = Vec::new();
        
        // Normalize features
        vec.push((features.num_tables as f32) / 10.0); // Normalize to 0-1 range
        vec.push((features.join_graph_size as f32) / 20.0);
        vec.push(features.avg_selectivity as f32);
        vec.push((features.scan_row_counts.iter().sum::<usize>() as f32) / 1_000_000.0); // Normalize
        vec.push((features.predicate_complexity as f32) / 10.0);
        vec.push((features.batch_size as f32) / 10000.0);
        vec.push((features.vector_width as f32) / 512.0);
        
        // Pad to num_features if needed
        while vec.len() < self.num_features {
            vec.push(0.0);
        }
        
        vec.truncate(self.num_features);
        vec
    }
    
    /// Get recommended join order (FULL RL-BASED JOIN ORDER POLICY)
    /// Uses policy weights to determine optimal join order
    pub fn recommend_join_order(&self, num_tables: usize, features: &RLFeatureVector, table_sizes: &[usize]) -> Vec<usize> {
        // Use policy to score different join orders
        let adjustment = self.compute_cost_adjustment(features);
        
        // Generate candidate join orders
        let mut candidates: Vec<(Vec<usize>, f64)> = Vec::new();
        
        // 1. Default order (smallest to largest)
        let mut default_order: Vec<usize> = (0..num_tables).collect();
        default_order.sort_by_key(|&i| table_sizes.get(i).copied().unwrap_or(0));
        let default_cost = self.score_join_order(&default_order, table_sizes, features);
        candidates.push((default_order, default_cost));
        
        // 2. Largest to smallest (for right-deep trees)
        let mut reverse_order: Vec<usize> = (0..num_tables).collect();
        reverse_order.sort_by_key(|&i| std::cmp::Reverse(table_sizes.get(i).copied().unwrap_or(0)));
        let reverse_cost = self.score_join_order(&reverse_order, table_sizes, features);
        candidates.push((reverse_order, reverse_cost));
        
        // 3. Policy-adjusted order (if adjustment suggests different strategy)
        if adjustment.abs() > 0.5 {
            // Use policy to influence order
            let mut policy_order: Vec<usize> = (0..num_tables).collect();
            // Sort by policy-weighted size
            policy_order.sort_by(|&a, &b| {
                let size_a = table_sizes.get(a).copied().unwrap_or(0) as f64;
                let size_b = table_sizes.get(b).copied().unwrap_or(0) as f64;
                // Adjust by policy
                let adjusted_a = size_a * (1.0 + adjustment as f64);
                let adjusted_b = size_b * (1.0 + adjustment as f64);
                adjusted_a.partial_cmp(&adjusted_b).unwrap_or(std::cmp::Ordering::Equal)
            });
            let policy_cost = self.score_join_order(&policy_order, table_sizes, features);
            candidates.push((policy_order, policy_cost));
        }
        
        // Return order with lowest cost
        candidates.into_iter()
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(order, _)| order)
            .unwrap_or_else(|| (0..num_tables).collect())
    }
    
    /// Score a join order using policy
    fn score_join_order(&self, order: &[usize], table_sizes: &[usize], features: &RLFeatureVector) -> f64 {
        // Simple cost model: sum of table sizes in order, adjusted by policy
        let base_cost: f64 = order.iter()
            .enumerate()
            .map(|(pos, &idx)| {
                let size = table_sizes.get(idx).copied().unwrap_or(0) as f64;
                size * (pos as f64 + 1.0) // Later joins are more expensive
            })
            .sum();
        
        let adjustment = self.compute_cost_adjustment(features);
        base_cost * (1.0 + adjustment as f64)
    }
    
    /// Get recommended physical join strategy (uses policy)
    pub fn recommend_join_strategy(&self, left_rows: usize, right_rows: usize, features: &RLFeatureVector) -> JoinStrategy {
        // Use policy adjustment to influence strategy
        let adjustment = self.compute_cost_adjustment(features);
        
        // Base heuristic
        let base_strategy = if left_rows < right_rows / 10 {
            JoinStrategy::BroadcastHashJoin
        } else if left_rows < 100 && right_rows < 100 {
            JoinStrategy::NestedLoopJoin
        } else {
            JoinStrategy::HashJoin
        };
        
        // Policy adjustment: if adjustment suggests hash join is better, prefer it
        if adjustment < -0.5 {
            JoinStrategy::HashJoin
        } else if adjustment > 0.5 {
            JoinStrategy::MergeJoin
        } else {
            base_strategy
        }
    }
    
    /// Get recommended aggregate strategy (uses policy)
    pub fn recommend_aggregate_strategy(&self, estimated_groups: usize, features: &RLFeatureVector) -> AggregateStrategy {
        let adjustment = self.compute_cost_adjustment(features);
        
        // Policy influences choice
        if adjustment < -0.3 {
            AggregateStrategy::HashAggregate // Policy suggests hash is better
        } else if estimated_groups < 1000 {
            AggregateStrategy::HashAggregate
        } else {
            AggregateStrategy::SortAggregate
        }
    }
}

/// Join strategy recommendation
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinStrategy {
    BroadcastHashJoin,
    MergeJoin,
    HashJoin,
    NestedLoopJoin,
}

/// Aggregate strategy recommendation
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AggregateStrategy {
    HashAggregate,
    SortAggregate,
}

/// Compute RL-adjusted cost
pub fn rl_cost(base_cost: f64, features: &RLFeatureVector, policy: &RLPolicy) -> f64 {
    let adjustment = policy.compute_cost_adjustment(features);
    let adjusted_cost = base_cost + adjustment;
    
    // Apply safety bounds
    adjusted_cost.max(base_cost * 0.1).min(base_cost * 10.0)
}

/// Update RL policy after query execution
pub fn rl_update(
    policy: &mut RLPolicy,
    features: &RLFeatureVector,
    execution_time_ms: f64,
    spill_penalty: f64,
) {
    // Reward is negative (we want to minimize cost)
    let reward = -(execution_time_ms + spill_penalty);
    
    policy.update(features, reward);
}

/// Extract RL features from a query plan
pub fn extract_features_from_plan(plan: &QueryPlan) -> RLFeatureVector {
    let mut num_tables = 0;
    let mut join_count = 0;
    let mut scan_row_counts = Vec::new();
    let mut predicate_count = 0;
    
    // Traverse plan to extract features
    extract_features_from_operator(&plan.root, &mut num_tables, &mut join_count, &mut scan_row_counts, &mut predicate_count);
    
    // Estimate average selectivity (simplified)
    let avg_selectivity = if predicate_count > 0 {
        0.1 // Default 10% selectivity per predicate
    } else {
        1.0
    };
    
    RLFeatureVector {
        num_tables,
        join_graph_size: join_count,
        avg_selectivity,
        scan_row_counts,
        predicate_complexity: predicate_count,
        batch_size: 8192, // Default from hardware profile
        vector_width: 256, // Default from hardware profile
    }
}

/// Recursively extract features from operator tree
fn extract_features_from_operator(
    op: &PlanOperator,
    num_tables: &mut usize,
    join_count: &mut usize,
    scan_row_counts: &mut Vec<usize>,
    predicate_count: &mut usize,
) {
    match op {
        PlanOperator::Scan { table, .. } => {
            *num_tables += 1;
            // Estimate row count (would use statistics in full implementation)
            scan_row_counts.push(10000); // Default estimate
        }
        PlanOperator::CTEScan { .. } => {
            scan_row_counts.push(1000); // CTEs are typically smaller
        }
        PlanOperator::DerivedTableScan { .. } => {
            scan_row_counts.push(1000);
        }
        PlanOperator::Filter { input, predicates, .. } => {
            *predicate_count += predicates.len();
            extract_features_from_operator(input, num_tables, join_count, scan_row_counts, predicate_count);
        }
        PlanOperator::Join { left, right, .. } => {
            *join_count += 1;
            extract_features_from_operator(left, num_tables, join_count, scan_row_counts, predicate_count);
            extract_features_from_operator(right, num_tables, join_count, scan_row_counts, predicate_count);
        }
        PlanOperator::BitsetJoin { left, right, .. } => {
            *join_count += 1;
            extract_features_from_operator(left, num_tables, join_count, scan_row_counts, predicate_count);
            extract_features_from_operator(right, num_tables, join_count, scan_row_counts, predicate_count);
        }
        PlanOperator::Aggregate { input, .. } |
        PlanOperator::Project { input, .. } |
        PlanOperator::Sort { input, .. } |
        PlanOperator::Limit { input, .. } |
        PlanOperator::Distinct { input } |
        PlanOperator::Having { input, .. } |
        PlanOperator::Window { input, .. } |
        PlanOperator::Fused { input, .. } => {
            extract_features_from_operator(input, num_tables, join_count, scan_row_counts, predicate_count);
        }
        _ => {}
    }
}

