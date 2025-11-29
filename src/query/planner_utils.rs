/// Utility functions for planner configuration and management
use crate::query::planner::QueryPlanner;
use crate::query::planner_config::PlannerConfig;
use crate::query::adaptive_optimizer::AdaptiveOptimizer;
use crate::hypergraph::graph::HyperGraph;
use std::sync::Arc;

/// Enable cascades optimizer on an existing planner
pub fn enable_cascades_optimizer(planner: &mut QueryPlanner) {
    planner.enable_cascades();
    planner.config.use_cascades = true;
}

/// Disable cascades optimizer
pub fn disable_cascades_optimizer(planner: &mut QueryPlanner) {
    planner.disable_cascades();
    planner.config.use_cascades = false;
}

/// Update planner configuration
pub fn update_planner_config(planner: &mut QueryPlanner, config: PlannerConfig) {
    planner.config = config.clone();
    planner.use_cascades = config.use_cascades;
    
    // Update learned CE alpha
    planner.learned_ce.set_alpha(config.learned_ce_alpha);
    
    // Update adaptive optimizer (need to get mutable reference from Arc)
    if let Some(adaptive) = Arc::get_mut(&mut planner.adaptive_optimizer) {
        adaptive.set_threshold(config.reoptimize_threshold);
        adaptive.set_enabled(config.enable_adaptive);
    } else {
        // If Arc is shared, create a new one
        let mut new_adaptive = AdaptiveOptimizer::new();
        new_adaptive.set_threshold(config.reoptimize_threshold);
        new_adaptive.set_enabled(config.enable_adaptive);
        planner.adaptive_optimizer = Arc::new(new_adaptive);
    }
    
    // Update training pipeline
    if let Some(ref mut training) = planner.training_pipeline {
        training.set_retrain_interval(config.retrain_interval);
    }
    
    // Reinitialize cascades optimizer if needed
    if config.use_cascades && planner.cascades_optimizer.is_none() {
        planner.enable_cascades();
    } else if !config.use_cascades && planner.cascades_optimizer.is_some() {
        planner.disable_cascades();
    }
}

/// Get current planner statistics
pub fn get_planner_stats(planner: &QueryPlanner) -> PlannerStats {
    PlannerStats {
        use_cascades: planner.use_cascades,
        learned_ce_alpha: planner.learned_ce.alpha,
        adaptive_enabled: planner.adaptive_optimizer.enabled,
        training_enabled: planner.training_pipeline.as_ref()
            .map(|t| t.enabled)
            .unwrap_or(false),
        training_examples: planner.training_pipeline.as_ref()
            .map(|t| t.num_training_examples())
            .unwrap_or(0),
        retrain_interval: planner.config.retrain_interval,
        queries_since_retrain: planner.training_pipeline.as_ref()
            .map(|t| t.query_count)
            .unwrap_or(0),
    }
}

/// Planner statistics
#[derive(Debug, Clone)]
pub struct PlannerStats {
    pub use_cascades: bool,
    pub learned_ce_alpha: f64,
    pub adaptive_enabled: bool,
    pub training_enabled: bool,
    pub training_examples: usize,
    pub retrain_interval: usize,
    pub queries_since_retrain: usize,
}

