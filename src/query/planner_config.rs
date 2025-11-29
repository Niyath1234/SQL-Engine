/// Configuration for query planner optimizations
use serde::{Deserialize, Serialize};

/// Planner configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlannerConfig {
    /// Enable cascades optimizer
    pub use_cascades: bool,
    
    /// Alpha parameter for learned cardinality estimation (0.0 = pure ML, 1.0 = pure stats)
    pub learned_ce_alpha: f64,
    
    /// Retrain interval (number of queries)
    pub retrain_interval: usize,
    
    /// Enable adaptive optimization
    pub enable_adaptive: bool,
    
    /// Reoptimization threshold (error percentage)
    pub reoptimize_threshold: f64,
    
    /// Enable approximate-first execution
    pub enable_approximate_first: bool,
    
    /// Enable predictive spill
    pub enable_predictive_spill: bool,
    
    /// Stage 5: Enable RL-based cost model
    pub enable_rl_cost_model: bool,
    
    /// Stage 5: Enable cross-query optimization (shared execution)
    pub enable_shared_execution: bool,
    
    /// Stage 5: Enable speculative planning and query prediction
    pub enable_speculative_planning: bool,
    
    /// Stage 5: Enable statistics drift detection and auto-refresh
    pub enable_stats_drift_detection: bool,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            use_cascades: true, // Enable cascades optimizer by default (more advanced optimization)
            learned_ce_alpha: 0.5, // Equal weight between stats and ML
            retrain_interval: 1000, // Retrain every 1000 queries
            enable_adaptive: true, // Enable adaptive optimization by default
            reoptimize_threshold: 0.5, // 50% error threshold
            enable_approximate_first: true,
            enable_predictive_spill: true,
            // Stage 5 features (enabled by default for self-optimizing engine)
            enable_rl_cost_model: true,
            enable_shared_execution: true,
            enable_speculative_planning: true,
            enable_stats_drift_detection: true,
        }
    }
}

impl PlannerConfig {
    /// Create config with cascades optimizer enabled
    pub fn with_cascades() -> Self {
        Self {
            use_cascades: true,
            ..Default::default()
        }
    }
    
    /// Create config optimized for production
    pub fn production() -> Self {
        Self {
            use_cascades: true,
            learned_ce_alpha: 0.3, // Favor ML models in production
            retrain_interval: 500, // Retrain more frequently
            enable_adaptive: true,
            reoptimize_threshold: 0.3, // More aggressive reoptimization
            enable_approximate_first: true,
            enable_predictive_spill: true,
            enable_rl_cost_model: true,
            enable_shared_execution: true,
            enable_speculative_planning: true,
            enable_stats_drift_detection: true,
        }
    }
    
    /// Create config optimized for development
    pub fn development() -> Self {
        Self {
            use_cascades: true, // Use cascades optimizer (can be disabled for debugging if needed)
            learned_ce_alpha: 0.7, // Favor statistics in development
            retrain_interval: 100, // Retrain more frequently for testing
            enable_adaptive: true,
            reoptimize_threshold: 0.5,
            enable_approximate_first: false, // Disable for exact results
            enable_predictive_spill: false, // Disable for simpler debugging
            enable_rl_cost_model: false, // Disable for simpler debugging
            enable_shared_execution: false,
            enable_speculative_planning: false,
            enable_stats_drift_detection: false,
        }
    }
    
    /// Create config with cascades optimizer disabled (for debugging or comparison)
    pub fn traditional_only() -> Self {
        Self {
            use_cascades: false, // Use traditional optimizer only
            learned_ce_alpha: 0.5,
            retrain_interval: 1000,
            enable_adaptive: true,
            reoptimize_threshold: 0.5,
            enable_approximate_first: true,
            enable_predictive_spill: true,
            enable_rl_cost_model: true,
            enable_shared_execution: true,
            enable_speculative_planning: true,
            enable_stats_drift_detection: true,
        }
    }
}

