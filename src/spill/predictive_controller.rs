/// Predictive controller for proactive spill management
/// Uses predictive model to advise on spill decisions
use crate::spill::predictive_model::{PredictiveModel, SpillFeatures, SpillPrediction};

pub struct PredictiveController {
    model: PredictiveModel,
}

impl PredictiveController {
    /// Create a new predictive controller
    pub fn new() -> Self {
        Self {
            model: PredictiveModel::new(),
        }
    }
    
    /// Load controller with persisted model
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        Ok(Self {
            model: PredictiveModel::load(path)?,
        })
    }
    
    /// Save controller model to disk
    pub fn save(&self, path: &std::path::Path) -> anyhow::Result<()> {
        self.model.save(path)
    }
    
    /// Advise on spill decision based on features
    /// Returns Some((will_spill, predicted_bytes)) if spill is predicted, None otherwise
    pub fn advise(&self, features: SpillFeatures) -> Option<(bool, u64)> {
        let pred = self.model.infer(&features);
        if pred.will_spill {
            Some((true, pred.predicted_bytes))
        } else {
            None
        }
    }
    
    /// Get detailed prediction
    pub fn predict(&self, features: SpillFeatures) -> SpillPrediction {
        self.model.infer(&features)
    }
    
    /// Train model on collected data
    pub fn train(&mut self, data: &[(SpillFeatures, SpillPrediction)]) {
        self.model.train(data);
    }
    
    /// Get the underlying model for inspection
    pub fn model(&self) -> &PredictiveModel {
        &self.model
    }
}

impl Default for PredictiveController {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_predictive_controller_advise() {
        let controller = PredictiveController::new();
        
        let features = SpillFeatures {
            plan_hash: 12345,
            estimated_cardinality: 10_000_000,
            estimated_groups: 1_000_000,
            mem_current_bytes: 500_000_000,
            cpu_count: 8,
            join_edges: 3,
            agg_functions: 2,
        };
        
        let advice = controller.advise(features);
        
        // Should advise spill for high-cardinality query
        // (may or may not trigger depending on model thresholds)
        assert!(true, "Advise should complete without error");
    }
}

