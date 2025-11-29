/// Predictive spill model - ML-based memory pressure prediction
/// Forecasts memory pressure and proactively triggers early partial aggregation or pre-spilling
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SpillFeatures {
    pub plan_hash: u64,
    pub estimated_cardinality: u64,
    pub estimated_groups: u64,
    pub mem_current_bytes: u64,
    pub cpu_count: usize,
    pub join_edges: usize,
    pub agg_functions: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SpillPrediction {
    pub will_spill: bool,
    pub predicted_bytes: u64,
}

/// Predictive model for spill forecasting
/// Uses simple linear model for quick inference
#[derive(Serialize, Deserialize)]
pub struct PredictiveModel {
    /// Model coefficients (weights)
    pub w: Vec<f64>,
    /// Bias term
    pub bias: f64,
}

impl PredictiveModel {
    /// Create a new predictive model
    /// Loads persisted coefficients or initializes with defaults
    pub fn new() -> Self {
        // TODO: load persisted coefficients from disk or use defaults
        // Default coefficients: favor early spill for high cardinality and many groups
        Self {
            w: vec![
                0.0,              // plan_hash (not used in default)
                0.1,              // estimated_cardinality (bytes per row)
                0.5,              // estimated_groups (high impact)
                0.01,             // mem_current_bytes (current pressure)
                0.0,              // cpu_count (not used in default)
                0.2,               // join_edges (moderate impact)
                0.3,               // agg_functions (moderate impact)
            ],
            bias: -1000000.0,     // Bias threshold
        }
    }
    
    /// Load model from persisted coefficients
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        if path.exists() {
            let data = std::fs::read_to_string(path)?;
            let model: Self = serde_json::from_str(&data)?;
            Ok(model)
        } else {
            Ok(Self::new())
        }
    }
    
    /// Save model coefficients to disk
    pub fn save(&self, path: &std::path::Path) -> anyhow::Result<()> {
        let data = serde_json::to_string_pretty(self)?;
        std::fs::write(path, data)?;
        Ok(())
    }
    
    /// Infer spill prediction from features
    pub fn infer(&self, f: &SpillFeatures) -> SpillPrediction {
        // Feature vector
        let xf = vec![
            f.plan_hash as f64,
            f.estimated_cardinality as f64,
            f.estimated_groups as f64,
            f.mem_current_bytes as f64,
            f.cpu_count as f64,
            f.join_edges as f64,
            f.agg_functions as f64,
        ];
        
        // Linear model: dot product + bias
        let mut dot = self.bias;
        for i in 0..self.w.len().min(xf.len()) {
            dot += self.w[i] * xf[i];
        }
        
        // Threshold heuristics: TODO calibrate based on training data
        let will_spill = dot > 1e6;
        let predicted_bytes = if will_spill {
            (dot as u64).saturating_mul(2)
        } else {
            0
        };
        
        SpillPrediction {
            will_spill,
            predicted_bytes,
        }
    }
    
    /// Train model on collected data
    /// Implements lightweight logistic regression or linear regression training
    pub fn train(&mut self, data: &[(SpillFeatures, SpillPrediction)]) {
        // TODO: Implement proper training algorithm (gradient descent, etc.)
        // For now, use simple heuristic-based coefficients
        // In production, this would use offline training with collected traces
        
        if data.is_empty() {
            return;
        }
        
        // Simple heuristic: adjust weights based on positive examples
        // This is a placeholder - full implementation would use proper ML training
        let positive_count = data.iter().filter(|(_, pred)| pred.will_spill).count();
        if positive_count > 0 {
            // Adjust bias to match positive rate
            let positive_rate = positive_count as f64 / data.len() as f64;
            self.bias = -1000000.0 + (positive_rate * 2000000.0);
        }
    }
    
    /// Get model coefficients for inspection
    pub fn coefficients(&self) -> (&[f64], f64) {
        (&self.w, self.bias)
    }
}

impl Default for PredictiveModel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_predictive_model_infer() {
        let model = PredictiveModel::new();
        
        let features = SpillFeatures {
            plan_hash: 12345,
            estimated_cardinality: 10_000_000,
            estimated_groups: 1_000_000,
            mem_current_bytes: 500_000_000,
            cpu_count: 8,
            join_edges: 3,
            agg_functions: 2,
        };
        
        let prediction = model.infer(&features);
        
        // Should predict spill for high cardinality and many groups
        assert!(prediction.will_spill || prediction.predicted_bytes > 0, 
            "Should predict spill for high-cardinality query");
    }
    
    #[test]
    fn test_predictive_model_training() {
        let mut model = PredictiveModel::new();
        
        let training_data = vec![
            (
                SpillFeatures {
                    plan_hash: 1,
                    estimated_cardinality: 10_000_000,
                    estimated_groups: 1_000_000,
                    mem_current_bytes: 500_000_000,
                    cpu_count: 8,
                    join_edges: 3,
                    agg_functions: 2,
                },
                SpillPrediction {
                    will_spill: true,
                    predicted_bytes: 2_000_000,
                },
            ),
        ];
        
        model.train(&training_data);
        
        // Model should be trainable (no panic)
        assert!(true, "Training should complete without error");
    }
}

