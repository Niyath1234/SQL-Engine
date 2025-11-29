/// Linear regression model for learned index
/// Predicts position of a value in sorted data using linear regression
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LinearModel {
    pub slope: f64,
    pub intercept: f64,
}

impl LinearModel {
    /// Train a linear model from sorted values
    /// Uses linear regression to learn the mapping: position = slope * value + intercept
    pub fn train(values: &[i64]) -> Self {
        if values.is_empty() {
            return Self {
                slope: 0.0,
                intercept: 0.0,
            };
        }
        
        let n = values.len() as f64;
        
        // X = position (0, 1, 2, ...), Y = value
        let sum_x: f64 = (0..values.len()).map(|i| i as f64).sum();
        let sum_y: f64 = values.iter().map(|v| *v as f64).sum();
        let sum_xy: f64 = values.iter().enumerate().map(|(i, v)| i as f64 * *v as f64).sum();
        let sum_x2: f64 = (0..values.len()).map(|i| (i as f64).powi(2)).sum();
        
        // Linear regression: y = slope * x + intercept
        // Solving for: position = slope * value + intercept
        // But we have: value = f(position), so we need to invert
        // Actually, we want: position = f(value), so we train on (value, position) pairs
        // X = value, Y = position
        
        let denominator = n * sum_x2 - sum_x * sum_x;
        let slope = if denominator.abs() < 1e-10 {
            // Avoid division by zero
            if values.len() > 1 {
                (values[values.len() - 1] - values[0]) as f64 / (values.len() - 1) as f64
            } else {
                1.0
            }
        } else {
            (n * sum_xy - sum_x * sum_y) / denominator
        };
        
        let intercept = (sum_y - slope * sum_x) / n;
        
        Self { slope, intercept }
    }
    
    /// Predict the position of a key
    /// Returns the predicted position (may need local search to find exact position)
    pub fn predict(&self, key: i64) -> usize {
        // Invert the model: if we trained value = f(position), we need position = f^-1(value)
        // For linear: value = slope * position + intercept
        // So: position = (value - intercept) / slope
        let key_f64 = key as f64;
        let predicted = (key_f64 - self.intercept) / self.slope.max(1e-10);
        
        // Clamp to reasonable bounds
        let clamped = predicted.max(0.0).min(1e12);
        clamped.round() as usize
    }
    
    /// Get the model parameters
    pub fn slope(&self) -> f64 {
        self.slope
    }
    
    pub fn intercept(&self) -> f64 {
        self.intercept
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_linear_model_training() {
        // Test with sorted values
        let values = vec![10, 20, 30, 40, 50];
        let model = LinearModel::train(&values);
        
        // Model should predict positions reasonably
        assert!(model.slope > 0.0, "Slope should be positive for sorted data");
    }
    
    #[test]
    fn test_linear_model_prediction() {
        // Test prediction on sorted data
        let values: Vec<i64> = (0..100).map(|i| i * 10).collect();
        let model = LinearModel::train(&values);
        
        // Predict position of value 50 (should be around position 5)
        let pred = model.predict(50);
        assert!(pred >= 4 && pred <= 6, "Prediction should be close to actual position");
    }
}

