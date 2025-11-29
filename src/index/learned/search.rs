/// Learned index search implementation
/// Uses linear model to predict position, then performs local search
use super::model::LinearModel;

/// Learned index for fast position prediction
pub struct LearnedIndex {
    pub model: LinearModel,
}

impl LearnedIndex {
    /// Create a new learned index from a trained model
    pub fn new(model: LinearModel) -> Self {
        Self { model }
    }
    
    /// Build a learned index from sorted values
    pub fn build(values: &[i64]) -> Self {
        let model = LinearModel::train(values);
        Self::new(model)
    }
    
    /// Search for a key in sorted values
    /// Returns the position of the first value >= key
    /// Uses model prediction + local search window to handle prediction errors
    pub fn search(&self, key: i64, values: &[i64]) -> usize {
        if values.is_empty() {
            return 0;
        }
        
        // Predict position using learned model
        let guess = self.model.predict(key);
        
        // Local search window to handle model prediction errors
        // Search in a window around the predicted position
        let window_size = 32;
        let start = guess.saturating_sub(window_size);
        let end = (guess + window_size).min(values.len());
        
        // Linear search in the window
        for i in start..end {
            if values[i] >= key {
                return i;
            }
        }
        
        // If not found in window, return end position
        // (key is larger than all values in the window)
        if end < values.len() {
            // Continue search from end of window
            for i in end..values.len() {
                if values[i] >= key {
                    return i;
                }
            }
        }
        
        // Key not found, return length (position after last element)
        values.len()
    }
    
    /// Search for exact key match
    /// Returns Some(position) if key is found, None otherwise
    pub fn search_exact(&self, key: i64, values: &[i64]) -> Option<usize> {
        let pos = self.search(key, values);
        if pos < values.len() && values[pos] == key {
            Some(pos)
        } else {
            None
        }
    }
    
    /// Search for range [min_key, max_key]
    /// Returns (start_pos, end_pos) where start_pos is inclusive and end_pos is exclusive
    pub fn search_range(&self, min_key: i64, max_key: i64, values: &[i64]) -> (usize, usize) {
        let start_pos = self.search(min_key, values);
        let end_pos = self.search(max_key + 1, values); // +1 to make end exclusive
        (start_pos, end_pos)
    }
    
    /// Get the model used by this index
    pub fn model(&self) -> &LinearModel {
        &self.model
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_learned_index_search() {
        // Create sorted values
        let values: Vec<i64> = (0..1000).map(|i| i * 10).collect();
        let index = LearnedIndex::build(&values);
        
        // Search for value 500 (should be at position 50)
        let pos = index.search(500, &values);
        assert_eq!(pos, 50, "Should find value 500 at position 50");
        
        // Search for value 501 (should be at position 51)
        let pos = index.search(501, &values);
        assert_eq!(pos, 51, "Should find value 501 at position 51");
    }
    
    #[test]
    fn test_learned_index_search_range() {
        let values: Vec<i64> = (0..1000).map(|i| i * 10).collect();
        let index = LearnedIndex::build(&values);
        
        // Search for range [500, 600]
        let (start, end) = index.search_range(500, 600, &values);
        assert_eq!(start, 50, "Start should be position 50");
        assert_eq!(end, 61, "End should be position 61 (exclusive)");
    }
}

