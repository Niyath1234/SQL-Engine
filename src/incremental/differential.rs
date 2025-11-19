/// Differential Dataflow - Incremental computation with multiset weights
/// Based on Naiad/differential dataflow concepts
use crate::storage::fragment::Value;
use std::collections::HashMap;
use std::hash::Hash;

/// Multiset weight: +1 for insertion, -1 for deletion
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Weight(pub i64);

impl Weight {
    pub fn insertion() -> Self {
        Weight(1)
    }
    
    pub fn deletion() -> Self {
        Weight(-1)
    }
    
    pub fn zero() -> Self {
        Weight(0)
    }
    
    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }
    
    pub fn combine(&self, other: Weight) -> Weight {
        Weight(self.0 + other.0)
    }
}

/// Differential collection - represents changes with weights
#[derive(Clone, Debug)]
pub struct DifferentialCollection<T: Clone + Hash + Eq> {
    /// Map from tuple to weight
    weights: HashMap<T, Weight>,
    /// Stable state (committed)
    stable: HashMap<T, Weight>,
    /// Recent deltas (not yet committed)
    deltas: HashMap<T, Weight>,
}

impl<T: Clone + Hash + Eq> DifferentialCollection<T> {
    pub fn new() -> Self {
        Self {
            weights: HashMap::new(),
            stable: HashMap::new(),
            deltas: HashMap::new(),
        }
    }
    
    /// Add a tuple with weight
    pub fn add(&mut self, tuple: T, weight: Weight) {
        let entry = self.weights.entry(tuple.clone()).or_insert(Weight::zero());
        *entry = entry.combine(weight);
        
        // Also add to deltas
        let delta_entry = self.deltas.entry(tuple).or_insert(Weight::zero());
        *delta_entry = delta_entry.combine(weight);
    }
    
    /// Insert a tuple
    pub fn insert(&mut self, tuple: T) {
        self.add(tuple, Weight::insertion());
    }
    
    /// Delete a tuple
    pub fn delete(&mut self, tuple: T) {
        self.add(tuple, Weight::deletion());
    }
    
    /// Commit deltas to stable state
    pub fn commit(&mut self) {
        for (tuple, delta_weight) in self.deltas.drain() {
            let stable_entry = self.stable.entry(tuple).or_insert(Weight::zero());
            *stable_entry = stable_entry.combine(delta_weight);
        }
    }
    
    /// Get weight for a tuple
    pub fn get_weight(&self, tuple: &T) -> Weight {
        self.weights.get(tuple).copied().unwrap_or(Weight::zero())
    }
    
    /// Iterate over all tuples with non-zero weights
    pub fn iter(&self) -> impl Iterator<Item = (&T, &Weight)> {
        self.weights.iter().filter(|(_, w)| !w.is_zero())
    }
    
    /// Check if collection is empty
    pub fn is_empty(&self) -> bool {
        self.weights.values().all(|w| w.is_zero())
    }
}

impl<T: Clone + Hash + Eq> Default for DifferentialCollection<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Differential join operator
pub struct DifferentialJoin<T: Clone + Hash + Eq> {
    left: DifferentialCollection<T>,
    right: DifferentialCollection<T>,
    join_key_fn: Box<dyn Fn(&T) -> Value>,
}

impl<T: Clone + Hash + Eq> DifferentialJoin<T> {
    pub fn new(
        left: DifferentialCollection<T>,
        right: DifferentialCollection<T>,
        join_key_fn: Box<dyn Fn(&T) -> Value>,
    ) -> Self {
        Self {
            left,
            right,
            join_key_fn,
        }
    }
    
    /// Compute join output for new deltas
    pub fn compute_delta(
        &self,
        left_delta: &DifferentialCollection<T>,
        right_delta: &DifferentialCollection<T>,
    ) -> DifferentialCollection<(T, T)> {
        let mut result = DifferentialCollection::new();
        
        // Join left deltas with right stable
        for (left_tuple, left_weight) in left_delta.iter() {
            let left_key = (self.join_key_fn)(left_tuple);
            
            // Find matching tuples in right stable
            for (right_tuple, right_weight) in self.right.stable.iter() {
                let right_key = (self.join_key_fn)(right_tuple);
                if left_key == right_key {
                    let output_weight = Weight(left_weight.0 * right_weight.0);
                    if !output_weight.is_zero() {
                        result.add((left_tuple.clone(), right_tuple.clone()), output_weight);
                    }
                }
            }
        }
        
        // Join right deltas with left stable
        for (right_tuple, right_weight) in right_delta.iter() {
            let right_key = (self.join_key_fn)(right_tuple);
            
            for (left_tuple, left_weight) in self.left.stable.iter() {
                let left_key = (self.join_key_fn)(left_tuple);
                if left_key == right_key {
                    let output_weight = Weight(left_weight.0 * right_weight.0);
                    if !output_weight.is_zero() {
                        result.add((left_tuple.clone(), right_tuple.clone()), output_weight);
                    }
                }
            }
        }
        
        // Join left deltas with right deltas
        for (left_tuple, left_weight) in left_delta.iter() {
            let left_key = (self.join_key_fn)(left_tuple);
            
            for (right_tuple, right_weight) in right_delta.iter() {
                let right_key = (self.join_key_fn)(right_tuple);
                if left_key == right_key {
                    let output_weight = Weight(left_weight.0 * right_weight.0);
                    if !output_weight.is_zero() {
                        result.add((left_tuple.clone(), right_tuple.clone()), output_weight);
                    }
                }
            }
        }
        
        result
    }
}

/// Differential aggregate operator
pub struct DifferentialAggregate<T: Clone + Hash + Eq, K: Clone + Hash + Eq> {
    /// Group key function
    key_fn: Box<dyn Fn(&T) -> K>,
    /// Aggregate function
    agg_fn: Box<dyn Fn(&[Weight]) -> Weight>,
    /// Current aggregates
    aggregates: HashMap<K, Weight>,
}

impl<T: Clone + Hash + Eq, K: Clone + Hash + Eq> DifferentialAggregate<T, K> {
    pub fn new(
        key_fn: Box<dyn Fn(&T) -> K>,
        agg_fn: Box<dyn Fn(&[Weight]) -> Weight>,
    ) -> Self {
        Self {
            key_fn,
            agg_fn,
            aggregates: HashMap::new(),
        }
    }
    
    /// Update aggregates with new deltas
    pub fn update(&mut self, deltas: &DifferentialCollection<T>) -> HashMap<K, Weight> {
        let mut changes = HashMap::new();
        
        for (tuple, weight) in deltas.iter() {
            let key = (self.key_fn)(tuple);
            let old_agg = self.aggregates.get(&key).copied().unwrap_or(Weight::zero());
            
            // Compute new aggregate
            let new_agg = (self.agg_fn)(&[old_agg, *weight]);
            
            if new_agg != old_agg {
                changes.insert(key.clone(), Weight(new_agg.0 - old_agg.0));
                self.aggregates.insert(key, new_agg);
            }
        }
        
        changes
    }
    
    /// Get current aggregate for a key
    pub fn get_aggregate(&self, key: &K) -> Weight {
        self.aggregates.get(key).copied().unwrap_or(Weight::zero())
    }
}

/// Helper to create sum aggregate function
pub fn sum_aggregate(weights: &[Weight]) -> Weight {
    weights.iter().fold(Weight::zero(), |acc, w| acc.combine(*w))
}

/// Helper to create count aggregate function
pub fn count_aggregate(weights: &[Weight]) -> Weight {
    weights.iter().fold(Weight::zero(), |acc, w| {
        Weight(acc.0 + if w.0 > 0 { 1 } else { 0 })
    })
}

