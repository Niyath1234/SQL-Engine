/// Factorized Query Processing - Store results in factorized form
/// Avoids materializing repeated substructures
use crate::storage::fragment::Value;
use std::collections::HashMap;
use std::hash::Hash;

/// Factorized representation of query results
/// Instead of flat tuples, stores nested structures
pub struct FactorizedRelation {
    /// Root level values
    root_values: Vec<Value>,
    /// Nested relations keyed by root value
    nested: HashMap<Value, FactorizedRelation>,
    /// Whether this is a leaf (no nesting)
    is_leaf: bool,
}

impl FactorizedRelation {
    pub fn new_leaf(values: Vec<Value>) -> Self {
        Self {
            root_values: values,
            nested: HashMap::new(),
            is_leaf: true,
        }
    }
    
    pub fn new_nested(root_values: Vec<Value>) -> Self {
        Self {
            root_values,
            nested: HashMap::new(),
            is_leaf: false,
        }
    }
    
    /// Add nested relation for a root value
    pub fn add_nested(&mut self, root_value: Value, nested: FactorizedRelation) {
        self.nested.insert(root_value, nested);
    }
    
    /// Get nested relation for a root value
    pub fn get_nested(&self, root_value: &Value) -> Option<&FactorizedRelation> {
        self.nested.get(root_value)
    }
    
    /// Materialize to flat tuples (de-factorize)
    pub fn materialize(&self) -> Vec<Vec<Value>> {
        let mut result = vec![];
        self.materialize_recursive(&mut result, vec![]);
        result
    }
    
    fn materialize_recursive(&self, result: &mut Vec<Vec<Value>>, prefix: Vec<Value>) {
        if self.is_leaf {
            // Leaf: output tuple
            let mut tuple = prefix.clone();
            tuple.extend_from_slice(&self.root_values);
            result.push(tuple);
        } else {
            // Nested: recurse for each root value
            for root_val in &self.root_values {
                let mut new_prefix = prefix.clone();
                new_prefix.push(root_val.clone());
                
                if let Some(nested) = self.nested.get(root_val) {
                    nested.materialize_recursive(result, new_prefix);
                } else {
                    // No nested relation, output with empty nested part
                    result.push(new_prefix);
                }
            }
        }
    }
    
    /// Get cardinality without materializing
    pub fn cardinality(&self) -> usize {
        if self.is_leaf {
            self.root_values.len()
        } else {
            self.root_values.len() * self.nested.values().map(|n| n.cardinality()).sum::<usize>()
        }
    }
}

/// Factorized aggregate - compute aggregates on factorized data
pub struct FactorizedAggregate {
    /// Aggregates per group key
    aggregates: HashMap<Vec<Value>, AggregateState>,
}

struct AggregateState {
    sum: f64,
    count: usize,
    min: Option<Value>,
    max: Option<Value>,
}

impl FactorizedAggregate {
    pub fn new() -> Self {
        Self {
            aggregates: HashMap::new(),
        }
    }
    
    /// Add value to aggregate for a group
    pub fn add(&mut self, group_key: Vec<Value>, value: Value) {
        let state = self.aggregates.entry(group_key).or_insert_with(|| AggregateState {
            sum: 0.0,
            count: 0,
            min: None,
            max: None,
        });
        
        // Update aggregates
        if let Value::Float64(v) = value {
            state.sum += v;
        } else if let Value::Int64(v) = value {
            state.sum += v as f64;
        }
        
        state.count += 1;
        
        // Update min/max
        match &mut state.min {
            None => state.min = Some(value.clone()),
            Some(current) => {
                if value < *current {
                    *current = value.clone();
                }
            }
        }
        
        match &mut state.max {
            None => state.max = Some(value.clone()),
            Some(current) => {
                if value > *current {
                    *current = value.clone();
                }
            }
        }
    }
    
    /// Get aggregate result for a group
    pub fn get(&self, group_key: &[Value]) -> Option<AggregateResult> {
        self.aggregates.get(group_key).map(|state| AggregateResult {
            sum: state.sum,
            count: state.count,
            avg: if state.count > 0 { state.sum / state.count as f64 } else { 0.0 },
            min: state.min.clone(),
            max: state.max.clone(),
        })
    }
    
    /// Materialize all aggregates
    pub fn materialize(&self) -> Vec<(Vec<Value>, AggregateResult)> {
        self.aggregates
            .iter()
            .map(|(key, state)| {
                (key.clone(), AggregateResult {
                    sum: state.sum,
                    count: state.count,
                    avg: if state.count > 0 { state.sum / state.count as f64 } else { 0.0 },
                    min: state.min.clone(),
                    max: state.max.clone(),
                })
            })
            .collect()
    }
}

pub struct AggregateResult {
    pub sum: f64,
    pub count: usize,
    pub avg: f64,
    pub min: Option<Value>,
    pub max: Option<Value>,
}

impl Default for FactorizedAggregate {
    fn default() -> Self {
        Self::new()
    }
}

/// Build factorized relation from join result
pub fn build_factorized_join(
    left: &FactorizedRelation,
    right: &FactorizedRelation,
    join_key: usize, // Index of join key in root_values
) -> FactorizedRelation {
    let mut result = FactorizedRelation::new_nested(vec![]);
    
    // For each value in left root
    for left_root in &left.root_values {
        // Find matching values in right
        if let Some(right_nested) = right.get_nested(left_root) {
            // Create nested relation
            let mut nested = FactorizedRelation::new_nested(vec![]);
            // TODO: Merge left and right nested relations
            result.add_nested(left_root.clone(), nested);
        }
    }
    
    result
}

