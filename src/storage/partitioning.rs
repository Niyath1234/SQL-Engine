/// Data partitioning for improved query performance
/// Partitions allow skipping entire data chunks during query execution
use crate::storage::fragment::{ColumnFragment, Value};
use crate::hypergraph::node::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Partition key specification
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionKey {
    /// Column name(s) used for partitioning
    pub columns: Vec<String>,
    /// Partition function (Hash, Range, List, etc.)
    pub function: PartitionFunction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PartitionFunction {
    /// Hash partitioning (distributes evenly)
    Hash { num_partitions: usize },
    /// Range partitioning (for sorted data like dates)
    Range { ranges: Vec<PartitionRange> },
    /// List partitioning (explicit value lists)
    List { partitions: Vec<Vec<Value>> },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionRange {
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub min_inclusive: bool,
    pub max_inclusive: bool,
}

/// Represents a single partition
#[derive(Clone, Debug)]
pub struct Partition {
    /// Partition ID
    pub id: PartitionId,
    /// Partition key values (for identification)
    pub key_values: Vec<Value>,
    /// Column fragments in this partition (organized by column name)
    pub column_fragments: HashMap<String, Vec<ColumnFragment>>,
    /// Statistics for this partition
    pub stats: PartitionStatistics,
    /// Node IDs associated with this partition
    pub node_ids: Vec<NodeId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionId(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionStatistics {
    pub row_count: usize,
    pub size_bytes: usize,
    pub min_values: HashMap<String, Value>,
    pub max_values: HashMap<String, Value>,
}

/// Partition manager for a table
#[derive(Clone, Debug)]
pub struct PartitionManager {
    /// Partition key specification
    pub partition_key: Option<PartitionKey>,
    /// All partitions for this table
    pub partitions: HashMap<PartitionId, Partition>,
    /// Mapping from partition key values to partition IDs
    pub key_to_partition: HashMap<Vec<Value>, PartitionId>,
}

impl PartitionManager {
    pub fn new() -> Self {
        Self {
            partition_key: None,
            partitions: HashMap::new(),
            key_to_partition: HashMap::new(),
        }
    }

    pub fn with_partition_key(partition_key: PartitionKey) -> Self {
        Self {
            partition_key: Some(partition_key),
            partitions: HashMap::new(),
            key_to_partition: HashMap::new(),
        }
    }

    /// Add a partition
    pub fn add_partition(&mut self, partition: Partition) {
        let partition_id = partition.id;
        let key_values = partition.key_values.clone();
        self.partitions.insert(partition_id, partition);
        self.key_to_partition.insert(key_values, partition_id);
    }

    /// Get partition by ID
    pub fn get_partition(&self, id: PartitionId) -> Option<&Partition> {
        self.partitions.get(&id)
    }

    /// Find partitions that match a predicate (for partition pruning)
    pub fn prune_partitions(
        &self,
        predicates: &[PartitionPredicate],
    ) -> Vec<PartitionId> {
        if self.partition_key.is_none() {
            // No partitioning - return all partitions
            return self.partitions.keys().copied().collect();
        }

        let mut matching_partitions = Vec::new();

        for (partition_id, partition) in &self.partitions {
            if self.matches_predicates(partition, predicates) {
                matching_partitions.push(*partition_id);
            }
        }

        matching_partitions
    }

    /// Check if a partition matches the given predicates
    fn matches_predicates(
        &self,
        partition: &Partition,
        predicates: &[PartitionPredicate],
    ) -> bool {
        for predicate in predicates {
            match predicate {
                PartitionPredicate::Equals { column, value } => {
                    if let Some(partition_value) = partition
                        .key_values
                        .iter()
                        .enumerate()
                        .find(|(i, _)| {
                            self.partition_key
                                .as_ref()
                                .map(|pk| pk.columns.get(*i).map(|c| c == column).unwrap_or(false))
                                .unwrap_or(false)
                        })
                        .map(|(_, v)| v)
                    {
                        if *partition_value != *value {
                            return false;
                        }
                    }
                }
                PartitionPredicate::Range {
                    column,
                    min,
                    max,
                    min_inclusive,
                    max_inclusive,
                } => {
                    if let Some(partition_value) = partition
                        .key_values
                        .iter()
                        .enumerate()
                        .find(|(i, _)| {
                            self.partition_key
                                .as_ref()
                                .map(|pk| pk.columns.get(*i).map(|c| c == column).unwrap_or(false))
                                .unwrap_or(false)
                        })
                        .map(|(_, v)| v)
                    {
                        // Check if partition range overlaps with query range
                        if let Some(min_val) = min {
                            if let Some(partition_max) = partition
                                .stats
                                .max_values
                                .get(column)
                            {
                                let cmp = partition_max.cmp(&min_val);
                                if cmp == std::cmp::Ordering::Less
                                    || (!min_inclusive && cmp == std::cmp::Ordering::Equal)
                                {
                                    return false;
                                }
                            }
                        }
                        if let Some(max_val) = max {
                            if let Some(partition_min) = partition
                                .stats
                                .min_values
                                .get(column)
                            {
                                let cmp = partition_min.cmp(&max_val);
                                if cmp == std::cmp::Ordering::Greater
                                    || (!max_inclusive && cmp == std::cmp::Ordering::Equal)
                                {
                                    return false;
                                }
                            }
                        }
                    }
                }
                PartitionPredicate::In { column, values } => {
                    if let Some(partition_value) = partition
                        .key_values
                        .iter()
                        .enumerate()
                        .find(|(i, _)| {
                            self.partition_key
                                .as_ref()
                                .map(|pk| pk.columns.get(*i).map(|c| c == column).unwrap_or(false))
                                .unwrap_or(false)
                        })
                        .map(|(_, v)| v)
                    {
                        if !values.iter().any(|v| v == partition_value) {
                            return false;
                        }
                    }
                }
            }
        }

        true
    }
}

/// Predicate for partition pruning
#[derive(Clone, Debug)]
pub enum PartitionPredicate {
    Equals { column: String, value: Value },
    Range {
        column: String,
        min: Option<Value>,
        max: Option<Value>,
        min_inclusive: bool,
        max_inclusive: bool,
    },
    In { column: String, values: Vec<Value> },
}

impl PartitionPredicate {
    /// Get the column name from the predicate
    pub fn get_column(&self) -> &str {
        match self {
            PartitionPredicate::Equals { column, .. } => column,
            PartitionPredicate::Range { column, .. } => column,
            PartitionPredicate::In { column, .. } => column,
        }
    }
}

// Value now implements Ord, so we can use it directly
// No need for compare_values helper function anymore

impl Default for PartitionManager {
    fn default() -> Self {
        Self::new()
    }
}

