//! Join Rule Registry - Authoritative join relationships

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Join rule state (approval workflow)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum RuleState {
    /// Proposed (inference suggested this, not yet approved)
    Proposed,
    
    /// Approved (authoritative, can be used in queries)
    Approved,
    
    /// Blocked (explicitly disallowed)
    Blocked,
    
    /// Deprecated (was approved, now deprecated)
    Deprecated,
}

/// Join rule - authoritative join relationship
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinRule {
    /// Unique rule ID
    pub id: String,
    
    /// Left table name
    pub left_table: String,
    
    /// Left key column(s)
    pub left_key: Vec<String>,
    
    /// Right table name
    pub right_table: String,
    
    /// Right key column(s)
    pub right_key: Vec<String>,
    
    /// Join type (inner, left, etc.)
    pub join_type: String,
    
    /// Expected cardinality (1:1, 1:N, N:M)
    pub cardinality: String,
    
    /// Rule state
    pub state: RuleState,
    
    /// Confidence score (0.0-1.0) if inferred
    pub confidence: Option<f64>,
    
    /// Justification/reason for this rule
    pub justification: Option<String>,
    
    /// Timestamp when rule was created
    pub created_at: u64,
    
    /// Timestamp when rule was last updated
    pub updated_at: u64,
}

impl Hash for JoinRule {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.left_table.hash(state);
        let mut left_key = self.left_key.clone();
        left_key.sort();
        left_key.hash(state);
        self.right_table.hash(state);
        let mut right_key = self.right_key.clone();
        right_key.sort();
        right_key.hash(state);
        self.join_type.hash(state);
        self.cardinality.hash(state);
        self.state.hash(state);
    }
}

impl JoinRule {
    pub fn new(
        id: String,
        left_table: String,
        left_key: Vec<String>,
        right_table: String,
        right_key: Vec<String>,
        join_type: String,
        cardinality: String,
    ) -> Self {
        let now = Self::now_timestamp();
        Self {
            id,
            left_table,
            left_key,
            right_table,
            right_key,
            join_type,
            cardinality,
            state: RuleState::Proposed,
            confidence: None,
            justification: None,
            created_at: now,
            updated_at: now,
        }
    }
    
    /// Approve this rule
    pub fn approve(&mut self) {
        self.state = RuleState::Approved;
        self.updated_at = Self::now_timestamp();
    }
    
    /// Check if rule is approved (can be used in queries)
    pub fn is_approved(&self) -> bool {
        matches!(self.state, RuleState::Approved)
    }
    
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

/// Join Rule Registry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinRuleRegistry {
    /// Rule ID → JoinRule
    rules: HashMap<String, JoinRule>,
    
    /// Table pair → Rule IDs (for fast lookup)
    table_pairs: HashMap<(String, String), Vec<String>>,
}

impl Hash for JoinRuleRegistry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Only hash approved rules for deterministic hashing
        let mut approved_rules: Vec<_> = self.rules
            .values()
            .filter(|r| r.is_approved())
            .collect();
        approved_rules.sort_by_key(|r| &r.id);
        for rule in approved_rules {
            rule.hash(state);
        }
    }
}

impl JoinRuleRegistry {
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
            table_pairs: HashMap::new(),
        }
    }
    
    /// Register a join rule
    pub fn register_rule(&mut self, rule: JoinRule) {
        let id = rule.id.clone();
        let left = rule.left_table.clone();
        let right = rule.right_table.clone();
        
        // Store rule
        self.rules.insert(id.clone(), rule);
        
        // Index by table pair (both directions)
        self.table_pairs
            .entry((left.clone(), right.clone()))
            .or_insert_with(Vec::new)
            .push(id.clone());
        self.table_pairs
            .entry((right, left))
            .or_insert_with(Vec::new)
            .push(id);
    }
    
    /// Get rule by ID
    pub fn get_rule(&self, rule_id: &str) -> Option<&JoinRule> {
        self.rules.get(rule_id)
    }
    
    /// Get approved rules between two tables
    pub fn get_approved_rules(&self, left_table: &str, right_table: &str) -> Vec<&JoinRule> {
        self.table_pairs
            .get(&(left_table.to_string(), right_table.to_string()))
            .iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.rules.get(id))
            .filter(|r| r.is_approved())
            .collect()
    }
    
    /// List all approved rules
    pub fn list_approved_rules(&self) -> Vec<&JoinRule> {
        self.rules.values().filter(|r| r.is_approved()).collect()
    }
    
    /// List all rules (including proposed)
    pub fn list_all_rules(&self) -> Vec<&JoinRule> {
        self.rules.values().collect()
    }
}

impl Default for JoinRuleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Filter rule - business rule that applies default filters to tables
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilterRule {
    /// Unique rule ID
    pub id: String,
    
    /// Table name this rule applies to
    pub table_name: String,
    
    /// Column name to filter
    pub column: String,
    
    /// Operator: "eq", "ne", "gt", "gte", "lt", "lte", "in", "like", "between"
    pub operator: String,
    
    /// Value to filter by (JSON value: string, number, array, etc.)
    pub value: serde_json::Value,
    
    /// Whether this rule is mandatory (always apply) or optional
    pub mandatory: bool,
    
    /// Rule state (Approved/Blocked/Deprecated)
    pub state: RuleState,
    
    /// Justification/reason for this rule
    pub justification: Option<String>,
    
    /// Timestamp when rule was created
    pub created_at: u64,
    
    /// Timestamp when rule was last updated
    pub updated_at: u64,
}

impl FilterRule {
    pub fn new(
        id: String,
        table_name: String,
        column: String,
        operator: String,
        value: serde_json::Value,
        mandatory: bool,
    ) -> Self {
        let now = Self::now_timestamp();
        Self {
            id,
            table_name,
            column,
            operator,
            value,
            mandatory,
            state: RuleState::Proposed,
            justification: None,
            created_at: now,
            updated_at: now,
        }
    }
    
    /// Approve this rule
    pub fn approve(&mut self) {
        self.state = RuleState::Approved;
        self.updated_at = Self::now_timestamp();
    }
    
    /// Check if rule is approved (can be used in queries)
    pub fn is_approved(&self) -> bool {
        matches!(self.state, RuleState::Approved)
    }
    
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

/// Filter Rule Registry - stores business filter rules
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilterRuleRegistry {
    /// Rule ID → FilterRule
    rules: HashMap<String, FilterRule>,
    
    /// Table name → Rule IDs (for fast lookup)
    table_rules: HashMap<String, Vec<String>>,
}

impl FilterRuleRegistry {
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
            table_rules: HashMap::new(),
        }
    }
    
    /// Register a filter rule
    pub fn register_rule(&mut self, rule: FilterRule) {
        let id = rule.id.clone();
        let table = rule.table_name.clone();
        
        // Store rule
        self.rules.insert(id.clone(), rule);
        
        // Index by table
        self.table_rules
            .entry(table)
            .or_insert_with(Vec::new)
            .push(id);
    }
    
    /// Get rule by ID
    pub fn get_rule(&self, rule_id: &str) -> Option<&FilterRule> {
        self.rules.get(rule_id)
    }
    
    /// Get approved mandatory rules for a table
    pub fn get_mandatory_rules(&self, table_name: &str) -> Vec<&FilterRule> {
        self.table_rules
            .get(table_name)
            .iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.rules.get(id))
            .filter(|r| r.is_approved() && r.mandatory)
            .collect()
    }
    
    /// Get all approved rules for a table (mandatory + optional)
    pub fn get_approved_rules(&self, table_name: &str) -> Vec<&FilterRule> {
        self.table_rules
            .get(table_name)
            .iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.rules.get(id))
            .filter(|r| matches!(r.state, RuleState::Approved))
            .collect()
    }
    
    /// List all approved rules
    pub fn list_approved_rules(&self) -> Vec<&FilterRule> {
        self.rules.values()
            .filter(|r| matches!(r.state, RuleState::Approved))
            .collect()
    }
}

impl Default for FilterRuleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

