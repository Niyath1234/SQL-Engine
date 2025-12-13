//! Policy Registry - RBAC, SQL allowed verbs, query limits

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

/// RBAC Policy - what tables/columns a user can access
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RBACPolicy {
    /// User ID (None = default policy for unauthenticated)
    pub user_id: Option<String>,
    
    /// Allowed table names (empty = all tables)
    pub allowed_tables: HashSet<String>,
    
    /// Allowed column names per table (empty = all columns)
    pub allowed_columns: HashMap<String, HashSet<String>>,
    
    /// Denied table names (explicit deny)
    pub denied_tables: HashSet<String>,
}

impl Hash for RBACPolicy {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let Some(ref uid) = self.user_id {
            uid.hash(state);
        }
        let mut tables: Vec<_> = self.allowed_tables.iter().collect();
        tables.sort();
        for table in tables {
            table.hash(state);
        }
        // Hash denied tables
        let mut denied: Vec<_> = self.denied_tables.iter().collect();
        denied.sort();
        for table in denied {
            table.hash(state);
        }
    }
}

/// SQL Policy - what SQL verbs are allowed
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SQLPolicy {
    /// Allowed SQL verbs (e.g., ["SELECT", "WITH"])
    pub allowed_verbs: HashSet<String>,
    
    /// Allow DDL (CREATE, ALTER, DROP)
    pub allow_ddl: bool,
    
    /// Allow DML (INSERT, UPDATE, DELETE)
    pub allow_dml: bool,
}

impl Default for SQLPolicy {
    fn default() -> Self {
        let mut allowed = HashSet::new();
        allowed.insert("SELECT".to_string());
        allowed.insert("WITH".to_string());
        Self {
            allowed_verbs: allowed,
            allow_ddl: false,
            allow_dml: false,
        }
    }
}

impl Hash for SQLPolicy {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash allowed_verbs by sorting and hashing each element
        let mut verbs: Vec<_> = self.allowed_verbs.iter().collect();
        verbs.sort();
        for verb in verbs {
            verb.hash(state);
        }
        self.allow_ddl.hash(state);
        self.allow_dml.hash(state);
    }
}

/// Query Policy - limits and constraints
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryPolicy {
    /// Maximum rows to return (forced LIMIT)
    pub max_rows: Option<u64>,
    
    /// Maximum execution time (ms)
    pub max_time_ms: Option<u64>,
    
    /// Maximum cost (estimated)
    pub max_cost: Option<f64>,
    
    /// Allow approximate queries
    pub allow_approximate: bool,
}

impl Hash for QueryPolicy {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.max_rows.hash(state);
        self.max_time_ms.hash(state);
        // Option<f64> doesn't implement Hash, so we need to handle it manually
        if let Some(cost) = self.max_cost {
            // Convert to u64 for hashing (multiply by 1e6 to preserve precision)
            ((cost * 1_000_000.0) as u64).hash(state);
        } else {
            0u64.hash(state);
        }
        self.allow_approximate.hash(state);
    }
}

/// Policy Registry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicyRegistry {
    /// User ID → RBAC Policy
    rbac_policies: HashMap<Option<String>, RBACPolicy>,
    
    /// User ID → SQL Policy
    sql_policies: HashMap<Option<String>, SQLPolicy>,
    
    /// User ID → Query Policy
    query_policies: HashMap<Option<String>, QueryPolicy>,
    
    /// Default policies (for unauthenticated users)
    pub default_sql_policy: SQLPolicy,
    pub default_query_policy: QueryPolicy,
}

impl Hash for PolicyRegistry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash policies for deterministic cache keying
        let mut user_ids: Vec<_> = self.rbac_policies.keys().collect();
        user_ids.sort();
        for uid in user_ids {
            if let Some(policy) = self.rbac_policies.get(uid) {
                policy.hash(state);
            }
        }
        self.default_sql_policy.hash(state);
        self.default_query_policy.hash(state);
    }
}

impl PolicyRegistry {
    pub fn new() -> Self {
        Self {
            rbac_policies: HashMap::new(),
            sql_policies: HashMap::new(),
            query_policies: HashMap::new(),
            default_sql_policy: SQLPolicy::default(),
            default_query_policy: QueryPolicy {
                max_rows: Some(10000), // Default: 10k row limit
                max_time_ms: Some(30000), // Default: 30s timeout
                max_cost: None,
                allow_approximate: false,
            },
        }
    }
    
    /// Get RBAC policy for a user
    pub fn get_rbac_policy(&self, user_id: Option<&str>) -> Option<&RBACPolicy> {
        let key = user_id.map(|s| s.to_string());
        self.rbac_policies.get(&key)
    }
    
    /// Get SQL policy for a user
    pub fn get_sql_policy(&self, user_id: Option<&str>) -> &SQLPolicy {
        let key = user_id.map(|s| s.to_string());
        self.sql_policies.get(&key).unwrap_or(&self.default_sql_policy)
    }
    
    /// Get query policy for a user
    pub fn get_query_policy(&self, user_id: Option<&str>) -> &QueryPolicy {
        let key = user_id.map(|s| s.to_string());
        self.query_policies.get(&key).unwrap_or(&self.default_query_policy)
    }
    
    /// Register RBAC policy
    pub fn register_rbac_policy(&mut self, user_id: Option<String>, policy: RBACPolicy) {
        self.rbac_policies.insert(user_id, policy);
    }
    
    /// Register SQL policy
    pub fn register_sql_policy(&mut self, user_id: Option<String>, policy: SQLPolicy) {
        self.sql_policies.insert(user_id, policy);
    }
    
    /// Register query policy
    pub fn register_query_policy(&mut self, user_id: Option<String>, policy: QueryPolicy) {
        self.query_policies.insert(user_id, policy);
    }
}

impl Default for PolicyRegistry {
    fn default() -> Self {
        Self::new()
    }
}

