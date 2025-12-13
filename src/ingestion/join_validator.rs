//! Join Validator - Validates joins against rules and schema

use crate::worldstate::rules::JoinRuleRegistry;
use crate::worldstate::schema::SchemaRegistry;
use crate::worldstate::keys::KeyRegistry;
use anyhow::{Result, Context};

/// Join validation result
#[derive(Clone, Debug)]
pub enum JoinValidationResult {
    /// Join is valid (approved rule exists)
    Valid {
        rule_id: String,
        rule: crate::worldstate::rules::JoinRule,
    },
    
    /// Join is invalid (no approved rule)
    Invalid {
        reason: String,
        suggested_rules: Vec<String>, // Rule IDs that could be approved
    },
    
    /// Join would create Cartesian product
    CartesianProduct {
        reason: String,
    },
}

/// Join Validator - Enforces rule-based join safety
pub struct JoinValidator;

impl JoinValidator {
    pub fn new() -> Self {
        Self
    }
    
    /// Validate a join between two tables
    pub fn validate_join(
        &self,
        rule_registry: &JoinRuleRegistry,
        schema_registry: &SchemaRegistry,
        _key_registry: &KeyRegistry,
        left_table: &str,
        left_key: &[String],
        right_table: &str,
        right_key: &[String],
        join_type: &str,
    ) -> JoinValidationResult {
        // Check 1: No Cartesian products (must have join keys)
        if left_key.is_empty() || right_key.is_empty() {
            return JoinValidationResult::CartesianProduct {
                reason: "Join keys are empty - would create Cartesian product".to_string(),
            };
        }
        
        // Check 2: Keys must exist in schemas
        if !self.keys_exist(schema_registry, left_table, left_key) {
            return JoinValidationResult::Invalid {
                reason: format!("Left join keys {:?} not found in table {}", left_key, left_table),
                suggested_rules: Vec::new(),
            };
        }
        
        if !self.keys_exist(schema_registry, right_table, right_key) {
            return JoinValidationResult::Invalid {
                reason: format!("Right join keys {:?} not found in table {}", right_key, right_table),
                suggested_rules: Vec::new(),
            };
        }
        
        // Check 3: Type compatibility
        if !self.types_compatible(schema_registry, left_table, left_key, right_table, right_key) {
            return JoinValidationResult::Invalid {
                reason: "Join keys have incompatible types".to_string(),
                suggested_rules: Vec::new(),
            };
        }
        
        // Check 4: Must have approved rule
        let approved_rules = rule_registry.get_approved_rules(left_table, right_table);
        
        // Check if any approved rule matches this join
        for rule in &approved_rules {
            if self.rule_matches(rule, left_table, left_key, right_table, right_key, join_type) {
                return JoinValidationResult::Valid {
                    rule_id: rule.id.clone(),
                    rule: (*rule).clone(),
                };
            }
        }
        
        // No approved rule found - check for proposed rules
        let all_rules = rule_registry.list_all_rules();
        let proposed: Vec<_> = all_rules.iter()
            .filter(|r| matches!(r.state, crate::worldstate::rules::RuleState::Proposed))
            .filter(|r| {
                (r.left_table == left_table && r.right_table == right_table) ||
                (r.left_table == right_table && r.right_table == left_table)
            })
            .map(|r| r.id.clone())
            .collect();
        
        JoinValidationResult::Invalid {
            reason: format!(
                "No approved join rule found for {} JOIN {} ON {:?} = {:?}",
                left_table, right_table, left_key, right_key
            ),
            suggested_rules: proposed,
        }
    }
    
    /// Check if keys exist in table schema
    fn keys_exist(
        &self,
        schema_registry: &SchemaRegistry,
        table: &str,
        keys: &[String],
    ) -> bool {
        let schema = match schema_registry.get_table(table) {
            Some(s) => s,
            None => return false,
        };
        
        let column_names: std::collections::HashSet<&str> = schema.columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        
        keys.iter().all(|key| column_names.contains(key.as_str()))
    }
    
    /// Check if key types are compatible
    fn types_compatible(
        &self,
        schema_registry: &SchemaRegistry,
        left_table: &str,
        left_keys: &[String],
        right_table: &str,
        right_keys: &[String],
    ) -> bool {
        if left_keys.len() != right_keys.len() {
            return false;
        }
        
        let left_schema = match schema_registry.get_table(left_table) {
            Some(s) => s,
            None => return false,
        };
        
        let right_schema = match schema_registry.get_table(right_table) {
            Some(s) => s,
            None => return false,
        };
        
        for (left_key, right_key) in left_keys.iter().zip(right_keys.iter()) {
            let left_col = left_schema.get_column(left_key);
            let right_col = right_schema.get_column(right_key);
            
            match (left_col, right_col) {
                (Some(l), Some(r)) => {
                    if !self.types_compatible_single(&l.data_type, &r.data_type) {
                        return false;
                    }
                }
                _ => return false,
            }
        }
        
        true
    }
    
    /// Check if two types are compatible for joining
    fn types_compatible_single(&self, type1: &str, type2: &str) -> bool {
        // Normalize types
        let t1 = type1.to_uppercase();
        let t2 = type2.to_uppercase();
        
        // Exact match
        if t1 == t2 {
            return true;
        }
        
        // Numeric compatibility
        let numeric_types = ["INT", "INT64", "FLOAT", "FLOAT64"];
        if numeric_types.contains(&t1.as_str()) && numeric_types.contains(&t2.as_str()) {
            return true;
        }
        
        // String types
        if (t1 == "VARCHAR" || t1 == "UTF8" || t1 == "STRING") &&
           (t2 == "VARCHAR" || t2 == "UTF8" || t2 == "STRING") {
            return true;
        }
        
        false
    }
    
    /// Check if a rule matches the join being validated
    fn rule_matches(
        &self,
        rule: &crate::worldstate::rules::JoinRule,
        left_table: &str,
        left_key: &[String],
        right_table: &str,
        right_key: &[String],
        join_type: &str,
    ) -> bool {
        // Check table match (both directions)
        let tables_match = (rule.left_table == left_table && rule.right_table == right_table) ||
                          (rule.left_table == right_table && rule.right_table == left_table);
        
        if !tables_match {
            return false;
        }
        
        // Check key match (handle both directions)
        let keys_match = if rule.left_table == left_table {
            rule.left_key == left_key && rule.right_key == right_key
        } else {
            rule.left_key == right_key && rule.right_key == left_key
        };
        
        // Check join type (if specified)
        let type_match = rule.join_type.to_lowercase() == join_type.to_lowercase() ||
                        rule.join_type.is_empty();
        
        tables_match && keys_match && type_match
    }
}

impl Default for JoinValidator {
    fn default() -> Self {
        Self::new()
    }
}

