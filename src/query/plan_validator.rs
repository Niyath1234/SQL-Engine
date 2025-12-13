//! Plan Validator - Validates query plans against join rules

use crate::query::plan::{QueryPlan, PlanOperator};
use crate::worldstate::rules::JoinRuleRegistry;
use crate::worldstate::schema::SchemaRegistry;
use crate::worldstate::keys::KeyRegistry;
use crate::ingestion::join_validator::JoinValidator;
use anyhow::Result;

/// Plan Validator - Validates plans against WorldState rules
pub struct PlanValidator;

impl PlanValidator {
    pub fn new() -> Self {
        Self
    }
    
    /// Validate a query plan against join rules
    pub fn validate_plan(
        &self,
        plan: &QueryPlan,
        rule_registry: &JoinRuleRegistry,
        schema_registry: &SchemaRegistry,
        _key_registry: &KeyRegistry,
    ) -> Result<()> {
        // Extract all joins from the plan
        let joins = self.extract_joins(&plan.root);
        
        // Validate each join
        let validator = JoinValidator::new();
        for join in joins {
            let validation = validator.validate_join(
                rule_registry,
                schema_registry,
                _key_registry,
                &join.left_table,
                &join.left_keys,
                &join.right_table,
                &join.right_keys,
                &join.join_type,
            );
            
            match validation {
                crate::ingestion::JoinValidationResult::Valid { .. } => {
                    // Join is approved, continue
                }
                crate::ingestion::JoinValidationResult::Invalid { reason, suggested_rules } => {
                    return Err(anyhow::anyhow!(
                        "Join validation failed: {}\n\nSuggested rules to approve: {:?}",
                        reason,
                        suggested_rules
                    ));
                }
                crate::ingestion::JoinValidationResult::CartesianProduct { reason } => {
                    return Err(anyhow::anyhow!(
                        "Join would create Cartesian product: {}",
                        reason
                    ));
                }
            }
        }
        
        Ok(())
    }
    
    /// Extract all joins from a plan operator tree
    fn extract_joins(&self, op: &PlanOperator) -> Vec<JoinInfo> {
        let mut joins = Vec::new();
        self.extract_joins_recursive(op, &mut joins);
        joins
    }
    
    /// Recursively extract joins from plan tree
    fn extract_joins_recursive(&self, op: &PlanOperator, joins: &mut Vec<JoinInfo>) {
        match op {
            PlanOperator::Join { predicate, join_type, .. } => {
                joins.push(JoinInfo {
                    left_table: predicate.left.0.clone(),
                    left_keys: vec![predicate.left.1.clone()],
                    right_table: predicate.right.0.clone(),
                    right_keys: vec![predicate.right.1.clone()],
                    join_type: format!("{:?}", join_type).to_lowercase(),
                });
            }
            PlanOperator::BitsetJoin { predicate, join_type, .. } => {
                joins.push(JoinInfo {
                    left_table: predicate.left.0.clone(),
                    left_keys: vec![predicate.left.1.clone()],
                    right_table: predicate.right.0.clone(),
                    right_keys: vec![predicate.right.1.clone()],
                    join_type: format!("{:?}", join_type).to_lowercase(),
                });
            }
            _ => {
                // Recursively check children based on operator type
                match op {
                    PlanOperator::Join { left, right, .. } => {
                        self.extract_joins_recursive(left, joins);
                        self.extract_joins_recursive(right, joins);
                    }
                    PlanOperator::BitsetJoin { left, right, .. } => {
                        self.extract_joins_recursive(left, joins);
                        self.extract_joins_recursive(right, joins);
                    }
                    PlanOperator::Filter { input, .. } => {
                        self.extract_joins_recursive(input, joins);
                    }
                    PlanOperator::Project { input, .. } => {
                        self.extract_joins_recursive(input, joins);
                    }
                    PlanOperator::Aggregate { input, .. } => {
                        self.extract_joins_recursive(input, joins);
                    }
                    PlanOperator::Sort { input, .. } => {
                        self.extract_joins_recursive(input, joins);
                    }
                    PlanOperator::Limit { input, .. } => {
                        self.extract_joins_recursive(input, joins);
                    }
                    _ => {
                        // No children or unknown operator
                    }
                }
            }
        }
    }
}

/// Join information extracted from plan
#[derive(Clone, Debug)]
struct JoinInfo {
    left_table: String,
    left_keys: Vec<String>,
    right_table: String,
    right_keys: Vec<String>,
    join_type: String,
}

impl Default for PlanValidator {
    fn default() -> Self {
        Self::new()
    }
}

