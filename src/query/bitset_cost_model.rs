/// Bitset Engine V3: Cost Model
/// 
/// Planner decisions to choose bitset join vs hash join
/// Based on distinct_count, selectivity, table sizes, and bitmap density

/// Bitset cost model for join selection
pub struct BitsetCostModel;

impl BitsetCostModel {
    /// Estimate cost of bitset join
    pub fn cost_estimate_bitset_join(
        distinct_count: usize,
        filter_selectivity: f64,
        fact_table_size: usize,
        dimension_size: usize,
        bitmap_density: f64,
    ) -> f64 {
        // Base cost: building bitmap indexes
        let build_cost = (dimension_size as f64) * 0.1; // 0.1 cost per dimension row
        
        // Cost: evaluating filters into bitsets
        let filter_cost = dimension_size as f64 * filter_selectivity * 0.05;
        
        // Cost: converting dimension bitsets to fact bitsets
        let conversion_cost = fact_table_size as f64 * 0.01;
        
        // Cost: intersecting bitsets
        let intersect_cost = fact_table_size as f64 * bitmap_density * 0.02;
        
        // Cost: materialization
        let materialize_cost = fact_table_size as f64 * filter_selectivity * 0.1;
        
        build_cost + filter_cost + conversion_cost + intersect_cost + materialize_cost
    }
    
    /// Estimate cost of hash join
    pub fn cost_estimate_hash_join(
        fact_table_size: usize,
        dimension_size: usize,
        filter_selectivity: f64,
    ) -> f64 {
        // Build phase: build hash table from dimension
        let build_cost = dimension_size as f64 * 0.2;
        
        // Probe phase: probe fact table
        let probe_cost = fact_table_size as f64 * 0.15;
        
        // Materialization
        let materialize_cost = fact_table_size as f64 * filter_selectivity * 0.1;
        
        build_cost + probe_cost + materialize_cost
    }
    
    /// Decide if should use bitset join
    pub fn should_use_bitset_join(
        distinct_count: usize,
        filter_selectivity: f64,
        fact_table_size: usize,
        dimension_size: usize,
        bitmap_density: f64,
        is_star_schema: bool,
    ) -> bool {
        // High cardinality threshold
        const HIGH_CARD_THRESHOLD: usize = 1_000_000;
        if distinct_count > HIGH_CARD_THRESHOLD {
            return false; // Use hash join
        }
        
        // Star schema: always use bitset join
        if is_star_schema {
            return true;
        }
        
        // Low selectivity: bitset join is better
        if filter_selectivity < 0.2 {
            return true;
        }
        
        // Compare costs
        let bitset_cost = Self::cost_estimate_bitset_join(
            distinct_count,
            filter_selectivity,
            fact_table_size,
            dimension_size,
            bitmap_density,
        );
        
        let hash_cost = Self::cost_estimate_hash_join(
            fact_table_size,
            dimension_size,
            filter_selectivity,
        );
        
        bitset_cost < hash_cost
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_cost_model() {
        // Low cardinality, low selectivity → bitset join
        assert!(BitsetCostModel::should_use_bitset_join(
            1000,
            0.1,
            1_000_000,
            10_000,
            0.05,
            false,
        ));
        
        // High cardinality → hash join
        assert!(!BitsetCostModel::should_use_bitset_join(
            2_000_000,
            0.1,
            1_000_000,
            10_000,
            0.05,
            false,
        ));
    }
}

