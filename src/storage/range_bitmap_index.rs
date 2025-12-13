/// Bitset Engine V3: Range Bitmap Index
/// 
/// Interval trees of bitsets for fast range predicates
/// Enables efficient BETWEEN and range queries

use crate::storage::bitset_v3::Bitset;
use crate::storage::fragment::Value;
use std::sync::Arc;
use anyhow::Result;

/// Segment tree node for range queries
struct SegmentTreeNode {
    /// Range start (inclusive)
    range_start: Value,
    
    /// Range end (inclusive)
    range_end: Value,
    
    /// Bitset of rows in this range
    bitset: Bitset,
    
    /// Left child (smaller range)
    left: Option<Arc<SegmentTreeNode>>,
    
    /// Right child (larger range)
    right: Option<Arc<SegmentTreeNode>>,
}

/// Range bitmap index for fast range queries
pub struct RangeBitmapIndex {
    /// Root of segment tree
    root: Option<Arc<SegmentTreeNode>>,
    
    /// Total number of rows
    row_count: usize,
    
    /// Column name
    column_name: String,
}

impl RangeBitmapIndex {
    /// Build range bitmap index from column values
    pub fn build_from_column(
        column_name: String,
        values: &[Value],
    ) -> Result<Self> {
        if values.is_empty() {
            return Ok(Self {
                root: None,
                row_count: 0,
                column_name,
            });
        }
        
        // Find min and max values
        let (min_val, max_val) = Self::find_min_max(values)?;
        
        // Build segment tree
        let root = Self::build_segment_tree(values, &min_val, &max_val, 0, values.len() - 1)?;
        
        Ok(Self {
            root: Some(Arc::new(root)),
            row_count: values.len(),
            column_name,
        })
    }
    
    /// Query range and return bitset of matching rows
    pub fn query_range(&self, lo: &Value, hi: &Value) -> Bitset {
        if let Some(ref root) = self.root {
            Self::query_range_recursive(root, lo, hi, self.row_count)
        } else {
            Bitset::new(0)
        }
    }
    
    /// Recursive range query
    fn query_range_recursive(
        node: &SegmentTreeNode,
        lo: &Value,
        hi: &Value,
        capacity: usize,
    ) -> Bitset {
        // If query range doesn't overlap with node range, return empty
        if Self::compare_values(hi, &node.range_start) == std::cmp::Ordering::Less || 
           Self::compare_values(lo, &node.range_end) == std::cmp::Ordering::Greater {
            return Bitset::new(capacity);
        }
        
        // If node range is completely within query range, return node bitset
        if Self::compare_values(lo, &node.range_start) != std::cmp::Ordering::Greater &&
           Self::compare_values(hi, &node.range_end) != std::cmp::Ordering::Less {
            return node.bitset.clone();
        }
        
        // Otherwise, merge results from children
        let mut result = Bitset::new(capacity);
        
        if let Some(ref left) = node.left {
            let left_result = Self::query_range_recursive(left, lo, hi, capacity);
            result = result.union(&left_result);
        }
        
        if let Some(ref right) = node.right {
            let right_result = Self::query_range_recursive(right, lo, hi, capacity);
            result = result.union(&right_result);
        }
        
        result
    }
    
    /// Build segment tree recursively
    fn build_segment_tree(
        values: &[Value],
        min_val: &Value,
        max_val: &Value,
        start_idx: usize,
        end_idx: usize,
    ) -> Result<SegmentTreeNode> {
        let capacity = values.len();
        let mut bitset = Bitset::new(capacity);
        
        // Mark all rows in this range
        for i in start_idx..=end_idx.min(values.len() - 1) {
            bitset.set(i);
        }
        
        if start_idx == end_idx {
            // Leaf node
            Ok(SegmentTreeNode {
                range_start: values[start_idx].clone(),
                range_end: values[start_idx].clone(),
                bitset,
                left: None,
                right: None,
            })
        } else {
            // Internal node: split range
            let mid_idx = (start_idx + end_idx) / 2;
            let mid_val = &values[mid_idx];
            
            let left = if start_idx <= mid_idx {
                Some(Arc::new(Self::build_segment_tree(
                    values, min_val, mid_val, start_idx, mid_idx,
                )?))
            } else {
                None
            };
            
            let right = if mid_idx + 1 <= end_idx {
                Some(Arc::new(Self::build_segment_tree(
                    values, mid_val, max_val, mid_idx + 1, end_idx,
                )?))
            } else {
                None
            };
            
            Ok(SegmentTreeNode {
                range_start: min_val.clone(),
                range_end: max_val.clone(),
                bitset,
                left,
                right,
            })
        }
    }
    
    /// Find min and max values
    fn find_min_max(values: &[Value]) -> Result<(Value, Value)> {
        if values.is_empty() {
            anyhow::bail!("Cannot find min/max of empty array");
        }
        
        let mut min_val = &values[0];
        let mut max_val = &values[0];
        
        for val in values.iter().skip(1) {
            if Self::compare_values(val, min_val) == std::cmp::Ordering::Less {
                min_val = val;
            }
            if Self::compare_values(val, max_val) == std::cmp::Ordering::Greater {
                max_val = val;
            }
        }
        
        Ok((min_val.clone(), max_val.clone()))
    }
    
    /// Compare two values
    fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Int64(x), Value::Int64(y)) => x.cmp(y),
            (Value::Float64(x), Value::Float64(y)) => {
                ordered_float::OrderedFloat(*x).cmp(&ordered_float::OrderedFloat(*y))
            }
            (Value::String(x), Value::String(y)) => x.cmp(y),
            _ => std::cmp::Ordering::Equal, // Type mismatch
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_range_bitmap_index() {
        let values: Vec<Value> = (0..100)
            .map(|i| Value::Int64(i))
            .collect();
        
        let index = RangeBitmapIndex::build_from_column("col1".to_string(), &values).unwrap();
        
        // Query range [10, 20]
        let result = index.query_range(&Value::Int64(10), &Value::Int64(20));
        assert!(result.get(10));
        assert!(result.get(20));
        assert!(!result.get(5));
        assert!(!result.get(25));
    }
}

