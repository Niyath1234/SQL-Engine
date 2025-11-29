use arrow::array::*;

/// Very fast filtering kernel for equality predicates
/// Monomorphized for specific types to avoid runtime dispatch

/// Filter equality kernel for Int64 arrays
/// Finds all rows where value equals the comparison value
/// 
/// # Arguments
/// * `values` - The Int64Array to filter
/// * `cmp` - The value to compare against
/// * `out` - Output vector of row indices that match
pub fn filter_eq_i64(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    out.clear();
    for i in 0..values.len() {
        if !values.is_null(i) && values.value(i) == cmp {
            out.push(i as u32);
        }
    }
}

/// Filter equality kernel for Float64 arrays
pub fn filter_eq_f64(values: &Float64Array, cmp: f64, out: &mut Vec<u32>) {
    out.clear();
    for i in 0..values.len() {
        if !values.is_null(i) && values.value(i) == cmp {
            out.push(i as u32);
        }
    }
}

/// Filter equality kernel for Int32 arrays
pub fn filter_eq_i32(values: &Int32Array, cmp: i32, out: &mut Vec<u32>) {
    out.clear();
    for i in 0..values.len() {
        if !values.is_null(i) && values.value(i) == cmp {
            out.push(i as u32);
        }
    }
}

/// Filter equality kernel for String arrays
pub fn filter_eq_string(values: &StringArray, cmp: &str, out: &mut Vec<u32>) {
    out.clear();
    for i in 0..values.len() {
        if !values.is_null(i) && values.value(i) == cmp {
            out.push(i as u32);
        }
    }
}

/// Filter less-than kernel for Int64 arrays
pub fn filter_lt_i64(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    out.clear();
    for i in 0..values.len() {
        if !values.is_null(i) && values.value(i) < cmp {
            out.push(i as u32);
        }
    }
}

/// Filter greater-than kernel for Int64 arrays
pub fn filter_gt_i64(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    out.clear();
    for i in 0..values.len() {
        if !values.is_null(i) && values.value(i) > cmp {
            out.push(i as u32);
        }
    }
}

/// Filter less-than-or-equal kernel for Int64 arrays
pub fn filter_le_i64(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    out.clear();
    for i in 0..values.len() {
        if !values.is_null(i) && values.value(i) <= cmp {
            out.push(i as u32);
        }
    }
}

/// Filter greater-than-or-equal kernel for Int64 arrays
pub fn filter_ge_i64(values: &Int64Array, cmp: i64, out: &mut Vec<u32>) {
    out.clear();
    for i in 0..values.len() {
        if !values.is_null(i) && values.value(i) >= cmp {
            out.push(i as u32);
        }
    }
}

