use arrow::array::*;

/// FAST SUM KERNEL
/// Monomorphized kernel for SUM on numeric types
/// Avoids runtime dispatch overhead by specializing for specific types

/// Sum kernel for Int64 arrays
/// Returns the sum of all non-null values
/// 
/// # Arguments
/// * `values` - The Int64Array to sum
/// * `selection` - Optional selection vector (row indices to include)
/// 
/// # Returns
/// The sum of all selected non-null values
pub fn sum_i64(values: &Int64Array, selection: Option<&Vec<u32>>) -> i64 {
    let mut s = 0i64;
    match selection {
        Some(sel) => {
            for &idx in sel {
                if !values.is_null(idx as usize) {
                    s += values.value(idx as usize);
                }
            }
        }
        None => {
            for i in 0..values.len() {
                if !values.is_null(i) {
                    s += values.value(i);
                }
            }
        }
    }
    s
}

/// Sum kernel for Float64 arrays
/// Returns the sum of all non-null values
pub fn sum_f64(values: &Float64Array, selection: Option<&Vec<u32>>) -> f64 {
    let mut s = 0.0f64;
    match selection {
        Some(sel) => {
            for &idx in sel {
                if !values.is_null(idx as usize) {
                    s += values.value(idx as usize);
                }
            }
        }
        None => {
            for i in 0..values.len() {
                if !values.is_null(i) {
                    s += values.value(i);
                }
            }
        }
    }
    s
}

/// Sum kernel for Int32 arrays
pub fn sum_i32(values: &Int32Array, selection: Option<&Vec<u32>>) -> i64 {
    let mut s = 0i64;
    match selection {
        Some(sel) => {
            for &idx in sel {
                if !values.is_null(idx as usize) {
                    s += values.value(idx as usize) as i64;
                }
            }
        }
        None => {
            for i in 0..values.len() {
                if !values.is_null(i) {
                    s += values.value(i) as i64;
                }
            }
        }
    }
    s
}

