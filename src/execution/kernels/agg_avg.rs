use arrow::array::*;

/// Average state for computing averages incrementally
#[derive(Clone, Debug)]
pub struct AvgState {
    pub sum: f64,
    pub count: u64,
}

impl AvgState {
    /// Create a new empty average state
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }

    /// Get the average value
    pub fn avg(&self) -> f64 {
        if self.count > 0 {
            self.sum / self.count as f64
        } else {
            0.0
        }
    }

    /// Merge another state into this one
    pub fn merge(&mut self, other: &AvgState) {
        self.sum += other.sum;
        self.count += other.count;
    }
}

impl Default for AvgState {
    fn default() -> Self {
        Self::new()
    }
}

/// Average kernel for Float64 arrays
/// Computes sum and count for average calculation
/// 
/// # Arguments
/// * `values` - The Float64Array to compute average over
/// * `sel` - Optional selection vector (row indices to include)
/// 
/// # Returns
/// AvgState containing sum and count
pub fn avg_f64(values: &Float64Array, sel: Option<&Vec<u32>>) -> AvgState {
    let mut st = AvgState { sum: 0.0, count: 0 };
    match sel {
        Some(ids) => {
            for &i in ids {
                if !values.is_null(i as usize) {
                    st.sum += values.value(i as usize);
                    st.count += 1;
                }
            }
        }
        None => {
            for i in 0..values.len() {
                if !values.is_null(i) {
                    st.sum += values.value(i);
                    st.count += 1;
                }
            }
        }
    }
    st
}

/// Average kernel for Int64 arrays
/// Computes sum and count, returns as f64 average
pub fn avg_i64(values: &Int64Array, sel: Option<&Vec<u32>>) -> AvgState {
    let mut st = AvgState { sum: 0.0, count: 0 };
    match sel {
        Some(ids) => {
            for &i in ids {
                if !values.is_null(i as usize) {
                    st.sum += values.value(i as usize) as f64;
                    st.count += 1;
                }
            }
        }
        None => {
            for i in 0..values.len() {
                if !values.is_null(i) {
                    st.sum += values.value(i) as f64;
                    st.count += 1;
                }
            }
        }
    }
    st
}

/// Average kernel for Int32 arrays
pub fn avg_i32(values: &Int32Array, sel: Option<&Vec<u32>>) -> AvgState {
    let mut st = AvgState { sum: 0.0, count: 0 };
    match sel {
        Some(ids) => {
            for &i in ids {
                if !values.is_null(i as usize) {
                    st.sum += values.value(i as usize) as f64;
                    st.count += 1;
                }
            }
        }
        None => {
            for i in 0..values.len() {
                if !values.is_null(i) {
                    st.sum += values.value(i) as f64;
                    st.count += 1;
                }
            }
        }
    }
    st
}

