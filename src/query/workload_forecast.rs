/// Query Workload Forecasting
/// Predicts future query workloads using time-series analysis
use crate::query::fingerprint::QueryFingerprint;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// Workload forecast
#[derive(Clone, Debug)]
pub struct WorkloadForecast {
    pub predicted_queries: Vec<PredictedQuery>,
    pub confidence: f64,
    pub time_horizon: Duration,
}

/// Predicted query
#[derive(Clone, Debug)]
pub struct PredictedQuery {
    pub fingerprint: QueryFingerprint,
    pub expected_frequency: f64, // Queries per hour
    pub time_windows: Vec<TimeWindow>, // When this query is likely to run
}

/// Time window for query prediction
#[derive(Clone, Debug)]
pub struct TimeWindow {
    pub start: SystemTime,
    pub end: SystemTime,
    pub probability: f64,
}

/// Workload forecaster
pub struct WorkloadForecaster {
    /// Historical query sequence with timestamps
    query_history: Arc<Mutex<VecDeque<(QueryFingerprint, SystemTime)>>>,
    /// Query frequency patterns: fingerprint -> hourly frequencies
    frequency_patterns: Arc<Mutex<HashMap<u64, VecDeque<f64>>>>,
    /// Maximum history to keep
    max_history: usize,
}

impl WorkloadForecaster {
    pub fn new(max_history: usize) -> Self {
        Self {
            query_history: Arc::new(Mutex::new(VecDeque::new())),
            frequency_patterns: Arc::new(Mutex::new(HashMap::new())),
            max_history,
        }
    }
    
    /// Record a query execution with timestamp
    pub fn record_query(&self, fingerprint: &QueryFingerprint) {
        let now = SystemTime::now();
        let mut history = self.query_history.lock().unwrap();
        
        history.push_back((fingerprint.clone(), now));
        
        // Keep only recent history
        while history.len() > self.max_history {
            history.pop_front();
        }
        
        // Update frequency patterns
        drop(history);
        self.update_frequency_patterns(fingerprint, now);
    }
    
    /// Update frequency patterns for a query
    fn update_frequency_patterns(&self, fingerprint: &QueryFingerprint, timestamp: SystemTime) {
        let mut patterns = self.frequency_patterns.lock().unwrap();
        let freq_history = patterns.entry(fingerprint.hash).or_insert_with(VecDeque::new);
        
        // Compute hourly frequency from recent history
        let history = self.query_history.lock().unwrap();
        let one_hour_ago = timestamp.checked_sub(Duration::from_secs(3600)).unwrap_or(timestamp);
        
        let recent_count = history.iter()
            .filter(|(fp, ts)| fp.hash == fingerprint.hash && *ts >= one_hour_ago)
            .count();
        
        let hourly_freq = recent_count as f64;
        freq_history.push_back(hourly_freq);
        
        // Keep only last 24 hours of frequency data
        while freq_history.len() > 24 {
            freq_history.pop_front();
        }
    }
    
    /// Forecast workload for next time horizon
    pub fn forecast(&self, time_horizon: Duration) -> WorkloadForecast {
        let patterns = self.frequency_patterns.lock().unwrap();
        let mut predicted_queries = Vec::new();
        
        // Predict queries based on frequency patterns
        for (hash, freq_history) in patterns.iter() {
            if freq_history.len() >= 3 {
                // Use moving average for prediction
                let recent_avg: f64 = freq_history.iter().rev().take(3).sum::<f64>() / 3.0;
                
                if recent_avg > 0.1 { // At least 0.1 queries per hour
                    // Find fingerprint from history
                    let history = self.query_history.lock().unwrap();
                    if let Some((fingerprint, _)) = history.iter().find(|(fp, _)| fp.hash == *hash) {
                        // Predict time windows based on historical patterns
                        let time_windows = self.predict_time_windows(hash, &history);
                        
                        predicted_queries.push(PredictedQuery {
                            fingerprint: fingerprint.clone(),
                            expected_frequency: recent_avg,
                            time_windows,
                        });
                    }
                }
            }
        }
        
        // Sort by expected frequency
        predicted_queries.sort_by(|a, b| b.expected_frequency.partial_cmp(&a.expected_frequency).unwrap());
        
        // Compute overall confidence (based on pattern stability)
        let confidence = self.compute_confidence(&patterns);
        
        WorkloadForecast {
            predicted_queries,
            confidence,
            time_horizon,
        }
    }
    
    /// Predict time windows when query is likely to run
    fn predict_time_windows(&self, hash: &u64, history: &VecDeque<(QueryFingerprint, SystemTime)>) -> Vec<TimeWindow> {
        // Analyze historical timestamps to find patterns
        let timestamps: Vec<SystemTime> = history.iter()
            .filter(|(fp, _)| fp.hash == *hash)
            .map(|(_, ts)| *ts)
            .collect();
        
        if timestamps.len() < 3 {
            return Vec::new();
        }
        
        // Simple heuristic: predict next window based on average interval
        let intervals: Vec<Duration> = timestamps.windows(2)
            .map(|w| w[1].duration_since(w[0]).unwrap_or(Duration::ZERO))
            .collect();
        
        if let Some(avg_interval) = intervals.first() {
            let now = SystemTime::now();
            let next_window_start = now + *avg_interval;
            let next_window_end = next_window_start + Duration::from_secs(3600); // 1 hour window
            
            vec![TimeWindow {
                start: next_window_start,
                end: next_window_end,
                probability: 0.7, // Default probability
            }]
        } else {
            Vec::new()
        }
    }
    
    /// Compute forecast confidence
    fn compute_confidence(&self, patterns: &HashMap<u64, VecDeque<f64>>) -> f64 {
        if patterns.is_empty() {
            return 0.0;
        }
        
        // Confidence based on pattern stability (variance)
        let mut confidences = Vec::new();
        
        for freq_history in patterns.values() {
            if freq_history.len() >= 3 {
                let values: Vec<f64> = freq_history.iter().cloned().collect();
                let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
                let variance: f64 = values.iter()
                    .map(|v| (v - mean).powi(2))
                    .sum::<f64>() / values.len() as f64;
                
                // Lower variance = higher confidence
                let confidence = 1.0 / (1.0 + variance);
                confidences.push(confidence);
            }
        }
        
        if confidences.is_empty() {
            0.5 // Default confidence
        } else {
            confidences.iter().sum::<f64>() / confidences.len() as f64
        }
    }
}

