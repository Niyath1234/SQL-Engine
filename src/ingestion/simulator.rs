//! Simulator Connector - Artificial data feed for development/testing

use crate::ingestion::connector::{IngestionConnector, ConnectorResult, Checkpoint};
use serde_json::{Value, json};
use anyhow::Result;
use std::sync::atomic::{AtomicU64, Ordering};

/// Simulator Connector - Generates artificial JSON data
pub struct SimulatorConnector {
    source_id: String,
    record_count: AtomicU64,
    batch_size: usize,
    schema_type: SimulatorSchema,
}

#[derive(Clone, Debug)]
pub enum SimulatorSchema {
    /// Simple flat records
    Flat,
    
    /// Records with nested objects
    Nested,
    
    /// Records with arrays (creates child tables)
    WithArrays,
    
    /// E-commerce style (orders, products, customers)
    ECommerce,
}

impl SimulatorConnector {
    pub fn new(source_id: String, schema_type: SimulatorSchema) -> Self {
        Self {
            source_id,
            record_count: AtomicU64::new(0),
            batch_size: 100,
            schema_type,
        }
    }
    
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
    
    /// Generate a batch of records
    fn generate_batch(&self, start_id: u64, count: usize) -> Vec<Value> {
        (0..count)
            .map(|i| self.generate_record(start_id + i as u64))
            .collect()
    }
    
    /// Generate a single record based on schema type
    fn generate_record(&self, id: u64) -> Value {
        match &self.schema_type {
            SimulatorSchema::Flat => {
                json!({
                    "id": id,
                    "name": format!("Record_{}", id),
                    "value": (id as f64) * 10.0,
                    "active": id % 2 == 0,
                })
            }
            SimulatorSchema::Nested => {
                json!({
                    "id": id,
                    "user": {
                        "name": format!("User_{}", id),
                        "email": format!("user{}@example.com", id),
                    },
                    "metadata": {
                        "created_at": format!("2024-01-{:02}", (id % 28) + 1),
                        "version": (id % 10) as i64,
                    },
                })
            }
            SimulatorSchema::WithArrays => {
                json!({
                    "id": id,
                    "order_id": format!("ORD_{}", id),
                    "items": [
                        {
                            "product_id": id * 10,
                            "quantity": (id % 5) + 1,
                            "price": (id as f64) * 10.5,
                        },
                        {
                            "product_id": id * 10 + 1,
                            "quantity": (id % 3) + 1,
                            "price": (id as f64) * 8.2,
                        },
                    ],
                })
            }
            SimulatorSchema::ECommerce => {
                json!({
                    "order_id": id,
                    "customer_id": id % 100,
                    "order_date": format!("2024-01-{:02}", (id % 28) + 1),
                    "total_amount": (id as f64) * 25.99,
                    "status": if id % 3 == 0 { "completed" } else { "pending" },
                    "items": [
                        {
                            "product_id": id * 10,
                            "product_name": format!("Product_{}", id * 10),
                            "quantity": (id % 5) + 1,
                            "unit_price": (id as f64) * 10.5,
                        },
                    ],
                })
            }
        }
    }
}

impl IngestionConnector for SimulatorConnector {
    fn fetch(&mut self, checkpoint: Option<Checkpoint>) -> Result<ConnectorResult> {
        let start_id = checkpoint
            .as_ref()
            .map(|cp| cp.records_fetched)
            .unwrap_or(0);
        
        // Generate batch
        let payloads = self.generate_batch(start_id, self.batch_size);
        let records_fetched = start_id + payloads.len() as u64;
        
        // Update record count
        self.record_count.store(records_fetched, Ordering::SeqCst);
        
        // Create new checkpoint
        let checkpoint = Checkpoint {
            cursor: format!("cursor_{}", records_fetched),
            last_fetch_at: Checkpoint::now_timestamp(),
            records_fetched,
        };
        
        // Simulate "has more" - limit to 1000 records for demo
        let has_more = records_fetched < 1000;
        
        Ok(ConnectorResult {
            payloads,
            checkpoint,
            has_more,
        })
    }
    
    fn source_id(&self) -> &str {
        &self.source_id
    }
    
    fn source_type(&self) -> &str {
        "simulator"
    }
    
    fn source_uri(&self) -> Option<&str> {
        None
    }
}

