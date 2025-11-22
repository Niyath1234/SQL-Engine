/// Write-Ahead Logging (WAL) for Durability
/// Implements ACID durability by logging all transaction changes before commit
use crate::storage::fragment::Value;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write, BufReader, BufRead};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

/// WAL entry type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WALEntryType {
    Begin { txn_id: u64 },
    Insert { txn_id: u64, table: String, row: HashMap<String, Value> },
    Update { txn_id: u64, table: String, row_id: usize, changes: HashMap<String, Value> },
    Delete { txn_id: u64, table: String, row_id: usize },
    CreateTable { txn_id: u64, table: String, schema: Vec<(String, String)> },
    DropTable { txn_id: u64, table: String },
    Commit { txn_id: u64 },
    Rollback { txn_id: u64 },
    Checkpoint,
}

/// WAL entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WALEntry {
    pub sequence_number: u64,
    pub entry_type: WALEntryType,
    pub timestamp: u64, // Unix timestamp in milliseconds
}

/// Write-Ahead Log manager
pub struct WALManager {
    /// WAL file path
    wal_path: PathBuf,
    /// Current sequence number
    sequence_number: Arc<RwLock<u64>>,
    /// WAL file writer
    writer: Arc<RwLock<Option<BufWriter<File>>>>,
    /// Checkpoint threshold (number of entries before checkpoint)
    checkpoint_threshold: u64,
}

impl WALManager {
    /// Create a new WAL manager
    pub fn new(wal_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&wal_dir)?;
        let wal_path = wal_dir.join("wal.log");
        
        // Open WAL file in append mode
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;
        let writer = BufWriter::new(file);
        
        // Determine current sequence number from existing WAL
        let sequence = Self::read_last_sequence(&wal_path)?;
        
        Ok(Self {
            wal_path,
            sequence_number: Arc::new(RwLock::new(sequence + 1)),
            writer: Arc::new(RwLock::new(Some(writer))),
            checkpoint_threshold: 1000,
        })
    }
    
    /// Read last sequence number from WAL file
    fn read_last_sequence(wal_path: &PathBuf) -> Result<u64> {
        if !wal_path.exists() {
            return Ok(0);
        }
        
        let file = File::open(wal_path)?;
        let mut last_seq = 0u64;
        
        // Read all entries to find the last sequence number
        // In production, we'd use a more efficient approach (seek to end, read backwards)
        for line in read_lines(file) {
            if let Ok(line) = line {
                if let Ok(entry) = serde_json::from_str::<WALEntry>(&line) {
                    last_seq = last_seq.max(entry.sequence_number);
                }
            }
        }
        
        Ok(last_seq)
    }
    
    /// Write a WAL entry
    pub fn write_entry(&self, entry_type: WALEntryType) -> Result<u64> {
        let mut seq = self.sequence_number.write().unwrap();
        let sequence = *seq;
        *seq += 1;
        
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let entry = WALEntry {
            sequence_number: sequence,
            entry_type,
            timestamp,
        };
        
        // Serialize and write to WAL
        let json = serde_json::to_string(&entry)?;
        let mut writer = self.writer.write().unwrap();
        if let Some(ref mut w) = *writer {
            writeln!(w, "{}", json)?;
            w.flush()?;
        }
        
        // Checkpoint if needed
        if sequence % self.checkpoint_threshold == 0 {
            self.checkpoint()?;
        }
        
        Ok(sequence)
    }
    
    /// Create a checkpoint (mark all committed transactions as safe)
    pub fn checkpoint(&self) -> Result<()> {
        self.write_entry(WALEntryType::Checkpoint)?;
        Ok(())
    }
    
    /// Recover transactions from WAL (used on startup)
    pub fn recover(&self) -> Result<Vec<u64>> {
        // In a real implementation, we'd:
        // 1. Read all WAL entries
        // 2. Identify incomplete transactions (BEGIN without COMMIT/ROLLBACK)
        // 3. Rollback incomplete transactions
        // 4. Return list of transactions to replay
        
        let file = File::open(&self.wal_path)?;
        let mut active_transactions: HashMap<u64, bool> = HashMap::new();
        let mut incomplete_txns = Vec::new();
        
        for line in read_lines(file) {
            if let Ok(line) = line {
                if let Ok(entry) = serde_json::from_str::<WALEntry>(&line) {
                    match entry.entry_type {
                        WALEntryType::Begin { txn_id } => {
                            active_transactions.insert(txn_id, false);
                        }
                        WALEntryType::Commit { txn_id } | WALEntryType::Rollback { txn_id } => {
                            active_transactions.remove(&txn_id);
                        }
                        _ => {}
                    }
                }
            }
        }
        
        // Incomplete transactions (BEGIN but no COMMIT/ROLLBACK)
        incomplete_txns.extend(active_transactions.keys().cloned());
        
        Ok(incomplete_txns)
    }
    
    /// Close WAL file
    pub fn close(&self) -> Result<()> {
        let mut writer = self.writer.write().unwrap();
        if let Some(ref mut w) = *writer {
            w.flush()?;
        }
        *writer = None;
        Ok(())
    }
}

impl Default for WALManager {
    fn default() -> Self {
        // Default to current directory
        Self::new(PathBuf::from(".")).unwrap()
    }
}

// Helper function to read lines from file
fn read_lines(file: File) -> std::io::Lines<BufReader<File>> {
    BufReader::new(file).lines()
}

