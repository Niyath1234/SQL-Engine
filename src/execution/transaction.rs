/// Transaction Management and ACID Properties
/// Implements transaction support for the SQL engine
use crate::storage::fragment::Value;
use crate::hypergraph::graph::HyperGraph;
use crate::hypergraph::node::NodeId;
use anyhow::Result;
use std::sync::{Arc, RwLock};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

/// Transaction isolation level
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Transaction state
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
}

/// Transaction context - tracks changes in a transaction
#[derive(Clone, Debug)]
pub struct Transaction {
    /// Unique transaction ID
    pub id: u64,
    /// Transaction state
    pub state: TransactionState,
    /// Isolation level
    pub isolation_level: IsolationLevel,
    /// Start time
    pub start_time: SystemTime,
    /// Tables modified in this transaction
    pub modified_tables: HashSet<String>,
    /// Read set (for conflict detection)
    pub read_set: HashSet<(String, usize)>, // (table_name, row_id)
    /// Write set (for conflict detection)
    pub write_set: HashSet<(String, usize)>, // (table_name, row_id)
    /// Snapshot of data at transaction start (for MVCC)
    pub snapshot: HashMap<String, Vec<Value>>,
}

impl Transaction {
    pub fn new(id: u64, isolation_level: IsolationLevel) -> Self {
        Self {
            id,
            state: TransactionState::Active,
            isolation_level,
            start_time: SystemTime::now(),
            modified_tables: HashSet::new(),
            read_set: HashSet::new(),
            write_set: HashSet::new(),
            snapshot: HashMap::new(),
        }
    }
    
    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }
    
    pub fn commit(&mut self) {
        self.state = TransactionState::Committed;
    }
    
    pub fn rollback(&mut self) {
        self.state = TransactionState::RolledBack;
    }
}

/// Transaction manager - manages all active transactions
pub struct TransactionManager {
    /// Active transactions: transaction_id -> Transaction
    transactions: Arc<RwLock<HashMap<u64, Transaction>>>,
    /// Next transaction ID
    next_txn_id: Arc<RwLock<u64>>,
    /// Table locks: table_name -> (transaction_id, lock_type)
    table_locks: Arc<RwLock<HashMap<String, (u64, LockType)>>>,
    /// Row locks: (table_name, row_id) -> transaction_id
    row_locks: Arc<RwLock<HashMap<(String, usize), u64>>>,
    /// Transaction snapshots for MVCC (transaction_id -> snapshot)
    snapshots: Arc<RwLock<HashMap<u64, HashMap<String, Vec<Value>>>>>,
    /// Write-Ahead Log manager (optional)
    wal: Option<std::sync::Arc<crate::execution::wal::WALManager>>,
}

/// Lock type
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockType {
    Shared,   // Read lock
    Exclusive, // Write lock
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            next_txn_id: Arc::new(RwLock::new(1)),
            table_locks: Arc::new(RwLock::new(HashMap::new())),
            row_locks: Arc::new(RwLock::new(HashMap::new())),
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            wal: None,
        }
    }
    
    /// Create transaction manager with WAL
    pub fn with_wal(wal_dir: std::path::PathBuf) -> anyhow::Result<Self> {
        let wal = std::sync::Arc::new(crate::execution::wal::WALManager::new(wal_dir)?);
        Ok(Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            next_txn_id: Arc::new(RwLock::new(1)),
            table_locks: Arc::new(RwLock::new(HashMap::new())),
            row_locks: Arc::new(RwLock::new(HashMap::new())),
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            wal: Some(wal),
        })
    }
    
    /// Begin a new transaction
    pub fn begin(&self, isolation_level: IsolationLevel) -> Result<u64> {
        let mut next_id = self.next_txn_id.write().unwrap();
        let txn_id = *next_id;
        *next_id += 1;
        
        let transaction = Transaction::new(txn_id, isolation_level);
        let mut transactions = self.transactions.write().unwrap();
        transactions.insert(txn_id, transaction);
        
        // Create MVCC snapshot for transaction (for RepeatableRead and Serializable)
        if matches!(isolation_level, IsolationLevel::RepeatableRead | IsolationLevel::Serializable) {
            let mut snapshots = self.snapshots.write().unwrap();
            snapshots.insert(txn_id, HashMap::new()); // Snapshot will be populated on first read
        }
        
        // Write to WAL
        if let Some(ref wal) = self.wal {
            wal.write_entry(crate::execution::wal::WALEntryType::Begin { txn_id })?;
        }
        
        Ok(txn_id)
    }
    
    /// Commit a transaction (ensures atomicity)
    pub fn commit(&self, txn_id: u64) -> Result<()> {
        // Check for conflicts based on isolation level (consistency check)
        let transactions = self.transactions.read().unwrap();
        let txn = transactions.get(&txn_id)
            .ok_or_else(|| anyhow::anyhow!("Transaction {} not found", txn_id))?;
        
        if !txn.is_active() {
            anyhow::bail!("Transaction {} is not active", txn_id);
        }
        
        // Serializability conflict detection
        if txn.isolation_level == IsolationLevel::Serializable {
            if let Err(e) = self.check_serializability_conflicts(txn_id, &txn) {
                // Conflict detected, must rollback
                drop(transactions);
                self.rollback(txn_id)?;
                return Err(e);
            }
        }
        
        drop(transactions);
        
        // Write commit to WAL (durability)
        if let Some(ref wal) = self.wal {
            wal.write_entry(crate::execution::wal::WALEntryType::Commit { txn_id })?;
            wal.write_entry(crate::execution::wal::WALEntryType::Checkpoint)?;
        }
        
        // Atomically commit transaction
        let mut transactions = self.transactions.write().unwrap();
        if let Some(txn) = transactions.get_mut(&txn_id) {
            // Release locks
            drop(transactions);
            self.release_locks(txn_id)?;
            
            let mut transactions = self.transactions.write().unwrap();
            if let Some(txn) = transactions.get_mut(&txn_id) {
                txn.commit();
            }
            
            // Remove snapshot
            let mut snapshots = self.snapshots.write().unwrap();
            snapshots.remove(&txn_id);
        }
        
        Ok(())
    }
    
    /// Rollback a transaction (atomicity - all changes undone)
    pub fn rollback(&self, txn_id: u64) -> Result<()> {
        // Write rollback to WAL
        if let Some(ref wal) = self.wal {
            wal.write_entry(crate::execution::wal::WALEntryType::Rollback { txn_id })?;
        }
        
        let mut transactions = self.transactions.write().unwrap();
        if let Some(txn) = transactions.get_mut(&txn_id) {
            if !txn.is_active() {
                anyhow::bail!("Transaction {} is not active", txn_id);
            }
            
            // Release locks
            drop(transactions);
            self.release_locks(txn_id)?;
            
            let mut transactions = self.transactions.write().unwrap();
            if let Some(txn) = transactions.get_mut(&txn_id) {
                txn.rollback();
            }
            
            // Remove snapshot
            let mut snapshots = self.snapshots.write().unwrap();
            snapshots.remove(&txn_id);
        } else {
            anyhow::bail!("Transaction {} not found", txn_id)
        }
        
        Ok(())
    }
    
    /// Acquire a lock on a table (with isolation level awareness)
    pub fn acquire_table_lock(&self, txn_id: u64, table_name: &str, lock_type: LockType) -> Result<()> {
        // Get transaction to check isolation level
        let transactions = self.transactions.read().unwrap();
        let txn = transactions.get(&txn_id)
            .ok_or_else(|| anyhow::anyhow!("Transaction {} not found", txn_id))?;
        
        let isolation = txn.isolation_level;
        drop(transactions);
        
        let mut locks = self.table_locks.write().unwrap();
        
        // Isolation level-specific locking behavior
        match isolation {
            IsolationLevel::ReadUncommitted => {
                // No locks for reads (dirty reads allowed)
                if lock_type == LockType::Exclusive {
                    // Only acquire locks for writes
                    if let Some((existing_txn_id, _)) = locks.get(table_name) {
                        if *existing_txn_id != txn_id {
                            anyhow::bail!("Table '{}' is locked by transaction {}", table_name, existing_txn_id);
                        }
                    }
                    locks.insert(table_name.to_string(), (txn_id, lock_type));
                }
            }
            IsolationLevel::ReadCommitted => {
                // Acquire locks for both reads and writes
                if let Some((existing_txn_id, existing_lock)) = locks.get(table_name) {
                    if *existing_txn_id != txn_id {
                        match (lock_type, *existing_lock) {
                            (LockType::Exclusive, _) | (_, LockType::Exclusive) => {
                                anyhow::bail!("Table '{}' is locked by transaction {}", table_name, existing_txn_id);
                            }
                            (LockType::Shared, LockType::Shared) => {
                                // Multiple shared locks allowed
                            }
                        }
                    }
                }
                locks.insert(table_name.to_string(), (txn_id, lock_type));
            }
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                // Strict locking for both reads and writes
                if let Some((existing_txn_id, existing_lock)) = locks.get(table_name) {
                    if *existing_txn_id != txn_id {
                        match (lock_type, *existing_lock) {
                            (LockType::Exclusive, _) | (_, LockType::Exclusive) => {
                                anyhow::bail!("Table '{}' is locked by transaction {}", table_name, existing_txn_id);
                            }
                            (LockType::Shared, LockType::Shared) => {
                                // Multiple shared locks allowed
                            }
                        }
                    }
                }
                locks.insert(table_name.to_string(), (txn_id, lock_type));
            }
        }
        
        Ok(())
    }
    
    /// Check for serializability conflicts
    fn check_serializability_conflicts(&self, txn_id: u64, txn: &Transaction) -> Result<()> {
        let transactions = self.transactions.read().unwrap();
        
        // Check for write-write conflicts with other active transactions
        for (other_txn_id, other_txn) in transactions.iter() {
            if *other_txn_id == txn_id || !other_txn.is_active() {
                continue;
            }
            
            // Check if there's a write-write conflict
            let txn_writes: HashSet<_> = txn.write_set.iter().collect();
            let other_writes: HashSet<_> = other_txn.write_set.iter().collect();
            
            if !txn_writes.is_disjoint(&other_writes) {
                anyhow::bail!(
                    "Serializability conflict: Transaction {} and {} both wrote to the same rows",
                    txn_id, other_txn_id
                );
            }
            
            // Check for read-write conflicts (transaction read what another transaction wrote)
            let txn_reads: HashSet<_> = txn.read_set.iter().collect();
            if !txn_reads.is_disjoint(&other_writes) {
                anyhow::bail!(
                    "Serializability conflict: Transaction {} read rows that transaction {} wrote",
                    txn_id, other_txn_id
                );
            }
        }
        
        Ok(())
    }
    
    /// Acquire a lock on a row
    pub fn acquire_row_lock(&self, txn_id: u64, table_name: &str, row_id: usize) -> Result<()> {
        let mut locks = self.row_locks.write().unwrap();
        let key = (table_name.to_string(), row_id);
        
        if let Some(existing_txn_id) = locks.get(&key) {
            if *existing_txn_id != txn_id {
                anyhow::bail!("Row ({}, {}) is locked by transaction {}", table_name, row_id, existing_txn_id);
            }
        }
        
        locks.insert(key, txn_id);
        Ok(())
    }
    
    /// Release all locks for a transaction
    fn release_locks(&self, txn_id: u64) -> Result<()> {
        let mut table_locks = self.table_locks.write().unwrap();
        let mut row_locks = self.row_locks.write().unwrap();
        
        // Remove table locks
        table_locks.retain(|_, (id, _)| *id != txn_id);
        
        // Remove row locks
        row_locks.retain(|_, id| *id != txn_id);
        
        Ok(())
    }
    
    /// Get transaction
    pub fn get_transaction(&self, txn_id: u64) -> Option<Transaction> {
        let transactions = self.transactions.read().unwrap();
        transactions.get(&txn_id).cloned()
    }
    
    /// Check if transaction is active
    pub fn is_active(&self, txn_id: u64) -> bool {
        if let Some(txn) = self.get_transaction(txn_id) {
            txn.is_active()
        } else {
            false
        }
    }
    
    /// Mark table as modified in transaction
    pub fn mark_table_modified(&self, txn_id: u64, table_name: &str) -> Result<()> {
        let mut transactions = self.transactions.write().unwrap();
        if let Some(txn) = transactions.get_mut(&txn_id) {
            txn.modified_tables.insert(table_name.to_string());
            Ok(())
        } else {
            anyhow::bail!("Transaction {} not found", txn_id)
        }
    }
    
    /// Track a read operation (for conflict detection)
    pub fn track_read(&self, txn_id: u64, table_name: &str, row_id: usize) -> Result<()> {
        let mut transactions = self.transactions.write().unwrap();
        if let Some(txn) = transactions.get_mut(&txn_id) {
            txn.read_set.insert((table_name.to_string(), row_id));
            Ok(())
        } else {
            anyhow::bail!("Transaction {} not found", txn_id)
        }
    }
    
    /// Track a write operation (for conflict detection)
    pub fn track_write(&self, txn_id: u64, table_name: &str, row_id: usize) -> Result<()> {
        let mut transactions = self.transactions.write().unwrap();
        if let Some(txn) = transactions.get_mut(&txn_id) {
            txn.write_set.insert((table_name.to_string(), row_id));
            Ok(())
        } else {
            anyhow::bail!("Transaction {} not found", txn_id)
        }
    }
    
    /// Get MVCC snapshot for transaction
    pub fn get_snapshot(&self, txn_id: u64) -> Option<HashMap<String, Vec<Value>>> {
        let snapshots = self.snapshots.read().unwrap();
        snapshots.get(&txn_id).cloned()
    }
    
    /// Set MVCC snapshot for transaction
    pub fn set_snapshot(&self, txn_id: u64, snapshot: HashMap<String, Vec<Value>>) -> Result<()> {
        let mut snapshots = self.snapshots.write().unwrap();
        snapshots.insert(txn_id, snapshot);
        Ok(())
    }
    
    /// Get WAL manager (if available)
    pub fn wal(&self) -> Option<&std::sync::Arc<crate::execution::wal::WALManager>> {
        self.wal.as_ref()
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

