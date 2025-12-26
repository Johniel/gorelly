// Package transaction provides transaction management for the database.
// It implements ACID properties: Atomicity, Consistency, Isolation, and Durability.
package transaction

import (
	"errors"
	"sync"
	"time"

	"github.com/Johniel/gorelly/disk"
)

var (
	// ErrTransactionNotActive is returned when an operation is attempted on a non-active transaction.
	ErrTransactionNotActive = errors.New("transaction is not active")
	// ErrTransactionAlreadyCommitted is returned when attempting to commit an already committed transaction.
	ErrTransactionAlreadyCommitted = errors.New("transaction already committed")
	// ErrTransactionAlreadyAborted is returned when attempting to abort an already aborted transaction.
	ErrTransactionAlreadyAborted = errors.New("transaction already aborted")
)

// TransactionState represents the state of a transaction.
type TransactionState int

const (
	// TransactionStateActive indicates the transaction is currently executing.
	TransactionStateActive TransactionState = iota
	// TransactionStateCommitted indicates the transaction has been committed.
	TransactionStateCommitted
	// TransactionStateFailed indicates the transaction has failed.
	TransactionStateFailed
	// TransactionStateAborted indicates the transaction has been aborted.
	TransactionStateAborted
	// TransactionStateTerminated indicates the transaction has been terminated.
	TransactionStateTerminated
)

// TransactionID uniquely identifies a transaction.
type TransactionID uint64

// Transaction represents a database transaction.
// It tracks the state, start time, and operations performed within the transaction.
type Transaction struct {
	ID        TransactionID
	State     TransactionState
	StartTime time.Time
	mu        sync.RWMutex
}

// NewTransaction creates a new transaction with the given ID.
func NewTransaction(id TransactionID) *Transaction {
	return &Transaction{
		ID:        id,
		State:     TransactionStateActive,
		StartTime: time.Now(),
	}
}

// Begin initializes the transaction state.
func (txn *Transaction) Begin() {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.State = TransactionStateActive
	txn.StartTime = time.Now()
}

// Commit commits the transaction.
// Note: This method only updates the transaction state.
// For full commit with logging and lock release, use TransactionManager.Commit().
func (txn *Transaction) Commit() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.State != TransactionStateActive {
		if txn.State == TransactionStateCommitted {
			return ErrTransactionAlreadyCommitted
		}
		return ErrTransactionNotActive
	}

	txn.State = TransactionStateCommitted
	return nil
}

// Abort aborts the transaction.
// Note: This method only updates the transaction state.
// For full abort with rollback, logging, and lock release, use TransactionManager.Abort().
func (txn *Transaction) Abort() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.State == TransactionStateTerminated {
		return nil
	}

	if txn.State == TransactionStateAborted {
		return ErrTransactionAlreadyAborted
	}

	txn.State = TransactionStateFailed
	txn.State = TransactionStateAborted
	return nil
}

// IsActive returns true if the transaction is currently active.
func (txn *Transaction) IsActive() bool {
	txn.mu.RLock()
	defer txn.mu.RUnlock()
	return txn.State == TransactionStateActive
}

// IsCommitted returns true if the transaction has been committed.
func (txn *Transaction) IsCommitted() bool {
	txn.mu.RLock()
	defer txn.mu.RUnlock()
	return txn.State == TransactionStateCommitted
}

// IsAborted returns true if the transaction has been aborted.
func (txn *Transaction) IsAborted() bool {
	txn.mu.RLock()
	defer txn.mu.RUnlock()
	return txn.State == TransactionStateAborted || txn.State == TransactionStateFailed
}

// TransactionManager manages all transactions in the database.
// It assigns transaction IDs and tracks active transactions.
// It integrates with LogManager for WAL and LockManager for concurrency control.
type TransactionManager struct {
	nextTxnID       TransactionID
	activeTxns      map[TransactionID]*Transaction
	logManager      *LogManager      // Optional: for WAL logging
	lockManager     *LockManager     // Optional: for lock management
	recoveryManager *RecoveryManager // Optional: for rollback operations
	mu              sync.RWMutex
}

// NewTransactionManager creates a new transaction manager.
// For full transaction support with logging and locking, use NewTransactionManagerWithManagers.
func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		nextTxnID:  1,
		activeTxns: make(map[TransactionID]*Transaction),
	}
}

// NewTransactionManagerWithManagers creates a new transaction manager with logging and locking support.
// logManager: Required for WAL logging (can be nil to disable logging)
// lockManager: Required for concurrency control (can be nil to disable locking)
// recoveryManager: Optional, used for rollback operations (can be nil)
func NewTransactionManagerWithManagers(logManager *LogManager, lockManager *LockManager, recoveryManager *RecoveryManager) *TransactionManager {
	return &TransactionManager{
		nextTxnID:       1,
		activeTxns:      make(map[TransactionID]*Transaction),
		logManager:      logManager,
		lockManager:     lockManager,
		recoveryManager: recoveryManager,
	}
}

// SetManagers sets the log manager, lock manager, and recovery manager for an existing transaction manager.
// This allows configuring managers after creation for backward compatibility.
func (tm *TransactionManager) SetManagers(logManager *LogManager, lockManager *LockManager, recoveryManager *RecoveryManager) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.logManager = logManager
	tm.lockManager = lockManager
	tm.recoveryManager = recoveryManager
}

// Begin starts a new transaction and returns it.
// If LogManager is configured, it writes a Begin log record.
func (tm *TransactionManager) Begin() *Transaction {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txnID := tm.nextTxnID
	tm.nextTxnID++

	txn := NewTransaction(txnID)
	tm.activeTxns[txnID] = txn

	// Write Begin log record if LogManager is configured
	if tm.logManager != nil {
		beginRecord := &LogRecord{
			Type:  LogRecordTypeBegin,
			TxnID: txnID,
		}
		// Ignore errors during Begin logging - transaction can still proceed
		_ = tm.logManager.AppendLog(beginRecord)
	}

	return txn
}

// Commit commits a transaction.
// It writes a Commit log record, flushes the log, releases all locks, and transitions to Terminated state.
func (tm *TransactionManager) Commit(txn *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Validate and transition to committed state using Transaction.Commit
	// Note: Transaction.Commit locks txn.mu internally, which is safe here
	if err := txn.Commit(); err != nil {
		return err
	}

	// Write Commit log record if LogManager is configured
	if tm.logManager != nil {
		commitRecord := &LogRecord{
			Type:  LogRecordTypeCommit,
			TxnID: txn.ID,
		}
		if err := tm.logManager.AppendLog(commitRecord); err != nil {
			// If log write fails, we should rollback the transaction state
			// For now, we'll return the error and let the caller handle it
			return err
		}
		// Flush log to ensure durability
		if err := tm.logManager.Flush(); err != nil {
			return err
		}
	}

	// Release all locks if LockManager is configured
	if tm.lockManager != nil {
		tm.lockManager.UnlockAll(txn)
	}

	// Remove from active transactions and transition to terminated
	delete(tm.activeTxns, txn.ID)
	txn.State = TransactionStateTerminated

	return nil
}

// Abort aborts a transaction.
// It performs rollback, writes an Abort log record, releases all locks, and transitions to Terminated state.
func (tm *TransactionManager) Abort(txn *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Validate and transition to aborted state using Transaction.Abort
	// Note: Transaction.Abort transitions to Failed then Aborted state
	// Transaction.Abort locks txn.mu internally, which is safe here
	if err := txn.Abort(); err != nil {
		// If already terminated, return early
		if txn.State == TransactionStateTerminated {
			return nil
		}
		// For other errors, continue with abort process
	}

	// Perform rollback if RecoveryManager is configured
	if tm.recoveryManager != nil {
		if err := tm.recoveryManager.Rollback(txn); err != nil {
			// Log error but continue with abort process
			// The transaction will still be aborted even if rollback fails
		}
	}

	// Write Abort log record if LogManager is configured
	if tm.logManager != nil {
		abortRecord := &LogRecord{
			Type:  LogRecordTypeAbort,
			TxnID: txn.ID,
		}
		// Ignore errors during Abort logging - transaction will still be aborted
		_ = tm.logManager.AppendLog(abortRecord)
	}

	// Release all locks if LockManager is configured
	if tm.lockManager != nil {
		tm.lockManager.UnlockAll(txn)
	}

	// Remove from active transactions and transition to terminated
	delete(tm.activeTxns, txn.ID)
	txn.State = TransactionStateTerminated

	return nil
}

// GetTransaction retrieves a transaction by ID.
func (tm *TransactionManager) GetTransaction(txnID TransactionID) (*Transaction, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	txn, ok := tm.activeTxns[txnID]
	return txn, ok
}

// RID represents a Tuple ID (page ID + slot ID).
type RID struct {
	PageID disk.PageID
	SlotID int
}
