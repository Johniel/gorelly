// Package transaction provides lock management for concurrent transaction execution.
package transaction

import (
	"errors"
	"sync"
)

var (
	// ErrDeadlock is returned when a deadlock is detected.
	ErrDeadlock = errors.New("deadlock detected")
	// ErrLockTimeout is returned when a lock request times out.
	ErrLockTimeout = errors.New("lock request timed out")
)

// LockMode represents the type of lock.
type LockMode int

const (
	// LockModeShared represents a shared (read) lock.
	LockModeShared LockMode = iota
	// LockModeExclusive represents an exclusive (write) lock.
	LockModeExclusive
)

// LockRequest represents a pending lock request.
type LockRequest struct {
	TxnID   TransactionID
	Mode    LockMode
	Granted bool
	Cond    *sync.Cond
}

// LockManager manages locks for database records.
// It implements Two-Phase Locking (2PL) protocol.
type LockManager struct {
	lockTable map[RID][]*LockRequest                   // Maps RID to list of lock requests
	waitFor   map[TransactionID]map[TransactionID]bool // Wait-for graph for deadlock detection
	mu        sync.RWMutex
}

// NewLockManager creates a new lock manager.
func NewLockManager() *LockManager {
	return &LockManager{
		lockTable: make(map[RID][]*LockRequest),
		waitFor:   make(map[TransactionID]map[TransactionID]bool),
	}
}

// LockShared acquires a shared lock on the given RID for the transaction.
func (lm *LockManager) LockShared(txn *Transaction, rid RID) error {
	if !txn.IsActive() {
		return ErrTransactionNotActive
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Check if lock can be granted immediately
	if lm.canGrantLock(rid, LockModeShared) {
		lm.grantLock(rid, txn.ID, LockModeShared)
		return nil
	}

	// Add to wait queue
	req := &LockRequest{
		TxnID:   txn.ID,
		Mode:    LockModeShared,
		Granted: false,
		Cond:    sync.NewCond(&lm.mu),
	}
	lm.lockTable[rid] = append(lm.lockTable[rid], req)

	// Wait for lock
	for !req.Granted {
		req.Cond.Wait()
		// Check again for deadlock after waking up
		if lm.hasDeadlock(txn.ID) {
			lm.removeRequest(rid, req)
			return ErrDeadlock
		}
	}

	return nil
}

func (lm *LockManager) LockExclusive(txn *Transaction, rid RID) error {
	if !txn.IsActive() {
		return ErrTransactionNotActive
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Check if lock can be granted immediately
	if lm.canGrantLock(rid, LockModeExclusive) {
		lm.grantLock(rid, txn.ID, LockModeExclusive)
		return nil
	}

	// Add to wait queue
	req := &LockRequest{
		TxnID:   txn.ID,
		Mode:    LockModeExclusive,
		Granted: false,
		Cond:    sync.NewCond(&lm.mu),
	}
	lm.lockTable[rid] = append(lm.lockTable[rid], req)

	// Check for deadlock
	if lm.hasDeadlock(txn.ID) {
		lm.removeRequest(rid, req)
		return ErrDeadlock
	}

	// Wait for lock
	for !req.Granted {
		req.Cond.Wait()
		// Check again for deadlock after waking up
		if lm.hasDeadlock(txn.ID) {
			lm.removeRequest(rid, req)
			return ErrDeadlock
		}
	}

	return nil
}

func (lm *LockManager) Unlock(txn *Transaction, rid RID) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	requests := lm.lockTable[rid]
	newRequests := make([]*LockRequest, 0, len(requests))

	for _, req := range requests {
		if req.TxnID == txn.ID && req.Granted {
			// Remove this lock
			continue
		}
		newRequests = append(newRequests, req)
	}

	lm.lockTable[rid] = newRequests

	// Update wait-for graph
	lm.updateWaitForGraph(rid)

	// Try to grant pending locks
	lm.grantPendingLocks(rid)

	return nil
}

func (lm *LockManager) UnlockAll(txn *Transaction) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for rid, requests := range lm.lockTable {
		newRequests := make([]*LockRequest, 0, len(requests))
		for _, req := range requests {
			if req.TxnID == txn.ID && req.Granted {
				continue
			}
			newRequests = append(newRequests, req)
		}
		lm.lockTable[rid] = newRequests
		lm.updateWaitForGraph(rid)
		lm.grantPendingLocks(rid)
	}

	// Remove from wait-for graph
	delete(lm.waitFor, txn.ID)
	for _, waiters := range lm.waitFor {
		delete(waiters, txn.ID)
	}
}

// canGrantLock checks if a lock can be granted immediately.
func (lm *LockManager) canGrantLock(rid RID, mode LockMode) bool {
	requests := lm.lockTable[rid]

	if len(requests) == 0 {
		return true
	}

	// Check if there are any granted locks
	hasGrantedLocks := false
	for _, req := range requests {
		if req.Granted {
			hasGrantedLocks = true
			if req.Mode == LockModeExclusive {
				// Exclusive lock is held, cannot grant
				return false
			}
			if mode == LockModeExclusive {
				// Want exclusive but shared locks are held
				return false
			}
		}
	}

	// If requesting shared lock and only shared locks are granted, can grant
	if mode == LockModeShared && hasGrantedLocks {
		// Check if all granted locks are shared
		allShared := true
		for _, req := range requests {
			if req.Granted && req.Mode == LockModeExclusive {
				allShared = false
				break
			}
		}
		return allShared
	}

	return !hasGrantedLocks
}

// grantLock grants a lock to a transaction.
func (lm *LockManager) grantLock(rid RID, txnID TransactionID, mode LockMode) {
	requests := lm.lockTable[rid]
	for _, req := range requests {
		if req.TxnID == txnID && !req.Granted {
			req.Granted = true
			req.Cond.Broadcast()
			return
		}
	}

	// Create new request if not found
	req := &LockRequest{
		TxnID:   txnID,
		Mode:    mode,
		Granted: true,
		Cond:    sync.NewCond(&lm.mu),
	}
	lm.lockTable[rid] = append(lm.lockTable[rid], req)
}

// hasDeadlock checks if there is a deadlock involving the given transaction.
//
// A deadlock occurs when there is a cycle in the wait-for graph, meaning
// a circular chain of transactions waiting for each other.
//
// This method uses depth-first search (DFS) to detect cycles starting from
// the given transaction.
//
// Returns:
//   - true if a deadlock cycle is detected
//   - false if no deadlock exists
func (lm *LockManager) hasDeadlock(txnID TransactionID) bool {
	visited := make(map[TransactionID]bool)
	return lm.dfsDeadlock(txnID, visited, make(map[TransactionID]bool))
}

func (lm *LockManager) dfsDeadlock(txnID TransactionID, visited map[TransactionID]bool, recStack map[TransactionID]bool) bool {
	visited[txnID] = true
	recStack[txnID] = true

	waiters := lm.waitFor[txnID]
	for waiterID := range waiters {
		if !visited[waiterID] {
			if lm.dfsDeadlock(waiterID, visited, recStack) {
				return true
			}
		} else if recStack[waiterID] {
			// Cycle detected
			return true
		}
	}

	recStack[txnID] = false
	return false
}

func (lm *LockManager) removeRequest(rid RID, req *LockRequest) {
	requests := lm.lockTable[rid]
	newRequests := make([]*LockRequest, 0, len(requests))
	for _, r := range requests {
		if r != req {
			newRequests = append(newRequests, r)
		}
	}
	lm.lockTable[rid] = newRequests

	// Update wait-for graph
	delete(lm.waitFor, req.TxnID)
	for _, waiters := range lm.waitFor {
		delete(waiters, req.TxnID)
	}
}
