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
// Locks are used to control concurrent access to database tuples.
type LockMode int

const (
	// LockModeShared represents a shared (read) lock.
	// Multiple transactions can hold shared locks on the same tuple simultaneously.
	// Shared locks are compatible with other shared locks but not with exclusive locks.
	LockModeShared LockMode = iota

	// LockModeExclusive represents an exclusive (write) lock.
	// Only one transaction can hold an exclusive lock on a tuple at a time.
	// Exclusive locks are not compatible with any other lock mode (shared or exclusive).
	LockModeExclusive
)

// LockRequest represents a pending or granted lock request.
// Each request is associated with a transaction and a lock mode.
type LockRequest struct {
	TxnID   TransactionID // The transaction requesting the lock
	Mode    LockMode      // The type of lock requested (shared or exclusive)
	Granted bool          // Whether the lock has been granted
	Cond    *sync.Cond    // Condition variable for waiting on lock grant
}

// LockManager manages locks for database tuples to ensure serializable isolation.
// It implements the Two-Phase Locking (2PL) protocol with deadlock detection.
//
// Two-Phase Locking Protocol:
//   - Growing Phase: Transactions can acquire locks but cannot release any locks.
//   - Shrinking Phase: Transactions can release locks but cannot acquire new locks.
//
// Lock Compatibility Matrix:
//   - Shared + Shared: Compatible (multiple readers allowed)
//   - Shared + Exclusive: Not compatible (readers and writers conflict)
//   - Exclusive + Exclusive: Not compatible (only one writer allowed)
//
// Deadlock Detection:
//
//	The LockManager maintains a wait-for graph to detect deadlocks.
//	A deadlock occurs when there is a cycle in the wait-for graph.
//	When a deadlock is detected, one of the transactions is aborted.
//
// Thread Safety:
//
//	All operations are protected by a read-write mutex to ensure thread safety.
type LockManager struct {
	// lockTable maps each tuple (RID) to a list of lock requests.
	// Requests are stored in FIFO order to ensure fairness.
	lockTable map[RID][]*LockRequest

	// waitFor represents the wait-for graph for deadlock detection.
	// waitFor[txnA][txnB] = true means transaction A is waiting for transaction B.
	waitFor map[TransactionID]map[TransactionID]bool

	// mu protects all LockManager state from concurrent access.
	mu sync.RWMutex
}

// NewLockManager creates a new lock manager.
func NewLockManager() *LockManager {
	return &LockManager{
		lockTable: make(map[RID][]*LockRequest),
		waitFor:   make(map[TransactionID]map[TransactionID]bool),
	}
}

// LockShared acquires a shared (read) lock on the given tuple for the transaction.
//
// A shared lock allows multiple transactions to read the same tuple concurrently.
// The lock is granted immediately if:
//   - No locks are held on the tuple, or
//   - Only shared locks are held on the tuple
//
// If the lock cannot be granted immediately, the transaction waits until:
//   - The lock becomes available, or
//   - A deadlock is detected (in which case ErrDeadlock is returned)
//
// Returns:
//   - nil if the lock was successfully acquired
//   - ErrTransactionNotActive if the transaction is not active
//   - ErrDeadlock if a deadlock is detected
//
// Example:
//
//	txn := tm.Begin()
//	rid := RID{PageID: 1, SlotID: 0}
//	if err := lm.LockShared(txn, rid); err != nil {
//	    // Handle error
//	}
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

	// Update wait-for graph before checking for deadlock
	lm.updateWaitForGraph(rid)

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

// LockExclusive acquires an exclusive (write) lock on the given tuple for the transaction.
//
// An exclusive lock ensures that only one transaction can modify the tuple at a time.
// The lock is granted immediately only if:
//   - No locks are held on the tuple (neither shared nor exclusive)
//
// If the lock cannot be granted immediately, the transaction waits until:
//   - All existing locks are released, or
//   - A deadlock is detected (in which case ErrDeadlock is returned)
//
// Returns:
//   - nil if the lock was successfully acquired
//   - ErrTransactionNotActive if the transaction is not active
//   - ErrDeadlock if a deadlock is detected
//
// Example:
//
//	txn := tm.Begin()
//	rid := RID{PageID: 1, SlotID: 0}
//	if err := lm.LockExclusive(txn, rid); err != nil {
//	    // Handle error
//	}
//	// Now safe to modify the tuple
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

	// Update wait-for graph before checking for deadlock
	lm.updateWaitForGraph(rid)

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

// Unlock releases all locks held by the transaction on the given tuple.
//
// After unlocking, the LockManager attempts to grant any pending locks that
// were waiting for this lock to be released. This may wake up waiting transactions.
//
// The wait-for graph is updated to reflect the lock release, which may break
// deadlock cycles.
//
// Returns:
//   - nil on success
//
// Note: This method is part of the shrinking phase of 2PL. After unlocking,
// the transaction should not acquire new locks (though this is not enforced
// by the LockManager itself).
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

	// Try to grant pending locks
	lm.grantPendingLocks(rid)

	// Update wait-for graph after granting locks (as grants may change wait relationships)
	lm.updateWaitForGraph(rid)

	return nil
}

// UnlockAll releases all locks held by the transaction across all tuples.
//
// This is typically called when a transaction commits or aborts to ensure
// all resources are released. After unlocking, all pending locks that were
// waiting for any of these locks are considered for granting.
//
// The transaction is also removed from the wait-for graph to clean up
// deadlock detection state.
//
// This method is thread-safe and should be called during transaction cleanup.
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
		lm.grantPendingLocks(rid)
		lm.updateWaitForGraph(rid)
	}

	// Remove from wait-for graph
	delete(lm.waitFor, txn.ID)
	for _, waiters := range lm.waitFor {
		delete(waiters, txn.ID)
	}
}

// canGrantLock checks if a lock can be granted immediately based on lock compatibility.
//
// Lock compatibility rules:
//   - Shared locks: Can be granted if only shared locks (or no locks) are held
//   - Exclusive locks: Can be granted only if no locks are held
//
// This method implements the lock compatibility matrix:
//   - Shared + Shared: Compatible ✓
//   - Shared + Exclusive: Not compatible ✗
//   - Exclusive + Exclusive: Not compatible ✗
//
// Returns:
//   - true if the lock can be granted immediately
//   - false if the lock must wait
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

// grantLock grants a lock to a transaction and notifies waiting threads.
//
// If a pending request exists for the transaction, it is marked as granted.
// Otherwise, a new granted request is created and added to the lock table.
//
// After granting, all waiting threads are notified via Broadcast() so they
// can check if their lock requests can now be granted.
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

// grantPendingLocks attempts to grant any pending locks that can now be granted.
//
// This is called after a lock is released to check if any waiting transactions
// can now acquire their requested locks. Locks are granted in FIFO order
// (based on request order in the lock table).
//
// When a lock is granted, the waiting transaction is notified via Broadcast()
// so it can proceed.
func (lm *LockManager) grantPendingLocks(rid RID) {
	requests := lm.lockTable[rid]
	for _, req := range requests {
		if !req.Granted {
			if lm.canGrantLock(rid, req.Mode) {
				req.Granted = true
				req.Cond.Broadcast()
				// For exclusive locks, only grant one at a time to maintain FIFO order
				if req.Mode == LockModeExclusive {
					return
				}
				// For shared locks, continue to grant more shared locks
				// Note: After granting, we need to re-check canGrantLock for the next request
				// because the granted lock affects compatibility
			} else {
				// If we can't grant this lock, we can't grant any later locks either
				// (they're waiting for this one or earlier ones)
				return
			}
		}
	}
}

// updateWaitForGraph updates the wait-for graph to reflect current lock wait relationships.
//
// The wait-for graph is a directed graph where an edge from transaction A to
// transaction B means A is waiting for B to release a lock.
//
// This method:
//  1. Identifies all transactions that currently hold locks on the tuple
//  2. For each waiting transaction, clears old edges related to this RID and adds new edges to all holding transactions
//
// The graph is used by hasDeadlock() to detect cycles, which indicate deadlocks.
func (lm *LockManager) updateWaitForGraph(rid RID) {
	requests := lm.lockTable[rid]

	// Find granted transactions (currently holding locks)
	grantedTxns := make(map[TransactionID]bool)
	// Collect all transactions involved with this RID (for cleanup)
	allTxns := make(map[TransactionID]bool)
	for _, req := range requests {
		allTxns[req.TxnID] = true
		if req.Granted {
			grantedTxns[req.TxnID] = true
		}
	}

	// Update wait-for graph: for each waiting transaction, update edges
	for _, req := range requests {
		if !req.Granted {
			if lm.waitFor[req.TxnID] == nil {
				lm.waitFor[req.TxnID] = make(map[TransactionID]bool)
			}
			// Remove old edges related to this RID (transactions that were holding locks but no longer are)
			for txnID := range allTxns {
				if !grantedTxns[txnID] {
					delete(lm.waitFor[req.TxnID], txnID)
				}
			}
			// Add edges to all currently granted transactions
			for txnID := range grantedTxns {
				lm.waitFor[req.TxnID][txnID] = true
			}
		}
	}
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

// dfsDeadlock performs depth-first search to detect cycles in the wait-for graph.
//
// This is a recursive helper function for hasDeadlock(). It uses the standard
// DFS cycle detection algorithm with a recursion stack.
//
// Important: The wait-for graph is a DIRECTED graph (not undirected).
//   - Edge A -> B means "transaction A is waiting for transaction B"
//   - Direction matters: A -> B is different from B -> A
//   - In an undirected graph, visited alone might work, but in a directed graph,
//     we need recStack to distinguish cycles from multiple paths to the same node.
//
// Why both visited and recStack are needed:
//
//	visited: Tracks all nodes visited during the entire DFS traversal.
//	         Prevents infinite loops and redundant work.
//
//	recStack: Tracks nodes in the current recursion path (backtracking path).
//	          Only nodes in recStack can form a cycle with the current node.
//
// Why NOT just check visited[waiterID]?
//
//	If we return true whenever visited[waiterID] is true, we would incorrectly
//	detect cycles in DAGs (Directed Acyclic Graphs) with multiple paths to
//	the same node.
//
// Example: False positive if only checking visited:
//
//	Graph: A -> B -> C -> D
//	       A -> E -> C
//
//	This is NOT a cycle (it's a DAG), but if we only check visited:
//
//	1. Explore A->B->C->D:
//	   visited = {A, B, C, D}
//	   recStack = {A, B, C, D}
//
//	2. Backtrack to A:
//	   recStack = {A}  (B, C, D removed)
//
//	3. Explore A->E->C:
//	   - C is in visited ✓
//	   - If we return true here, we'd incorrectly detect a cycle!
//	   - But C is NOT in recStack, so it's safe (visited in different branch)
//
//	With recStack check:
//	   - visited[C] = true ✓
//	   - recStack[C] = false ✗
//	   - No cycle detected (correct!)
//
// Example: True cycle detection:
//
//	Graph: A -> B -> C -> A
//
//	When exploring A->B->C->A:
//	   - A is in visited (from start) ✓
//	   - A is in recStack (still in current path) ✓
//	   - Cycle detected correctly!
//
// Summary:
//
//	visited[waiterID] = true  → Node was visited before (could be in any branch)
//	recStack[waiterID] = true → Node is in current path (forms a cycle!)
//
// Algorithm:
//  1. Mark current node as visited and add to recursion stack
//  2. For each neighbor (transaction this one is waiting for):
//     - If not visited, recursively check for cycles
//     - If visited and in recursion stack, cycle detected
//  3. Remove from recursion stack before returning (backtracking)
//
// Parameters:
//   - txnID: Current transaction being examined
//   - visited: Set of transactions already visited in this DFS traversal
//   - recStack: Set of transactions in the current recursion path
//
// Returns:
//   - true if a cycle is detected
//   - false if no cycle exists from this transaction
func (lm *LockManager) dfsDeadlock(txnID TransactionID, visited map[TransactionID]bool, recStack map[TransactionID]bool) bool {
	visited[txnID] = true
	recStack[txnID] = true

	// waitFor[txnID] contains transactions that txnID is waiting for
	waitingFor := lm.waitFor[txnID]
	for blockingTxnID := range waitingFor {
		if !visited[blockingTxnID] {
			if lm.dfsDeadlock(blockingTxnID, visited, recStack) {
				return true
			}
		} else if recStack[blockingTxnID] {
			// Cycle detected: blockingTxnID is in the current recursion path
			return true
		}
	}

	recStack[txnID] = false
	return false
}

// removeRequest removes a lock request from the queue and updates the wait-for graph.
//
// This is typically called when a deadlock is detected and a transaction
// needs to be removed from the wait queue. The request is removed from the
// lock table, and all wait-for graph edges involving this transaction are
// cleaned up.
//
// After removing the request, grantPendingLocks is called to check if any
// waiting transactions can now acquire their locks.
func (lm *LockManager) removeRequest(rid RID, req *LockRequest) {
	requests := lm.lockTable[rid]
	newRequests := make([]*LockRequest, 0, len(requests))
	for _, r := range requests {
		if r != req {
			newRequests = append(newRequests, r)
		}
	}
	lm.lockTable[rid] = newRequests

	// Remove from wait-for graph
	delete(lm.waitFor, req.TxnID)
	for _, waiters := range lm.waitFor {
		delete(waiters, req.TxnID)
	}

	// Update wait-for graph for this RID after removing the request
	lm.updateWaitForGraph(rid)

	// Try to grant pending locks after removing the request
	lm.grantPendingLocks(rid)
}
