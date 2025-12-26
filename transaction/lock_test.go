package transaction

import (
	"github.com/Johniel/gorelly/disk"

	"sync"
	"testing"
	"time"
)

func TestLockManagerBasic(t *testing.T) {
	lm := NewLockManager()
	tm := NewTransactionManager()

	txn := tm.Begin()
	rid := RID{PageID: disk.PageID(1), SlotID: 0}

	// Test LockShared
	if err := lm.LockShared(txn, rid); err != nil {
		t.Fatalf("Failed to acquire shared lock: %v", err)
	}

	// Test Unlock
	if err := lm.Unlock(txn, rid); err != nil {
		t.Fatalf("Failed to unlock: %v", err)
	}
}

func TestLockManagerSharedCompatibility(t *testing.T) {
	lm := NewLockManager()
	tm := NewTransactionManager()

	rid := RID{PageID: disk.PageID(1), SlotID: 0}

	// Multiple shared locks should be compatible
	txn1 := tm.Begin()
	if err := lm.LockShared(txn1, rid); err != nil {
		t.Fatalf("Failed to acquire shared lock 1: %v", err)
	}

	txn2 := tm.Begin()
	if err := lm.LockShared(txn2, rid); err != nil {
		t.Fatalf("Failed to acquire shared lock 2: %v", err)
	}

	// Both should hold the lock
	lm.Unlock(txn1, rid)
	lm.Unlock(txn2, rid)
}

func TestLockManagerExclusiveExclusiveConflict(t *testing.T) {
	lm := NewLockManager()
	tm := NewTransactionManager()

	rid := RID{PageID: disk.PageID(1), SlotID: 0}

	txn1 := tm.Begin()
	if err := lm.LockExclusive(txn1, rid); err != nil {
		t.Fatalf("Failed to acquire exclusive lock 1: %v", err)
	}

	// Second exclusive lock should wait
	txn2 := tm.Begin()
	lockAcquired := make(chan bool, 1)
	go func() {
		err := lm.LockExclusive(txn2, rid)
		if err != nil {
			t.Errorf("Failed to acquire exclusive lock 2: %v", err)
		}
		lockAcquired <- true
	}()

	// Wait a bit to ensure txn2 is waiting
	time.Sleep(50 * time.Millisecond)

	// Unlock txn1, which should allow txn2 to proceed
	lm.Unlock(txn1, rid)

	// Wait for txn2 to acquire lock
	select {
	case <-lockAcquired:
		// Success
		lm.Unlock(txn2, rid)
	case <-time.After(1 * time.Second):
		t.Error("Transaction 2 did not acquire lock after unlock")
	}
}

func TestLockManagerSharedExclusiveConflict(t *testing.T) {
	lm := NewLockManager()
	tm := NewTransactionManager()

	rid := RID{PageID: disk.PageID(1), SlotID: 0}

	txn1 := tm.Begin()
	if err := lm.LockShared(txn1, rid); err != nil {
		t.Fatalf("Failed to acquire shared lock: %v", err)
	}

	// Exclusive lock should wait
	txn2 := tm.Begin()
	lockAcquired := make(chan bool, 1)
	go func() {
		err := lm.LockExclusive(txn2, rid)
		if err != nil {
			t.Errorf("Failed to acquire exclusive lock: %v", err)
		}
		lockAcquired <- true
	}()

	// Wait a bit to ensure txn2 is waiting
	time.Sleep(50 * time.Millisecond)

	// Unlock txn1, which should allow txn2 to proceed
	lm.Unlock(txn1, rid)

	// Wait for txn2 to acquire lock
	select {
	case <-lockAcquired:
		// Success
		lm.Unlock(txn2, rid)
	case <-time.After(1 * time.Second):
		t.Error("Transaction 2 did not acquire exclusive lock after shared lock release")
	}
}

func TestLockManagerExclusiveSharedConflict(t *testing.T) {
	lm := NewLockManager()
	tm := NewTransactionManager()

	rid := RID{PageID: disk.PageID(1), SlotID: 0}

	txn1 := tm.Begin()
	if err := lm.LockExclusive(txn1, rid); err != nil {
		t.Fatalf("Failed to acquire exclusive lock: %v", err)
	}

	// Shared lock should wait
	txn2 := tm.Begin()
	lockAcquired := make(chan bool, 1)
	go func() {
		err := lm.LockShared(txn2, rid)
		if err != nil {
			t.Errorf("Failed to acquire shared lock: %v", err)
		}
		lockAcquired <- true
	}()

	// Wait a bit to ensure txn2 is waiting
	time.Sleep(50 * time.Millisecond)

	// Unlock txn1, which should allow txn2 to proceed
	lm.Unlock(txn1, rid)

	// Wait for txn2 to acquire lock
	select {
	case <-lockAcquired:
		// Success
		lm.Unlock(txn2, rid)
	case <-time.After(1 * time.Second):
		t.Error("Transaction 2 did not acquire shared lock after exclusive lock release")
	}
}

func TestLockManagerUnlockAll(t *testing.T) {
	lm := NewLockManager()
	tm := NewTransactionManager()

	txn := tm.Begin()
	rid1 := RID{PageID: disk.PageID(1), SlotID: 0}
	rid2 := RID{PageID: disk.PageID(2), SlotID: 0}

	// Acquire multiple locks
	if err := lm.LockShared(txn, rid1); err != nil {
		t.Fatalf("Failed to acquire lock 1: %v", err)
	}
	if err := lm.LockExclusive(txn, rid2); err != nil {
		t.Fatalf("Failed to acquire lock 2: %v", err)
	}

	// UnlockAll should release all locks
	lm.UnlockAll(txn)

	// Verify locks are released by acquiring exclusive locks
	txn2 := tm.Begin()
	if err := lm.LockExclusive(txn2, rid1); err != nil {
		t.Errorf("Lock 1 should have been released: %v", err)
	}
	if err := lm.LockExclusive(txn2, rid2); err != nil {
		t.Errorf("Lock 2 should have been released: %v", err)
	}
	lm.UnlockAll(txn2)
}

func TestLockManagerDeadlockDetection(t *testing.T) {
	lm := NewLockManager()
	tm := NewTransactionManager()

	rid1 := RID{PageID: disk.PageID(1), SlotID: 0}
	rid2 := RID{PageID: disk.PageID(2), SlotID: 0}

	txn1 := tm.Begin()
	txn2 := tm.Begin()

	// Create a deadlock scenario:
	// txn1 locks rid1, txn2 locks rid2
	// txn1 tries to lock rid2, txn2 tries to lock rid1

	if err := lm.LockExclusive(txn1, rid1); err != nil {
		t.Fatalf("Failed to acquire lock 1 for txn1: %v", err)
	}
	if err := lm.LockExclusive(txn2, rid2); err != nil {
		t.Fatalf("Failed to acquire lock 2 for txn2: %v", err)
	}

	// txn1 tries to lock rid2 (will wait)
	txn1LockAcquired := make(chan error, 1)
	go func() {
		err := lm.LockExclusive(txn1, rid2)
		txn1LockAcquired <- err
	}()

	// Wait a bit to ensure txn1 is waiting
	time.Sleep(50 * time.Millisecond)

	// txn2 tries to lock rid1 (should detect deadlock)
	err := lm.LockExclusive(txn2, rid1)
	if err != ErrDeadlock {
		t.Errorf("Expected deadlock error, got %v", err)
	}

	// Cleanup: unlock txn2's lock on rid2 first, then unlock txn1's lock on rid1
	// This allows txn1 to acquire rid2
	lm.Unlock(txn2, rid2)
	lm.Unlock(txn1, rid1)

	// Wait for txn1 to acquire rid2
	select {
	case err := <-txn1LockAcquired:
		if err != nil {
			t.Errorf("txn1 should eventually acquire lock: %v", err)
		} else {
			lm.Unlock(txn1, rid2)
		}
	case <-time.After(1 * time.Second):
		t.Error("txn1 did not acquire lock after deadlock resolution")
	}
}

func TestLockManagerInactiveTransaction(t *testing.T) {
	lm := NewLockManager()
	tm := NewTransactionManager()

	txn := tm.Begin()
	rid := RID{PageID: disk.PageID(1), SlotID: 0}

	// Commit transaction
	if err := tm.Commit(txn); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Try to acquire lock with inactive transaction
	err := lm.LockShared(txn, rid)
	if err != ErrTransactionNotActive {
		t.Errorf("Expected ErrTransactionNotActive, got %v", err)
	}
}

func TestLockManagerConcurrentLocks(t *testing.T) {
	lm := NewLockManager()
	tm := NewTransactionManager()

	rid := RID{PageID: disk.PageID(1), SlotID: 0}
	numTxns := 10

	var wg sync.WaitGroup
	errors := make(chan error, numTxns)

	// Start multiple transactions trying to acquire shared locks
	for i := 0; i < numTxns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			txn := tm.Begin()
			if err := lm.LockShared(txn, rid); err != nil {
				errors <- err
				return
			}
			time.Sleep(10 * time.Millisecond)
			lm.Unlock(txn, rid)
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Error acquiring lock: %v", err)
	}
}

func TestLockManagerFIFOOrder(t *testing.T) {
	lm := NewLockManager()
	tm := NewTransactionManager()

	rid := RID{PageID: disk.PageID(1), SlotID: 0}

	// txn1 acquires exclusive lock
	txn1 := tm.Begin()
	if err := lm.LockExclusive(txn1, rid); err != nil {
		t.Fatalf("Failed to acquire exclusive lock: %v", err)
	}

	// txn2 and txn3 wait for the lock
	txn2 := tm.Begin()
	txn3 := tm.Begin()

	acquiredOrder := make(chan TransactionID, 2)

	// Start txn2 first to ensure it's added to the queue before txn3
	txn2Started := make(chan struct{})
	go func() {
		txn2Started <- struct{}{}
		lm.LockExclusive(txn2, rid)
		acquiredOrder <- txn2.ID
	}()

	// Wait for txn2 to start (ensures it's added to queue first)
	<-txn2Started
	time.Sleep(10 * time.Millisecond)

	// Then start txn3
	go func() {
		lm.LockExclusive(txn3, rid)
		acquiredOrder <- txn3.ID
	}()

	// Wait a bit to ensure both are waiting
	time.Sleep(50 * time.Millisecond)

	// Unlock txn1
	lm.Unlock(txn1, rid)

	// First transaction to acquire should be txn2 (FIFO)
	firstID := <-acquiredOrder
	if firstID != txn2.ID {
		t.Errorf("Expected txn2 to acquire lock first (FIFO), but txn%d acquired it", firstID)
	}

	// Unlock txn2, txn3 should acquire
	lm.Unlock(txn2, rid)
	secondID := <-acquiredOrder
	if secondID != txn3.ID {
		t.Errorf("Expected txn3 to acquire lock second, but txn%d acquired it", secondID)
	}

	lm.Unlock(txn3, rid)
}
