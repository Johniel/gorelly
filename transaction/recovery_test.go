package transaction

import (
	"os"
	"testing"

	"github.com/Johniel/gorelly/buffer"
	"github.com/Johniel/gorelly/disk"
)

func TestRecoveryManagerRollback(t *testing.T) {
	// Setup
	tmpfile, err := os.CreateTemp("", "test_recovery_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	logFile, err := os.CreateTemp("", "test_recovery_*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(logFile.Name())
	defer logFile.Close()

	dm, err := disk.NewDiskManager(tmpfile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	logManager, err := NewLogManager(logFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer logManager.Close()

	rm := NewRecoveryManager(logManager, bufmgr)
	tm := NewTransactionManager()

	// Create a page and update it
	var buf *buffer.Buffer
	buf, err = bufmgr.CreateBuffer()
	if err != nil {
		t.Fatalf("Failed to create buffer: %v", err)
	}
	pageID := buf.PageID
	buf, err = bufmgr.FetchBuffer(pageID)
	if err != nil {
		t.Fatalf("Failed to fetch buffer: %v", err)
	}

	// Initial value
	initialValue := []byte{0, 0, 0, 0}
	copy(buf.Page[100:104], initialValue)
	buf.IsDirty = true
	bufmgr.Flush()

	// Start transaction and log update
	txn := tm.Begin()

	// Log the update
	updateRecord := &LogRecord{
		Type:     LogRecordTypeUpdate,
		TxnID:    txn.ID,
		PageID:   pageID,
		Offset:   100,
		OldValue: initialValue,
		NewValue: []byte{1, 2, 3, 4},
	}
	if err := logManager.AppendLog(updateRecord); err != nil {
		t.Fatalf("Failed to append log: %v", err)
	}

	// Apply the update
	buf, err = bufmgr.FetchBuffer(pageID)
	if err != nil {
		t.Fatalf("Failed to fetch buffer: %v", err)
	}
	copy(buf.Page[100:104], []byte{1, 2, 3, 4})
	buf.IsDirty = true

	// Rollback
	if err := rm.Rollback(txn); err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify rollback: page should have initial value
	buf, err = bufmgr.FetchBuffer(pageID)
	if err != nil {
		t.Fatalf("Failed to fetch buffer: %v", err)
	}

	rolledBackValue := buf.Page[100:104]
	if !equalBytes(rolledBackValue, initialValue) {
		t.Errorf("Rollback failed: expected %v, got %v", initialValue, rolledBackValue)
	}
}

func TestRecoveryManagerRecover(t *testing.T) {
	// Setup
	tmpfile, err := os.CreateTemp("", "test_recovery_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	logFile, err := os.CreateTemp("", "test_recovery_*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(logFile.Name())
	defer logFile.Close()

	dm, err := disk.NewDiskManager(tmpfile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	logManager, err := NewLogManager(logFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer logManager.Close()

	rm := NewRecoveryManager(logManager, bufmgr)

	// Create a page
	var buf *buffer.Buffer
	buf, err = bufmgr.CreateBuffer()
	if err != nil {
		t.Fatalf("Failed to create buffer: %v", err)
	}
	pageID := buf.PageID
	buf, err = bufmgr.FetchBuffer(pageID)
	if err != nil {
		t.Fatalf("Failed to fetch buffer: %v", err)
	}

	// Initial value
	initialValue := []byte{0, 0, 0, 0}
	copy(buf.Page[100:104], initialValue)
	buf.IsDirty = true
	bufmgr.Flush()

	// Transaction 1: Committed (should be redone)
	txn1ID := TransactionID(1)
	beginRecord1 := &LogRecord{
		Type:     LogRecordTypeBegin,
		TxnID:    txn1ID,
		PageID:   disk.PageID(0),
		Offset:   0,
		OldValue: nil,
		NewValue: nil,
	}
	if err := logManager.AppendLog(beginRecord1); err != nil {
		t.Fatalf("Failed to append begin log: %v", err)
	}

	updateRecord1 := &LogRecord{
		Type:     LogRecordTypeUpdate,
		TxnID:    txn1ID,
		PageID:   pageID,
		Offset:   100,
		OldValue: initialValue,
		NewValue: []byte{1, 1, 1, 1},
	}
	if err := logManager.AppendLog(updateRecord1); err != nil {
		t.Fatalf("Failed to append update log: %v", err)
	}

	commitRecord1 := &LogRecord{
		Type:     LogRecordTypeCommit,
		TxnID:    txn1ID,
		PageID:   disk.PageID(0),
		Offset:   0,
		OldValue: nil,
		NewValue: nil,
	}
	if err := logManager.AppendLog(commitRecord1); err != nil {
		t.Fatalf("Failed to append commit log: %v", err)
	}

	// Transaction 2: Not committed (should be undone)
	txn2ID := TransactionID(2)
	beginRecord2 := &LogRecord{
		Type:     LogRecordTypeBegin,
		TxnID:    txn2ID,
		PageID:   disk.PageID(0),
		Offset:   0,
		OldValue: nil,
		NewValue: nil,
	}
	if err := logManager.AppendLog(beginRecord2); err != nil {
		t.Fatalf("Failed to append begin log: %v", err)
	}

	updateRecord2 := &LogRecord{
		Type:     LogRecordTypeUpdate,
		TxnID:    txn2ID,
		PageID:   pageID,
		Offset:   100,
		OldValue: []byte{1, 1, 1, 1},
		NewValue: []byte{2, 2, 2, 2},
	}
	if err := logManager.AppendLog(updateRecord2); err != nil {
		t.Fatalf("Failed to append update log: %v", err)
	}

	// Simulate crash: apply txn1's update but not txn2's
	buf, err = bufmgr.FetchBuffer(pageID)
	if err != nil {
		t.Fatalf("Failed to fetch buffer: %v", err)
	}
	copy(buf.Page[100:104], []byte{1, 1, 1, 1})
	buf.IsDirty = true
	bufmgr.Flush()

	// Recover
	if err := rm.Recover(); err != nil {
		t.Fatalf("Failed to recover: %v", err)
	}

	// Verify: txn1 should be redone, txn2 should be undone
	// Final value should be txn1's value (since txn2 was undone)
	buf, err = bufmgr.FetchBuffer(pageID)
	if err != nil {
		t.Fatalf("Failed to fetch buffer: %v", err)
	}

	finalValue := buf.Page[100:104]
	expectedValue := []byte{1, 1, 1, 1}
	if !equalBytes(finalValue, expectedValue) {
		t.Errorf("Recovery failed: expected %v, got %v", expectedValue, finalValue)
	}
}

func TestRecoveryManagerMultipleUpdates(t *testing.T) {
	// Setup
	tmpfile, err := os.CreateTemp("", "test_recovery_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	logFile, err := os.CreateTemp("", "test_recovery_*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(logFile.Name())
	defer logFile.Close()

	dm, err := disk.NewDiskManager(tmpfile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	logManager, err := NewLogManager(logFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer logManager.Close()

	rm := NewRecoveryManager(logManager, bufmgr)
	tm := NewTransactionManager()

	// Create a page
	var buf *buffer.Buffer
	buf, err = bufmgr.CreateBuffer()
	if err != nil {
		t.Fatalf("Failed to create buffer: %v", err)
	}
	pageID := buf.PageID
	buf, err = bufmgr.FetchBuffer(pageID)
	if err != nil {
		t.Fatalf("Failed to fetch buffer: %v", err)
	}

	// Initial value
	initialValue := []byte{0, 0, 0, 0}
	copy(buf.Page[100:104], initialValue)
	buf.IsDirty = true
	bufmgr.Flush()

	// Transaction with multiple updates
	txn := tm.Begin()

	// First update
	update1 := &LogRecord{
		Type:     LogRecordTypeUpdate,
		TxnID:    txn.ID,
		PageID:   pageID,
		Offset:   100,
		OldValue: initialValue,
		NewValue: []byte{1, 1, 1, 1},
	}
	if err := logManager.AppendLog(update1); err != nil {
		t.Fatalf("Failed to append log: %v", err)
	}

	// Second update
	update2 := &LogRecord{
		Type:     LogRecordTypeUpdate,
		TxnID:    txn.ID,
		PageID:   pageID,
		Offset:   200,
		OldValue: []byte{0, 0, 0, 0},
		NewValue: []byte{2, 2, 2, 2},
	}
	if err := logManager.AppendLog(update2); err != nil {
		t.Fatalf("Failed to append log: %v", err)
	}

	// Apply updates
	buf, err = bufmgr.FetchBuffer(pageID)
	if err != nil {
		t.Fatalf("Failed to fetch buffer: %v", err)
	}
	copy(buf.Page[100:104], []byte{1, 1, 1, 1})
	copy(buf.Page[200:204], []byte{2, 2, 2, 2})
	buf.IsDirty = true

	// Rollback
	if err := rm.Rollback(txn); err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify both updates are rolled back
	buf, err = bufmgr.FetchBuffer(pageID)
	if err != nil {
		t.Fatalf("Failed to fetch buffer: %v", err)
	}

	value1 := buf.Page[100:104]
	if !equalBytes(value1, initialValue) {
		t.Errorf("Rollback failed at offset 100: expected %v, got %v", initialValue, value1)
	}

	value2 := buf.Page[200:204]
	if !equalBytes(value2, initialValue) {
		t.Errorf("Rollback failed at offset 200: expected %v, got %v", initialValue, value2)
	}
}

// Helper function to compare byte slices
func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
