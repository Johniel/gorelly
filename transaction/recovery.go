// Package transaction provides recovery functionality for transaction rollback and system recovery.
package transaction

import (
	"github.com/Johniel/gorelly/buffer"
	"github.com/Johniel/gorelly/disk"
)

type RecoveryManager struct {
	logManager *LogManager
	bufmgr     *buffer.BufferPoolManager
}

// NewRecoveryManager creates a new recovery manager.
func NewRecoveryManager(logManager *LogManager, bufmgr *buffer.BufferPoolManager) *RecoveryManager {
	return &RecoveryManager{
		logManager: logManager,
		bufmgr:     bufmgr,
	}
}

func (rm *RecoveryManager) Rollback(txn *Transaction) error {
	records, err := rm.logManager.ReadLog()
	if err != nil {
		return err
	}

	var txnRecords []*LogRecord
	for i := len(records) - 1; 0 <= i; i-- {
		if records[i].TxnID == txn.ID {
			if records[i].Type == LogRecordTypeCommit || records[i].Type == LogRecordTypeAbort {
				break
			}
			if records[i].Type == LogRecordTypeUpdate {
				txnRecords = append(txnRecords, records[i])
			}
		}
	}

	for _, record := range txnRecords {
		if err := rm.undoUpdate(record); err != nil {
			return err
		}
	}
	return nil
}

func (rm *RecoveryManager) undoUpdate(record *LogRecord) error {
	buf, err := rm.bufmgr.FetchPage(record.PageID)
	if err != nil {
		return err
	}

	copy(buf.Page[record.Offset:record.Offset+len(record.OldValue)], record.OldValue)
	buf.IsDirty = true

	return nil
}

// redoUpdate redoes a single update operation.
func (rm *RecoveryManager) redoUpdate(record *LogRecord) error {
	buf, err := rm.bufmgr.FetchPage(record.PageID)
	if err != nil {
		return err
	}

	// Apply new value
	copy(buf.Page[record.Offset:record.Offset+len(record.NewValue)], record.NewValue)
	buf.IsDirty = true

	return nil
}

func (rm *RecoveryManager) Recover() error {
	records, err := rm.logManager.ReadLog()
	if err != nil {
		return err
	}

	activeTxns := make(map[TransactionID]bool)
	committedTxns := make(map[TransactionID]bool)

	for _, record := range records {
		switch record.Type {
		case LogRecordTypeBegin:
			activeTxns[record.TxnID] = true
		case LogRecordTypeCommit:
			committedTxns[record.TxnID] = true
			delete(activeTxns, record.TxnID)
		case LogRecordTypeAbort:
			delete(activeTxns, record.TxnID)
		}
	}

	dirtyPages := make(map[disk.PageID]bool)
	for _, record := range records {
		if record.Type == LogRecordTypeUpdate {
			if activeTxns[record.TxnID] {
				dirtyPages[record.PageID] = true
			}
		}
	}

	// Phase 2: Redo Phase
	// Redo all committed transactions
	for _, record := range records {
		if record.Type == LogRecordTypeUpdate {
			if committedTxns[record.TxnID] {
				if err := rm.redoUpdate(record); err != nil {
					return err
				}
			}
		}
	}

	// Phase 3: Undo Phase
	// Undo all uncommitted transactions
	for txnID := range activeTxns {
		// Find all records for this transaction in reverse order
		var txnRecords []*LogRecord
		for i := len(records) - 1; i >= 0; i-- {
			if records[i].TxnID == txnID {
				if records[i].Type == LogRecordTypeBegin {
					break
				}
				if records[i].Type == LogRecordTypeUpdate {
					txnRecords = append(txnRecords, records[i])
				}
			}
		}

		// Undo changes
		for _, record := range txnRecords {
			if err := rm.undoUpdate(record); err != nil {
				return err
			}
		}
	}

	return rm.bufmgr.Flush()
}
