package transaction

import (
	"os"
	"testing"

	"github.com/Johniel/gorelly/disk"
)

func TestLogManagerBasic(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_log_*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	lm, err := NewLogManager(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer lm.Close()

	// Test AppendLog
	record := &LogRecord{
		Type:     LogRecordTypeUpdate,
		TxnID:    1,
		PageID:   disk.PageID(10),
		Offset:   100,
		OldValue: []byte{0, 0, 0, 0},
		NewValue: []byte{1, 2, 3, 4},
	}

	if err := lm.AppendLog(record); err != nil {
		t.Fatalf("Failed to append log: %v", err)
	}

	if record.LSN == 0 {
		t.Error("LSN should be assigned")
	}
	if record.LSN != 1 {
		t.Errorf("Expected LSN 1, got %d", record.LSN)
	}
}

func TestLogManagerReadLog(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_log_*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	lm, err := NewLogManager(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer lm.Close()

	// Append multiple records
	records := []*LogRecord{
		{
			Type:     LogRecordTypeBegin,
			TxnID:    1,
			PageID:   disk.PageID(0),
			Offset:   0,
			OldValue: nil,
			NewValue: nil,
		},
		{
			Type:     LogRecordTypeUpdate,
			TxnID:    1,
			PageID:   disk.PageID(10),
			Offset:   100,
			OldValue: []byte{0, 0},
			NewValue: []byte{1, 2},
		},
		{
			Type:     LogRecordTypeCommit,
			TxnID:    1,
			PageID:   disk.PageID(0),
			Offset:   0,
			OldValue: nil,
			NewValue: nil,
		},
	}

	for _, record := range records {
		if err := lm.AppendLog(record); err != nil {
			t.Fatalf("Failed to append log: %v", err)
		}
	}

	// Read logs
	readRecords, err := lm.ReadLog()
	if err != nil {
		t.Fatalf("Failed to read log: %v", err)
	}

	if len(readRecords) != len(records) {
		t.Errorf("Expected %d records, got %d", len(records), len(readRecords))
	}

	// Verify records
	for i, expected := range records {
		if i >= len(readRecords) {
			t.Fatalf("Missing record %d", i)
		}
		actual := readRecords[i]

		if actual.Type != expected.Type {
			t.Errorf("Record %d: Expected type %v, got %v", i, expected.Type, actual.Type)
		}
		if actual.TxnID != expected.TxnID {
			t.Errorf("Record %d: Expected TxnID %d, got %d", i, expected.TxnID, actual.TxnID)
		}
		if actual.PageID != expected.PageID {
			t.Errorf("Record %d: Expected PageID %d, got %d", i, expected.PageID, actual.PageID)
		}
		if actual.Offset != expected.Offset {
			t.Errorf("Record %d: Expected Offset %d, got %d", i, expected.Offset, actual.Offset)
		}
		if len(actual.OldValue) != len(expected.OldValue) {
			t.Errorf("Record %d: Expected OldValue length %d, got %d", i, len(expected.OldValue), len(actual.OldValue))
		}
		if len(actual.NewValue) != len(expected.NewValue) {
			t.Errorf("Record %d: Expected NewValue length %d, got %d", i, len(expected.NewValue), len(actual.NewValue))
		}
		if actual.LSN != uint64(i+1) {
			t.Errorf("Record %d: Expected LSN %d, got %d", i, i+1, actual.LSN)
		}
	}
}

func TestLogManagerLSNRecovery(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_log_*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	// First session: write some logs
	lm1, err := NewLogManager(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}

	record1 := &LogRecord{
		Type:     LogRecordTypeBegin,
		TxnID:    1,
		PageID:   disk.PageID(0),
		Offset:   0,
		OldValue: nil,
		NewValue: nil,
	}
	if err := lm1.AppendLog(record1); err != nil {
		t.Fatalf("Failed to append log: %v", err)
	}
	lm1.Close()

	// Second session: should recover LSN
	lm2, err := NewLogManager(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer lm2.Close()

	record2 := &LogRecord{
		Type:     LogRecordTypeCommit,
		TxnID:    1,
		PageID:   disk.PageID(0),
		Offset:   0,
		OldValue: nil,
		NewValue: nil,
	}
	if err := lm2.AppendLog(record2); err != nil {
		t.Fatalf("Failed to append log: %v", err)
	}

	if record2.LSN != 2 {
		t.Errorf("Expected LSN 2 (after recovery), got %d", record2.LSN)
	}
}

func TestLogManagerFlush(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_log_*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	lm, err := NewLogManager(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer lm.Close()

	record := &LogRecord{
		Type:     LogRecordTypeUpdate,
		TxnID:    1,
		PageID:   disk.PageID(10),
		Offset:   100,
		OldValue: []byte{0},
		NewValue: []byte{1},
	}

	if err := lm.AppendLog(record); err != nil {
		t.Fatalf("Failed to append log: %v", err)
	}

	// Flush should succeed
	if err := lm.Flush(); err != nil {
		t.Fatalf("Failed to flush log: %v", err)
	}
}

func TestLogManagerEmptyFile(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_log_*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	lm, err := NewLogManager(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer lm.Close()

	// First record should have LSN 1
	record := &LogRecord{
		Type:     LogRecordTypeBegin,
		TxnID:    1,
		PageID:   disk.PageID(0),
		Offset:   0,
		OldValue: nil,
		NewValue: nil,
	}

	if err := lm.AppendLog(record); err != nil {
		t.Fatalf("Failed to append log: %v", err)
	}

	if record.LSN != 1 {
		t.Errorf("Expected LSN 1 for first record, got %d", record.LSN)
	}
}

func TestLogManagerLargeValues(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_log_*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	lm, err := NewLogManager(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer lm.Close()

	// Test with large values
	largeOldValue := make([]byte, 1000)
	largeNewValue := make([]byte, 2000)
	for i := range largeOldValue {
		largeOldValue[i] = byte(i % 256)
	}
	for i := range largeNewValue {
		largeNewValue[i] = byte((i + 100) % 256)
	}

	record := &LogRecord{
		Type:     LogRecordTypeUpdate,
		TxnID:    1,
		PageID:   disk.PageID(10),
		Offset:   100,
		OldValue: largeOldValue,
		NewValue: largeNewValue,
	}

	if err := lm.AppendLog(record); err != nil {
		t.Fatalf("Failed to append log with large values: %v", err)
	}

	// Read and verify
	readRecords, err := lm.ReadLog()
	if err != nil {
		t.Fatalf("Failed to read log: %v", err)
	}

	if len(readRecords) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(readRecords))
	}

	readRecord := readRecords[0]
	if len(readRecord.OldValue) != len(largeOldValue) {
		t.Errorf("Expected OldValue length %d, got %d", len(largeOldValue), len(readRecord.OldValue))
	}
	if len(readRecord.NewValue) != len(largeNewValue) {
		t.Errorf("Expected NewValue length %d, got %d", len(largeNewValue), len(readRecord.NewValue))
	}
}

func TestLogManagerAllRecordTypes(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_log_*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	lm, err := NewLogManager(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer lm.Close()

	recordTypes := []LogRecordType{
		LogRecordTypeUpdate,
		LogRecordTypeCommit,
		LogRecordTypeAbort,
		LogRecordTypeBegin,
		LogRecordTypeCheckpoint,
	}

	for i, recordType := range recordTypes {
		record := &LogRecord{
			Type:     recordType,
			TxnID:    TransactionID(i + 1),
			PageID:   disk.PageID(i),
			Offset:   i * 10,
			OldValue: []byte{byte(i)},
			NewValue: []byte{byte(i + 1)},
		}

		if err := lm.AppendLog(record); err != nil {
			t.Fatalf("Failed to append log record type %v: %v", recordType, err)
		}
	}

	// Read and verify
	readRecords, err := lm.ReadLog()
	if err != nil {
		t.Fatalf("Failed to read log: %v", err)
	}

	if len(readRecords) != len(recordTypes) {
		t.Errorf("Expected %d records, got %d", len(recordTypes), len(readRecords))
	}

	for i, expectedType := range recordTypes {
		if i >= len(readRecords) {
			break
		}
		if readRecords[i].Type != expectedType {
			t.Errorf("Record %d: Expected type %v, got %v", i, expectedType, readRecords[i].Type)
		}
	}
}
