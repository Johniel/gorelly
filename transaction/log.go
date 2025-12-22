package transaction

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/Johniel/gorelly/disk"
)

var (
	ErrLogCorrupted = errors.New("log file is corrupted")
)

type LogRecordType int

const (
	LogRecordTypeUpdate LogRecordType = iota
	LogRecordTypeCommit
	LogRecordTypeAbort
	LogRecordTypeBegin
	LogRecordTypeCheckpoint
)

type LogRecord struct {
	Type     LogRecordType
	TxnID    TransactionID
	PageID   disk.PageID
	Offset   int
	OldValue []byte
	NewValue []byte
	LSN      uint64 // Log Sequence Number
}

type LogManager struct {
	logFile *os.File
	nextLSN uint64
	mu      sync.Mutex
}

func NewLogManager(logPath string) (*LogManager, error) {
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	lm := &LogManager{
		logFile: file,
		nextLSN: 1,
	}

	// Recover LSN from log file
	if err := lm.recoverLSN(); err != nil {
		return nil, err
	}

	return lm, nil
}

// recoverLSN recovers the next LSN from the log file.
func (lm *LogManager) recoverLSN() error {
	stat, err := lm.logFile.Stat()
	if err != nil {
		return err
	}

	if stat.Size() == 0 {
		lm.nextLSN = 1
		return nil
	}

	lm.logFile.Seek(0, io.SeekStart)
	var lastLSN uint64
	for {
		var lsn uint64
		if err := binary.Read(lm.logFile, binary.BigEndian, &lsn); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		lastLSN = lsn

		var recordSize uint32
		if err := binary.Read(lm.logFile, binary.BigEndian, &recordSize); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		lm.logFile.Seek(int64(recordSize), io.SeekCurrent)
	}
	lm.nextLSN = lastLSN + 1
	return nil
}

func (lm *LogManager) AppendLog(record *LogRecord) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	record.LSN = lm.nextLSN
	lm.nextLSN++

	// Serialize log record
	data := lm.serializeRecord(record)

	// Write to log file
	if _, err := lm.logFile.Write(data); err != nil {
		return err
	}

	return lm.logFile.Sync()
}

func (lm *LogManager) serializeRecord(record *LogRecord) []byte {
	buf := make([]byte, 0, 1024)

	// LSN
	lsnBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(lsnBytes, record.LSN)
	buf = append(buf, lsnBytes...)

	// Record size (will be filled later)
	sizePos := len(buf)
	buf = append(buf, make([]byte, 4)...)

	// Type
	typeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(typeBytes, uint32(record.Type))
	buf = append(buf, typeBytes...)

	// TxnID
	txnIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(txnIDBytes, uint64(record.TxnID))
	buf = append(buf, txnIDBytes...)

	// PageID
	pageIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(pageIDBytes, uint64(record.PageID))
	buf = append(buf, pageIDBytes...)

	// Offset
	offsetBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(offsetBytes, uint32(record.Offset))
	buf = append(buf, offsetBytes...)

	// OldValue length and data
	oldValueLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(oldValueLenBytes, uint32(len(record.OldValue)))
	buf = append(buf, oldValueLenBytes...)
	buf = append(buf, record.OldValue...)

	// NewValue length and data
	newValueLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(newValueLenBytes, uint32(len(record.NewValue)))
	buf = append(buf, newValueLenBytes...)
	buf = append(buf, record.NewValue...)

	// Fill in record size
	binary.BigEndian.PutUint32(buf[sizePos:], uint32(len(buf)-sizePos-4))

	return buf
}

// ReadLog reads log records from the log file.
func (lm *LogManager) ReadLog() ([]*LogRecord, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.logFile.Seek(0, io.SeekStart)
	var records []*LogRecord

	for {
		var lsn uint64
		if err := binary.Read(lm.logFile, binary.BigEndian, &lsn); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		var recordSize uint32
		if err := binary.Read(lm.logFile, binary.BigEndian, &recordSize); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		recordData := make([]byte, recordSize)
		if _, err := io.ReadFull(lm.logFile, recordData); err != nil {
			return nil, err
		}

		record := lm.deserializeRecord(lsn, recordData)
		records = append(records, record)
	}

	return records, nil
}

func (lm *LogManager) deserializeRecord(lsn uint64, data []byte) *LogRecord {
	pos := 0

	// Type
	typeVal := binary.BigEndian.Uint32(data[pos:])
	pos += 4

	// TxnID
	txnID := TransactionID(binary.BigEndian.Uint64(data[pos:]))
	pos += 8

	// PageID
	pageID := disk.PageID(binary.BigEndian.Uint64(data[pos:]))
	pos += 8

	// Offset
	offset := int(binary.BigEndian.Uint32(data[pos:]))
	pos += 4

	// OldValue
	oldValueLen := int(binary.BigEndian.Uint32(data[pos:]))
	pos += 4
	oldValue := make([]byte, oldValueLen)
	copy(oldValue, data[pos:pos+oldValueLen])
	pos += oldValueLen

	// NewValue
	newValueLen := int(binary.BigEndian.Uint32(data[pos:]))
	pos += 4
	newValue := make([]byte, newValueLen)
	copy(newValue, data[pos:pos+newValueLen])

	return &LogRecord{
		LSN:      lsn,
		Type:     LogRecordType(typeVal),
		TxnID:    txnID,
		PageID:   pageID,
		Offset:   offset,
		OldValue: oldValue,
		NewValue: newValue,
	}
}

// Flush flushes the log to disk.
func (lm *LogManager) Flush() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.logFile.Sync()
}

// Close closes the log file.
func (lm *LogManager) Close() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.logFile.Close()
}
