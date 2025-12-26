// Package disk provides disk I/O management for the database.
// It handles reading and writing pages to/from disk files.
package disk

import (
	"encoding/binary"
	"io"
	"os"
)

// PageSize is the size of a page in bytes (4KB).
const PageSize = 4096

// PageID represents a unique identifier for a page on disk.
// It is used to locate pages in the heap file.
type PageID uint64

// InvalidPageID represents an invalid or uninitialized page ID.
const InvalidPageID = PageID(^uint64(0))

func (p PageID) Valid() bool {
	return p != InvalidPageID
}

func (p PageID) ToU64() uint64 {
	return uint64(p)
}

func PageIDFromBytes(bytes []byte) PageID {
	return PageID(binary.LittleEndian.Uint64(bytes))
}

func (p PageID) ToBytes() []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(p))
	return bytes
}

// DiskManager manages disk I/O operations for the database.
// It handles reading and writing pages to/from a heap file.
// The heap file is organized as a sequence of fixed-size pages.
type DiskManager struct {
	heapFile   *os.File
	nextPageID uint64
}

func NewDiskManager(heapFile *os.File) (*DiskManager, error) {
	stat, err := heapFile.Stat()
	if err != nil {
		return nil, err
	}
	heapFileSize := stat.Size()
	nextPageID := uint64(heapFileSize) / PageSize
	return &DiskManager{
		heapFile:   heapFile,
		nextPageID: nextPageID,
	}, nil
}

func OpenDiskManager(heapFilePath string) (*DiskManager, error) {
	heapFile, err := os.OpenFile(heapFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return NewDiskManager(heapFile)
}

func (dm *DiskManager) ReadPageData(pageID PageID, data []byte) error {
	offset := int64(PageSize) * int64(pageID.ToU64())
	_, err := dm.heapFile.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = io.ReadFull(dm.heapFile, data)
	return err
}

func (dm *DiskManager) WritePageData(pageID PageID, data []byte) error {
	offset := int64(PageSize) * int64(pageID.ToU64())
	_, err := dm.heapFile.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = dm.heapFile.Write(data)
	return err
}

func (dm *DiskManager) AllocatePage() PageID {
	pageID := dm.nextPageID
	dm.nextPageID++
	return PageID(pageID)
}

func (dm *DiskManager) Sync() error {
	if err := dm.heapFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (dm *DiskManager) Close() error {
	return dm.heapFile.Close()
}
