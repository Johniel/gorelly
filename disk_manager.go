package gorelly

import (
	"io/fs"
	"os"
	"path/filepath"
)

const (
	PageSize = 4096
)

type Page [PageSize]byte

type PageID int64

type DiskManager struct {
	HeapFile   *os.File
	NextPageID PageID
}

func (m *DiskManager) AllocatePage() PageID {
	pageID := PageID(m.NextPageID)
	m.NextPageID += 1
	return pageID
}

func (m *DiskManager) ReadPageData(pageID PageID, data *Page) error {
	offset := int64(PageSize * m.NextPageID)
	m.HeapFile.Seek(offset, 0)
	_, err := m.HeapFile.Read(data[:])
	if err != nil {
		return err
	}
	return nil
}

func (m *DiskManager) WritePageData(pageID PageID, data Page) error {
	offset := int64(PageSize * pageID)
	m.HeapFile.Seek(offset, 0)
	_, err := m.HeapFile.Write(data[:])
	return err
}

func NewDiskManager(path string) (*DiskManager, error) {
	fileSystem := os.DirFS(filepath.Dir(path))
	fileinfo, err := fs.Stat(fileSystem, path)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &DiskManager{
		HeapFile:   file,
		NextPageID: PageID(fileinfo.Size() / PageSize),
	}, nil
}

func Open(path string) (*DiskManager, error) {
	return NewDiskManager(path)
}
