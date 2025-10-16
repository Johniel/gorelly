package gorelly

import (
	"errors"
)

type BufferID int64

const (
	NilBufferID = -1
)

type Buffer struct {
	PageID  PageID
	Page    Page
	IsDirty bool
}

type Frame struct {
	UsageCount uint64
	Buffer     *Buffer
}

type BufferPool struct {
	Buffers      []*Frame
	NextVictimID BufferID
}

func (p *BufferPool) Size() int {
	return len(p.Buffers)
}

func (p *BufferPool) Evict() BufferID {
	for i := 0; i < p.Size(); i += 1 {
		nextVictimID := (p.NextVictimID + 1) % BufferID(p.Size())
		if p.Buffers[nextVictimID] == nil || p.Buffers[nextVictimID].UsageCount == 0 {
			return BufferID(nextVictimID)
		}
		p.NextVictimID = nextVictimID
	}
	return NilBufferID
}

type BufferPoolManager struct {
	Disk      *DiskManager
	Pool      *BufferPool
	PageTable map[PageID]BufferID
}

var (
	ErrPageNotFound = errors.New("page not found")
	ErrNoFreeBuffer = errors.New("no free buffer")
)

func (m *BufferPoolManager) FetchPage(pageID PageID) (*Buffer, error) {
	bufferID, ok := m.PageTable[pageID]
	if ok {
		frame := m.Pool.Buffers[bufferID]
		frame.UsageCount += 1
		cloned := *frame.Buffer
		return &cloned, nil
	}
	bufferID = m.Pool.Evict()
	if bufferID == NilBufferID {
		return nil, ErrNoFreeBuffer
	}
	frame := m.Pool.Buffers[bufferID]
	evictPageID := frame.Buffer.PageID
	buffer := frame.Buffer
	if buffer.IsDirty {
		m.Disk.WritePageData(evictPageID, buffer.Page)
		buffer.IsDirty = false
	}
	buffer.PageID = pageID
	m.Disk.ReadPageData(buffer.PageID, &buffer.Page)
	frame.UsageCount += 1
	cloned := *frame.Buffer
	delete(m.PageTable, evictPageID)
	m.PageTable[pageID] = bufferID
	return &cloned, nil
}

func (m *BufferPoolManager) CreatePage() {}
