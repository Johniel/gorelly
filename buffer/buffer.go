// Package buffer provides buffer pool management for database pages.
// It implements a page cache that keeps frequently accessed pages in memory.
package buffer

import (
	"errors"
	"io"
	"sync"

	"github.com/Johniel/gorelly/disk"
)

var (
	// ErrNoFreeBuffer is returned when no free buffer is available in the buffer pool.
	ErrNoFreeBuffer = errors.New("no free buffer available in buffer pool")
)

// BufferID identifies a buffer slot in the buffer pool.
type BufferID uint

// Page represents a fixed-size page (4096 bytes).
type Page = [disk.PageSize]byte

// Buffer represents a cached page in memory.
// It contains the page data and metadata about its state.
type Buffer struct {
	PageID  disk.PageID
	Page    *Page
	IsDirty bool
	mu      sync.Mutex
}

func NewBuffer() *Buffer {
	return &Buffer{
		PageID:  disk.InvalidPageID,
		Page:    &Page{},
		IsDirty: false,
	}
}

// Frame wraps a Buffer with usage tracking for the buffer pool replacement algorithm.
type Frame struct {
	UsageCount uint64  // Number of times this buffer has been accessed
	Buffer     *Buffer // The actual buffer
	mu         sync.RWMutex
}

// BufferPool manages a fixed-size pool of page buffers.
// It implements a clock replacement algorithm to evict pages when the pool is full.
type BufferPool struct {
	buffers      []*Frame
	nextVictimID BufferID // Next buffer to consider for eviction (clock hand)
	mu           sync.Mutex
}

func NewBufferPool(poolSize int) *BufferPool {
	buffers := make([]*Frame, poolSize)
	for i := range buffers {
		buffers[i] = &Frame{
			Buffer: NewBuffer(),
		}
	}
	return &BufferPool{
		buffers:      buffers,
		nextVictimID: 0,
	}
}

func (bp *BufferPool) Size() int {
	return len(bp.buffers)
}

func (bp *BufferPool) Evict() (BufferID, bool) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	poolSize := bp.Size()
	consecutivePinned := 0

	for {
		nextVictimID := bp.nextVictimID
		frame := bp.buffers[nextVictimID]
		frame.mu.Lock()

		if frame.UsageCount == 0 {
			frame.mu.Unlock()
			return nextVictimID, true
		}

		// Check if buffer is still referenced elsewhere
		// In Go, we can't easily check reference count, so we use a simpler approach
		// If usage count is high, we decrement it
		if frame.UsageCount > 0 {
			frame.UsageCount--
			consecutivePinned = 0
		} else {
			consecutivePinned++
			if consecutivePinned >= poolSize {
				frame.mu.Unlock()
				return 0, false
			}
		}
		frame.mu.Unlock()

		bp.nextVictimID = BufferID((uint(nextVictimID) + 1) % uint(poolSize))
	}
}

// BufferPoolManager coordinates between disk I/O and the buffer pool.
// It maintains a page table mapping page IDs to buffer slots and handles
// page fetching, creation, and eviction.
type BufferPoolManager struct {
	disk      *disk.DiskManager
	pool      *BufferPool
	pageTable map[disk.PageID]BufferID // Maps page IDs to buffer slots
	mu        sync.RWMutex
}

func NewBufferPoolManager(dm *disk.DiskManager, pool *BufferPool) *BufferPoolManager {
	return &BufferPoolManager{
		disk:      dm,
		pool:      pool,
		pageTable: map[disk.PageID]BufferID{},
	}
}

func (bpm *BufferPoolManager) FetchPage(pageID disk.PageID) (*Buffer, error) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	if bufferID, ok := bpm.pageTable[pageID]; ok {
		frame := bpm.pool.buffers[bufferID]
		frame.mu.Lock()
		frame.UsageCount++
		frame.mu.Unlock()
		return frame.Buffer, nil
	}

	bufferID, ok := bpm.pool.Evict()
	if !ok {
		return nil, ErrNoFreeBuffer
	}

	frame := bpm.pool.buffers[bufferID]
	frame.mu.Lock()
	defer frame.mu.Unlock()

	evictPageID := frame.Buffer.PageID
	if frame.Buffer.IsDirty {
		if err := bpm.disk.WritePageData(evictPageID, frame.Buffer.Page[:]); err != nil {
			return nil, err
		}
	}

	frame.Buffer.PageID = pageID
	frame.Buffer.IsDirty = false
	if err := bpm.disk.ReadPageData(pageID, frame.Buffer.Page[:]); err != nil {
		if err != io.EOF {
			return nil, err
		}
		// If EOF, page doesn't exist yet, initialize with zeros
		*frame.Buffer.Page = Page{}
	}

	delete(bpm.pageTable, evictPageID)
	bpm.pageTable[pageID] = bufferID
	return frame.Buffer, nil
}

func (bpm *BufferPoolManager) CreatePage() (*Buffer, error) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	bufferID, ok := bpm.pool.Evict()
	if !ok {
		return nil, ErrNoFreeBuffer
	}

	frame := bpm.pool.buffers[bufferID]
	frame.mu.Lock()
	defer frame.mu.Unlock()

	evictPageID := frame.Buffer.PageID
	if frame.Buffer.IsDirty {
		if err := bpm.disk.WritePageData(evictPageID, frame.Buffer.Page[:]); err != nil {
			return nil, err
		}
	}

	pageID := bpm.disk.AllocatePage()
	*frame.Buffer = *NewBuffer()
	frame.Buffer.PageID = pageID
	frame.UsageCount = 1

	delete(bpm.pageTable, evictPageID)
	bpm.pageTable[pageID] = bufferID

	return frame.Buffer, nil
}

func (bpm *BufferPoolManager) Flush() error {
	bpm.mu.RLock()
	defer bpm.mu.RUnlock()

	for pageID, bufferID := range bpm.pageTable {
		frame := bpm.pool.buffers[bufferID]
		frame.mu.RLock()
		if frame.Buffer.IsDirty {
			if err := bpm.disk.WritePageData(pageID, frame.Buffer.Page[:]); err != nil {
				frame.mu.RUnlock()
				return err
			}
			frame.Buffer.IsDirty = false
		}
		frame.mu.RUnlock()
	}

	return bpm.disk.Sync()
}
