// Package btree provides meta page structures for B+ tree metadata.
package btree

import (
	"unsafe"

	"github.com/Johniel/gorelly/disk"
)

// MetaHeader contains metadata for a B+ tree.
type MetaHeader struct {
	RootPageID disk.PageID // Page ID of the root node
}

// MetaHeaderSize is the size of the meta header (8 bytes for PageID).
const MetaHeaderSize = 8

// Meta represents a meta page containing B+ tree metadata.
// The meta page stores the root page ID of the tree.
type Meta struct {
	header *MetaHeader
}

func NewMeta(page []byte) *Meta {
	if len(page) < MetaHeaderSize {
		panic("meta page too small")
	}
	header := (*MetaHeader)(unsafe.Pointer(&page[0]))
	return &Meta{header: header}
}

func (m *Meta) RootPageID() disk.PageID {
	return m.header.RootPageID
}

func (m *Meta) SetRootPageID(pageId disk.PageID) {
	m.header.RootPageID = pageId
}
