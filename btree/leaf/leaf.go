package leaf

import (
	"unsafe"

	"github.com/Johniel/gorelly/disk"
	"github.com/Johniel/gorelly/slotted"
)

const LeafHeaderSize = 16

// LeafHeader contains metadata for a leaf node.
type LeafHeader struct {
	PrevPageID disk.PageID // Previous leaf page ID (for sequential traversal)
	NextPageID disk.PageID // Next leaf page ID (for sequential traversal)
}

// Leaf represents a leaf node in a B+ tree.
// Leaf nodes store key-value pairs and are linked together in a linked list
// for efficient sequential access and range queries.
//
// Body structure:
//   - body is a Slotted page structure that manages variable-length Pair records
//   - Each Pair contains a key and its associated value (both encoded as bytes)
//   - Pairs are stored in sorted order by key
//   - The body manages the storage layout: pointer array at the beginning,
//     free space in the middle, and data records at the end (stored backwards)
//
// Example: A leaf node with 2 pairs (key1→value1, key2→value2):
//   - body.Data(0) returns the first Pair (key1, value1)
//   - body.Data(1) returns the second Pair (key2, value2)
//   - header.PrevPageId and header.NextPageId link this leaf to adjacent leaves
//     for sequential traversal
type Leaf struct {
	header *LeafHeader
	body   *slotted.Slotted // Slotted page structure storing Pair records (key-value pairs)
	page   []byte           // Keep reference to full page for header updates
}

func NewLeaf(bodyBytes []byte) *Leaf {
	// bodyBytes is the body part after node header
	// Leaf header comes right after the node header
	if len(bodyBytes) < LeafHeaderSize {
		panic("leaf header must fit")
	}
	header := (*LeafHeader)(unsafe.Pointer(&bodyBytes[0]))
	slottedBody := bodyBytes[LeafHeaderSize:]
	body := slotted.NewSlotted(slottedBody)
	return &Leaf{
		header: header,
		body:   body,
		page:   bodyBytes,
	}
}

func (l *Leaf) PrevPageID() disk.PageID {
	if l.header.PrevPageID.Valid() {
		return l.header.PrevPageID
	}
	return disk.InvalidPageID
}

func (l *Leaf) NextPageID() disk.PageID {
	if l.header.NextPageID.Valid() {
		return l.header.NextPageID
	}
	return disk.InvalidPageID
}

func (l *Leaf) NumPairs() int {
	return l.body.NumSlots()
}

func (l *Leaf) SearchSlotID(key []byte) (int, error) {
	// TODO:
	return 0, nil
}

func (l *Leaf) PairAt(slotID int) *Pair {
	data := l.body.Data(slotID)
	return PairFromBytes(data)
}

func (l *Leaf) MaxPairSize() int {
	return l.body.Capacity()/2 - 4 // slotted.PointerSize
}

func (l *Leaf) Initialize() {
	l.header.PrevPageID = disk.InvalidPageID
	l.header.NextPageID = disk.InvalidPageID
	l.body.Initialize()
}

func (l *Leaf) SetPrevPageID(prevPageId disk.PageID) {
	l.header.PrevPageID = prevPageId
}

func (l *Leaf) SetNextPageID(nextPageId disk.PageID) {
	l.header.NextPageID = nextPageId
}

func (l *Leaf) Insert(slotId int, key []byte, value []byte) bool {
	pair := &Pair{Key: key, Value: value}
	pairBytes := pair.ToBytes()
	if len(pairBytes) > l.MaxPairSize() {
		return false
	}
	if !l.body.Insert(slotId, len(pairBytes)) {
		return false
	}
	copy(l.body.Data(slotId), pairBytes)
	return true
}

func (l *Leaf) IsHalfFull() bool {
	return 2*l.body.FreeSpace() < l.body.Capacity()
}

func (l *Leaf) SplitInsert(newLeaf *Leaf, newKey []byte, newValue []byte) []byte {
	newLeaf.Initialize()
	for {
		if newLeaf.IsHalfFull() {
			index, _ := l.SearchSlotID(newKey)
			if !l.Insert(index, newKey, newValue) {
				panic("old leaf must have space")
			}
			break
		}
		if compareBytes(l.PairAt(0).Key, newKey) < 0 {
			l.Transfer(newLeaf)
		} else {
			if !newLeaf.Insert(newLeaf.NumPairs(), newKey, newValue) {
				panic("new leaf must have space")
			}
			for !newLeaf.IsHalfFull() {
				l.Transfer(newLeaf)
			}
			break
		}
	}
	return l.PairAt(0).Key
}

func (l *Leaf) Transfer(dest *Leaf) {
	nextIndex := dest.NumPairs()
	data := l.body.Data(0)
	if !dest.body.Insert(nextIndex, len(data)) {
		panic("no space in dest leaf")
	}
	l.body.Remove(0)
}

func compareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
