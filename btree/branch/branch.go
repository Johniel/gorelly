package branch

import (
	"unsafe"

	"github.com/Johniel/gorelly/disk"
	"github.com/Johniel/gorelly/slotted"
)

// BranchHeaderSize is the size of the branch header (8 bytes for PageID).
const BranchHeaderSize = 8

// BranchHeader contains metadata for a branch node.
type BranchHeader struct {
	RightChild disk.PageID // Rightmost child page ID (for keys greater than all stored keys)
}

// Branch represents a branch (internal) node in a B+ tree.
// Branch nodes store keys and child page IDs to navigate down the tree.
// Each key has an associated left child, and there's a rightmost child for keys
// greater than all stored keys.
//
// Body structure:
//   - body is a Slotted page structure that manages variable-length Pair records
//   - Each Pair contains a key and a child page ID (encoded as bytes)
//   - Pairs are stored in sorted order by key
//   - The body manages the storage layout: pointer array at the beginning,
//     free space in the middle, and data records at the end (stored backwards)
//
// Example: A branch node with 2 pairs (key1→pageId1, key2→pageId2):
//   - body.Data(0) returns the first Pair (key1, pageId1)
//   - body.Data(1) returns the second Pair (key2, pageId2)
//   - header.RightChild contains the rightmost child page ID
//     (for keys greater than key2)
type Branch struct {
	header *BranchHeader
	body   *slotted.Slotted // Slotted page structure storing Pair records (key-child page ID pairs)
	page   []byte           // Keep reference to full page for header updates
}

func NewBranch(bodyBytes []byte) *Branch {
	if len(bodyBytes) < BranchHeaderSize {
		panic("branch header must fit")
	}
	header := (*BranchHeader)(unsafe.Pointer(&bodyBytes[0]))
	slottedBody := bodyBytes[BranchHeaderSize:]
	body := slotted.NewSlotted(slottedBody)
	return &Branch{
		header: header,
		body:   body,
		page:   bodyBytes,
	}
}

func (b *Branch) Insert(slotID int, key []byte, pageID disk.PageID) bool {
	pair := &Pair{
		Key:   key,
		Value: pageID.ToBytes(),
	}
	pairBytes := pair.ToBytes()
	if len(pairBytes) > b.MaxPairSize() {
		return false
	}
	if !b.body.Insert(slotID, len(pairBytes)) {
		return false
	}
	copy(b.body.Data(slotID), pairBytes)
	return true
}

func (b *Branch) IsHalfFull() bool {
	return 2*b.body.FreeSpace() < b.body.Capacity()
}

func (b *Branch) SplitInsert(newBranch *Branch, newKey []byte, newPageID disk.PageID) []byte {
	newBranch.body.Initialize()
	for {
		if newBranch.IsHalfFull() {
			index, _ := b.SearchSlotID(newKey)
			if !b.Insert(index, newKey, newPageID) {
				panic("old branch must have space")
			}
			break
		}
		if compareBytes(b.PairAt(0).Key, newKey) < 0 {
			b.Transfer(newBranch)
		} else {
			if !newBranch.Insert(newBranch.NumPairs(), newKey, newPageID) {
				panic("new branch must have space")
			}
			for !newBranch.IsHalfFull() {
				b.Transfer(newBranch)
			}
			break
		}
	}
	return newBranch.FillRightChild()
}

func (b *Branch) Transfer(dest *Branch) {
	nextIndex := dest.NumPairs()
	data := b.body.Data(0)
	if !dest.body.Insert(nextIndex, len(data)) {
		panic("no space in dest branch")
	}
	copy(dest.body.Data(nextIndex), data)
	b.body.Remove(0)
}

func (b *Branch) NumPairs() int {
	return b.body.NumSlots()
}

func (b *Branch) SearchSlotID(key []byte) (int, error) {
	// TODO:
	return 0, nil
}

func (b *Branch) SearchChild(key []byte) disk.PageID {
	childIndex := b.SearchChildIndex(key)
	return b.ChildAt(childIndex)
}

func (b *Branch) SearchChildIndex(key []byte) int {
	slotID, err := b.SearchSlotID(key)
	if err == nil {
		return slotID + 1
	}
	return slotID
}

func (b *Branch) ChildAt(childIndex int) disk.PageID {
	if childIndex == b.NumPairs() {
		return b.header.RightChild
	}
	return disk.PageIDFromBytes(b.PairAt(childIndex).Value)
}

func (b *Branch) PairAt(srotID int) *Pair {
	data := b.body.Data(srotID)
	return PairFromBytes(data)
}

func (b *Branch) MaxPairSize() int {
	return b.body.Capacity()/2 - 4 // slotted.PointerSize
}

func (b *Branch) Initialize(key []byte, leftChild disk.PageID, rightChild disk.PageID) {
	b.body.Initialize()
	b.Insert(0, key, leftChild)
	b.header.RightChild = rightChild
}

func (b *Branch) FillRightChild() []byte {
	lastID := b.NumPairs() - 1
	pair := b.PairAt(lastID)
	rightChild := disk.PageIDFromBytes(pair.Value)
	keyVec := make([]byte, len(pair.Key))
	copy(keyVec, pair.Key)
	b.body.Remove(lastID)
	b.header.RightChild = rightChild
	return keyVec
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
