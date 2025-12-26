// Package internal provides internal node implementation for B+ trees.
// Internal nodes store keys and child page IDs to navigate the tree structure.

package internal

import (
	"unsafe"

	"github.com/Johniel/gorelly/bsearch"
	"github.com/Johniel/gorelly/disk"
	"github.com/Johniel/gorelly/slotted"
)

// InternalHeaderSize is the size of the internal node header (8 bytes for PageId).
const InternalHeaderSize = 8

// InternalHeader contains metadata for an internal node.
type InternalHeader struct {
	RightChild disk.PageID // Rightmost child page ID (for keys greater than all stored keys)
}

// InternalNode represents an internal node in a B+ tree.
// Internal nodes store keys and child page IDs to navigate down the tree.
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
// Example: An internal node with 2 pairs (key1→pageId1, key2→pageId2):
//   - body.Data(0) returns the first Pair (key1, pageId1)
//   - body.Data(1) returns the second Pair (key2, pageId2)
//   - header.RightChild contains the rightmost child page ID
//     (for keys greater than key2)
type InternalNode struct {
	header *InternalHeader
	body   *slotted.Slotted // Slotted page structure storing Pair records (key-child page ID pairs)
	page   []byte           // Keep reference to full page for header updates
}

func NewInternalNode(bodyBytes []byte) *InternalNode {
	// bodyBytes is the body part after node header
	// Internal header comes right after the node header
	if len(bodyBytes) < InternalHeaderSize {
		panic("internal header must fit")
	}
	header := (*InternalHeader)(unsafe.Pointer(&bodyBytes[0]))
	slottedBody := bodyBytes[InternalHeaderSize:]
	body := slotted.NewSlotted(slottedBody)
	return &InternalNode{
		header: header,
		body:   body,
		page:   bodyBytes,
	}
}

func (n *InternalNode) NumPairs() int {
	return n.body.NumSlots()
}

func (n *InternalNode) SearchSlotId(key []byte) (int, error) {
	return bsearch.BinarySearchBy(n.NumPairs(), func(slotID int) int {
		pair := n.PairAt(slotID)
		return compareBytes(pair.Key, key)
	})
}

func (n *InternalNode) SearchChild(key []byte) disk.PageID {
	childIdx := n.SearchChildIdx(key)
	return n.ChildAt(childIdx)
}

func (n *InternalNode) SearchChildIdx(key []byte) int {
	slotID, err := n.SearchSlotId(key)
	if err == nil {
		return slotID + 1
	}
	return slotID
}

func (n *InternalNode) ChildAt(childIdx int) disk.PageID {
	if childIdx == n.NumPairs() {
		return n.header.RightChild
	}
	return disk.PageIDFromBytes(n.PairAt(childIdx).Value)
}

func (n *InternalNode) PairAt(slotID int) *Pair {
	data := n.body.Data(slotID)
	return PairFromBytes(data)
}

func (n *InternalNode) MaxPairSize() int {
	return n.body.Capacity()/2 - 4 // slotted.PointerSize
}

func (n *InternalNode) Initialize(key []byte, leftChild disk.PageID, rightChild disk.PageID) {
	n.body.Initialize()
	n.Insert(0, key, leftChild)
	n.header.RightChild = rightChild
}

func (n *InternalNode) FillRightChild() []byte {
	lastId := n.NumPairs() - 1
	pair := n.PairAt(lastId)
	rightChild := disk.PageIDFromBytes(pair.Value)
	keyVec := make([]byte, len(pair.Key))
	copy(keyVec, pair.Key)
	n.body.Remove(lastId)
	n.header.RightChild = rightChild
	return keyVec
}

func (n *InternalNode) Insert(slotID int, key []byte, pageId disk.PageID) bool {
	pair := &Pair{
		Key:   key,
		Value: pageId.ToBytes(),
	}
	pairBytes := pair.ToBytes()
	if len(pairBytes) > n.MaxPairSize() {
		return false
	}
	if !n.body.Insert(slotID, len(pairBytes)) {
		return false
	}
	copy(n.body.Data(slotID), pairBytes)
	return true
}

func (n *InternalNode) IsHalfFull() bool {
	return 2*n.body.FreeSpace() < n.body.Capacity()
}

func (n *InternalNode) SplitInsert(newNode *InternalNode, newKey []byte, newPageId disk.PageID) []byte {
	newNode.body.Initialize()
	for {
		if newNode.IsHalfFull() {
			index, _ := n.SearchSlotId(newKey)
			if !n.Insert(index, newKey, newPageId) {
				panic("old internal node must have space")
			}
			break
		}
		if compareBytes(n.PairAt(0).Key, newKey) < 0 {
			n.Transfer(newNode)
		} else {
			if !newNode.Insert(newNode.NumPairs(), newKey, newPageId) {
				panic("new internal node must have space")
			}
			for !newNode.IsHalfFull() {
				n.Transfer(newNode)
			}
			break
		}
	}
	return newNode.FillRightChild()
}

func (n *InternalNode) Transfer(dest *InternalNode) {
	nextIndex := dest.NumPairs()
	data := n.body.Data(0)
	if !dest.body.Insert(nextIndex, len(data)) {
		panic("no space in dest internal node")
	}
	copy(dest.body.Data(nextIndex), data)
	n.body.Remove(0)
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
