// Package btree provides a B+ tree implementation for indexing and storing key-value pairs.
// B+ trees are balanced tree data structures optimized for disk-based storage.
package btree

import (
	"errors"

	"github.com/Johniel/gorelly/buffer"
	"github.com/Johniel/gorelly/disk"
)

var (
	// ErrDuplicateKey is returned when attempting to insert a key that already exists.
	ErrDuplicateKey = errors.New("duplicate key")
	// ErrKeyNotFound is returned when attempting to update or delete a key that doesn't exist.
	ErrKeyNotFound = errors.New("key not found")
)

// SearchMode specifies how to search in a B+ tree.
type SearchMode struct {
	IsStart bool   // If true, start from the beginning; if false, search for Key
	Key     []byte // The key to search for (only used if IsStart is false)
}

func NewSearchModeStart() SearchMode {
	return SearchMode{IsStart: true}
}

func NewSearchModeKey(key []byte) SearchMode {
	return SearchMode{IsStart: false, Key: key}
}

// BTree represents a B+ tree index.
// It stores key-value pairs in a balanced tree structure optimized for disk access.
type BTree struct {
	MetaPageID disk.PageID // Page ID of the meta page containing the root page ID
}

func CreateBTree(bufmgr *buffer.BufferPoolManager) (*BTree, error) {
	metaBuffer, err := bufmgr.CreateBuffer()
	if err != nil {
		return nil, err
	}
	meta := NewMeta(metaBuffer.Page[:])

	rootBuffer, err := bufmgr.CreateBuffer()
	if err != nil {
		return nil, err
	}
	rootNode := NewNode(rootBuffer.Page[:])
	rootNode.InitializeAsLeaf()
	leafNode := rootNode.AsLeaf()
	leafNode.Initialize()

	meta.SetRootPageID(rootBuffer.PageID)
	return &BTree{MetaPageID: metaBuffer.PageID}, nil
}

func NewBTree(metaPageId disk.PageID) *BTree {
	return &BTree{MetaPageID: metaPageId}
}

func (bt *BTree) FetchRootPage(bufmgr *buffer.BufferPoolManager) (*buffer.Buffer, error) {
	metaBuffer, err := bufmgr.FetchBuffer(bt.MetaPageID)
	if err != nil {
		return nil, err
	}
	meta := NewMeta(metaBuffer.Page[:])
	rootPageId := meta.RootPageID()
	return bufmgr.FetchBuffer(rootPageId)
}

func (bt *BTree) Search(bufmgr *buffer.BufferPoolManager, searchMode SearchMode) (*Iter, error) {
	rootPage, err := bt.FetchRootPage(bufmgr)
	if err != nil {
		return nil, err
	}
	return bt.searchInternal(bufmgr, rootPage, searchMode)
}

func (bt *BTree) searchInternal(bufmgr *buffer.BufferPoolManager, nodeBuffer *buffer.Buffer, searchMode SearchMode) (*Iter, error) {
	node := NewNode(nodeBuffer.Page[:])

	if node.IsLeaf() {
		leafNode := node.AsLeaf()
		slotID := 0
		var err error
		if !searchMode.IsStart {
			slotID, err = leafNode.SearchSlotID(searchMode.Key)
			if err != nil {
				// Not found, use insertion point
			}
		}
		isRightMost := leafNode.NumPairs() == slotID

		iter := &Iter{
			buffer: nodeBuffer,
			slotID: slotID,
		}
		if isRightMost {
			if err := iter.Advance(bufmgr); err != nil {
				return nil, err
			}
		}
		return iter, nil
	} else if node.IsBranch() {
		internalNode := node.AsBranch()
		var childPageId disk.PageID
		if searchMode.IsStart {
			childPageId = internalNode.ChildAt(0)
		} else {
			childPageId = internalNode.SearchChild(searchMode.Key)
		}
		childNodePage, err := bufmgr.FetchBuffer(childPageId)
		if err != nil {
			return nil, err
		}
		return bt.searchInternal(bufmgr, childNodePage, searchMode)
	}
	panic("unknown node type")
}

func (bt *BTree) Insert(bufmgr *buffer.BufferPoolManager, key []byte, value []byte) error {
	metaBuffer, err := bufmgr.FetchBuffer(bt.MetaPageID)
	if err != nil {
		return err
	}
	meta := NewMeta(metaBuffer.Page[:])
	rootPageId := meta.RootPageID()
	rootBuffer, err := bufmgr.FetchBuffer(rootPageId)
	if err != nil {
		return err
	}

	split, err := bt.insertInternal(bufmgr, rootBuffer, key, value)
	if err != nil {
		return err
	}

	if split != nil {
		newRootBuffer, err := bufmgr.CreateBuffer()
		if err != nil {
			return err
		}
		node := NewNode(newRootBuffer.Page[:])
		node.InitializeAsBranch()
		internalNode := node.AsBranch()
		internalNode.Initialize(split.Key, split.ChildPageId, rootPageId)
		meta.SetRootPageID(newRootBuffer.PageID)
		metaBuffer.IsDirty = true
	}
	return nil
}

// Split represents information propagated to the parent node when a node splits.
// It contains the promoted key and the page ID of the newly created child node.
type Split struct {
	Key         []byte      // Promoted key (minimum key of the new node)
	ChildPageId disk.PageID // Page ID of the newly created child node
}

func (bt *BTree) insertInternal(bufmgr *buffer.BufferPoolManager, nodeBuf *buffer.Buffer, key []byte, value []byte) (*Split, error) {
	node := NewNode(nodeBuf.Page[:])

	if node.IsLeaf() {
		leafNode := node.AsLeaf()
		slotID, err := leafNode.SearchSlotID(key)
		if err == nil {
			return nil, ErrDuplicateKey
		}

		if leafNode.Insert(slotID, key, value) {
			nodeBuf.IsDirty = true
			return nil, nil
		}

		// Need to split
		prevLeafPageId := leafNode.PrevPageID()
		var prevLeafBuffer *buffer.Buffer
		if prevLeafPageId.Valid() {
			var err error
			prevLeafBuffer, err = bufmgr.FetchBuffer(prevLeafPageId)
			if err != nil {
				return nil, err
			}
		}

		newLeafBuffer, err := bufmgr.CreateBuffer()
		if err != nil {
			return nil, err
		}

		if prevLeafBuffer != nil {
			prevNode := NewNode(prevLeafBuffer.Page[:])
			prevLeaf := prevNode.AsLeaf()
			prevLeaf.SetNextPageID(newLeafBuffer.PageID)
			prevLeafBuffer.IsDirty = true
		}
		leafNode.SetPrevPageID(newLeafBuffer.PageID)

		newLeafNode := NewNode(newLeafBuffer.Page[:])
		newLeafNode.InitializeAsLeaf()
		newLeaf := newLeafNode.AsLeaf()
		newLeaf.Initialize()
		splitKey := leafNode.SplitInsert(newLeaf, key, value)
		newLeaf.SetNextPageID(nodeBuf.PageID)
		if prevLeafPageId.Valid() {
			newLeaf.SetPrevPageID(prevLeafPageId)
		}
		nodeBuf.IsDirty = true
		return &Split{Key: splitKey, ChildPageId: newLeafBuffer.PageID}, nil
	} else if node.IsBranch() {
		internalNode := node.AsBranch()
		childIdx := internalNode.SearchChildIdx(key)
		childPageId := internalNode.ChildAt(childIdx)
		childNodeBuffer, err := bufmgr.FetchBuffer(childPageId)
		if err != nil {
			return nil, err
		}

		split, err := bt.insertInternal(bufmgr, childNodeBuffer, key, value)
		if err != nil {
			return nil, err
		}

		if split != nil {
			if internalNode.Insert(childIdx, split.Key, split.ChildPageId) {
				nodeBuf.IsDirty = true
				return nil, nil
			}

			// Need to split internal node
			newInternalBuffer, err := bufmgr.CreateBuffer()
			if err != nil {
				return nil, err
			}
			newInternalNodeWrapper := NewNode(newInternalBuffer.Page[:])
			newInternalNodeWrapper.InitializeAsBranch()
			newInternalNode := newInternalNodeWrapper.AsBranch()
			splitKey := internalNode.SplitInsert(newInternalNode, split.Key, split.ChildPageId)
			nodeBuf.IsDirty = true
			newInternalBuffer.IsDirty = true
			return &Split{Key: splitKey, ChildPageId: newInternalBuffer.PageID}, nil
		}
		return nil, nil
	}
	panic("unknown node type")
}

// Update updates the value for an existing key in the B+ tree.
// It returns ErrKeyNotFound if the key doesn't exist.
func (bt *BTree) Update(bufmgr *buffer.BufferPoolManager, key []byte, newValue []byte) error {
	metaBuffer, err := bufmgr.FetchBuffer(bt.MetaPageID)
	if err != nil {
		return err
	}
	meta := NewMeta(metaBuffer.Page[:])
	rootPageId := meta.RootPageID()
	rootBuffer, err := bufmgr.FetchBuffer(rootPageId)
	if err != nil {
		return err
	}

	return bt.updateInternal(bufmgr, rootBuffer, key, newValue)
}

func (bt *BTree) updateInternal(bufmgr *buffer.BufferPoolManager, nodeBuf *buffer.Buffer, key []byte, newValue []byte) error {
	node := NewNode(nodeBuf.Page[:])

	if node.IsLeaf() {
		leafNode := node.AsLeaf()
		slotID, err := leafNode.SearchSlotID(key)
		if err != nil {
			return ErrKeyNotFound
		}

		if leafNode.Update(slotID, newValue) {
			nodeBuf.IsDirty = true
			return nil
		}
		return ErrKeyNotFound
	} else if node.IsBranch() {
		internalNode := node.AsBranch()
		childIdx := internalNode.SearchChildIdx(key)
		childPageId := internalNode.ChildAt(childIdx)
		childNodeBuffer, err := bufmgr.FetchBuffer(childPageId)
		if err != nil {
			return err
		}

		return bt.updateInternal(bufmgr, childNodeBuffer, key, newValue)
	}
	panic("unknown node type")
}

// Iter is an iterator for traversing key-value pairs in a B+ tree.
// It supports sequential iteration across leaf nodes.
type Iter struct {
	buffer *buffer.Buffer // Current leaf page buffer
	slotID int            // Current slot index in the leaf
}

// Get returns the current key-value pair at the iterator's position.
// It returns the key, value, and a boolean indicating whether a pair was found.
// If the iterator is at the end or not positioned on a valid leaf node, it returns (nil, nil, false).
// The returned key and value are copies, so modifications to them will not affect the stored data.
func (it *Iter) Get() ([]byte, []byte, bool) {
	node := NewNode(it.buffer.Page[:])
	if !node.IsLeaf() {
		return nil, nil, false
	}
	leafNode := node.AsLeaf()
	if it.slotID < leafNode.NumPairs() {
		pair := leafNode.PairAt(it.slotID)
		key := make([]byte, len(pair.Key))
		value := make([]byte, len(pair.Value))
		copy(key, pair.Key)
		copy(value, pair.Value)
		return key, value, true
	}
	return nil, nil, false
}

// Advance moves the iterator to the next position.
// If the current slot is not the last in the leaf node, it increments the slot index.
// If the current slot is the last in the leaf node, it moves to the next leaf page
// by following the NextPageID link and resets the slot index to 0.
// If there is no next page, the iterator remains at the end position.
// Returns an error if fetching the next page fails.
func (it *Iter) Advance(bufmgr *buffer.BufferPoolManager) error {
	it.slotID++
	node := NewNode(it.buffer.Page[:])
	if !node.IsLeaf() {
		return nil
	}
	leafNode := node.AsLeaf()
	if it.slotID < leafNode.NumPairs() {
		return nil
	}
	nextPageId := leafNode.NextPageID()
	if nextPageId.Valid() {
		nextBuffer, err := bufmgr.FetchBuffer(nextPageId)
		if err != nil {
			return err
		}
		it.buffer = nextBuffer
		it.slotID = 0
	}
	return nil
}

// Next returns the current key-value pair and advances the iterator to the next position.
// It is equivalent to calling Get() followed by Advance(), but more efficient.
// Returns the key, value, a boolean indicating if a pair was found, and an error.
// If an error occurs during advancement, it returns (nil, nil, false, error).
// This method is typically used in a loop to iterate through all key-value pairs.
func (it *Iter) Next(bufmgr *buffer.BufferPoolManager) ([]byte, []byte, bool, error) {
	key, value, ok := it.Get()
	if err := it.Advance(bufmgr); err != nil {
		return nil, nil, false, err
	}
	return key, value, ok, nil
}
