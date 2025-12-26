// Package table provides table implementations for storing tuples.
// Tables use B+ trees as the underlying storage structure.
package table

import (
	"github.com/Johniel/gorelly/btree"
	"github.com/Johniel/gorelly/buffer"
	"github.com/Johniel/gorelly/disk"
	"github.com/Johniel/gorelly/tuple"
)

// SimpleTable represents a simple table without secondary indexes.
// Tuples are stored in a B+ tree with the first NumKeyElems elements as the primary key.
type SimpleTable struct {
	MetaPageID  disk.PageID // Page ID of the B+ tree meta page
	NumKeyElems int         // Number of elements that form the primary key
}

func (st *SimpleTable) Create(bufmgr *buffer.BufferPoolManager) error {
	bt, err := btree.CreateBTree(bufmgr)
	if err != nil {
		return err
	}
	st.MetaPageID = bt.MetaPageID
	return nil
}

func (st *SimpleTable) Insert(bufmgr *buffer.BufferPoolManager, tup [][]byte) error {
	bt := btree.NewBTree(st.MetaPageID)
	keyBytes := make([]byte, 0)
	tuple.Encode(tup[:st.NumKeyElems], &keyBytes)
	valueBytes := make([]byte, 0)
	tuple.Encode(tup[st.NumKeyElems:], &valueBytes)
	return bt.Insert(bufmgr, keyBytes, valueBytes)
}

// Update updates an existing tuple in the table.
// The tuple is identified by its primary key (first NumKeyElems elements).
// Returns an error if the key is not found.
func (st *SimpleTable) Update(bufmgr *buffer.BufferPoolManager, tup [][]byte) error {
	bt := btree.NewBTree(st.MetaPageID)
	keyBytes := make([]byte, 0)
	tuple.Encode(tup[:st.NumKeyElems], &keyBytes)
	valueBytes := make([]byte, 0)
	tuple.Encode(tup[st.NumKeyElems:], &valueBytes)
	return bt.Update(bufmgr, keyBytes, valueBytes)
}

func (st *SimpleTable) Delete(bufmgr *buffer.BufferPoolManager, tup [][]byte) error {
	bt := btree.NewBTree(st.MetaPageID)
	keyBytes := make([]byte, 0)
	tuple.Encode(tup[:st.NumKeyElems], &keyBytes)
	return bt.Delete(bufmgr, keyBytes)
}

// Table represents a table with support for unique secondary indexes.
// Tuples are stored in a B+ tree, and additional B+ trees are maintained for each unique index.
type Table struct {
	MetaPageID    disk.PageID    // Page ID of the primary B+ tree meta page
	NumKeyElems   int            // Number of elements that form the primary key
	UniqueIndices []*UniqueIndex // List of unique secondary indexes
}

func (t *Table) Create(bufmgr *buffer.BufferPoolManager) error {
	bt, err := btree.CreateBTree(bufmgr)
	if err != nil {
		return err
	}
	t.MetaPageID = bt.MetaPageID
	for _, uniqueIndex := range t.UniqueIndices {
		if err := uniqueIndex.Create(bufmgr); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) Insert(bufmgr *buffer.BufferPoolManager, tup [][]byte) error {
	bt := btree.NewBTree(t.MetaPageID)
	keyBytes := make([]byte, 0)
	tuple.Encode(tup[:t.NumKeyElems], &keyBytes)
	valueBytes := make([]byte, 0)
	tuple.Encode(tup[t.NumKeyElems:], &valueBytes)
	if err := bt.Insert(bufmgr, keyBytes, valueBytes); err != nil {
		return err
	}
	for _, uniqueIndex := range t.UniqueIndices {
		if err := uniqueIndex.Insert(bufmgr, keyBytes, tup); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) Update(bufmgr *buffer.BufferPoolManager, tup [][]byte) error {
	bt := btree.NewBTree(t.MetaPageID)
	keyBytes := make([]byte, 0)
	tuple.Encode(tup[:t.NumKeyElems], &keyBytes)
	valueBytes := make([]byte, 0)
	tuple.Encode(tup[t.NumKeyElems:], &valueBytes)
	return bt.Update(bufmgr, keyBytes, valueBytes)
}

// Delete removes a tuple from the table and all associated secondary indexes.
// The tuple is identified by its primary key (first NumKeyElems elements).
// Returns an error if the key is not found.
func (t *Table) Delete(bufmgr *buffer.BufferPoolManager, tup [][]byte) error {
	// First, fetch the old tuple to get the values for index deletion
	bt := btree.NewBTree(t.MetaPageID)
	keyBytes := make([]byte, 0)
	tuple.Encode(tup[:t.NumKeyElems], &keyBytes)

	// Search for the tuple to get the full tuple data
	iter, err := bt.Search(bufmgr, btree.NewSearchModeKey(keyBytes))
	if err != nil {
		return btree.ErrKeyNotFound
	}
	_, valueBytes, ok := iter.Get()
	if !ok {
		return btree.ErrKeyNotFound
	}

	// Decode the full tuple
	var fullTuple [][]byte
	tuple.Decode(keyBytes, &fullTuple)
	tuple.Decode(valueBytes, &fullTuple)

	// Delete from all secondary indexes
	for _, uniqueIndex := range t.UniqueIndices {
		if err := uniqueIndex.Delete(bufmgr, fullTuple); err != nil {
			// If index entry doesn't exist, continue (it might have been deleted already)
			if err != btree.ErrKeyNotFound {
				return err
			}
		}
	}

	// Delete from the primary table
	return bt.Delete(bufmgr, keyBytes)
}

// UniqueIndex represents a unique secondary index on a table.
// It maps secondary key values (Skey) to primary key values (Pkey).
type UniqueIndex struct {
	MetaPageID disk.PageID // Page ID of the B+ tree meta page for this index
	Skey       []int       // Indices of tuple elements that form the secondary key
}

func (ui *UniqueIndex) Create(bufmgr *buffer.BufferPoolManager) error {
	bt, err := btree.CreateBTree(bufmgr)
	if err != nil {
		return err
	}
	ui.MetaPageID = bt.MetaPageID
	return nil
}

func (ui *UniqueIndex) Insert(bufmgr *buffer.BufferPoolManager, pkey []byte, tup [][]byte) error {
	bt := btree.NewBTree(ui.MetaPageID)
	skeyBytes := make([]byte, 0)
	skeyElems := make([][]byte, len(ui.Skey))
	for i, idx := range ui.Skey {
		skeyElems[i] = tup[idx]
	}
	tuple.Encode(skeyElems, &skeyBytes)
	return bt.Insert(bufmgr, skeyBytes, pkey)
}

// Delete removes an index entry for the given tuple.
// It constructs the secondary key from the tuple and removes the corresponding entry.
func (ui *UniqueIndex) Delete(bufmgr *buffer.BufferPoolManager, tup [][]byte) error {
	bt := btree.NewBTree(ui.MetaPageID)
	skeyBytes := make([]byte, 0)
	skeyElems := make([][]byte, len(ui.Skey))
	for i, idx := range ui.Skey {
		skeyElems[i] = tup[idx]
	}
	tuple.Encode(skeyElems, &skeyBytes)
	return bt.Delete(bufmgr, skeyBytes)
}
