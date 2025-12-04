// Package table provides table implementations for storing records.
// Tables use B+ trees as the underlying storage structure.
package table

import (
	"github.com/Johniel/gorelly/btree"
	"github.com/Johniel/gorelly/buffer"
	"github.com/Johniel/gorelly/disk"
	"github.com/Johniel/gorelly/tuple"
)

// SimpleTable represents a simple table without secondary indexes.
// Records are stored in a B+ tree with the first NumKeyElems elements as the primary key.
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

func (st *SimpleTable) Insert(bufmgr *buffer.BufferPoolManager, record [][]byte) error {
	bt := btree.NewBTree(st.MetaPageID)
	keyBytes := make([]byte, 0)
	tuple.Encode(record[:st.NumKeyElems], &keyBytes)
	valueBytes := make([]byte, 0)
	tuple.Encode(record[:st.NumKeyElems], &valueBytes)
	return bt.Insert(bufmgr, keyBytes, valueBytes)
}

// Table represents a table with support for unique secondary indexes.
// Records are stored in a B+ tree, and additional B+ trees are maintained for each unique index.
type Table struct {
	MetaPageId    disk.PageID    // Page ID of the primary B+ tree meta page
	NumKeyElems   int            // Number of elements that form the primary key
	UniqueIndices []*UniqueIndex // List of unique secondary indexes
}

func (t *Table) Create(bufmgr *buffer.BufferPoolManager) error {
	bt, err := btree.CreateBTree(bufmgr)
	if err != nil {
		return err
	}
	t.MetaPageId = bt.MetaPageID
	for _, uniqueIndex := range t.UniqueIndices {
		if err := uniqueIndex.Create(bufmgr); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) Insert(bufmgr *buffer.BufferPoolManager, record [][]byte) error {
	bt := btree.NewBTree(t.MetaPageId)
	keyBytes := make([]byte, 0)
	tuple.Encode(record[:t.NumKeyElems], &keyBytes)
	valueBytes := make([]byte, 0)
	tuple.Encode(record[t.NumKeyElems:], &valueBytes)
	if err := bt.Insert(bufmgr, keyBytes, valueBytes); err != nil {
		return err
	}
	for _, uniqueIndex := range t.UniqueIndices {
		if err := uniqueIndex.Insert(bufmgr, keyBytes, record); err != nil {
			return err
		}
	}
	return nil
}

// UniqueIndex represents a unique secondary index on a table.
// It maps secondary key values (Skey) to primary key values (Pkey).
type UniqueIndex struct {
	MetaPageID disk.PageID // Page ID of the B+ tree meta page for this index
	Skey       []int       // Indices of record elements that form the secondary key
}

func (st *UniqueIndex) Create(bufmgr *buffer.BufferPoolManager) error {
	bt, err := btree.CreateBTree(bufmgr)
	if err != nil {
		return err
	}
	st.MetaPageID = bt.MetaPageID
	return nil
}

func (ui *UniqueIndex) Insert(bufmgr *buffer.BufferPoolManager, pkey []byte, record [][]byte) error {
	bt := btree.NewBTree(ui.MetaPageID)
	skeyBytes := make([]byte, 0)
	skeyElems := make([][]byte, len(ui.Skey))
	for i, idx := range ui.Skey {
		skeyElems[i] = record[idx]
	}
	tuple.Encode(skeyElems, &skeyBytes)
	return bt.Insert(bufmgr, skeyBytes, pkey)
}
