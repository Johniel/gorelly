package internal

import (
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/Johniel/gorelly/disk"
)

func TestInternalNodeInsertSearch(t *testing.T) {
	data := make([]byte, 100)
	node := NewInternalNode(data)

	key5 := make([]byte, 8)
	binary.BigEndian.PutUint64(key5, 5)
	node.Initialize(key5, disk.PageID(1), disk.PageID(2))

	key8 := make([]byte, 8)
	binary.BigEndian.PutUint64(key8, 8)
	if !node.Insert(1, key8, disk.PageID(3)) {
		t.Fatal("failed to insert key8")
	}

	key11 := make([]byte, 8)
	binary.BigEndian.PutUint64(key11, 11)
	if !node.Insert(2, key11, disk.PageID(4)) {
		t.Fatal("failed to insert key11")
	}

	tests := []struct {
		key      []byte
		expected disk.PageID
	}{
		{makeUint64Key(1), disk.PageID(1)},
		{makeUint64Key(5), disk.PageID(3)},
		{makeUint64Key(6), disk.PageID(3)},
		{makeUint64Key(8), disk.PageID(4)},
		{makeUint64Key(10), disk.PageID(4)},
		{makeUint64Key(11), disk.PageID(2)},
		{makeUint64Key(12), disk.PageID(2)},
	}

	for _, tt := range tests {
		result := node.SearchChild(tt.key)
		if result != tt.expected {
			t.Errorf("key %d: expected PageId(%d), got PageId(%d)",
				binary.BigEndian.Uint64(tt.key), tt.expected.ToU64(), result.ToU64())
		}
	}
}

func TestInternalNodeSplit(t *testing.T) {
	data := make([]byte, 100)
	node := NewInternalNode(data)

	key5 := make([]byte, 8)
	binary.BigEndian.PutUint64(key5, 5)
	node.Initialize(key5, disk.PageID(1), disk.PageID(2))

	key8 := make([]byte, 8)
	binary.BigEndian.PutUint64(key8, 8)
	if !node.Insert(1, key8, disk.PageID(3)) {
		t.Fatal("failed to insert key8")
	}

	key11 := make([]byte, 8)
	binary.BigEndian.PutUint64(key11, 11)
	if !node.Insert(2, key11, disk.PageID(4)) {
		t.Fatal("failed to insert key11")
	}

	data2 := make([]byte, 100)
	node2 := NewInternalNode(data2)
	key10 := make([]byte, 8)
	binary.BigEndian.PutUint64(key10, 10)
	midKey := node.SplitInsert(node2, key10, disk.PageID(5))

	expectedMidKey := makeUint64Key(8)
	if !reflect.DeepEqual(expectedMidKey, midKey) {
		t.Errorf("mid key: expected %v, got %v", expectedMidKey, midKey)
	}

	if node.NumPairs() != 2 {
		t.Errorf("node num_pairs: expected 2, got %d", node.NumPairs())
	}
	if node2.NumPairs() != 1 {
		t.Errorf("node2 num_pairs: expected 1, got %d", node2.NumPairs())
	}

	tests := []struct {
		node     *InternalNode
		key      []byte
		expected disk.PageID
	}{
		{node2, makeUint64Key(1), disk.PageID(1)},
		{node2, makeUint64Key(5), disk.PageID(3)},
		{node2, makeUint64Key(6), disk.PageID(3)},
		{node, makeUint64Key(9), disk.PageID(5)},
		{node, makeUint64Key(10), disk.PageID(4)},
		{node, makeUint64Key(11), disk.PageID(2)},
		{node, makeUint64Key(12), disk.PageID(2)},
	}

	for _, tt := range tests {
		result := tt.node.SearchChild(tt.key)
		if result != tt.expected {
			t.Errorf("node key %d: expected PageId(%d), got PageId(%d)",
				binary.BigEndian.Uint64(tt.key), tt.expected.ToU64(), result.ToU64())
		}
	}
}

func makeUint64Key(val uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, val)
	return key
}
