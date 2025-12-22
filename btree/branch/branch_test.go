package branch

import (
	"encoding/binary"
	"github.com/Johniel/gorelly/disk"
	"reflect"
	"testing"
)

func TestBranchInsertSearch(t *testing.T) {
	data := make([]byte, 100)
	branch := NewBranch(data)

	key5 := make([]byte, 8)
	binary.BigEndian.PutUint64(key5, 5)
	branch.Initialize(key5, disk.PageID(1), disk.PageID(2))

	key8 := make([]byte, 8)
	binary.BigEndian.PutUint64(key8, 8)
	if !branch.Insert(1, key8, disk.PageID(3)) {
		t.Fatal("failed to insert key8")
	}

	key11 := make([]byte, 8)
	binary.BigEndian.PutUint64(key11, 11)
	if !branch.Insert(2, key11, disk.PageID(4)) {
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
		result := branch.SearchChild(tt.key)
		if result != tt.expected {
			t.Errorf("key %d: expected PageID(%d), got PageID(%d)",
				binary.BigEndian.Uint64(tt.key), tt.expected.ToU64(), result.ToU64())
		}
	}
}

func TestBranchSplit(t *testing.T) {
	data := make([]byte, 100)
	branch := NewBranch(data)

	key5 := make([]byte, 8)
	binary.BigEndian.PutUint64(key5, 5)
	branch.Initialize(key5, disk.PageID(1), disk.PageID(2))

	key8 := make([]byte, 8)
	binary.BigEndian.PutUint64(key8, 8)
	if !branch.Insert(1, key8, disk.PageID(3)) {
		t.Fatal("failed to insert key8")
	}

	key11 := make([]byte, 8)
	binary.BigEndian.PutUint64(key11, 11)
	if !branch.Insert(2, key11, disk.PageID(4)) {
		t.Fatal("failed to insert key11")
	}

	data2 := make([]byte, 100)
	branch2 := NewBranch(data2)
	key10 := make([]byte, 8)
	binary.BigEndian.PutUint64(key10, 10)
	midKey := branch.SplitInsert(branch2, key10, disk.PageID(5))

	expectedMidKey := makeUint64Key(8)
	if !reflect.DeepEqual(expectedMidKey, midKey) {
		t.Errorf("mid key: expected %v, got %v", expectedMidKey, midKey)
	}

	if branch.NumPairs() != 2 {
		t.Errorf("branch num_pairs: expected 2, got %d", branch.NumPairs())
	}
	if branch2.NumPairs() != 1 {
		t.Errorf("branch2 num_pairs: expected 1, got %d", branch2.NumPairs())
	}

	tests := []struct {
		branch   *Branch
		key      []byte
		expected disk.PageID
	}{
		{branch2, makeUint64Key(1), disk.PageID(1)},
		{branch2, makeUint64Key(5), disk.PageID(3)},
		{branch2, makeUint64Key(6), disk.PageID(3)},
		{branch, makeUint64Key(9), disk.PageID(5)},
		{branch, makeUint64Key(10), disk.PageID(4)},
		{branch, makeUint64Key(11), disk.PageID(2)},
		{branch, makeUint64Key(12), disk.PageID(2)},
	}

	for _, tt := range tests {
		result := tt.branch.SearchChild(tt.key)
		if result != tt.expected {
			t.Errorf("branch key %d: expected PageID(%d), got PageID(%d)",
				binary.BigEndian.Uint64(tt.key), tt.expected.ToU64(), result.ToU64())
		}
	}
}

func makeUint64Key(val uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, val)
	return key
}
