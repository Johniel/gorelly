package btree

import (
	"encoding/binary"
	"os"
	"reflect"
	"testing"

	"github.com/Johniel/gorelly/buffer"
	"github.com/Johniel/gorelly/disk"
)

func TestBTree(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_btree_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	dm, err := disk.NewDiskManager(tmpfile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	bt, err := CreateBTree(bufmgr)
	if err != nil {
		t.Fatal(err)
	}

	key6 := make([]byte, 8)
	binary.BigEndian.PutUint64(key6, 6)
	if err := bt.Insert(bufmgr, key6, []byte("world")); err != nil {
		t.Fatal(err)
	}

	key3 := make([]byte, 8)
	binary.BigEndian.PutUint64(key3, 3)
	if err := bt.Insert(bufmgr, key3, []byte("hello")); err != nil {
		t.Fatal(err)
	}

	key8 := make([]byte, 8)
	binary.BigEndian.PutUint64(key8, 8)
	if err := bt.Insert(bufmgr, key8, []byte("!")); err != nil {
		t.Fatal(err)
	}

	key4 := make([]byte, 8)
	binary.BigEndian.PutUint64(key4, 4)
	if err := bt.Insert(bufmgr, key4, []byte(",")); err != nil {
		t.Fatal(err)
	}

	iter, err := bt.Search(bufmgr, NewSearchModeKey(key3))
	if err != nil {
		t.Fatal(err)
	}
	_, value, ok := iter.Get()
	if !ok {
		t.Fatal("expected to find value")
	}
	if !reflect.DeepEqual([]byte("hello"), value) {
		t.Errorf("expected 'hello', got %v", value)
	}

	iter, err = bt.Search(bufmgr, NewSearchModeKey(key8))
	if err != nil {
		t.Fatal(err)
	}
	_, value, ok = iter.Get()
	if !ok {
		t.Fatal("expected to find value")
	}
	if !reflect.DeepEqual([]byte("!"), value) {
		t.Errorf("expected '!', got %v", value)
	}
}

func TestBTreeSearchIter(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_btree_iter_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	dm, err := disk.NewDiskManager(tmpfile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	bt, err := CreateBTree(bufmgr)
	if err != nil {
		t.Fatal(err)
	}

	for i := uint64(0); i < 16; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i*2)
		value := make([]byte, 1024)
		if err := bt.Insert(bufmgr, key, value); err != nil {
			t.Fatal(err)
		}
	}

	for i := uint64(0); i < 15; i++ {
		searchKey := make([]byte, 8)
		binary.BigEndian.PutUint64(searchKey, i*2+1)
		iter, err := bt.Search(bufmgr, NewSearchModeKey(searchKey))
		if err != nil {
			t.Fatal(err)
		}
		key, _, ok := iter.Get()
		if !ok {
			t.Fatalf("expected to find value for search key %d", i*2+1)
		}
		expectedKey := make([]byte, 8)
		binary.BigEndian.PutUint64(expectedKey, (i+1)*2)
		if !reflect.DeepEqual(expectedKey, key) {
			t.Errorf("expected key %v, got %v", expectedKey, key)
		}
	}
}

func TestBTreeSplit(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_btree_split_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	dm, err := disk.NewDiskManager(tmpfile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	bt, err := CreateBTree(bufmgr)
	if err != nil {
		t.Fatal(err)
	}

	longDataList := [][]byte{
		make([]byte, 1000),
		make([]byte, 1000),
		make([]byte, 1000),
		make([]byte, 1000),
		make([]byte, 1000),
		make([]byte, 1000),
		make([]byte, 1000),
		make([]byte, 1000),
	}
	for i := range longDataList {
		for j := range longDataList[i] {
			longDataList[i][j] = byte(0xC0 + i)
		}
	}

	for _, data := range longDataList {
		if err := bt.Insert(bufmgr, data, data); err != nil {
			t.Fatal(err)
		}
	}

	for _, data := range longDataList {
		iter, err := bt.Search(bufmgr, NewSearchModeKey(data))
		if err != nil {
			t.Fatal(err)
		}
		k, v, ok := iter.Get()
		if !ok {
			t.Fatal("expected to find value")
		}
		if !reflect.DeepEqual(data, k) {
			t.Errorf("key mismatch: expected %v, got %v", data[:10], k[:10])
		}
		if !reflect.DeepEqual(data, v) {
			t.Errorf("value mismatch: expected %v, got %v", data[:10], v[:10])
		}
	}
}
