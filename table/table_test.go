package table

import (
	"os"
	"reflect"
	"testing"

	"github.com/Johniel/gorelly/btree"
	"github.com/Johniel/gorelly/buffer"
	"github.com/Johniel/gorelly/disk"
	"github.com/Johniel/gorelly/tuple"
)

func TestSimpleTableUpdate(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_simple_table_update_*.db")
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

	// Create a simple table
	simpleTable := &SimpleTable{
		MetaPageID:  disk.InvalidPageID,
		NumKeyElems: 1,
	}

	if err := simpleTable.Create(bufmgr); err != nil {
		t.Fatal(err)
	}

	// Insert initial tuple: [id, name, age]
	initialTuple := [][]byte{
		[]byte("1"),
		[]byte("Alice"),
		[]byte("30"),
	}

	if err := simpleTable.Insert(bufmgr, initialTuple); err != nil {
		t.Fatal(err)
	}

	// Flush to ensure data is written
	if err := bufmgr.Flush(); err != nil {
		t.Fatal(err)
	}

	// Test 1: Update existing tuple
	t.Run("UpdateExistingTuple", func(t *testing.T) {
		updatedTuple := [][]byte{
			[]byte("1"),   // Same key
			[]byte("Bob"), // Updated name
			[]byte("35"),  // Updated age
		}

		if err := simpleTable.Update(bufmgr, updatedTuple); err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		// Verify the update by searching
		bt := btree.NewBTree(simpleTable.MetaPageID)
		keyBytes := make([]byte, 0)
		tuple.Encode([][]byte{[]byte("1")}, &keyBytes)

		iter, err := bt.Search(bufmgr, btree.NewSearchModeKey(keyBytes))
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		_, valueBytes, ok := iter.Get()
		if !ok {
			t.Fatal("Expected to find updated tuple")
		}

		// Decode and verify the updated values
		var decodedValue [][]byte
		tuple.Decode(valueBytes, &decodedValue)
		if len(decodedValue) < 2 {
			t.Fatalf("Expected at least 2 values, got %d", len(decodedValue))
		}
		if string(decodedValue[0]) != "Bob" {
			t.Errorf("Expected name 'Bob', got '%s'", string(decodedValue[0]))
		}
		if string(decodedValue[1]) != "35" {
			t.Errorf("Expected age '35', got '%s'", string(decodedValue[1]))
		}
	})

	// Test 2: Update non-existent key
	t.Run("UpdateNonExistentKey", func(t *testing.T) {
		nonExistentTuple := [][]byte{
			[]byte("999"), // Non-existent key
			[]byte("Test"),
			[]byte("100"),
		}

		if err := simpleTable.Update(bufmgr, nonExistentTuple); err == nil {
			t.Error("Expected error when updating non-existent key")
		} else if err != btree.ErrKeyNotFound {
			t.Errorf("Expected ErrKeyNotFound, got %v", err)
		}
	})

	// Test 3: Update with different value sizes
	t.Run("UpdateWithDifferentValueSizes", func(t *testing.T) {
		// Insert a tuple with short value
		shortTuple := [][]byte{
			[]byte("2"),
			[]byte("X"),
			[]byte("Y"),
		}
		if err := simpleTable.Insert(bufmgr, shortTuple); err != nil {
			t.Fatal(err)
		}

		// Update with longer value
		longTuple := [][]byte{
			[]byte("2"),
			[]byte("VeryLongNameThatTakesMoreSpace"),
			[]byte("VeryLongAgeValue"),
		}
		if err := simpleTable.Update(bufmgr, longTuple); err != nil {
			t.Fatalf("Update with longer value failed: %v", err)
		}

		// Update with shorter value
		shorterTuple := [][]byte{
			[]byte("2"),
			[]byte("A"),
			[]byte("B"),
		}
		if err := simpleTable.Update(bufmgr, shorterTuple); err != nil {
			t.Fatalf("Update with shorter value failed: %v", err)
		}
	})
}

func TestTableUpdate(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_table_update_*.db")
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

	// Create a table with unique index
	tbl := &Table{
		MetaPageID:  disk.InvalidPageID,
		NumKeyElems: 1, // id is primary key
		UniqueIndices: []*UniqueIndex{
			{
				MetaPageID: disk.InvalidPageID,
				Skey:       []int{2}, // last_name (index 2) has unique index
			},
		},
	}

	if err := tbl.Create(bufmgr); err != nil {
		t.Fatal(err)
	}

	// Insert initial tuple: [id, first_name, last_name]
	initialTuple := [][]byte{
		[]byte("1"),
		[]byte("Alice"),
		[]byte("Smith"),
	}

	if err := tbl.Insert(bufmgr, initialTuple); err != nil {
		t.Fatal(err)
	}

	// Test: Update existing tuple
	t.Run("UpdateExistingTuple", func(t *testing.T) {
		updatedTuple := [][]byte{
			[]byte("1"),       // Same key
			[]byte("Bob"),     // Updated first_name
			[]byte("Johnson"), // Updated last_name
		}

		if err := tbl.Update(bufmgr, updatedTuple); err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		// Note: This test doesn't verify index updates because Delete is not implemented
		// A complete implementation would need to:
		// 1. Delete old index entries
		// 2. Insert new index entries
	})
}

func TestBTreeUpdate(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_btree_update_*.db")
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

	bt, err := btree.CreateBTree(bufmgr)
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("test_key")
	initialValue := []byte("initial_value")
	updatedValue := []byte("updated_value")

	// Insert initial value
	if err := bt.Insert(bufmgr, key, initialValue); err != nil {
		t.Fatal(err)
	}

	// Verify initial value
	iter, err := bt.Search(bufmgr, btree.NewSearchModeKey(key))
	if err != nil {
		t.Fatal(err)
	}
	gotKey, gotValue, ok := iter.Get()
	if !ok {
		t.Fatal("Expected to find value")
	}
	if !reflect.DeepEqual(key, gotKey) {
		t.Errorf("Expected key %v, got %v", key, gotKey)
	}
	if !reflect.DeepEqual(initialValue, gotValue) {
		t.Errorf("Expected value %v, got %v", initialValue, gotValue)
	}

	// Update value
	if err := bt.Update(bufmgr, key, updatedValue); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify updated value
	iter, err = bt.Search(bufmgr, btree.NewSearchModeKey(key))
	if err != nil {
		t.Fatal(err)
	}
	gotKey, gotValue, ok = iter.Get()
	if !ok {
		t.Fatal("Expected to find value after update")
	}
	if !reflect.DeepEqual(key, gotKey) {
		t.Errorf("Expected key %v, got %v", key, gotKey)
	}
	if !reflect.DeepEqual(updatedValue, gotValue) {
		t.Errorf("Expected value %v, got %v", updatedValue, gotValue)
	}

	// Test updating non-existent key
	nonExistentKey := []byte("non_existent_key")
	if err := bt.Update(bufmgr, nonExistentKey, []byte("value")); err == nil {
		t.Error("Expected error when updating non-existent key")
	} else if err != btree.ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

func TestSimpleTableDelete(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_simple_table_delete_*.db")
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

	// Create a simple table
	simpleTable := &SimpleTable{
		MetaPageID:  disk.InvalidPageID,
		NumKeyElems: 1,
	}

	if err := simpleTable.Create(bufmgr); err != nil {
		t.Fatal(err)
	}

	// Insert test tuples: [id, name, age]
	tuples := [][][]byte{
		{[]byte("1"), []byte("Alice"), []byte("30")},
		{[]byte("2"), []byte("Bob"), []byte("25")},
		{[]byte("3"), []byte("Charlie"), []byte("35")},
	}

	for _, tup := range tuples {
		if err := simpleTable.Insert(bufmgr, tup); err != nil {
			t.Fatal(err)
		}
	}

	// Flush to ensure data is written
	if err := bufmgr.Flush(); err != nil {
		t.Fatal(err)
	}

	// Test 1: Delete existing tuple
	t.Run("DeleteExistingTuple", func(t *testing.T) {
		keyTuple := [][]byte{[]byte("2")} // Delete Bob

		if err := simpleTable.Delete(bufmgr, keyTuple); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Flush to ensure deletion is persisted
		if err := bufmgr.Flush(); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}

		// Verify the tuple is deleted by searching
		bt := btree.NewBTree(simpleTable.MetaPageID)
		keyBytes := make([]byte, 0)
		tuple.Encode(keyTuple, &keyBytes)

		iter, err := bt.Search(bufmgr, btree.NewSearchModeKey(keyBytes))
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		gotKey, _, ok := iter.Get()
		if ok {
			// Check if the returned key matches the deleted key
			if reflect.DeepEqual(keyBytes, gotKey) {
				t.Error("Expected tuple to be deleted, but it still exists")
			}
		}
	})

	// Test 2: Delete non-existent key
	t.Run("DeleteNonExistentKey", func(t *testing.T) {
		nonExistentTuple := [][]byte{[]byte("999")}

		if err := simpleTable.Delete(bufmgr, nonExistentTuple); err == nil {
			t.Error("Expected error when deleting non-existent key")
		} else if err != btree.ErrKeyNotFound {
			t.Errorf("Expected ErrKeyNotFound, got %v", err)
		}
	})

	// Test 3: Verify remaining tuples still exist
	t.Run("VerifyRemainingTuples", func(t *testing.T) {
		bt := btree.NewBTree(simpleTable.MetaPageID)

		// Check Alice (id=1) still exists
		keyBytes := make([]byte, 0)
		tuple.Encode([][]byte{[]byte("1")}, &keyBytes)
		iter, err := bt.Search(bufmgr, btree.NewSearchModeKey(keyBytes))
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		_, _, ok := iter.Get()
		if !ok {
			t.Error("Expected Alice to still exist")
		}

		// Check Charlie (id=3) still exists
		keyBytes = make([]byte, 0)
		tuple.Encode([][]byte{[]byte("3")}, &keyBytes)
		iter, err = bt.Search(bufmgr, btree.NewSearchModeKey(keyBytes))
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		_, _, ok = iter.Get()
		if !ok {
			t.Error("Expected Charlie to still exist")
		}
	})
}

func TestTableDelete(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_table_delete_*.db")
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

	// Create a table with unique index
	tbl := &Table{
		MetaPageID:  disk.InvalidPageID,
		NumKeyElems: 1, // id is primary key
		UniqueIndices: []*UniqueIndex{
			{
				MetaPageID: disk.InvalidPageID,
				Skey:       []int{2}, // last_name (index 2) has unique index
			},
		},
	}

	if err := tbl.Create(bufmgr); err != nil {
		t.Fatal(err)
	}

	// Insert initial tuple: [id, first_name, last_name]
	initialTuple := [][]byte{
		[]byte("1"),
		[]byte("Alice"),
		[]byte("Smith"),
	}

	if err := tbl.Insert(bufmgr, initialTuple); err != nil {
		t.Fatal(err)
	}

	// Flush to ensure data is written
	if err := bufmgr.Flush(); err != nil {
		t.Fatal(err)
	}

	// Test: Delete existing tuple
	t.Run("DeleteExistingTuple", func(t *testing.T) {
		keyTuple := [][]byte{[]byte("1")}

		if err := tbl.Delete(bufmgr, keyTuple); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Verify the tuple is deleted from the primary table
		bt := btree.NewBTree(tbl.MetaPageID)
		keyBytes := make([]byte, 0)
		tuple.Encode(keyTuple, &keyBytes)

		iter, err := bt.Search(bufmgr, btree.NewSearchModeKey(keyBytes))
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		_, _, ok := iter.Get()
		if ok {
			t.Error("Expected tuple to be deleted from primary table, but it still exists")
		}

		// Verify the index entry is also deleted
		indexBt := btree.NewBTree(tbl.UniqueIndices[0].MetaPageID)
		skeyBytes := make([]byte, 0)
		tuple.Encode([][]byte{[]byte("Smith")}, &skeyBytes)

		indexIter, err := indexBt.Search(bufmgr, btree.NewSearchModeKey(skeyBytes))
		if err != nil {
			t.Fatalf("Index search failed: %v", err)
		}
		_, _, ok = indexIter.Get()
		if ok {
			t.Error("Expected index entry to be deleted, but it still exists")
		}
	})
}

func TestBTreeDelete(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_btree_delete_*.db")
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

	bt, err := btree.CreateBTree(bufmgr)
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("test_key")
	value := []byte("test_value")

	// Insert value
	if err := bt.Insert(bufmgr, key, value); err != nil {
		t.Fatal(err)
	}

	// Verify value exists
	iter, err := bt.Search(bufmgr, btree.NewSearchModeKey(key))
	if err != nil {
		t.Fatal(err)
	}
	gotKey, gotValue, ok := iter.Get()
	if !ok {
		t.Fatal("Expected to find value")
	}
	if !reflect.DeepEqual(key, gotKey) {
		t.Errorf("Expected key %v, got %v", key, gotKey)
	}
	if !reflect.DeepEqual(value, gotValue) {
		t.Errorf("Expected value %v, got %v", value, gotValue)
	}

	// Delete value
	if err := bt.Delete(bufmgr, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify value is deleted
	iter, err = bt.Search(bufmgr, btree.NewSearchModeKey(key))
	if err != nil {
		t.Fatal(err)
	}
	_, _, ok = iter.Get()
	if ok {
		t.Error("Expected value to be deleted, but it still exists")
	}

	// Test deleting non-existent key
	nonExistentKey := []byte("non_existent_key")
	if err := bt.Delete(bufmgr, nonExistentKey); err == nil {
		t.Error("Expected error when deleting non-existent key")
	} else if err != btree.ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}
