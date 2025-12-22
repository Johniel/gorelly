package query

import (
	"os"
	"reflect"
	"testing"

	"github.com/Johniel/gorelly/buffer"
	"github.com/Johniel/gorelly/disk"
	"github.com/Johniel/gorelly/table"
)

func TestProject(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_project_*.db")
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
	simpleTable := &table.SimpleTable{
		MetaPageID:  disk.InvalidPageID,
		NumKeyElems: 1,
	}

	if err := simpleTable.Create(bufmgr); err != nil {
		t.Fatal(err)
	}

	// Insert test data: [id, first_name, last_name, age]
	tuples := [][][]byte{
		{[]byte("1"), []byte("Alice"), []byte("Smith"), []byte("30")},
		{[]byte("2"), []byte("Bob"), []byte("Johnson"), []byte("25")},
		{[]byte("3"), []byte("Charlie"), []byte("Williams"), []byte("35")},
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

	// Test 1: Project single column (first_name, index 1)
	t.Run("ProjectSingleColumn", func(t *testing.T) {
		project := &Project{
			InnerPlan: &SeqScan{
				TableMetaPageID: simpleTable.MetaPageID,
				SearchMode:      NewTupleSearchModeStart(),
				WhileCond: func(pkey [][]byte) bool {
					return true
				},
			},
			ColumnIndices: []int{1}, // first_name
		}

		executor, err := project.Start(bufmgr)
		if err != nil {
			t.Fatal(err)
		}

		expected := [][][]byte{
			{[]byte("Alice")},
			{[]byte("Bob")},
			{[]byte("Charlie")},
		}

		i := 0
		for {
			tuple, ok, err := executor.Next(bufmgr)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if i >= len(expected) {
				t.Fatalf("Got more tuples than expected")
			}
			if !reflect.DeepEqual(tuple, expected[i]) {
				t.Errorf("Tuple %d: expected %v, got %v", i, expected[i], tuple)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d tuples, got %d", len(expected), i)
		}
	})

	// Test 2: Project multiple columns (first_name and last_name, indices 1 and 2)
	t.Run("ProjectMultipleColumns", func(t *testing.T) {
		project := &Project{
			InnerPlan: &SeqScan{
				TableMetaPageID: simpleTable.MetaPageID,
				SearchMode:      NewTupleSearchModeStart(),
				WhileCond: func(pkey [][]byte) bool {
					return true
				},
			},
			ColumnIndices: []int{1, 2}, // first_name, last_name
		}

		executor, err := project.Start(bufmgr)
		if err != nil {
			t.Fatal(err)
		}

		expected := [][][]byte{
			{[]byte("Alice"), []byte("Smith")},
			{[]byte("Bob"), []byte("Johnson")},
			{[]byte("Charlie"), []byte("Williams")},
		}

		i := 0
		for {
			tuple, ok, err := executor.Next(bufmgr)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if i >= len(expected) {
				t.Fatalf("Got more tuples than expected")
			}
			if !reflect.DeepEqual(tuple, expected[i]) {
				t.Errorf("Tuple %d: expected %v, got %v", i, expected[i], tuple)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d tuples, got %d", len(expected), i)
		}
	})

	// Test 3: Project columns in different order (last_name, first_name, indices 2 and 1)
	t.Run("ProjectColumnsInDifferentOrder", func(t *testing.T) {
		project := &Project{
			InnerPlan: &SeqScan{
				TableMetaPageID: simpleTable.MetaPageID,
				SearchMode:      NewTupleSearchModeStart(),
				WhileCond: func(pkey [][]byte) bool {
					return true
				},
			},
			ColumnIndices: []int{2, 1}, // last_name, first_name (reversed order)
		}

		executor, err := project.Start(bufmgr)
		if err != nil {
			t.Fatal(err)
		}

		expected := [][][]byte{
			{[]byte("Smith"), []byte("Alice")},
			{[]byte("Johnson"), []byte("Bob")},
			{[]byte("Williams"), []byte("Charlie")},
		}

		i := 0
		for {
			tuple, ok, err := executor.Next(bufmgr)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if i >= len(expected) {
				t.Fatalf("Got more tuples than expected")
			}
			if !reflect.DeepEqual(tuple, expected[i]) {
				t.Errorf("Tuple %d: expected %v, got %v", i, expected[i], tuple)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d tuples, got %d", len(expected), i)
		}
	})

	// Test 4: Project with Filter (combined operations)
	t.Run("ProjectWithFilter", func(t *testing.T) {
		filter := &Filter{
			InnerPlan: &SeqScan{
				TableMetaPageID: simpleTable.MetaPageID,
				SearchMode:      NewTupleSearchModeStart(),
				WhileCond: func(pkey [][]byte) bool {
					return true
				},
			},
			Cond: func(tup [][]byte) bool {
				// Filter: age >= "30"
				if len(tup) > 3 {
					return string(tup[3]) >= "30"
				}
				return false
			},
		}

		project := &Project{
			InnerPlan:     filter,
			ColumnIndices: []int{1, 2}, // first_name, last_name
		}

		executor, err := project.Start(bufmgr)
		if err != nil {
			t.Fatal(err)
		}

		expected := [][][]byte{
			{[]byte("Alice"), []byte("Smith")},
			{[]byte("Charlie"), []byte("Williams")},
		}

		i := 0
		for {
			tuple, ok, err := executor.Next(bufmgr)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if i >= len(expected) {
				t.Fatalf("Got more tuples than expected")
			}
			if !reflect.DeepEqual(tuple, expected[i]) {
				t.Errorf("Tuple %d: expected %v, got %v", i, expected[i], tuple)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d tuples, got %d", len(expected), i)
		}
	})

	// Test 5: Project with out-of-range column index
	t.Run("ProjectOutOfRangeColumn", func(t *testing.T) {
		project := &Project{
			InnerPlan: &SeqScan{
				TableMetaPageID: simpleTable.MetaPageID,
				SearchMode:      NewTupleSearchModeStart(),
				WhileCond: func(pkey [][]byte) bool {
					return true
				},
			},
			ColumnIndices: []int{1, 10}, // first_name and out-of-range index
		}

		executor, err := project.Start(bufmgr)
		if err != nil {
			t.Fatal(err)
		}

		// Should return tuples with empty byte slice for out-of-range column
		i := 0
		for {
			tuple, ok, err := executor.Next(bufmgr)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if len(tuple) != 2 {
				t.Errorf("Expected tuple length 2, got %d", len(tuple))
			}
			if len(tuple[0]) == 0 {
				t.Errorf("First column should not be empty")
			}
			if len(tuple[1]) != 0 {
				t.Errorf("Out-of-range column should be empty, got %v", tuple[1])
			}
			i++
		}
		if i != 3 {
			t.Errorf("Expected 3 tuples, got %d", i)
		}
	})
}
