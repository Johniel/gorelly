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

func TestSort(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_sort_*.db")
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

	// Insert test data: (id, name, age)
	// id=1, name="Charlie", age="25"
	// id=2, name="Alice", age="30"
	// id=3, name="Bob", age="20"
	testData := [][][]byte{
		{[]byte("1"), []byte("Charlie"), []byte("25")},
		{[]byte("2"), []byte("Alice"), []byte("30")},
		{[]byte("3"), []byte("Bob"), []byte("20")},
	}

	for _, tup := range testData {
		err := simpleTable.Insert(bufmgr, tup)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Test 1: Sort by name (column 1) ascending
	t.Run("SortSingleColumnAscending", func(t *testing.T) {
		seqScan := &SeqScan{
			TableMetaPageID: simpleTable.MetaPageID,
			SearchMode:      NewTupleSearchModeStart(),
			WhileCond: func(pkey [][]byte) bool {
				return true
			},
		}

		sort := &Sort{
			InnerPlan: seqScan,
			SortKeys: []SortKey{
				{ColumnIndex: 1, Ascending: true}, // Sort by name ascending
			},
		}

		executor, err := sort.Start(bufmgr)
		if err != nil {
			t.Fatal(err)
		}

		expected := [][][]byte{
			{[]byte("2"), []byte("Alice"), []byte("30")},
			{[]byte("3"), []byte("Bob"), []byte("20")},
			{[]byte("1"), []byte("Charlie"), []byte("25")},
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

	// Test 2: Sort by name (column 1) descending
	t.Run("SortSingleColumnDescending", func(t *testing.T) {
		seqScan := &SeqScan{
			TableMetaPageID: simpleTable.MetaPageID,
			SearchMode:      NewTupleSearchModeStart(),
			WhileCond: func(pkey [][]byte) bool {
				return true
			},
		}

		sort := &Sort{
			InnerPlan: seqScan,
			SortKeys: []SortKey{
				{ColumnIndex: 1, Ascending: false}, // Sort by name descending
			},
		}

		executor, err := sort.Start(bufmgr)
		if err != nil {
			t.Fatal(err)
		}

		expected := [][][]byte{
			{[]byte("1"), []byte("Charlie"), []byte("25")},
			{[]byte("3"), []byte("Bob"), []byte("20")},
			{[]byte("2"), []byte("Alice"), []byte("30")},
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

	// Test 3: Sort by age (column 2) ascending
	t.Run("SortByAgeAscending", func(t *testing.T) {
		seqScan := &SeqScan{
			TableMetaPageID: simpleTable.MetaPageID,
			SearchMode:      NewTupleSearchModeStart(),
			WhileCond: func(pkey [][]byte) bool {
				return true
			},
		}

		sort := &Sort{
			InnerPlan: seqScan,
			SortKeys: []SortKey{
				{ColumnIndex: 2, Ascending: true}, // Sort by age ascending
			},
		}

		executor, err := sort.Start(bufmgr)
		if err != nil {
			t.Fatal(err)
		}

		expected := [][][]byte{
			{[]byte("3"), []byte("Bob"), []byte("20")},
			{[]byte("1"), []byte("Charlie"), []byte("25")},
			{[]byte("2"), []byte("Alice"), []byte("30")},
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

	// Test 4: Multi-column sort (first by age ascending, then by name ascending)
	t.Run("SortMultiColumn", func(t *testing.T) {
		// Add more test data to make multi-column sort meaningful
		// id=4, name="Alice", age="25" (same age as Charlie, different name)
		err := simpleTable.Insert(bufmgr, [][]byte{[]byte("4"), []byte("Alice"), []byte("25")})
		if err != nil {
			t.Fatal(err)
		}

		seqScan := &SeqScan{
			TableMetaPageID: simpleTable.MetaPageID,
			SearchMode:      NewTupleSearchModeStart(),
			WhileCond: func(pkey [][]byte) bool {
				return true
			},
		}

		sort := &Sort{
			InnerPlan: seqScan,
			SortKeys: []SortKey{
				{ColumnIndex: 2, Ascending: true}, // First sort by age ascending
				{ColumnIndex: 1, Ascending: true}, // Then sort by name ascending
			},
		}

		executor, err := sort.Start(bufmgr)
		if err != nil {
			t.Fatal(err)
		}

		expected := [][][]byte{
			{[]byte("3"), []byte("Bob"), []byte("20")},
			{[]byte("4"), []byte("Alice"), []byte("25")}, // Alice comes before Charlie (same age)
			{[]byte("1"), []byte("Charlie"), []byte("25")},
			{[]byte("2"), []byte("Alice"), []byte("30")},
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

	// Test 5: Sort with Filter
	t.Run("SortWithFilter", func(t *testing.T) {
		filter := &Filter{
			InnerPlan: &SeqScan{
				TableMetaPageID: simpleTable.MetaPageID,
				SearchMode:      NewTupleSearchModeStart(),
				WhileCond: func(pkey [][]byte) bool {
					return true
				},
			},
			Cond: func(tup [][]byte) bool {
				// Filter: age >= "25"
				if len(tup) > 2 {
					return string(tup[2]) >= "25"
				}
				return false
			},
		}

		sort := &Sort{
			InnerPlan: filter,
			SortKeys: []SortKey{
				{ColumnIndex: 1, Ascending: true}, // Sort by name ascending
			},
		}

		executor, err := sort.Start(bufmgr)
		if err != nil {
			t.Fatal(err)
		}

		// Filter: age >= "25", then sort by name ascending
		// Expected order: Alice (id=2 or id=4), Alice (id=4 or id=2), Charlie (id=1)
		// Since Alice records have the same name, their relative order may vary
		// We'll check that we get exactly 3 tuples with the correct values
		expectedNames := []string{"Alice", "Alice", "Charlie"}
		i := 0
		aliceCount := 0
		charlieCount := 0
		for {
			tuple, ok, err := executor.Next(bufmgr)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if i >= len(expectedNames) {
				t.Fatalf("Got more tuples than expected")
			}
			// Check that name matches expected order
			name := string(tuple[1])
			if name != expectedNames[i] {
				t.Errorf("Tuple %d: expected name %s, got %s", i, expectedNames[i], name)
			}
			// Check that age is valid (25 or 30 for Alice, 25 for Charlie)
			age := string(tuple[2])
			if name == "Alice" {
				if age != "25" && age != "30" {
					t.Errorf("Tuple %d: Alice should have age 25 or 30, got %s", i, age)
				}
				aliceCount++
			} else if name == "Charlie" {
				if age != "25" {
					t.Errorf("Tuple %d: Charlie should have age 25, got %s", i, age)
				}
				charlieCount++
			}
			i++
		}
		if i != len(expectedNames) {
			t.Errorf("Expected %d tuples, got %d", len(expectedNames), i)
		}
		if aliceCount != 2 {
			t.Errorf("Expected 2 Alice tuples, got %d", aliceCount)
		}
		if charlieCount != 1 {
			t.Errorf("Expected 1 Charlie tuple, got %d", charlieCount)
		}
	})

	// Test 6: Sort empty result
	t.Run("SortEmptyResult", func(t *testing.T) {
		filter := &Filter{
			InnerPlan: &SeqScan{
				TableMetaPageID: simpleTable.MetaPageID,
				SearchMode:      NewTupleSearchModeStart(),
				WhileCond: func(pkey [][]byte) bool {
					return true
				},
			},
			Cond: func(tup [][]byte) bool {
				return false // Filter out all tuples
			},
		}

		sort := &Sort{
			InnerPlan: filter,
			SortKeys: []SortKey{
				{ColumnIndex: 1, Ascending: true},
			},
		}

		executor, err := sort.Start(bufmgr)
		if err != nil {
			t.Fatal(err)
		}

		tuple, ok, err := executor.Next(bufmgr)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Errorf("Expected no tuples, got %v", tuple)
		}
	})
}
