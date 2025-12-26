// Package query provides a query execution engine for the database.
// It implements various query plans including sequential scans, index scans, and filters.
package query

import (
	"sort"

	"github.com/Johniel/gorelly/btree"
	"github.com/Johniel/gorelly/buffer"
	"github.com/Johniel/gorelly/disk"
	"github.com/Johniel/gorelly/tuple"
)

// Tuple represents a database record as a slice of byte slices.
// Each element in the tuple corresponds to a column value.
type Tuple = [][]byte

// TupleSlice is an alias for a slice of tuples (used for function parameters).
type TupleSlice = [][]byte

// TupleSearchMode specifies how to search for tuples in a table or index.
type TupleSearchMode struct {
	IsStart bool     // If true, start from the beginning; if false, search for Key
	Key     [][]byte // The key to search for (only used if IsStart is false)
}

func NewTupleSearchModeStart() TupleSearchMode {
	return TupleSearchMode{IsStart: true}
}

func NewTupleSearchModeKey(key [][]byte) TupleSearchMode {
	return TupleSearchMode{IsStart: false, Key: key}
}

func (tsm TupleSearchMode) Encode() btree.SearchMode {
	if tsm.IsStart {
		return btree.NewSearchModeStart()
	}
	keyBytes := make([]byte, 0)
	tuple.Encode(tsm.Key, &keyBytes)
	return btree.NewSearchModeKey(keyBytes)
}

// Executor executes a query plan and produces tuples one at a time.
type Executor interface {
	// Next returns the next tuple from the execution result.
	// Returns (nil, false, nil) when there are no more tuples.
	Next(bufmgr *buffer.BufferPoolManager) (Tuple, bool, error)
}

// PlanNode represents a query plan node that can be executed.
type PlanNode interface {
	// Start initializes and returns an Executor for this plan node.
	Start(bufmgr *buffer.BufferPoolManager) (Executor, error)
}

// SeqScan performs a sequential scan on a table.
// It scans the table starting from SearchMode and continues while WhileCond returns true.
type SeqScan struct {
	TableMetaPageID disk.PageID           // Page ID of the table's B+ tree meta page
	SearchMode      TupleSearchMode       // Starting point for the scan
	WhileCond       func(TupleSlice) bool // Condition to continue scanning
}

func (ss *SeqScan) Start(bufmgr *buffer.BufferPoolManager) (Executor, error) {
	bt := btree.NewBTree(ss.TableMetaPageID)
	tableIter, err := bt.Search(bufmgr, ss.SearchMode.Encode())
	if err != nil {
		return nil, err
	}
	return &ExecSeqScan{
		tableIter: tableIter,
		whileCond: ss.WhileCond,
	}, nil
}

// ExecSeqScan is the executor for sequential scan operations.
type ExecSeqScan struct {
	tableIter *btree.Iter
	whileCond func(TupleSlice) bool
}

func (ess *ExecSeqScan) Next(bufmgr *buffer.BufferPoolManager) (Tuple, bool, error) {
	pkeyBytes, tupleBytes, ok, err := ess.tableIter.Next(bufmgr)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	pkey := make([][]byte, 0)
	tuple.Decode(pkeyBytes, &pkey)
	if !ess.whileCond(pkey) {
		return nil, false, nil
	}
	result := make([][]byte, len(pkey))
	copy(result, pkey)
	tuple.Decode(tupleBytes, &result)
	return result, true, nil
}

type Filter struct {
	InnerPlan PlanNode
	Cond      func(TupleSlice) bool
}

func (f *Filter) Start(bufmgr *buffer.BufferPoolManager) (Executor, error) {
	innerIter, err := f.InnerPlan.Start(bufmgr)
	if err != nil {
		return nil, err
	}
	return &ExecFilter{
		innerIter: innerIter,
		cond:      f.Cond,
	}, nil
}

// ExecFilter is the executor for filter operations.
type ExecFilter struct {
	innerIter Executor
	cond      func(TupleSlice) bool
}

func (ef *ExecFilter) Next(bufmgr *buffer.BufferPoolManager) (Tuple, bool, error) {
	for {
		tuple, ok, err := ef.innerIter.Next(bufmgr)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		if ef.cond(tuple) {
			return tuple, true, nil
		}
	}
}

type IndexScan struct {
	TableMetaPageID disk.PageID
	IndexMetaPageID disk.PageID
	SearchMode      TupleSearchMode
	WhileCond       func(TupleSlice) bool
}

func (is *IndexScan) Start(bufmgr *buffer.BufferPoolManager) (Executor, error) {
	tableBtree := btree.NewBTree(is.TableMetaPageID)
	indexBtree := btree.NewBTree(is.IndexMetaPageID)
	indexIter, err := indexBtree.Search(bufmgr, is.SearchMode.Encode())
	if err != nil {
		return nil, err
	}
	return &ExecIndexScan{
		tableBtree: tableBtree,
		indexIter:  indexIter,
		whileCond:  is.WhileCond,
	}, nil
}

// ExecIndexScan is the executor for index scan operations.
type ExecIndexScan struct {
	tableBtree *btree.BTree
	indexIter  *btree.Iter
	whileCond  func(TupleSlice) bool
}

func (eis *ExecIndexScan) Next(bufmgr *buffer.BufferPoolManager) (Tuple, bool, error) {
	skeyBytes, pkeyBytes, ok, err := eis.indexIter.Next(bufmgr)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	skey := make([][]byte, 0)
	tuple.Decode(skeyBytes, &skey)
	if !eis.whileCond(skey) {
		return nil, false, nil
	}
	tableIter, err := eis.tableBtree.Search(bufmgr, btree.NewSearchModeKey(pkeyBytes))
	if err != nil {
		return nil, false, err
	}
	_, tupleBytes, ok, err := tableIter.Next(bufmgr)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	result := make([][]byte, 0)
	tuple.Decode(pkeyBytes, &result)
	tuple.Decode(tupleBytes, &result)
	return result, true, nil
}

type IndexOnlyScan struct {
	IndexMetaPageID disk.PageID
	SearchMode      TupleSearchMode
	WhileCond       func(TupleSlice) bool
}

func (ios *IndexOnlyScan) Start(bufmgr *buffer.BufferPoolManager) (Executor, error) {
	bt := btree.NewBTree(ios.IndexMetaPageID)
	indexIter, err := bt.Search(bufmgr, ios.SearchMode.Encode())
	if err != nil {
		return nil, err
	}
	return &ExecIndexOnlyScan{
		indexIter: indexIter,
		whileCond: ios.WhileCond,
	}, nil
}

// ExecIndexOnlyScan is the executor for index-only scan operations.
type ExecIndexOnlyScan struct {
	indexIter *btree.Iter
	whileCond func(TupleSlice) bool
}

func (eios *ExecIndexOnlyScan) Next(bufmgr *buffer.BufferPoolManager) (Tuple, bool, error) {
	skeyBytes, pkeyBytes, ok, err := eios.indexIter.Next(bufmgr)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	skey := make([][]byte, 0)
	tuple.Decode(skeyBytes, &skey)
	if !eios.whileCond(skey) {
		return nil, false, nil
	}
	result := make([][]byte, len(skey))
	copy(result, skey)
	tuple.Decode(pkeyBytes, &result)
	return result, true, nil
}

type Project struct {
	InnerPlan     PlanNode
	ColumnIndices []int
}

func (p *Project) Start(bufmgr *buffer.BufferPoolManager) (Executor, error) {
	innerIter, err := p.InnerPlan.Start(bufmgr)
	if err != nil {
		return nil, err
	}
	return &ExecProject{
		innerIter:     innerIter,
		columnIndices: p.ColumnIndices,
	}, nil
}

type ExecProject struct {
	innerIter     Executor
	columnIndices []int
}

func (ep *ExecProject) Next(bufmgr *buffer.BufferPoolManager) (Tuple, bool, error) {
	inputTuple, ok, err := ep.innerIter.Next(bufmgr)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	result := make([][]byte, len(ep.columnIndices))
	for i, colIdx := range ep.columnIndices {
		if colIdx < 0 || colIdx >= len(inputTuple) {
			result[i] = []byte{}
		} else {
			result[i] = make([]byte, len(inputTuple[colIdx]))
			copy(result[i], inputTuple[colIdx])
		}
	}

	return result, true, nil
}

// SortKey specifies a column to sort by and the sort direction.
type SortKey struct {
	ColumnIndex int  // Index of the column to sort by (0-based)
	Ascending   bool // true for ascending order, false for descending order
}

// Sort performs sorting on tuples from an inner plan.
// It sorts tuples based on the specified sort keys.
// Multiple sort keys can be specified for multi-column sorting.
type Sort struct {
	InnerPlan PlanNode  // The inner plan node to sort
	SortKeys  []SortKey // Sort keys specifying columns and sort directions
}

func (s *Sort) Start(bufmgr *buffer.BufferPoolManager) (Executor, error) {
	innerIter, err := s.InnerPlan.Start(bufmgr)
	if err != nil {
		return nil, err
	}

	// Read all tuples from inner executor
	var tuples []Tuple
	for {
		tup, ok, err := innerIter.Next(bufmgr)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		// Make a deep copy of the tuple
		tupleCopy := make([][]byte, len(tup))
		for i := range tup {
			tupleCopy[i] = make([]byte, len(tup[i]))
			copy(tupleCopy[i], tup[i])
		}
		tuples = append(tuples, tupleCopy)
	}

	// Sort tuples
	sort.Slice(tuples, func(i, j int) bool {
		return compareTuples(tuples[i], tuples[j], s.SortKeys) < 0
	})

	return &ExecSort{
		tuples:  tuples,
		current: 0,
	}, nil
}

// ExecSort is the executor for sort operations.
type ExecSort struct {
	tuples  []Tuple // Sorted tuples
	current int     // Current position in the sorted tuples
}

func (es *ExecSort) Next(bufmgr *buffer.BufferPoolManager) (Tuple, bool, error) {
	if es.current >= len(es.tuples) {
		return nil, false, nil
	}

	// Return a copy of the current tuple
	tup := es.tuples[es.current]
	result := make([][]byte, len(tup))
	for i := range tup {
		result[i] = make([]byte, len(tup[i]))
		copy(result[i], tup[i])
	}

	es.current++
	return result, true, nil
}

// compareTuples compares two tuples based on the specified sort keys.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func compareTuples(a, b Tuple, sortKeys []SortKey) int {
	for _, key := range sortKeys {
		colIdx := key.ColumnIndex
		if colIdx < 0 || colIdx >= len(a) || colIdx >= len(b) {
			// Invalid column index, treat as equal
			continue
		}

		// Compare the column values
		cmp := compareBytes(a[colIdx], b[colIdx])
		if cmp != 0 {
			// If descending order, reverse the comparison
			if !key.Ascending {
				cmp = -cmp
			}
			return cmp
		}
		// If equal, continue to next sort key
	}
	return 0 // All sort keys are equal
}

// compareBytes compares two byte slices lexicographically.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
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
