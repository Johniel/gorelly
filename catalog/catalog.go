package catalog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Johniel/gorelly/btree"
	"github.com/Johniel/gorelly/buffer"
	"github.com/Johniel/gorelly/disk"
	"github.com/Johniel/gorelly/table"
	"github.com/Johniel/gorelly/tuple"
	"sync"
)

var (
	ErrTableNotFound         = errors.New("table not found")
	ErrTableExists           = errors.New("table already exists")
	ErrCatalogNotInitialized = errors.New("catalog tables not initialized")
)

type ColumnType int

const (
	ColumnTypeInt ColumnType = iota
	ColumnTypeVarchar
	ColumnTypeBlob
)

func (ct ColumnType) String() string {
	switch ct {
	case ColumnTypeInt:
		return "INT"
	case ColumnTypeVarchar:
		return "VARCHAR"
	case ColumnTypeBlob:
		return "BLOB"
	default:
		return "UNKNOWN"
	}
}

type ColumnDef struct {
	Name         string
	Type         ColumnType
	Size         int
	Nullable     bool
	IsPrimaryKey bool
}

type TableSchema struct {
	TableID     uint32
	TableName   string
	MetaPageID  disk.PageID
	NumKeyElems int // Number of primary key elements
	Columns     []ColumnDef
	Indexes     []IndexDef
}

type IndexDef struct {
	IndexID       uint32
	IndexName     string
	TableID       uint32
	MetaPageID    disk.PageID
	IsUnique      bool
	ColumnIndices []int
}

type CatalogManager struct {
	bufmgr *buffer.BufferPoolManager

	tablesCatalog  *table.Table
	columnsCatalog *table.Table
	indexesCatalog *table.Table

	nextTableID uint32
	nextIndexID uint32

	schemaCache map[string]*TableSchema
	mu          sync.RWMutex
}

func NewCatalogManager(bufmgr *buffer.BufferPoolManager) (*CatalogManager, error) {
	cm := &CatalogManager{
		bufmgr:      bufmgr,
		schemaCache: make(map[string]*TableSchema),
		nextTableID: 1,
		nextIndexID: 1,
	}

	if err := cm.initializeCatalogTables(); err != nil {
		return nil, err
	}

	return cm, nil
}

func (cm *CatalogManager) initializeCatalogTables() error {
	tablesCatalog := &table.SimpleTable{
		MetaPageID:  disk.PageID(0),
		NumKeyElems: 1, // table_id is the primary key
	}
	if err := tablesCatalog.Create(cm.bufmgr); err != nil {
		// Table might already exist, use existing
		tablesCatalog.MetaPageID = disk.PageID(0)
	}
	cm.tablesCatalog = &table.Table{
		MetaPageID:  tablesCatalog.MetaPageID,
		NumKeyElems: 1,
	}

	// Try to create columns_catalog
	// Schema: [table_id (PK), column_index (PK), column_name, column_type, column_size, nullable, is_primary_key]
	columnsCatalog := &table.SimpleTable{
		MetaPageID:  disk.PageID(1),
		NumKeyElems: 2, // table_id + column_index is the composite primary key
	}
	if err := columnsCatalog.Create(cm.bufmgr); err != nil {
		// Table might already exist, use existing
		columnsCatalog.MetaPageID = disk.PageID(1)
	}
	cm.columnsCatalog = &table.Table{
		MetaPageID:  columnsCatalog.MetaPageID,
		NumKeyElems: 2,
	}

	// Try to create indexes_catalog
	// Schema: [index_id (PK), index_name, table_id, meta_page_id, is_unique, column_indices]
	indexesCatalog := &table.SimpleTable{
		MetaPageID:  disk.PageID(2),
		NumKeyElems: 1, // index_id is the primary key
	}
	if err := indexesCatalog.Create(cm.bufmgr); err != nil {
		// Table might already exist, use existing
		indexesCatalog.MetaPageID = disk.PageID(2)
	}
	cm.indexesCatalog = &table.Table{
		MetaPageID:  indexesCatalog.MetaPageID,
		NumKeyElems: 1,
	}
	return nil
}

func (cm *CatalogManager) CreateTable(tableName string, columns []ColumnDef) (*TableSchema, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Check if table already exists
	if _, exists := cm.schemaCache[tableName]; exists {
		return nil, ErrTableExists
	}

	// Check catalog table
	if cm.tableExistsInCatalog(tableName) {
		return nil, ErrTableExists
	}

	tableID := cm.nextIndexID
	cm.nextIndexID += 1

	numKeyElems := 0
	for _, col := range columns {
		if col.IsPrimaryKey {
			numKeyElems++
		}
	}

	bt, err := btree.CreateBTree(cm.bufmgr)
	if err != nil {
		return nil, fmt.Errorf("failed to create B+ tree: %w", err)
	}

	schema := &TableSchema{
		TableID:     tableID,
		TableName:   tableName,
		MetaPageID:  bt.MetaPageID,
		NumKeyElems: numKeyElems,
		Columns:     columns,
		Indexes:     []IndexDef{},
	}

	// Insert into tables_catalog
	if err := cm.insertTableRecord(tableID, tableName, bt.MetaPageID, numKeyElems); err != nil {
		return nil, fmt.Errorf("failed to insert table record: %w", err)
	}

	// Insert into columns_catalog
	for i, col := range columns {
		if err := cm.insertColumnRecord(tableID, i, col); err != nil {
			return nil, fmt.Errorf("failed to insert column record: %w", err)
		}
	}

	// Cache the schema
	cm.schemaCache[tableName] = schema

	return schema, nil
}

func (cm *CatalogManager) insertColumnRecord(tableID uint32, columnIndex int, col ColumnDef) error {
	tableIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(tableIDBytes, tableID)

	columnIndexBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(columnIndexBytes, uint32(columnIndex))

	columnTypeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(columnTypeBytes, uint32(col.Type))

	columnSizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(columnSizeBytes, uint32(col.Size))

	nullableBytes := make([]byte, 1)
	if col.Nullable {
		nullableBytes[0] = 1
	} else {
		nullableBytes[0] = 0
	}

	isPrimaryKeyBytes := make([]byte, 1)
	if col.IsPrimaryKey {
		isPrimaryKeyBytes[0] = 1
	} else {
		isPrimaryKeyBytes[0] = 0
	}

	tup := [][]byte{
		tableIDBytes,      // PK part 1
		columnIndexBytes,  // PK part 2
		[]byte(col.Name),  // column_name
		columnTypeBytes,   // column_type
		columnSizeBytes,   // column_size
		nullableBytes,     // nullable
		isPrimaryKeyBytes, // is_primary_key
	}

	return cm.columnsCatalog.Insert(cm.bufmgr, tup)
}

func (cm *CatalogManager) insertTableRecord(
	tableID uint32,
	tableName string,
	metaPageID disk.PageID,
	numKeyElems int,
) error {
	tableIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(tableIDBytes, tableID)

	metaPageIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(metaPageIDBytes, uint64(metaPageID))

	numKeyElemsBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(numKeyElemsBytes, uint32(numKeyElems))

	tup := [][]byte{
		tableIDBytes,      // PK
		[]byte(tableName), // table_name
		metaPageIDBytes,   // meta_page_id
		numKeyElemsBytes,  // num_key_elems
	}

	return cm.tablesCatalog.Insert(cm.bufmgr, tup)
}

func (cm *CatalogManager) tableExistsInCatalog(tableName string) bool {
	_, _, _, err := cm.findTableInCatalog(tableName)
	return err == nil
}

func (cm *CatalogManager) findTableInCatalog(tableName string) (uint32, disk.PageID, int, error) {
	bt := btree.NewBTree(cm.tablesCatalog.MetaPageID)
	iter, err := bt.Search(cm.bufmgr, btree.NewSearchModeStart())
	if err != nil {
		return 0, disk.InvalidPageID, 0, err
	}

	for {
		keyBytes, valueBytes, ok, err := iter.Next(cm.bufmgr)
		if err != nil {
			return 0, disk.InvalidPageID, 0, err
		}
		if !ok {
			break
		}

		var keyElems [][]byte
		tuple.Decode(keyBytes, &keyElems)
		var valueElems [][]byte
		tuple.Decode(valueBytes, &valueElems)

		if 0 < len(valueElems) && string(valueElems[0]) == tableName {
			tableID := binary.BigEndian.Uint32(keyElems[0])
			metaPageID := disk.PageID(binary.BigEndian.Uint32(keyElems[1]))
			numKeyElements := int(binary.BigEndian.Uint32(keyElems[2]))
			return tableID, metaPageID, numKeyElements, nil
		}
	}

	return 0, disk.InvalidPageID, 0, ErrTableNotFound
}
