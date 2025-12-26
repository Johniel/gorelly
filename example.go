// Package internal provides example code demonstrating how to use the relly database.
// This file contains comprehensive examples for common database operations.
package internal

import (
	"fmt"
	"os"

	"github.com/Johniel/gorelly/btree"
	"github.com/Johniel/gorelly/buffer"
	"github.com/Johniel/gorelly/disk"
	"github.com/Johniel/gorelly/query"
	"github.com/Johniel/gorelly/table"
	"github.com/Johniel/gorelly/transaction"
	"github.com/Johniel/gorelly/tuple"
)

// ExampleBasicTableCreation demonstrates how to create a simple table and insert tuples.
func ExampleBasicTableCreation() {
	// 1. ディスクマネージャーとバッファプールマネージャーを作成
	dm, err := disk.OpenDiskManager("example.rly")
	if err != nil {
		fmt.Printf("Error creating disk manager: %v\n", err)
		return
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10) // 10ページのバッファプール
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	// 2. シンプルテーブルを作成
	// 最初の1要素がプライマリキー
	simpleTable := &table.SimpleTable{
		MetaPageID:  disk.InvalidPageID,
		NumKeyElems: 1,
	}

	if err := simpleTable.Create(bufmgr); err != nil {
		fmt.Printf("Error creating table: %v\n", err)
		return
	}

	fmt.Printf("Table created with meta page ID: %d\n", simpleTable.MetaPageID)

	// 3. タプルを挿入
	tuples := [][][]byte{
		{[]byte("z"), []byte("Alice"), []byte("Smith")},
		{[]byte("x"), []byte("Bob"), []byte("Johnson")},
		{[]byte("y"), []byte("Charlie"), []byte("Williams")},
		{[]byte("w"), []byte("Dave"), []byte("Miller")},
		{[]byte("v"), []byte("Eve"), []byte("Brown")},
	}

	for _, tup := range tuples {
		if err := simpleTable.Insert(bufmgr, tup); err != nil {
			fmt.Printf("Error inserting tuple: %v\n", err)
			return
		}
		fmt.Printf("Inserted tuple: %s\n", string(tup[0]))
	}

	// 4. 変更をディスクにフラッシュ
	if err := bufmgr.Flush(); err != nil {
		fmt.Printf("Error flushing: %v\n", err)
		return
	}

	fmt.Println("All tuples inserted successfully")
}

// ExampleTableWithIndex demonstrates how to create a table with secondary indexes.
func ExampleTableWithIndex() {
	dm, err := disk.OpenDiskManager("table_with_index.rly")
	if err != nil {
		fmt.Printf("Error creating disk manager: %v\n", err)
		return
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	// テーブルを作成（last_nameにユニークインデックス）
	tbl := &table.Table{
		MetaPageID:  disk.InvalidPageID,
		NumKeyElems: 1, // idがプライマリキー
		UniqueIndices: []*table.UniqueIndex{
			{
				MetaPageID: disk.InvalidPageID,
				Skey:       []int{2}, // last_name (インデックス2) にユニークインデックス
			},
		},
	}

	if err := tbl.Create(bufmgr); err != nil {
		fmt.Printf("Error creating table: %v\n", err)
		return
	}

	// タプルを挿入
	tuples := [][][]byte{
		{[]byte("z"), []byte("Alice"), []byte("Smith")},
		{[]byte("x"), []byte("Bob"), []byte("Johnson")},
		{[]byte("y"), []byte("Charlie"), []byte("Williams")},
	}

	for _, tup := range tuples {
		if err := tbl.Insert(bufmgr, tup); err != nil {
			fmt.Printf("Error inserting tuple: %v\n", err)
			return
		}
	}

	if err := bufmgr.Flush(); err != nil {
		fmt.Printf("Error flushing: %v\n", err)
		return
	}

	fmt.Println("Table with index created successfully")
}

// ExampleSequentialScan demonstrates how to perform a sequential scan on a table.
func ExampleSequentialScan() {
	dm, err := disk.OpenDiskManager("example.rly")
	if err != nil {
		fmt.Printf("Error opening disk manager: %v\n", err)
		return
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	// テーブルのメタページID（実際の使用時は保存された値を使用）
	tableMetaPageID := disk.PageID(0)

	// シーケンシャルスキャンを実行
	// 最初から最後までスキャン
	seqScan := &query.SeqScan{
		TableMetaPageID: tableMetaPageID,
		SearchMode:      query.NewTupleSearchModeStart(),
		WhileCond: func(pkey [][]byte) bool {
			// すべてのタプルを返す
			return true
		},
	}

	executor, err := seqScan.Start(bufmgr)
	if err != nil {
		fmt.Printf("Error starting scan: %v\n", err)
		return
	}

	fmt.Println("Sequential scan results:")
	for {
		tuple, ok, err := executor.Next(bufmgr)
		if err != nil {
			fmt.Printf("Error reading tuple: %v\n", err)
			return
		}
		if !ok {
			break
		}
		fmt.Printf("Tuple: %v\n", tuple)
	}
}

// ExampleFilteredScan demonstrates how to filter records during a scan.
func ExampleFilteredScan() {
	dm, err := disk.OpenDiskManager("example.rly")
	if err != nil {
		fmt.Printf("Error opening disk manager: %v\n", err)
		return
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	tableMetaPageID := disk.PageID(0)

	// フィルタ付きスキャン
	// last_nameが"Smith"のレコードのみを取得
	filter := &query.Filter{
		InnerPlan: &query.SeqScan{
			TableMetaPageID: tableMetaPageID,
			SearchMode:      query.NewTupleSearchModeStart(),
			WhileCond: func(pkey [][]byte) bool {
				return true
			},
		},
		Cond: func(tup [][]byte) bool {
			// last_name (インデックス2) が "Smith" のタプルをフィルタ
			if len(tup) > 2 {
				return string(tup[2]) == "Smith"
			}
			return false
		},
	}

	executor, err := filter.Start(bufmgr)
	if err != nil {
		fmt.Printf("Error starting filtered scan: %v\n", err)
		return
	}

	fmt.Println("Filtered scan results (last_name == 'Smith'):")
	for {
		tuple, ok, err := executor.Next(bufmgr)
		if err != nil {
			fmt.Printf("Error reading tuple: %v\n", err)
			return
		}
		if !ok {
			break
		}
		fmt.Printf("Tuple: %v\n", tuple)
	}
}

// ExampleIndexScan demonstrates how to use an index for efficient lookups.
func ExampleIndexScan() {
	dm, err := disk.OpenDiskManager("table_with_index.rly")
	if err != nil {
		fmt.Printf("Error opening disk manager: %v\n", err)
		return
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	// テーブルとインデックスのメタページID（実際の使用時は保存された値を使用）
	tableMetaPageID := disk.PageID(0)
	indexMetaPageID := disk.PageID(1) // インデックスのメタページID

	// インデックススキャンを使用してlast_nameで検索
	indexScan := &query.IndexScan{
		TableMetaPageID: tableMetaPageID,
		IndexMetaPageID: indexMetaPageID,
		SearchMode: query.NewTupleSearchModeKey([][]byte{
			[]byte("Smith"), // last_nameで検索
		}),
		WhileCond: func(skey [][]byte) bool {
			// 完全一致のみ
			return string(skey[0]) == "Smith"
		},
	}

	executor, err := indexScan.Start(bufmgr)
	if err != nil {
		fmt.Printf("Error starting index scan: %v\n", err)
		return
	}

	fmt.Println("Index scan results (last_name == 'Smith'):")
	for {
		tuple, ok, err := executor.Next(bufmgr)
		if err != nil {
			fmt.Printf("Error reading tuple: %v\n", err)
			return
		}
		if !ok {
			break
		}
		fmt.Printf("Tuple: %v\n", tuple)
	}
}

// ExampleBTreeDirectUsage demonstrates how to use B+ tree directly.
func ExampleBTreeDirectUsage() {
	dm, err := disk.OpenDiskManager("btree_example.rly")
	if err != nil {
		fmt.Printf("Error creating disk manager: %v\n", err)
		return
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	// B+ツリーを作成
	bt, err := btree.CreateBTree(bufmgr)
	if err != nil {
		fmt.Printf("Error creating B+ tree: %v\n", err)
		return
	}

	fmt.Printf("B+ tree created with meta page ID: %d\n", bt.MetaPageID)

	// キー・バリューペアを挿入
	keyValuePairs := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	}

	for _, kv := range keyValuePairs {
		if err := bt.Insert(bufmgr, kv.key, kv.value); err != nil {
			fmt.Printf("Error inserting key-value pair: %v\n", err)
			return
		}
	}

	// すべてのキー・バリューペアを検索
	iter, err := bt.Search(bufmgr, btree.NewSearchModeStart())
	if err != nil {
		fmt.Printf("Error searching B+ tree: %v\n", err)
		return
	}

	fmt.Println("B+ tree contents:")
	for {
		keyBytes, valueBytes, ok, err := iter.Next(bufmgr)
		if err != nil {
			fmt.Printf("Error reading from iterator: %v\n", err)
			return
		}
		if !ok {
			break
		}
		fmt.Printf("Key: %s, Value: %s\n", string(keyBytes), string(valueBytes))
	}

	if err := bufmgr.Flush(); err != nil {
		fmt.Printf("Error flushing: %v\n", err)
		return
	}
}

// ExampleTransactionUsage demonstrates how to use transactions.
func ExampleTransactionUsage() {
	dm, err := disk.OpenDiskManager("transaction_example.rly")
	if err != nil {
		fmt.Printf("Error creating disk manager: %v\n", err)
		return
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	// トランザクションマネージャーを作成
	tm := transaction.NewTransactionManager()

	// トランザクションを開始
	txn := tm.Begin()
	fmt.Printf("Transaction %d started\n", txn.ID)

	// テーブルを作成
	simpleTable := &table.SimpleTable{
		MetaPageID:  disk.InvalidPageID,
		NumKeyElems: 1,
	}

	if err := simpleTable.Create(bufmgr); err != nil {
		fmt.Printf("Error creating table: %v\n", err)
		tm.Abort(txn)
		return
	}

	// タプルを挿入
	tuples := [][][]byte{
		{[]byte("1"), []byte("Alice"), []byte("Smith")},
		{[]byte("2"), []byte("Bob"), []byte("Johnson")},
	}

	for _, tup := range tuples {
		if err := simpleTable.Insert(bufmgr, tup); err != nil {
			fmt.Printf("Error inserting tuple: %v\n", err)
			tm.Abort(txn)
			return
		}
	}

	// トランザクションをコミット
	if err := tm.Commit(txn); err != nil {
		fmt.Printf("Error committing transaction: %v\n", err)
		return
	}

	fmt.Println("Transaction committed successfully")

	// 変更をフラッシュ
	if err := bufmgr.Flush(); err != nil {
		fmt.Printf("Error flushing: %v\n", err)
		return
	}
}

// ExampleLogManagerUsage demonstrates how to use Write-Ahead Logging (WAL).
func ExampleLogManagerUsage() {
	// ログファイルのパス
	logPath := "/tmp/relly_example.log"
	defer os.Remove(logPath) // クリーンアップ

	// LogManagerを作成
	logManager, err := transaction.NewLogManager(logPath)
	if err != nil {
		fmt.Printf("Error creating log manager: %v\n", err)
		return
	}
	defer logManager.Close()

	// トランザクションを作成
	tm := transaction.NewTransactionManager()
	txn := tm.Begin()
	fmt.Printf("Transaction %d started\n", txn.ID)

	// ページ更新のログレコードを作成
	// 実際の使用では、ページを更新する前にログを記録します
	updateRecord := &transaction.LogRecord{
		Type:     transaction.LogRecordTypeUpdate,
		TxnID:    txn.ID,
		PageID:   disk.PageID(1),
		Offset:   100,
		OldValue: []byte{0, 0, 0, 0},
		NewValue: []byte{1, 2, 3, 4},
	}

	// WALプロトコル: データを書き込む前にログを記録
	if err := logManager.AppendLog(updateRecord); err != nil {
		fmt.Printf("Error appending log: %v\n", err)
		tm.Abort(txn)
		return
	}
	fmt.Println("Log record appended (WAL protocol)")

	// ここで実際のページ更新を行う
	// （この例では簡略化のため、ログのみを記録）

	// コミットログレコードを記録
	commitRecord := &transaction.LogRecord{
		Type:  transaction.LogRecordTypeCommit,
		TxnID: txn.ID,
	}

	if err := logManager.AppendLog(commitRecord); err != nil {
		fmt.Printf("Error appending commit log: %v\n", err)
		tm.Abort(txn)
		return
	}

	// ログをフラッシュして永続化を保証
	if err := logManager.Flush(); err != nil {
		fmt.Printf("Error flushing log: %v\n", err)
		return
	}

	// トランザクションをコミット
	if err := tm.Commit(txn); err != nil {
		fmt.Printf("Error committing transaction: %v\n", err)
		return
	}

	fmt.Println("Transaction committed with WAL")

	// ログを読み取って確認
	records, err := logManager.ReadLog()
	if err != nil {
		fmt.Printf("Error reading log: %v\n", err)
		return
	}

	fmt.Printf("Total log records: %d\n", len(records))
	for i, record := range records {
		fmt.Printf("Record %d: Type=%d, TxnID=%d, LSN=%d\n",
			i, record.Type, record.TxnID, record.LSN)
	}
}

// ExampleTransactionWithWAL demonstrates a complete transaction with WAL and recovery.
func ExampleTransactionWithWAL() {
	dbFile := "wal_example.rly"
	logPath := "/tmp/relly_wal.log"
	defer os.Remove(dbFile)
	defer os.Remove(logPath)

	// ディスクマネージャーとバッファプールマネージャーを作成
	dm, err := disk.OpenDiskManager(dbFile)
	if err != nil {
		fmt.Printf("Error creating disk manager: %v\n", err)
		return
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	// LogManagerを作成
	logManager, err := transaction.NewLogManager(logPath)
	if err != nil {
		fmt.Printf("Error creating log manager: %v\n", err)
		return
	}
	defer logManager.Close()

	// RecoveryManagerを作成
	recoveryManager := transaction.NewRecoveryManager(logManager, bufmgr)

	// トランザクションマネージャーを作成
	tm := transaction.NewTransactionManager()

	// トランザクション1: 正常にコミット
	fmt.Println("=== Transaction 1: Successful Commit ===")
	txn1 := tm.Begin()
	fmt.Printf("Transaction %d started\n", txn1.ID)

	// テーブルを作成
	simpleTable := &table.SimpleTable{
		MetaPageID:  disk.InvalidPageID,
		NumKeyElems: 1,
	}

	if err := simpleTable.Create(bufmgr); err != nil {
		fmt.Printf("Error creating table: %v\n", err)
		tm.Abort(txn1)
		return
	}

	// タプルを挿入（実際の実装では、各操作の前にログを記録）
	tup1 := [][]byte{[]byte("1"), []byte("Alice"), []byte("Smith")}
	if err := simpleTable.Insert(bufmgr, tup1); err != nil {
		fmt.Printf("Error inserting tuple: %v\n", err)
		tm.Abort(txn1)
		return
	}

	// コミットログを記録
	commitRecord1 := &transaction.LogRecord{
		Type:  transaction.LogRecordTypeCommit,
		TxnID: txn1.ID,
	}
	if err := logManager.AppendLog(commitRecord1); err != nil {
		fmt.Printf("Error appending commit log: %v\n", err)
		tm.Abort(txn1)
		return
	}

	// ログをフラッシュ
	if err := logManager.Flush(); err != nil {
		fmt.Printf("Error flushing log: %v\n", err)
		return
	}

	// トランザクションをコミット
	if err := tm.Commit(txn1); err != nil {
		fmt.Printf("Error committing transaction: %v\n", err)
		return
	}
	fmt.Println("Transaction 1 committed successfully")

	// トランザクション2: ロールバック
	fmt.Println("\n=== Transaction 2: Rollback ===")
	txn2 := tm.Begin()
	fmt.Printf("Transaction %d started\n", txn2.ID)

	tup2 := [][]byte{[]byte("2"), []byte("Bob"), []byte("Johnson")}
	if err := simpleTable.Insert(bufmgr, tup2); err != nil {
		fmt.Printf("Error inserting tuple: %v\n", err)
		tm.Abort(txn2)
		return
	}

	// エラーが発生したと仮定してロールバック
	fmt.Println("Error occurred, rolling back transaction 2")
	if err := recoveryManager.Rollback(txn2); err != nil {
		fmt.Printf("Error during rollback: %v\n", err)
		return
	}

	if err := tm.Abort(txn2); err != nil {
		fmt.Printf("Error aborting transaction: %v\n", err)
		return
	}
	fmt.Println("Transaction 2 rolled back successfully")

	// 変更をフラッシュ
	if err := bufmgr.Flush(); err != nil {
		fmt.Printf("Error flushing: %v\n", err)
		return
	}

	fmt.Println("\n=== Recovery Test ===")
	// リカバリを実行（実際のクラッシュ後のリカバリをシミュレート）
	if err := recoveryManager.Recover(); err != nil {
		fmt.Printf("Error during recovery: %v\n", err)
		return
	}
	fmt.Println("Recovery completed successfully")
}

// ExampleTransactionWithLock demonstrates how to use transactions with locks.
func ExampleTransactionWithLock() {
	dm, err := disk.OpenDiskManager("transaction_lock_example.rly")
	if err != nil {
		fmt.Printf("Error creating disk manager: %v\n", err)
		return
	}
	defer dm.Close()

	// トランザクションマネージャーとロックマネージャーを作成
	tm := transaction.NewTransactionManager()
	lm := transaction.NewLockManager()

	// トランザクション1を開始
	txn1 := tm.Begin()
	fmt.Printf("Transaction 1 (%d) started\n", txn1.ID)

	// RID（Tuple ID）を定義
	rid := transaction.RID{
		PageID: disk.PageID(1),
		SlotID: 0,
	}

	// トランザクション1が共有ロックを取得
	if err := lm.LockShared(txn1, rid); err != nil {
		fmt.Printf("Error acquiring shared lock: %v\n", err)
		tm.Abort(txn1)
		return
	}
	fmt.Println("Transaction 1 acquired shared lock")

	// トランザクション2を開始
	txn2 := tm.Begin()
	fmt.Printf("Transaction 2 (%d) started\n", txn2.ID)

	// トランザクション2も共有ロックを取得（可能）
	if err := lm.LockShared(txn2, rid); err != nil {
		fmt.Printf("Error acquiring shared lock: %v\n", err)
		tm.Abort(txn2)
		return
	}
	fmt.Println("Transaction 2 acquired shared lock")

	// ロックを解放
	lm.Unlock(txn1, rid)
	lm.Unlock(txn2, rid)

	// トランザクションをコミット
	tm.Commit(txn1)
	tm.Commit(txn2)

	fmt.Println("Both transactions committed successfully")
}

// ExampleRangeQuery demonstrates how to perform a range query.
func ExampleRangeQuery() {
	dm, err := disk.OpenDiskManager("example.rly")
	if err != nil {
		fmt.Printf("Error opening disk manager: %v\n", err)
		return
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	tableMetaPageID := disk.PageID(0)

	// 範囲クエリ: キーが "w" から "y" までのタプルを取得
	seqScan := &query.SeqScan{
		TableMetaPageID: tableMetaPageID,
		SearchMode: query.NewTupleSearchModeKey([][]byte{
			[]byte("w"), // 開始キー
		}),
		WhileCond: func(pkey [][]byte) bool {
			// "y" まで続ける
			if len(pkey) > 0 {
				key := string(pkey[0])
				return key <= "y"
			}
			return false
		},
	}

	executor, err := seqScan.Start(bufmgr)
	if err != nil {
		fmt.Printf("Error starting range query: %v\n", err)
		return
	}

	fmt.Println("Range query results (keys from 'w' to 'y'):")
	for {
		tuple, ok, err := executor.Next(bufmgr)
		if err != nil {
			fmt.Printf("Error reading tuple: %v\n", err)
			return
		}
		if !ok {
			break
		}
		fmt.Printf("Tuple: %v\n", tuple)
	}
}

// ExampleTupleEncoding demonstrates how to encode and decode tuples.
func ExampleTupleEncoding() {
	// タプルをエンコード
	tup := [][]byte{
		[]byte("id1"),
		[]byte("Alice"),
		[]byte("Smith"),
	}

	encoded := make([]byte, 0)
	tuple.Encode(tup, &encoded)
	fmt.Printf("Encoded tuple: %v\n", encoded)

	// タプルをデコード
	decoded := make([][]byte, 0)
	tuple.Decode(encoded, &decoded)
	fmt.Printf("Decoded tuple: %v\n", decoded)

	// 各フィールドを表示
	for i, field := range decoded {
		fmt.Printf("Field %d: %s\n", i, string(field))
	}
}

// ExampleCompleteWorkflow demonstrates a complete database workflow.
func ExampleCompleteWorkflow() {
	// データベースファイル名
	dbFile := "complete_example.rly"

	// 既存のファイルを削除（クリーンな状態から開始）
	os.Remove(dbFile)

	// 1. ディスクマネージャーとバッファプールマネージャーを作成
	dm, err := disk.OpenDiskManager(dbFile)
	if err != nil {
		fmt.Printf("Error creating disk manager: %v\n", err)
		return
	}
	defer dm.Close()

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(dm, pool)

	// 2. テーブルを作成
	simpleTable := &table.SimpleTable{
		MetaPageID:  disk.InvalidPageID,
		NumKeyElems: 1,
	}

	if err := simpleTable.Create(bufmgr); err != nil {
		fmt.Printf("Error creating table: %v\n", err)
		return
	}

	fmt.Printf("Table created with meta page ID: %d\n", simpleTable.MetaPageID)

	// 3. データを挿入
	tuples := [][][]byte{
		{[]byte("1"), []byte("Alice"), []byte("Smith"), []byte("25")},
		{[]byte("2"), []byte("Bob"), []byte("Johnson"), []byte("30")},
		{[]byte("3"), []byte("Charlie"), []byte("Williams"), []byte("35")},
		{[]byte("4"), []byte("Dave"), []byte("Miller"), []byte("28")},
		{[]byte("5"), []byte("Eve"), []byte("Brown"), []byte("32")},
	}

	for _, tup := range tuples {
		if err := simpleTable.Insert(bufmgr, tup); err != nil {
			fmt.Printf("Error inserting tuple: %v\n", err)
			return
		}
	}

	// 4. 変更をフラッシュ
	if err := bufmgr.Flush(); err != nil {
		fmt.Printf("Error flushing: %v\n", err)
		return
	}

	// 5. データを読み取る（シーケンシャルスキャン）
	fmt.Println("\n=== Sequential Scan ===")
	seqScan := &query.SeqScan{
		TableMetaPageID: simpleTable.MetaPageID,
		SearchMode:      query.NewTupleSearchModeStart(),
		WhileCond: func(pkey [][]byte) bool {
			return true
		},
	}

	executor, err := seqScan.Start(bufmgr)
	if err != nil {
		fmt.Printf("Error starting scan: %v\n", err)
		return
	}

	for {
		tuple, ok, err := executor.Next(bufmgr)
		if err != nil {
			fmt.Printf("Error reading tuple: %v\n", err)
			return
		}
		if !ok {
			break
		}
		fmt.Printf("ID: %s, First: %s, Last: %s, Age: %s\n",
			string(tuple[0]), string(tuple[1]), string(tuple[2]), string(tuple[3]))
	}

	// 6. フィルタリング（年齢が30以上のレコード）
	fmt.Println("\n=== Filtered Scan (Age >= 30) ===")
	filter := &query.Filter{
		InnerPlan: &query.SeqScan{
			TableMetaPageID: simpleTable.MetaPageID,
			SearchMode:      query.NewTupleSearchModeStart(),
			WhileCond: func(pkey [][]byte) bool {
				return true
			},
		},
		Cond: func(tup [][]byte) bool {
			if len(tup) > 3 {
				age := string(tup[3])
				return age >= "30"
			}
			return false
		},
	}

	executor, err = filter.Start(bufmgr)
	if err != nil {
		fmt.Printf("Error starting filtered scan: %v\n", err)
		return
	}

	for {
		tuple, ok, err := executor.Next(bufmgr)
		if err != nil {
			fmt.Printf("Error reading tuple: %v\n", err)
			return
		}
		if !ok {
			break
		}
		fmt.Printf("ID: %s, First: %s, Last: %s, Age: %s\n",
			string(tuple[0]), string(tuple[1]), string(tuple[2]), string(tuple[3]))
	}

	fmt.Println("\nComplete workflow finished successfully")
}

// RunAllExamples runs all example functions.
// This can be used for testing and demonstration purposes.
func RunAllExamples() {
	fmt.Println("=== Example: Basic Table Creation ===")
	ExampleBasicTableCreation()
	fmt.Println()

	fmt.Println("=== Example: Table with Index ===")
	ExampleTableWithIndex()
	fmt.Println()

	fmt.Println("=== Example: Sequential Scan ===")
	ExampleSequentialScan()
	fmt.Println()

	fmt.Println("=== Example: Filtered Scan ===")
	ExampleFilteredScan()
	fmt.Println()

	fmt.Println("=== Example: Index Scan ===")
	ExampleIndexScan()
	fmt.Println()

	fmt.Println("=== Example: B+ Tree Direct Usage ===")
	ExampleBTreeDirectUsage()
	fmt.Println()

	fmt.Println("=== Example: Transaction Usage ===")
	ExampleTransactionUsage()
	fmt.Println()

	fmt.Println("=== Example: Transaction with Lock ===")
	ExampleTransactionWithLock()
	fmt.Println()

	fmt.Println("=== Example: Log Manager Usage ===")
	ExampleLogManagerUsage()
	fmt.Println()

	fmt.Println("=== Example: Transaction with WAL ===")
	ExampleTransactionWithWAL()
	fmt.Println()

	fmt.Println("=== Example: Range Query ===")
	ExampleRangeQuery()
	fmt.Println()

	fmt.Println("=== Example: Tuple Encoding ===")
	ExampleTupleEncoding()
	fmt.Println()

	fmt.Println("=== Example: Complete Workflow ===")
	ExampleCompleteWorkflow()
	fmt.Println()
}
