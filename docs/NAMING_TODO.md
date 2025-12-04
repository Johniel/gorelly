# 命名規則の改善TODO

このドキュメントは、rellyの実装で一般的なRDBMS命名規則と異なる型やメソッドを列挙したものです。
将来的なリファクタリングの参考として使用してください。

## 調査方法

以下の主要なRDBMS実装と学術的な命名規則を参考にしています：
- PostgreSQL
- MySQL InnoDB
- SQLite
- CMU 15-445 Database Systems Course
- データベースシステムの教科書（Ramakrishnan & Gehrke等）

## 一般的な命名規則との乖離

### 1. Buffer型 (`internal/buffer/buffer.go`)

**現在の実装:**
```go
type Buffer struct {
    PageId  disk.PageId
    Page    *Page
    IsDirty bool
}
```

**問題点:**
- 一般的には`Buffer`は`Frame`とほぼ同義で使われることが多い
- または、バッファプール全体を指す場合もある
- この実装では、ページデータとメタデータを含む構造体として使用されている

**一般的な命名:**
- `BufferDescriptor` (PostgreSQL風)
- `Frame` (CMU 15-445風、ただし構造が異なる)
- または、`Frame`に統合して`Buffer`を削除

**影響範囲:**
- `internal/buffer/buffer.go`全体
- `internal/btree/btree.go` (Iter構造体で使用)
- その他、`*buffer.Buffer`を使用しているすべてのコード

---

### 2. FetchPageメソッド (`internal/buffer/buffer.go`)

**現在の実装:**
```go
func (bpm *BufferPoolManager) FetchPage(pageId disk.PageId) (*Buffer, error)
```

**問題点:**
- 一般的には`FetchPage`は`Page*`を返す（CMU 15-445、SQLite等）
- この実装では`*Buffer`を返している
- メソッド名と返り値の型が不一致

**一般的な命名:**
- `FetchPage()` → `Page*`を返す（メタデータ管理は別API）
- または、`FetchBuffer()` → `Buffer*`を返す

**推奨される変更:**
- `FetchBuffer(pageId)` にリネーム（返り値が`*Buffer`であることを明確化）
- または、返り値を`*Page`に変更（大きな変更が必要）

**影響範囲:**
- `internal/buffer/buffer.go`
- `internal/btree/btree.go` (FetchRootPage, searchInternal等)
- `internal/table/table.go`
- `internal/query/query.go`
- すべてのテストファイル

---

### 3. CreatePageメソッド (`internal/buffer/buffer.go`)

**現在の実装:**
```go
func (bpm *BufferPoolManager) CreatePage() (*Buffer, error)
```

**問題点:**
- `FetchPage`と同様に、返り値が`*Buffer`であることをメソッド名から推測できない

**一般的な命名:**
- `CreatePage()` → `Page*`を返す
- または、`CreateBuffer()` → `Buffer*`を返す

**推奨される変更:**
- `CreateBuffer()` にリネーム

**影響範囲:**
- `internal/buffer/buffer.go`
- `internal/btree/btree.go` (CreateBTree等)
- `internal/table/table.go`
- すべてのテストファイル

---

### 4. Frame型 (`internal/buffer/buffer.go`)

**現在の実装:**
```go
type Frame struct {
    UsageCount uint64
    Buffer     *Buffer
}
```

**問題点:**
- 命名は一般的な用法と一致しているが、構造が異なる
- 一般的には`Frame`が直接ページを保持するスロットを指す
- この実装では`Frame`が`Buffer`をラップしている

**一般的な構造:**
```go
type Frame struct {
    Page        *Page
    PageId      PageId
    IsDirty     bool
    UsageCount  uint64
}
```

**推奨される変更:**
- `Frame`に`Buffer`の内容を統合し、`Buffer`型を削除
- または、`Frame`を`BufferFrame`にリネームして明確化

**影響範囲:**
- `internal/buffer/buffer.go`全体
- `BufferPool`構造体

---

### 5. Overflow型 (`internal/btree/btree.go`)

**現在の実装:**
```go
type Overflow struct {
    Key         []byte
    ChildPageId disk.PageId
}
```

**問題点:**
- "Overflow"はノードが満杯になった状態を表す用語として使われる
- 分割結果を表す構造体としては一般的ではない
- 学術的には"Promoted Key"という用語が一般的

**一般的な命名:**
- `Split` (Java実装などでよく使われる)
- `SplitResult` (分割結果を表す構造体として一般的)
- `SplitInfo` (分割情報を表す構造体として一般的)
- 学術的には"Promoted Key"という用語が使われるが、これはキー自体を指す用語

**推奨される変更:**
- `Split` にリネーム（シンプルで一般的）
- または、`SplitResult` にリネーム（より明確）

**影響範囲:**
- `internal/btree/btree.go` (insertInternalメソッド)
- `Overflow`型を使用しているすべてのコード

---

### 6. Branch型 (`internal/btree/branch/branch.go`)

**現在の実装:**
```go
type Branch struct {
    header *BranchHeader
    body   *slotted.Slotted
    page   []byte
}
```

**問題点:**
- 学術的には"Internal Node"が標準的な用語
- "Branch Node"はあまり使われない
- Wikipedia、GeeksforGeeks、学術文献では"Internal Node"が標準

**一般的な命名:**
- `InternalNode` (学術的に標準的な用語)
- `Internal` (簡略形)

**推奨される変更:**
- `Branch` → `InternalNode` にリネーム
- または、`Branch` → `Internal` にリネーム

**影響範囲:**
- `internal/btree/branch/` パッケージ全体
- `internal/btree/node.go` (AsBranchメソッド)
- `internal/btree/btree.go` (IsBranchメソッド等)
- すべてのテストファイル

**補足:**
- B+ツリーの構造は通常、Root Node、Internal Nodes、Leaf Nodesで構成される
- "Branch"という用語は、ツリー構造の一般的な用語としては使われるが、B+ツリーの文脈では"Internal Node"が標準

---

## 命名が一般的と一致している型

以下の型は一般的な命名規則と一致しています：

- `Page` - 固定サイズのページデータ（一致）
- `PageId` - ページ識別子（一致）
- `BufferPool` - バッファプール（一致）
- `BufferPoolManager` - バッファプールマネージャー（一致）
- `DiskManager` - ディスクマネージャー（一致）
- `Frame` - 命名は一致（ただし構造が異なる）
- `Leaf` - B+ツリーのリーフノード（一致）
- `Slotted` - スロッテッドページ（標準的な用語、PostgreSQL/MySQLでも使用）
- `Tuple` - データベースのタプル（標準的な用語）
- `Executor` / `PlanNode` - クエリ実行エンジンの用語（CMU 15-445等で使用）
- `Meta` / `MetaHeader` - メタデータページ（一般的な用語）

---

## リファクタリングの優先順位

1. **高優先度:**
   - `FetchPage` → `FetchBuffer` へのリネーム
   - `CreatePage` → `CreateBuffer` へのリネーム
   - `Overflow` → `Split` へのリネーム
   - 理由: メソッド名と返り値の不一致、学術的な命名規則との不一致は混乱を招く

2. **中優先度:**
   - `Buffer`型の再検討（`Frame`への統合または`BufferDescriptor`へのリネーム）
   - `Branch` → `InternalNode` へのリネーム
   - 理由: 構造の再設計や学術的な命名規則への統一が必要だが、一貫性が向上する

3. **低優先度:**
   - `Frame`構造の再設計
   - 理由: 現在の構造も明確で動作しているため、優先度は低い

## 追加の調査が必要な型

以下の型についても、学術的な命名規則との一致を確認することを推奨します：

- `SearchMode` - 検索モードを表す型（一般的な用語か確認が必要）
- `Iter` - イテレータ型（一般的な用語だが、B+ツリーの文脈での標準的な命名を確認）
- `Executor` / `PlanNode` - クエリ実行エンジンの用語（一般的な用語と確認済み）
- `Slotted` - スロッテッドページ（標準的な用語と確認済み）
- `Tuple` - タプル（標準的な用語と確認済み）

---

## 参考資料

- CMU 15-445 Database Systems Course: https://15445.courses.cs.cmu.edu/
- PostgreSQL Buffer Manager: https://www.interdb.jp/pg/pgsql08.html
- Database Management Systems (Ramakrishnan & Gehrke)
- B+ Tree Wikipedia: https://en.wikipedia.org/wiki/B%2B_tree
- PostgreSQL Page Layout: https://www.postgresql.org/docs/18/storage-page-layout.html
- Database Internals: A Deep Dive into Distributed Data Systems (Alex Petrov)

## 調査方法の補足

このドキュメントは、以下の方法で命名規則を調査しています：

1. **主要なRDBMS実装の調査**: PostgreSQL、MySQL InnoDB、SQLiteの実装を確認
2. **学術的な文献の調査**: データベースシステムの教科書や学術論文を参照
3. **教育用データベース実装の調査**: CMU 15-445などの教育用実装を参照
4. **一般的な用語の確認**: Wikipedia、GeeksforGeeksなどの技術リソースを参照

各型について、複数の情報源を参照して一般的な命名規則を確認しています。
