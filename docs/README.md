# relly 内部実装解説

このドキュメントは、rellyの内部実装について日本語で解説したものです。
各パッケージの役割、データ構造、アルゴリズムについて説明します。

## 目次

1. [全体アーキテクチャ](#全体アーキテクチャ)
2. [パッケージ構成](#パッケージ構成)
3. [各パッケージの詳細](#各パッケージの詳細)
   - [disk - ディスク管理](#disk---ディスク管理)（基礎層）
   - [memcmpable - メモリ比較可能なエンコーディング](#memcmpable---メモリ比較可能なエンコーディング)（ユーティリティ）
   - [tuple - タプルエンコーディング](#tuple---タプルエンコーディング)（ユーティリティ）
   - [bsearch - バイナリサーチ](#bsearch---バイナリサーチ)（ユーティリティ）
   - [buffer - バッファプール](#buffer---バッファプール)（diskの上に構築）
   - [slotted - スロッテッドページ](#slotted---スロッテッドページ)（bufferの上に構築）
   - [btree - B+ツリー](#btree---bツリー)（slotted、tuple、bsearchを使用）
   - [table - テーブル実装](#table---テーブル実装)（btreeの上に構築）
   - [query - クエリ実行エンジン](#query---クエリ実行エンジン)（tableの上に構築）
4. [データフローの例](#データフローの例)
5. [メモリ管理](#メモリ管理)
6. [パフォーマンスの考慮事項](#パフォーマンスの考慮事項)
7. [まとめ](#まとめ)

## 全体アーキテクチャ

rellyは、リレーショナルデータベースの基本的な機能を実装した学習用のRDBMSです。
以下のような階層構造になっています：

```
┌─────────────────────────────────────┐
│ query - クエリ実行エンジン          │
│  (SeqScan, IndexScan, Filter等)    │
└─────────────────────────────────────┘
           ↓
┌─────────────────────────────────────┐
│ table - テーブル実装                 │
│  (SimpleTable, Table, UniqueIndex)  │
└─────────────────────────────────────┘
           ↓
┌─────────────────────────────────────┐
│ btree - B+ツリーインデックス         │
│  (BTree, Leaf, Branch)              │
└─────────────────────────────────────┘
           ↓
┌─────────────────────────────────────┐
│ slotted - スロッテッドページ         │
│  (可変長レコードの格納)              │
└─────────────────────────────────────┘
           ↓
┌─────────────────────────────────────┐
│ buffer - バッファプールマネージャー   │
│  (ページキャッシュ)                  │
└─────────────────────────────────────┘
           ↓
┌─────────────────────────────────────┐
│ disk - ディスクI/O                   │
│  (ページの読み書き)                  │
└─────────────────────────────────────┘
```

## パッケージ構成

### コアパッケージ

- **disk**: ディスクファイルへのページの読み書きを管理
- **buffer**: メモリ内のページキャッシュ（バッファプール）を管理
- **slotted**: 可変長レコードを格納するスロッテッドページ構造
- **btree**: B+ツリーインデックスの実装

### ユーティリティパッケージ

- **memcmpable**: バイト列をメモリ比較可能な形式にエンコード/デコード
- **bsearch**: バイナリサーチの実装
- **tuple**: タプル（レコード）のエンコード/デコード

### 高レベルパッケージ

- **table**: テーブルとインデックスの管理
- **query**: クエリ実行プランの実装

## 各パッケージの詳細

> **学習の進め方**: このセクションは、下位層から上位層へ順に読むことを推奨します。
> 各パッケージは下位層のパッケージに依存しているため、順番に理解することで全体像が把握しやすくなります。

### disk - ディスク管理

ディスクファイルへのページの読み書きを管理するパッケージです。すべてのパッケージの基礎となる層です。

#### 概要

データベースはディスク上にデータを永続化する必要があります。`disk`パッケージは、固定サイズのページ（4096バイト）をディスクファイルに読み書きする機能を提供します。

#### ページの構造

- ページサイズ: 4096バイト（4KB）
- ページは固定サイズで、ページIDに基づいてオフセット計算される
- オフセット = `PageId * 4096`

#### 主要な型

- **`PageId`**: ページの識別子（uint64）
  - `Valid()`: ページIDが有効かどうかを判定
  - `ToU64()`: uint64に変換
  - `ToBytes()`: 8バイトのバイト列に変換
  - `PageIdFromBytes()`: バイト列からPageIdを復元

- **`DiskManager`**: ディスクI/Oを管理する構造体
  - `heapFile`: ヒープファイルへの参照
  - `nextPageId`: 次に割り当てるページID

#### 主要な関数・メソッド

- **`NewDiskManager(heapFile *os.File) (*DiskManager, error)`**: 既存のファイルからディスクマネージャーを作成。ファイルサイズから次のページIDを計算
- **`OpenDiskManager(heapFilePath string) (*DiskManager, error)`**: ファイルパスからディスクマネージャーを開く（存在しない場合は作成）
- **`ReadPageData(pageId PageId, data []byte) error`**: 指定されたページIDのデータを読み込む。オフセット計算を行い、ファイルから4096バイトを読み込む
- **`WritePageData(pageId PageId, data []byte) error`**: 指定されたページIDにデータを書き込む。オフセット計算を行い、ファイルに4096バイトを書き込む
- **`AllocatePage() PageId`**: 新しいページIDを割り当てる。`nextPageId`をインクリメントして返す
- **`Sync() error`**: ファイルシステムのバッファをディスクに同期
- **`Close() error`**: ファイルを閉じる

#### 使用例

```go
// ディスクマネージャーの作成
dm, err := disk.OpenDiskManager("database.db")

// ページの読み込み
var page [disk.PageSize]byte
err := dm.ReadPageData(pageId, page[:])

// ページの書き込み
err := dm.WritePageData(pageId, page[:])

// 新しいページの割り当て
newPageId := dm.AllocatePage()
```

### memcmpable - メモリ比較可能なエンコーディング

バイト列をメモリ比較（memcmp）可能な形式にエンコード/デコードするパッケージです。
これにより、エンコードされたバイト列を直接比較することで、元のデータの順序を保つことができます。

#### 概要

データベースでは、キーの順序を保持しながら比較・検索を行う必要があります。`memcmpable`パッケージは、任意のバイト列を、バイト列として直接比較できる形式にエンコードします。これは`tuple`パッケージで使用され、タプルの順序を保持するために重要です。

#### エンコーディング方式

- エスケープ長: 9バイト
- 8バイトごとにエスケープバイトを挿入
- 最後のチャンクには実際の長さを記録

#### 主要な関数

- **`EncodedSize(len int) int`**: 指定された長さのバイト列をエンコードした場合のサイズを計算
- **`Encode(src []byte, dst *[]byte)`**: バイト列をメモリ比較可能な形式にエンコード。8バイトごとにエスケープバイト（9バイト目）を挿入し、最後のチャンクには実際の長さを記録
- **`Decode(src *[]byte, dst *[]byte)`**: エンコードされたバイト列をデコード。`src`は消費される（in-placeで変更される）

#### 使用例

```go
var encoded []byte
memcmpable.Encode([]byte("hello"), &encoded)

var decoded []byte
rest := encoded
memcmpable.Decode(&rest, &decoded)
```

### tuple - タプルエンコーディング

タプル（レコード）をバイト列にエンコード/デコードするパッケージです。

#### 概要

データベースのレコード（タプル）は複数のカラムから構成されます。`tuple`パッケージは、タプルをバイト列にエンコード/デコードする機能を提供します。各要素は`memcmpable`エンコーディングで連結されるため、エンコードされたタプルをバイト列として直接比較することで、元のタプルの順序を保つことができます。

#### エンコーディング方式

各要素を`memcmpable`エンコーディングで連結します。これにより、エンコードされたタプルをバイト列として直接比較することで、元のタプルの順序を保つことができます。

#### 主要な関数

- **`Encode(elems [][]byte, bytes *[]byte)`**: タプル（バイトスライスのスライス）をバイト列にエンコード
  - 各要素を`memcmpable.Encode()`でエンコードして連結
  - 結果は`bytes`に追加される

- **`Decode(bytes []byte, elems *[][]byte)`**: バイト列をタプルにデコード
  - `memcmpable.Decode()`を繰り返し呼び出して各要素をデコード
  - デコードされた要素を`elems`に追加

- **`Pretty(elems [][]byte) string`**: タプルを人間が読みやすい形式でフォーマット
  - 有効なUTF-8シーケンスの場合は文字列として表示
  - バイナリデータの場合は16進数で表示

#### 使用例

```go
// エンコード
tuple := [][]byte{[]byte("hello"), []byte("world")}
var encoded []byte
tuple.Encode(tuple, &encoded)

// デコード
var decoded [][]byte
tuple.Decode(encoded, &decoded)

// フォーマット
fmt.Println(tuple.Pretty(decoded))
```

### bsearch - バイナリサーチ

ソート済みコレクションに対するバイナリサーチを提供します。

#### 概要

B+ツリーのリーフノードやブランチノードでは、ソート済みのキー配列に対して効率的に検索を行う必要があります。`bsearch`パッケージは、バイナリサーチアルゴリズムを提供し、O(log n)の時間計算量で検索を行います。

#### 主要な関数・型

- **`BinarySearchBy(size int, f func(int) int) (int, error)`**: ソート済みコレクションに対してバイナリサーチを実行
  - `size`: コレクションのサイズ
  - `f`: 比較関数。要素がターゲットより小さい場合は負の値、等しい場合は0、大きい場合は正の値を返す
  - 戻り値: 見つかった場合はインデックスとnil、見つからなかった場合は挿入位置と`ErrNotFound`

- **`ErrNotFound`**: 要素が見つからなかった場合のエラー

#### 使用方法

```go
idx, err := bsearch.BinarySearchBy(size, func(i int) int {
    if collection[i] < target {
        return -1
    } else if collection[i] > target {
        return 1
    }
    return 0
})
if err == nil {
    // 見つかった
} else {
    // 見つからなかった（idxは挿入位置）
}
```

### buffer - バッファプール

メモリ内のページキャッシュ（バッファプール）を管理するパッケージです。

#### 概要

ディスクI/Oは非常に遅いため、頻繁にアクセスされるページをメモリにキャッシュすることで、パフォーマンスを大幅に向上させることができます。`buffer`パッケージは、`disk`パッケージの上に構築され、ページキャッシュと置換アルゴリズムを提供します。

#### 主要な型

- **`BufferId`**: バッファスロットの識別子

- **`Buffer`**: キャッシュされたページデータ
  - `PageId`: このバッファが表すページID
  - `Page`: 実際のページデータ（4096バイト）
  - `IsDirty`: ページが変更されたかどうか（フラッシュが必要かどうか）

- **`Frame`**: バッファをラップし、使用状況を追跡
  - `UsageCount`: バッファへのアクセス回数（Clock置換アルゴリズムで使用）
  - `Buffer`: 実際のバッファ

- **`BufferPool`**: 固定サイズのバッファプール
  - `buffers`: バッファフレームの配列
  - `nextVictimId`: 次に置換候補とするバッファID（Clockアルゴリズムの針）

- **`BufferPoolManager`**: ディスクI/Oとバッファプールを統合管理
  - `disk`: ディスクマネージャー
  - `pool`: バッファプール
  - `pageTable`: ページIDからバッファIDへのマッピング

#### 置換アルゴリズム

Clock置換アルゴリズム（簡易版）を実装：
- 各バッファに使用カウント（`UsageCount`）を保持
- 置換候補を順番にチェック（時計の針のように）
- 使用カウントが0のバッファを置換対象とする

#### ページテーブル

`BufferPoolManager`はページテーブル（`pageTable`）を保持し、ページIDからバッファスロットへのマッピングを管理します。

#### 主要な関数・メソッド

- **`NewBuffer() *Buffer`**: 新しいバッファを作成

- **`NewBufferPool(poolSize int) *BufferPool`**: 指定されたサイズのバッファプールを作成

- **`Size() int`**: バッファプールのサイズを返す

- **`Evict() (BufferId, bool)`**: Clockアルゴリズムで置換対象のバッファを選択
  - 使用カウントが0のバッファを見つけるまで時計の針を進める
  - 見つかった場合はバッファIDと`true`を返す
  - すべてのバッファが使用中の場合は`false`を返す

- **`NewBufferPoolManager(dm *disk.DiskManager, pool *BufferPool) *BufferPoolManager`**: バッファプールマネージャーを作成

- **`FetchPage(pageId disk.PageId) (*Buffer, error)`**: ページを取得
  - ページテーブルをチェックし、キャッシュにあればそれを返す
  - キャッシュにない場合は、`Evict()`で置換対象を選択
  - 置換対象のバッファが`IsDirty`の場合はディスクに書き込む
  - ディスクからページを読み込む（存在しない場合は0で初期化）
  - ページテーブルを更新

- **`CreatePage() (*Buffer, error)`**: 新しいページを作成
  - `Evict()`で置換対象を選択
  - 新しいページIDを割り当て
  - バッファを初期化し、`IsDirty`を`true`に設定
  - ページテーブルを更新

- **`Flush() error`**: すべての`IsDirty`なバッファをディスクに書き込む
  - ページテーブル内のすべてのバッファをチェック
  - `IsDirty`が`true`のバッファをディスクに書き込み
  - `disk.Sync()`を呼び出してファイルシステムのバッファを同期

#### 使用例

```go
// バッファプールマネージャーの作成
pool := buffer.NewBufferPool(10)  // 10個のバッファ
bufmgr := buffer.NewBufferPoolManager(dm, pool)

// ページの取得（キャッシュから、またはディスクから読み込み）
buffer, err := bufmgr.FetchPage(pageId)

// 新しいページの作成
buffer, err := bufmgr.CreatePage()

// 変更をディスクにフラッシュ
err := bufmgr.Flush()
```

### slotted - スロッテッドページ

可変長レコードを効率的に格納するためのスロッテッドページ構造を提供します。

#### 概要

データベースのレコードは可変長です。固定サイズのページ内に可変長レコードを効率的に格納するために、スロッテッドページ構造を使用します。`slotted`パッケージは、`buffer`パッケージで管理されるページ内に可変長レコードを格納する機能を提供します。この構造は、B+ツリーのリーフノードやブランチノードで使用されます。

#### 主要な型

- **`Header`**: スロッテッドページのヘッダー
  - `NumSlots`: スロット（レコード）数
  - `FreeSpaceOffset`: フリースペースの開始位置

- **`Pointer`**: レコードへのポインタ
  - `Offset`: レコードの開始位置（bodyの先頭からのオフセット）
  - `Len`: レコードの長さ
  - `Range(bodyLen int) (start, end int)`: レコードの範囲を取得

- **`Slotted`**: スロッテッドページ構造
  - `header`: ヘッダーへのポインタ
  - `body`: 実際のバイト配列
  - `pointers`: ポインタのGo構造体配列（`body`と同期）

#### ページ構造

スロッテッドページは以下の3つの領域に分かれています：

```
┌─────────────────────────────────────────┐
│ Header (8バイト)                        │
│  - NumSlots: スロット数                 │
│  - FreeSpaceOffset: フリースペース位置  │
├─────────────────────────────────────────┤
│ Body                                     │
│  [0:pointersSize]                       │
│    → ポインタ配列（各4バイト）          │
│  [pointersSize:FreeSpaceOffset]         │
│    → 未使用領域                         │
│  [FreeSpaceOffset:end]                  │
│    → データレコード（後ろから前へ）     │
└─────────────────────────────────────────┘
```

#### bodyとpointersの関係

- **`body`**: 実際のバイト配列。先頭部分にポインタデータがバイナリ形式で格納
- **`pointers`**: Goの構造体配列。`body`から読み取ったポインタデータの作業用コピー

データフロー：
1. **読み込み時**: `body[0:pointersSize]`からバイナリデータを読み取り、`pointers`スライスに変換
2. **操作時**: `pointers`スライスを操作（Insert, Remove等）
3. **書き込み時**: `updatePointersInBody()`で`pointers`の変更を`body`に反映

#### 主要な関数・メソッド

- **`NewSlotted(bytes []byte) *Slotted`**: バイト配列からスロッテッドページを作成。ヘッダーとポインタ配列を読み込む

- **`Capacity() int`**: ページの容量（bodyのサイズ）を返す

- **`NumSlots() int`**: 現在のスロット数を返す

- **`FreeSpace() int`**: 利用可能なフリースペースのサイズを返す

- **`PointersSize() int`**: ポインタ配列のサイズを返す

- **`Data(index int) []byte`**: 指定されたインデックスのレコードデータを返す

- **`Initialize()`**: ページを初期化。スロット数を0にし、フリースペースを最大にする

- **`Insert(index int, dataLen int) bool`**: 指定されたインデックスにレコードを挿入
  - フリースペースが不足している場合は`false`を返す
  - レコードはページの後ろから前に向かって書き込まれる
  - ポインタ配列を更新する

- **`Remove(index int)`**: 指定されたインデックスのレコードを削除
  - レコードのサイズを0にして、後続のレコードを前にシフト

- **`Resize(index int, newLen int) bool`**: 指定されたインデックスのレコードのサイズを変更
  - サイズが増加する場合は、後続のレコードを前にシフトしてスペースを確保
  - サイズが減少する場合は、後続のレコードを前にシフトしてスペースを解放

- **`updatePointersInBody()`**: `pointers`スライスの変更を`body`のバイナリ形式に反映（内部メソッド）

#### レコードの格納方法

- レコードはページの**後ろから前へ**向かって格納される
- 新しいレコードは`FreeSpaceOffset`の位置から前に向かって書き込まれる
- ポインタ配列はページの**前から後ろへ**配置される

#### 具体例

2つのレコード（"hello"と"world"）を格納した場合：

```
body[0:8]     → ポインタ配列（2つのポインタ = 8バイト）
  [0:4]       → Pointer{Offset: 95, Len: 5}  ("hello"へのポインタ)
  [4:8]       → Pointer{Offset: 90, Len: 5}  ("world"へのポインタ)
body[8:90]    → 未使用領域（82バイト）
body[90:95]   → "world"のデータ（5バイト）
body[95:100]  → "hello"のデータ（5バイト）
```

### buffer - バッファプール

メモリ内のページキャッシュ（バッファプール）を管理するパッケージです。

#### 主要な型

- **`BufferId`**: バッファスロットの識別子

- **`Buffer`**: キャッシュされたページデータ
  - `PageId`: このバッファが表すページID
  - `Page`: 実際のページデータ（4096バイト）
  - `IsDirty`: ページが変更されたかどうか（フラッシュが必要かどうか）

- **`Frame`**: バッファをラップし、使用状況を追跡
  - `UsageCount`: バッファへのアクセス回数（Clock置換アルゴリズムで使用）
  - `Buffer`: 実際のバッファ

- **`BufferPool`**: 固定サイズのバッファプール
  - `buffers`: バッファフレームの配列
  - `nextVictimId`: 次に置換候補とするバッファID（Clockアルゴリズムの針）

- **`BufferPoolManager`**: ディスクI/Oとバッファプールを統合管理
  - `disk`: ディスクマネージャー
  - `pool`: バッファプール
  - `pageTable`: ページIDからバッファIDへのマッピング

#### 置換アルゴリズム

Clock置換アルゴリズム（簡易版）を実装：
- 各バッファに使用カウント（`UsageCount`）を保持
- 置換候補を順番にチェック（時計の針のように）
- 使用カウントが0のバッファを置換対象とする

#### ページテーブル

`BufferPoolManager`はページテーブル（`pageTable`）を保持し、ページIDからバッファスロットへのマッピングを管理します。

#### 主要な関数・メソッド

- **`NewBuffer() *Buffer`**: 新しいバッファを作成

- **`NewBufferPool(poolSize int) *BufferPool`**: 指定されたサイズのバッファプールを作成

- **`Size() int`**: バッファプールのサイズを返す

- **`Evict() (BufferId, bool)`**: Clockアルゴリズムで置換対象のバッファを選択
  - 使用カウントが0のバッファを見つけるまで時計の針を進める
  - 見つかった場合はバッファIDと`true`を返す
  - すべてのバッファが使用中の場合は`false`を返す

- **`NewBufferPoolManager(dm *disk.DiskManager, pool *BufferPool) *BufferPoolManager`**: バッファプールマネージャーを作成

- **`FetchPage(pageId disk.PageId) (*Buffer, error)`**: ページを取得
  - ページテーブルをチェックし、キャッシュにあればそれを返す
  - キャッシュにない場合は、`Evict()`で置換対象を選択
  - 置換対象のバッファが`IsDirty`の場合はディスクに書き込む
  - ディスクからページを読み込む（存在しない場合は0で初期化）
  - ページテーブルを更新

- **`CreatePage() (*Buffer, error)`**: 新しいページを作成
  - `Evict()`で置換対象を選択
  - 新しいページIDを割り当て
  - バッファを初期化し、`IsDirty`を`true`に設定
  - ページテーブルを更新

- **`Flush() error`**: すべての`IsDirty`なバッファをディスクに書き込む
  - ページテーブル内のすべてのバッファをチェック
  - `IsDirty`が`true`のバッファをディスクに書き込み
  - `disk.Sync()`を呼び出してファイルシステムのバッファを同期

#### 使用例

```go
// バッファプールマネージャーの作成
pool := buffer.NewBufferPool(10)  // 10個のバッファ
bufmgr := buffer.NewBufferPoolManager(dm, pool)

// ページの取得（キャッシュから、またはディスクから読み込み）
buffer, err := bufmgr.FetchPage(pageId)

// 新しいページの作成
buffer, err := bufmgr.CreatePage()

// 変更をディスクにフラッシュ
err := bufmgr.Flush()
```

### btree - B+ツリー

B+ツリーインデックスの実装です。キー・バリューペアを効率的に格納・検索できます。

#### 概要

B+ツリーは、データベースのインデックスとして広く使用されるデータ構造です。`btree`パッケージは、`slotted`パッケージを使用してページ内にデータを格納し、`tuple`パッケージでキー・バリューをエンコードし、`bsearch`パッケージで効率的に検索を行います。B+ツリーは、バランスされたツリー構造により、検索・挿入がO(log n)の時間計算量で実行できます。

#### B+ツリーの構造

B+ツリーは以下のノードタイプで構成されます：

1. **メタページ（Meta）**: ルートページIDを保持
2. **ブランチノード（Branch）**: 内部ノード。キーと子ページIDを保持
3. **リーフノード（Leaf）**: 葉ノード。キー・バリューペアを保持し、リンクリストで接続

#### ページレイアウト

```
┌─────────────────────────────────────┐
│ Node Header (8バイト)               │
│  - NodeType: "LEAF    " or "BRANCH  "│
├─────────────────────────────────────┤
│ Node Body                           │
│  (Leaf or Branch data)              │
└─────────────────────────────────────┘
```

#### リーフノードの構造

```
┌─────────────────────────────────────┐
│ Leaf Header (16バイト)              │
│  - PrevPageId: 前のリーフページID   │
│  - NextPageId: 次のリーフページID   │
├─────────────────────────────────────┤
│ Slotted Body                        │
│  (キー・バリューペアの配列)         │
└─────────────────────────────────────┘
```

#### ブランチノードの構造

```
┌─────────────────────────────────────┐
│ Branch Header (8バイト)             │
│  - RightChild: 右端の子ページID     │
├─────────────────────────────────────┤
│ Slotted Body                        │
│  (キー・子ページIDのペアの配列)     │
└─────────────────────────────────────┘
```

#### 検索アルゴリズム

1. ルートページから開始
2. ブランチノードの場合：
   - キーを比較して適切な子ページを選択
   - 子ページに再帰的に検索
3. リーフノードの場合：
   - バイナリサーチでキーを検索
   - 見つかったらそのバリューを返す

#### 挿入アルゴリズム

1. リーフノードまで再帰的に降りる
2. リーフノードに挿入を試みる
3. ページが満杯の場合：
   - ページを分割
   - 中間キーを親ノードに伝播
4. 親ノードも満杯の場合は再帰的に分割

#### 主要な型・関数・メソッド

##### SearchMode

- **`SearchMode`**: 検索モードを指定する構造体
  - `IsStart`: `true`の場合は先頭から開始、`false`の場合はキーで検索
  - `Key`: 検索するキー（`IsStart`が`false`の場合のみ使用）

- **`NewSearchModeStart() SearchMode`**: 先頭から開始する検索モードを作成

- **`NewSearchModeKey(key []byte) SearchMode`**: キーで検索する検索モードを作成

##### BTree

- **`BTree`**: B+ツリーを表す構造体
  - `MetaPageId`: メタページのページID（ルートページIDを保持）

- **`CreateBTree(bufmgr *buffer.BufferPoolManager) (*BTree, error)`**: 新しいB+ツリーを作成
  - メタページとルートリーフノードを作成
  - ルートページIDをメタページに設定

- **`NewBTree(metaPageId disk.PageId) *BTree`**: 既存のB+ツリーを開く

- **`FetchRootPage(bufmgr *buffer.BufferPoolManager) (*buffer.Buffer, error)`**: ルートページを取得

- **`Search(bufmgr *buffer.BufferPoolManager, searchMode SearchMode) (*Iter, error)`**: B+ツリーを検索
  - ルートページから開始して、リーフノードまで再帰的に降りる
  - リーフノードでバイナリサーチを実行
  - イテレータを返す

- **`Insert(bufmgr *buffer.BufferPoolManager, key []byte, value []byte) error`**: キー・バリューペアを挿入
  - ルートページから開始して、リーフノードまで再帰的に降りる
  - リーフノードに挿入を試みる
  - ページが満杯の場合は分割し、オーバーフローを親ノードに伝播
  - ルートが分割された場合は新しいルートを作成

- **`insertInternal()`**: 内部的な挿入処理（再帰的）
  - リーフノードの場合は直接挿入
  - ブランチノードの場合は子ノードに再帰的に挿入
  - オーバーフローが発生した場合は分割

##### Iter

- **`Iter`**: B+ツリーのイテレータ
  - `buffer`: 現在のリーフページのバッファ
  - `slotId`: 現在のスロットインデックス

- **`Get() ([]byte, []byte, bool)`**: 現在の位置のキー・バリューペアを取得

- **`Advance(bufmgr *buffer.BufferPoolManager) error`**: 次の位置に進む
  - 現在のリーフノード内で次のスロットに進む
  - リーフノードの終端に達した場合は、次のリーフノードに移動

- **`Next(bufmgr *buffer.BufferPoolManager) ([]byte, []byte, bool, error)`**: 次のキー・バリューペアを取得して進む
  - `Get()`で現在のペアを取得し、`Advance()`で次の位置に進む

##### Node

- **`Node`**: B+ツリーノード（リーフまたはブランチ）のラッパー
  - `header`: ノードヘッダー（ノードタイプを保持）
  - `body`: ノードボディ（リーフまたはブランチのデータ）

- **`NewNode(page []byte) *Node`**: ページからノードを作成

- **`InitializeAsLeaf()`**: ノードをリーフノードとして初期化

- **`InitializeAsBranch()`**: ノードをブランチノードとして初期化

- **`IsLeaf() bool`**: リーフノードかどうかを判定

- **`IsBranch() bool`**: ブランチノードかどうかを判定

- **`Body() []byte`**: ノードボディを取得

- **`AsLeaf() *leaf.Leaf`**: リーフノードとして取得

- **`AsBranch() *branch.Branch`**: ブランチノードとして取得

##### Meta

- **`Meta`**: B+ツリーのメタページ
  - `header`: メタヘッダー（ルートページIDを保持）

- **`NewMeta(page []byte) *Meta`**: ページからメタページを作成

- **`RootPageId() disk.PageId`**: ルートページIDを取得

- **`SetRootPageId(pageId disk.PageId)`**: ルートページIDを設定

##### Leaf

- **`Leaf`**: B+ツリーのリーフノード
  - `header`: リーフヘッダー（前後のリーフページIDを保持）
  - `body`: スロッテッドページ（キー・バリューペアを格納）

- **`NewLeaf(bodyBytes []byte) *Leaf`**: ボディからリーフノードを作成

- **`PrevPageId() disk.PageId`**: 前のリーフページIDを取得

- **`NextPageId() disk.PageId`**: 次のリーフページIDを取得

- **`NumPairs() int`**: キー・バリューペアの数を返す

- **`SearchSlotId(key []byte) (int, error)`**: キーに対応するスロットIDを検索（バイナリサーチ）

- **`PairAt(slotId int) *Pair`**: 指定されたスロットのペアを取得

- **`MaxPairSize() int`**: ペアの最大サイズを計算

- **`Initialize()`**: リーフノードを初期化（前後のページIDを無効化）

- **`SetPrevPageId(prevPageId disk.PageId)`**: 前のリーフページIDを設定

- **`SetNextPageId(nextPageId disk.PageId)`**: 次のリーフページIDを設定

- **`Insert(slotId int, key []byte, value []byte) bool`**: キー・バリューペアを挿入
  - 成功した場合は`true`、スペースが不足している場合は`false`

- **`IsHalfFull() bool`**: ページが半分以上埋まっているかどうかを判定（分割判定に使用）

- **`SplitInsert(newLeaf *Leaf, newKey []byte, newValue []byte) []byte`**: ページを分割して挿入
  - 新しいリーフノードに要素を転送
  - 両方のノードが半分以上埋まるように調整
  - 新しいリーフノードの最小キーを返す

- **`Transfer(dest *Leaf)`**: 最初のペアを別のリーフノードに転送

##### Branch

- **`Branch`**: B+ツリーのブランチノード（内部ノード）
  - `header`: ブランチヘッダー（右端の子ページIDを保持）
  - `body`: スロッテッドページ（キー・子ページIDのペアを格納）

- **`NewBranch(bodyBytes []byte) *Branch`**: ボディからブランチノードを作成

- **`NumPairs() int`**: キー・子ページIDのペアの数を返す

- **`SearchSlotId(key []byte) (int, error)`**: キーに対応するスロットIDを検索（バイナリサーチ）

- **`SearchChild(key []byte) disk.PageId`**: キーに対応する子ページIDを検索

- **`SearchChildIdx(key []byte) int`**: キーに対応する子インデックスを検索

- **`ChildAt(childIdx int) disk.PageId`**: 指定されたインデックスの子ページIDを取得
  - `childIdx`が`NumPairs()`の場合は右端の子ページIDを返す

- **`PairAt(slotId int) *Pair`**: 指定されたスロットのペアを取得

- **`MaxPairSize() int`**: ペアの最大サイズを計算

- **`Initialize(key []byte, leftChild disk.PageId, rightChild disk.PageId)`**: ブランチノードを初期化
  - 最初のキーと左の子ページIDを設定
  - 右端の子ページIDを設定

- **`FillRightChild() []byte`**: 最後のペアの子ページIDを右端の子として設定し、キーを返す

- **`Insert(slotId int, key []byte, pageId disk.PageId) bool`**: キー・子ページIDのペアを挿入

- **`IsHalfFull() bool`**: ページが半分以上埋まっているかどうかを判定

- **`SplitInsert(newBranch *Branch, newKey []byte, newPageId disk.PageId) []byte`**: ページを分割して挿入
  - 新しいブランチノードに要素を転送
  - 両方のノードが半分以上埋まるように調整
  - 新しいブランチノードの最小キーを返す

- **`Transfer(dest *Branch)`**: 最初のペアを別のブランチノードに転送

#### 使用例

```go
// B+ツリーの作成
btree, err := btree.CreateBTree(bufmgr)

// キー・バリューの挿入
err := btree.Insert(bufmgr, key, value)

// 検索
iter, err := btree.Search(bufmgr, btree.NewSearchModeKey(key))
for {
    key, value, ok, err := iter.Next(bufmgr)
    if !ok {
        break
    }
    // キー・バリューを処理
}
```

### table - テーブル実装

テーブルとインデックスの管理を提供します。

#### 概要

`table`パッケージは、`btree`パッケージの上に構築され、テーブルとインデックスの高レベルな操作を提供します。プライマリキーとユニークセカンダリインデックスをサポートし、レコードの挿入と管理を行います。レコードは`tuple`パッケージでエンコードされ、B+ツリーに格納されます。

#### 主要な型

##### SimpleTable

- **`SimpleTable`**: シンプルなテーブル実装。プライマリキーのみをサポート
  - `MetaPageId`: B+ツリーのメタページID
  - `NumKeyElems`: プライマリキーを構成する要素数（レコードの最初のN要素）

- **`Create(bufmgr *buffer.BufferPoolManager) error`**: テーブルを作成
  - 新しいB+ツリーを作成し、メタページIDを設定

- **`Insert(bufmgr *buffer.BufferPoolManager, record [][]byte) error`**: レコードを挿入
  - 最初の`NumKeyElems`要素をプライマリキーとしてエンコード
  - 残りの要素をバリューとしてエンコード
  - B+ツリーに挿入

##### Table

- **`Table`**: ユニークインデックスをサポートするテーブル実装
  - `MetaPageId`: プライマリB+ツリーのメタページID
  - `NumKeyElems`: プライマリキーを構成する要素数
  - `UniqueIndices`: ユニークセカンダリインデックスのリスト

- **`Create(bufmgr *buffer.BufferPoolManager) error`**: テーブルを作成
  - プライマリB+ツリーを作成
  - 各ユニークインデックスのB+ツリーを作成

- **`Insert(bufmgr *buffer.BufferPoolManager, record [][]byte) error`**: レコードを挿入
  - プライマリB+ツリーに挿入
  - 各ユニークインデックスにセカンダリキーとプライマリキーのマッピングを挿入

##### UniqueIndex

- **`UniqueIndex`**: ユニークなセカンダリインデックス
  - `MetaPageId`: B+ツリーのメタページID
  - `Skey`: セカンダリキーを構成するレコード要素のインデックス配列

- **`Create(bufmgr *buffer.BufferPoolManager) error`**: インデックスを作成
  - 新しいB+ツリーを作成し、メタページIDを設定

- **`Insert(bufmgr *buffer.BufferPoolManager, pkey []byte, record [][]byte) error`**: インデックスエントリを挿入
  - `Skey`で指定された要素からセカンダリキーを構築
  - セカンダリキーをキー、プライマリキーをバリューとしてB+ツリーに挿入

#### 使用例

```go
// SimpleTableの使用
table := &table.SimpleTable{
    MetaPageId:  disk.InvalidPageId,
    NumKeyElems: 2,  // 最初の2要素がプライマリキー
}

// テーブルの作成
err := table.Create(bufmgr)

// レコードの挿入
record := [][]byte{[]byte("key1"), []byte("key2"), []byte("value1")}
err := table.Insert(bufmgr, record)

// Tableの使用（ユニークインデックス付き）
table := &table.Table{
    MetaPageId:   disk.InvalidPageId,
    NumKeyElems: 2,
    UniqueIndices: []*table.UniqueIndex{
        {
            MetaPageId: disk.InvalidPageId,
            Skey:       []int{1, 2},  // 2番目と3番目の要素でインデックス
        },
    },
}
```

### query - クエリ実行エンジン

クエリ実行プランを実装するパッケージです。

#### 概要

`query`パッケージは、`table`パッケージの上に構築され、クエリ実行プランを実装します。シーケンシャルスキャン、インデックススキャン、フィルタ、インデックスオンリースキャンなどの操作を提供します。クエリはイテレータパターンで実装され、タプルを1つずつ返すことで、メモリ効率的な処理を実現します。

#### 主要な型

##### 基本型

- **`Tuple`**: データベースレコード（バイトスライスのスライス）
- **`TupleSlice`**: タプルスライスのエイリアス（関数パラメータ用）

- **`TupleSearchMode`**: テーブルまたはインデックスでの検索モード
  - `IsStart`: `true`の場合は先頭から開始、`false`の場合はキーで検索
  - `Key`: 検索するキー（タプル形式）

- **`NewTupleSearchModeStart() TupleSearchMode`**: 先頭から開始する検索モードを作成

- **`NewTupleSearchModeKey(key [][]byte) TupleSearchMode`**: キーで検索する検索モードを作成

- **`Encode() btree.SearchMode`**: `TupleSearchMode`を`btree.SearchMode`に変換

##### インターフェース

- **`Executor`**: クエリを実行してタプルを1つずつ返すインターフェース
  - `Next(bufmgr *buffer.BufferPoolManager) (Tuple, bool, error)`: 次のタプルを返す。タプルがない場合は`false`を返す

- **`PlanNode`**: クエリプランノードのインターフェース
  - `Start(bufmgr *buffer.BufferPoolManager) (Executor, error)`: 実行を開始し、`Executor`を返す

##### SeqScan（シーケンシャルスキャン）

- **`SeqScan`**: テーブル全体を順番にスキャンするプラン
  - `TableMetaPageId`: テーブルのB+ツリーメタページID
  - `SearchMode`: スキャンの開始点
  - `WhileCond`: スキャンを続ける条件（関数）

- **`Start(bufmgr *buffer.BufferPoolManager) (Executor, error)`**: スキャンを開始
  - B+ツリーで検索を開始し、イテレータを取得
  - `ExecSeqScan`を返す

- **`ExecSeqScan`**: シーケンシャルスキャンの実行器
  - `tableIter`: テーブルのB+ツリーイテレータ
  - `whileCond`: スキャンを続ける条件

- **`Next(bufmgr *buffer.BufferPoolManager) (Tuple, bool, error)`**: 次のタプルを返す
  - イテレータからプライマリキーとバリューを取得
  - `whileCond`でフィルタリング
  - タプルをデコードして返す

##### Filter（フィルタ）

- **`Filter`**: 条件に合致するタプルのみを返すプラン
  - `InnerPlan`: 内部プラン（フィルタを適用するプラン）
  - `Cond`: フィルタ条件（関数）

- **`Start(bufmgr *buffer.BufferPoolManager) (Executor, error)`**: フィルタを開始
  - 内部プランを開始し、`ExecFilter`を返す

- **`ExecFilter`**: フィルタの実行器
  - `innerIter`: 内部プランの実行器
  - `cond`: フィルタ条件

- **`Next(bufmgr *buffer.BufferPoolManager) (Tuple, bool, error)`**: 次のタプルを返す
  - 内部実行器からタプルを取得
  - 条件に合致するタプルのみを返す
  - 条件に合致しないタプルはスキップ

##### IndexScan（インデックススキャン）

- **`IndexScan`**: セカンダリインデックスを使用してテーブルをスキャンするプラン
  - `TableMetaPageId`: テーブルのB+ツリーメタページID
  - `IndexMetaPageId`: インデックスのB+ツリーメタページID
  - `SearchMode`: スキャンの開始点
  - `WhileCond`: スキャンを続ける条件

- **`Start(bufmgr *buffer.BufferPoolManager) (Executor, error)`**: インデックススキャンを開始
  - インデックスのB+ツリーで検索を開始
  - `ExecIndexScan`を返す

- **`ExecIndexScan`**: インデックススキャンの実行器
  - `tableBtree`: テーブルのB+ツリー
  - `indexIter`: インデックスのB+ツリーイテレータ
  - `whileCond`: スキャンを続ける条件

- **`Next(bufmgr *buffer.BufferPoolManager) (Tuple, bool, error)`**: 次のタプルを返す
  - インデックスイテレータからセカンダリキーとプライマリキーを取得
  - `whileCond`でフィルタリング
  - プライマリキーでテーブルを検索してレコードを取得
  - タプルをデコードして返す

##### IndexOnlyScan（インデックスオンリースキャン）

- **`IndexOnlyScan`**: インデックスから直接結果を返すプラン（テーブルへのアクセス不要）
  - `IndexMetaPageId`: インデックスのB+ツリーメタページID
  - `SearchMode`: スキャンの開始点
  - `WhileCond`: スキャンを続ける条件

- **`Start(bufmgr *buffer.BufferPoolManager) (Executor, error)`**: インデックスオンリースキャンを開始
  - インデックスのB+ツリーで検索を開始
  - `ExecIndexOnlyScan`を返す

- **`ExecIndexOnlyScan`**: インデックスオンリースキャンの実行器
  - `indexIter`: インデックスのB+ツリーイテレータ
  - `whileCond`: スキャンを続ける条件

- **`Next(bufmgr *buffer.BufferPoolManager) (Tuple, bool, error)`**: 次のタプルを返す
  - インデックスイテレータからセカンダリキーとプライマリキーを取得
  - `whileCond`でフィルタリング
  - セカンダリキーとプライマリキーを結合してタプルとして返す

#### 使用例

```go
// SeqScan（シーケンシャルスキャン）
plan := &query.SeqScan{
    TableMetaPageId: table.MetaPageId,
    SearchMode:      query.NewTupleSearchModeStart(),
    WhileCond: func(tuple query.TupleSlice) bool {
        // スキャンを続ける条件
        return true
    },
}

executor, err := plan.Start(bufmgr)
for {
    tuple, ok, err := executor.Next(bufmgr)
    if !ok {
        break
    }
    // タプルを処理
}

// IndexScan（インデックススキャン）
plan := &query.IndexScan{
    TableMetaPageId: table.MetaPageId,
    IndexMetaPageId: index.MetaPageId,
    SearchMode:      query.NewTupleSearchModeKey([][]byte{[]byte("search_key")}),
    WhileCond: func(tuple query.TupleSlice) bool {
        return true
    },
}

// Filter（フィルタ）
filter := &query.Filter{
    InnerPlan: seqScanPlan,
    Cond: func(tuple query.TupleSlice) bool {
        // フィルタ条件
        return len(tuple[0]) > 5
    },
}

// IndexOnlyScan（インデックスオンリースキャン）
plan := &query.IndexOnlyScan{
    IndexMetaPageId: index.MetaPageId,
    SearchMode:      query.NewTupleSearchModeStart(),
    WhileCond: func(tuple query.TupleSlice) bool {
        return true
    },
}
```

## データフローの例

### レコードの挿入

```
1. table.Insert() が呼ばれる
2. プライマリキーとバリューをエンコード
3. btree.Insert() でB+ツリーに挿入
4. リーフノードを見つける
5. slotted.Insert() でスロッテッドページに挿入
6. ページが満杯なら分割
7. 変更をバッファプールに記録
8. Flush()でディスクに書き込み
```

### レコードの検索

```
1. query.SeqScan.Start() が呼ばれる
2. btree.Search() でB+ツリーを検索
3. リーフノードを見つける
4. slotted.Data() でレコードを取得
5. tuple.Decode() でデコード
6. 条件に合致するタプルを返す
```

## メモリ管理

### バッファプールの役割

- 頻繁にアクセスされるページをメモリにキャッシュ
- ディスクI/Oを削減
- Clock置換アルゴリズムで効率的にページを管理

### ページのライフサイクル

1. **作成**: `CreatePage()`で新しいページを割り当て
2. **読み込み**: `FetchPage()`でディスクから読み込み（必要に応じて）
3. **変更**: バッファ内のページを変更（`IsDirty = true`）
4. **フラッシュ**: `Flush()`で変更をディスクに書き込み

## パフォーマンスの考慮事項

### B+ツリーの利点

- バランスされたツリー構造により、検索・挿入がO(log n)
- リーフノードがリンクリストで接続されているため、範囲検索が効率的
- ページ単位でディスクI/Oを行うため、ディスクアクセスが最適化される

### スロッテッドページの利点

- 可変長レコードを効率的に格納
- レコードの削除・サイズ変更時のデータ移動を最小化
- ポインタ配列により、レコードの順序変更が容易

## まとめ

rellyは、リレーショナルデータベースの基本的な機能を実装した学習用のRDBMSです。
各パッケージは明確に分離されており、それぞれが特定の責任を持っています。
この実装を通じて、RDBMSの内部構造と動作原理を理解することができます。
