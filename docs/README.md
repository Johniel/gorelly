# gorelly 内部実装解説

このドキュメントは、gorellyの内部実装について日本語で解説したものです。
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
   - [catalog - カタログテーブル](#catalog---カタログテーブル)（tableの上に構築、スキーマ情報の永続化）
   - [query - クエリ実行エンジン](#query---クエリ実行エンジン)（tableの上に構築）
   - [transaction - トランザクション管理](#transaction---トランザクション管理)（新規追加、Rust版にはない機能）
4. [データフローの例](#データフローの例)
5. [メモリ管理](#メモリ管理)
6. [パフォーマンスの考慮事項](#パフォーマンスの考慮事項)
7. [トランザクション機能](#トランザクション機能)
8. [まとめ](#まとめ)

## 全体アーキテクチャ

gorellyは、リレーショナルデータベースの基本的な機能を実装した学習用のRDBMSです。
以下のような階層構造になっています：

```
┌─────────────────────────────────────┐
│ query - クエリ実行エンジン          │
│  (SeqScan, IndexScan, Filter等)    │
└─────────────────────────────────────┘
           ↓
┌─────────────────────────────────────┐
│ catalog - カタログテーブル           │
│  (スキーマ情報の永続化)              │
└─────────────────────────────────────┘
           ↓
┌─────────────────────────────────────┐
│ table - テーブル実装                 │
│  (SimpleTable, Table, UniqueIndex)  │
└─────────────────────────────────────┘
           ↓
┌─────────────────────────────────────┐
│ btree - B+ツリーインデックス         │
│  (BTree, Leaf, InternalNode)        │
└─────────────────────────────────────┘
           ↓
┌─────────────────────────────────────┐
│ slotted - スロッテッドページ         │
│  (可変長タプルの格納)              │
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
- **slotted**: 可変長タプルを格納するスロッテッドページ構造
- **btree**: B+ツリーインデックスの実装

### ユーティリティパッケージ

- **memcmpable**: バイト列をメモリ比較可能な形式にエンコード/デコード
- **bsearch**: バイナリサーチの実装
- **tuple**: タプルのエンコード/デコード

### 高レベルパッケージ

- **table**: テーブルとインデックスの管理
- **catalog**: スキーマ情報の永続化（カタログテーブル）
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

データベースのタプルは複数のカラムから構成されます。`tuple`パッケージは、タプルをバイト列にエンコード/デコードする機能を提供します。各要素は`memcmpable`エンコーディングで連結されるため、エンコードされたタプルをバイト列として直接比較することで、元のタプルの順序を保つことができます。

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

- **`FetchBuffer(pageId disk.PageId) (*Buffer, error)`**: バッファを取得
  - ページテーブルをチェックし、キャッシュにあればそれを返す
  - キャッシュにない場合は、`Evict()`で置換対象を選択
  - 置換対象のバッファが`IsDirty`の場合はディスクに書き込む
  - ディスクからページを読み込む（存在しない場合は0で初期化）
  - ページテーブルを更新

- **`CreateBuffer() (*Buffer, error)`**: 新しいバッファを作成
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
buffer, err := bufmgr.FetchBuffer(pageId)

// 新しいページの作成
buffer, err := bufmgr.CreateBuffer()

// 変更をディスクにフラッシュ
err := bufmgr.Flush()
```

### slotted - スロッテッドページ

可変長レコードを効率的に格納するためのスロッテッドページ構造を提供します。

#### 概要

データベースのタプルは可変長です。固定サイズのページ内に可変長タプルを効率的に格納するために、スロッテッドページ構造を使用します。`slotted`パッケージは、`buffer`パッケージで管理されるページ内に可変長タプルを格納する機能を提供します。この構造は、B+ツリーのリーフノードや内部ノードで使用されます。

#### 主要な型

- **`Header`**: スロッテッドページのヘッダー
  - `NumSlots`: スロット（タプル）数
  - `FreeSpaceOffset`: フリースペースの開始位置

- **`Pointer`**: タプルへのポインタ
  - `Offset`: タプルの開始位置（bodyの先頭からのオフセット）
  - `Len`: タプルの長さ
  - `Range(bodyLen int) (start, end int)`: タプルの範囲を取得

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
│    → データタプル（後ろから前へ）     │
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

- **`Data(index int) []byte`**: 指定されたインデックスのタプルデータを返す

- **`Initialize()`**: ページを初期化。スロット数を0にし、フリースペースを最大にする

- **`Insert(index int, dataLen int) bool`**: 指定されたインデックスにタプルを挿入
  - フリースペースが不足している場合は`false`を返す
  - タプルはページの後ろから前に向かって書き込まれる
  - ポインタ配列を更新する

- **`Remove(index int)`**: 指定されたインデックスのタプルを削除
  - タプルのサイズを0にして、後続のタプルを前にシフト

- **`Resize(index int, newLen int) bool`**: 指定されたインデックスのタプルのサイズを変更
  - サイズが増加する場合は、後続のタプルを前にシフトしてスペースを確保
  - サイズが減少する場合は、後続のタプルを前にシフトしてスペースを解放

- **`updatePointersInBody()`**: `pointers`スライスの変更を`body`のバイナリ形式に反映（内部メソッド）

#### タプルの格納方法

- タプルはページの**後ろから前へ**向かって格納される
- 新しいタプルは`FreeSpaceOffset`の位置から前に向かって書き込まれる
- ポインタ配列はページの**前から後ろへ**配置される

#### 具体例

2つのタプル（"hello"と"world"）を格納した場合：

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

- **`FetchBuffer(pageId disk.PageId) (*Buffer, error)`**: バッファを取得
  - ページテーブルをチェックし、キャッシュにあればそれを返す
  - キャッシュにない場合は、`Evict()`で置換対象を選択
  - 置換対象のバッファが`IsDirty`の場合はディスクに書き込む
  - ディスクからページを読み込む（存在しない場合は0で初期化）
  - ページテーブルを更新

- **`CreateBuffer() (*Buffer, error)`**: 新しいバッファを作成
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
buffer, err := bufmgr.FetchBuffer(pageId)

// 新しいページの作成
buffer, err := bufmgr.CreateBuffer()

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
2. **内部ノード（Internal Node）**: キーと子ページIDを保持
3. **リーフノード（Leaf）**: 葉ノード。キー・バリューペアを保持し、リンクリストで接続

#### ページレイアウト

```
┌─────────────────────────────────────┐
│ Node Header (8バイト)               │
│  - NodeType: "LEAF    " or "BRANCH  "│
│    (Note: "BRANCH  " is used for internal nodes)│
├─────────────────────────────────────┤
│ Node Body                           │
│  (Leaf or Internal Node data)       │
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

#### 内部ノードの構造

```
┌─────────────────────────────────────┐
│ Internal Header (8バイト)          │
│  - RightChild: 右端の子ページID     │
├─────────────────────────────────────┤
│ Slotted Body                        │
│  (キー・子ページIDのペアの配列)     │
└─────────────────────────────────────┘
```

#### 検索アルゴリズム

1. ルートページから開始
2. 内部ノードの場合：
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
   - 中間キーを親ノードに伝播（`Split`構造体を使用）
4. 親ノードも満杯の場合は再帰的に分割

##### ノード分割とSplit

B+ツリーの挿入操作では、ノードが満杯になると分割（split）が発生します。分割が発生した場合、親ノードに新しいノードの情報を伝播する必要があります。この情報を`Split`構造体で表現します。

**`Split`構造体:**
```go
type Split struct {
    Key         []byte      // Promoted key (新しいノードの最小キー)
    ChildPageId disk.PageId // 新しく作成された子ノードのページID
}
```

**分割処理の流れ:**

1. **リーフノードの分割:**
   - リーフノードが満杯で挿入できない場合、新しいリーフノードを作成
   - 既存のリーフと新しいリーフにデータを分散（各ノードが半分以上埋まるように）
   - 新しいリーフの最小キーとページIDを`Split`として返す

2. **内部ノードでの処理:**
   - 子ノードから`Split`を受け取る
   - 親ノードに新しいエントリ（`Split.Key`と`Split.ChildPageId`）を追加
   - 親ノードに空きがあれば追加して終了
   - 親ノードも満杯の場合は、親ノードも分割し、さらに上位へ`Split`を伝播

3. **ルートノードの分割:**
   - ルートノードが分割された場合、新しいルートノードを作成
   - 新しいルートノードに2つの子ノードへのポインタを設定
   - メタページのルートページIDを更新

**具体例:**

```
挿入前:
        [50]
       /    \
   [10,30] [70,90]  ← リーフノード

[25を挿入] → [10,30]が満杯 → 分割

分割後:
        [50]
       /    \
   [10] [30,50] [70,90]  ← 新しいリーフが追加された

Split情報:
- Key: "30" (新しいリーフの最小キー、プロモートされたキー)
- ChildPageId: 新しいリーフノードのPageId

親ノードに追加:
        [30, 50]  ← 内部ノードに新しいエントリを追加
       /   |    \
   [10] [30,50] [70,90]
```

`Split`という名前は、ノード分割（split）の結果を表し、分割によって生じた新しいノードの情報（プロモートされたキーと新しい子ノードのページID）を親ノードに伝えるためのデータ構造であることを表現しています。

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
  - ページが満杯の場合は分割し、Splitを親ノードに伝播
  - ルートが分割された場合は新しいルートを作成

- **`insertInternal()`**: 内部的な挿入処理（再帰的）
  - リーフノードの場合は直接挿入
  - 内部ノードの場合は子ノードに再帰的に挿入
  - Splitが発生した場合は分割

##### Split

- **`Split`**: ノード分割時に親ノードに伝播する情報を表す構造体
  - `Key`: Promoted key（プロモートされたキー）。新しいノードの最小キーで、親ノードでこのキーを使って新しいエントリを作成する
  - `ChildPageId`: 新しく作成された子ノードのページID。親ノードでこのページIDへのポインタを追加する

  **使用例:**
  - リーフノードが分割された場合、新しいリーフの最小キーとページIDを`Split`として返す
  - 内部ノードが分割された場合、新しい内部ノードの最小キーとページIDを`Split`として返す
  - `insertInternal()`は、分割が発生しなかった場合は`nil`を返し、分割が発生した場合は`Split`を返す

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

- **`Node`**: B+ツリーノード（リーフまたは内部ノード）のラッパー
  - `header`: ノードヘッダー（ノードタイプを保持）
  - `body`: ノードボディ（リーフまたは内部ノードのデータ）

- **`NewNode(page []byte) *Node`**: ページからノードを作成

- **`InitializeAsLeaf()`**: ノードをリーフノードとして初期化

- **`InitializeAsBranch()`**: ノードを内部ノードとして初期化（注: ディスクフォーマットとの互換性のため"BRANCH"という名前を使用）

- **`IsLeaf() bool`**: リーフノードかどうかを判定

- **`IsBranch() bool`**: 内部ノードかどうかを判定（注: ディスクフォーマットとの互換性のため"Branch"という名前を使用）

- **`Body() []byte`**: ノードボディを取得

- **`AsLeaf() *leaf.Leaf`**: リーフノードとして取得

- **`AsBranch() *internal.InternalNode`**: 内部ノードとして取得（注: ディスクフォーマットとの互換性のため"Branch"という名前を使用）

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

##### InternalNode

- **`InternalNode`**: B+ツリーの内部ノード（学術的に標準的な用語）
  - `header`: 内部ノードヘッダー（右端の子ページIDを保持）
  - `body`: スロッテッドページ（キー・子ページIDのペアを格納）

- **`NewInternalNode(bodyBytes []byte) *InternalNode`**: ボディから内部ノードを作成

- **`NumPairs() int`**: キー・子ページIDのペアの数を返す

- **`SearchSlotId(key []byte) (int, error)`**: キーに対応するスロットIDを検索（バイナリサーチ）

- **`SearchChild(key []byte) disk.PageId`**: キーに対応する子ページIDを検索

- **`SearchChildIdx(key []byte) int`**: キーに対応する子インデックスを検索

- **`ChildAt(childIdx int) disk.PageId`**: 指定されたインデックスの子ページIDを取得
  - `childIdx`が`NumPairs()`の場合は右端の子ページIDを返す

- **`PairAt(slotId int) *Pair`**: 指定されたスロットのペアを取得

- **`MaxPairSize() int`**: ペアの最大サイズを計算

- **`Initialize(key []byte, leftChild disk.PageId, rightChild disk.PageId)`**: 内部ノードを初期化
  - 最初のキーと左の子ページIDを設定
  - 右端の子ページIDを設定

- **`FillRightChild() []byte`**: 最後のペアの子ページIDを右端の子として設定し、キーを返す

- **`Insert(slotId int, key []byte, pageId disk.PageId) bool`**: キー・子ページIDのペアを挿入

- **`IsHalfFull() bool`**: ページが半分以上埋まっているかどうかを判定

- **`SplitInsert(newNode *InternalNode, newKey []byte, newPageId disk.PageId) []byte`**: ページを分割して挿入
  - 新しい内部ノードに要素を転送
  - 両方のノードが半分以上埋まるように調整
  - 新しい内部ノードの最小キー（プロモートされたキー）を返す

- **`Transfer(dest *InternalNode)`**: 最初のペアを別の内部ノードに転送

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

`table`パッケージは、`btree`パッケージの上に構築され、テーブルとインデックスの高レベルな操作を提供します。プライマリキーとユニークセカンダリインデックスをサポートし、タプルの挿入と管理を行います。タプルは`tuple`パッケージでエンコードされ、B+ツリーに格納されます。

#### 主要な型

##### SimpleTable

- **`SimpleTable`**: シンプルなテーブル実装。プライマリキーのみをサポート
  - `MetaPageId`: B+ツリーのメタページID
  - `NumKeyElems`: プライマリキーを構成する要素数（タプルの最初のN要素）

- **`Create(bufmgr *buffer.BufferPoolManager) error`**: テーブルを作成
  - 新しいB+ツリーを作成し、メタページIDを設定

- **`Insert(bufmgr *buffer.BufferPoolManager, tuple [][]byte) error`**: タプルを挿入
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

- **`Insert(bufmgr *buffer.BufferPoolManager, tuple [][]byte) error`**: タプルを挿入
  - プライマリB+ツリーに挿入
  - 各ユニークインデックスにセカンダリキーとプライマリキーのマッピングを挿入

##### UniqueIndex

- **`UniqueIndex`**: ユニークなセカンダリインデックス
  - `MetaPageId`: B+ツリーのメタページID
  - `Skey`: セカンダリキーを構成するタプル要素のインデックス配列

- **`Create(bufmgr *buffer.BufferPoolManager) error`**: インデックスを作成
  - 新しいB+ツリーを作成し、メタページIDを設定

- **`Insert(bufmgr *buffer.BufferPoolManager, pkey []byte, tuple [][]byte) error`**: インデックスエントリを挿入
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

// タプルの挿入
tuple := [][]byte{[]byte("key1"), []byte("key2"), []byte("value1")}
err := table.Insert(bufmgr, tuple)

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

### catalog - カタログテーブル

#### 概要

`catalog`パッケージは、データベースのスキーマ情報（テーブル定義、カラム定義、インデックス定義）を永続化するためのカタログテーブルを提供します。カタログテーブルは、PostgreSQLやMySQLなどの主要なデータベースシステムで採用されている標準的なアプローチです。

#### カタログテーブル方式

カタログテーブル方式では、スキーマ情報を通常のテーブルとして保存します。これにより、既存のテーブル実装を再利用でき、SQLでスキーマ情報をクエリできるという利点があります。

**主要なカタログテーブル:**

1. **tables_catalog**: テーブル情報を保存
   - `table_id` (PK): テーブルID
   - `table_name`: テーブル名
   - `meta_page_id`: B+ツリーのメタページID
   - `num_key_elems`: プライマリキーの要素数

2. **columns_catalog**: カラム情報を保存
   - `table_id` (PK part 1): テーブルID
   - `column_index` (PK part 2): カラム番号
   - `column_name`: カラム名
   - `column_type`: データ型（INT, VARCHAR, BLOB）
   - `column_size`: サイズ（VARCHAR用）
   - `nullable`: NULL許可フラグ
   - `is_primary_key`: プライマリキーフラグ

3. **indexes_catalog**: インデックス情報を保存
   - `index_id` (PK): インデックスID
   - `index_name`: インデックス名
   - `table_id`: テーブルID
   - `meta_page_id`: B+ツリーのメタページID
   - `is_unique`: ユニークインデックスフラグ
   - `column_indices`: インデックスキーのカラム番号配列

#### 主要な型

##### ColumnType

データ型を表す列挙型です。

```go
type ColumnType int

const (
    ColumnTypeInt     ColumnType = iota  // 整数型
    ColumnTypeVarchar                    // 可変長文字列型
    ColumnTypeBlob                       // バイナリラージオブジェクト型
)
```

##### ColumnDef

カラムの定義を表す構造体です。

```go
type ColumnDef struct {
    Name         string     // カラム名
    Type         ColumnType // データ型
    Size         int        // サイズ（VARCHAR用、0は無制限）
    Nullable     bool       // NULL許可
    IsPrimaryKey bool       // プライマリキーかどうか
}
```

##### TableSchema

テーブルのスキーマ情報を表す構造体です。

```go
type TableSchema struct {
    TableID     uint32       // テーブルID
    TableName   string       // テーブル名
    MetaPageID  disk.PageID  // B+ツリーのメタページID
    NumKeyElems int          // プライマリキーの要素数
    Columns     []ColumnDef  // カラム定義のリスト
    Indexes     []IndexDef   // インデックス定義のリスト
}
```

##### IndexDef

インデックスの定義を表す構造体です。

```go
type IndexDef struct {
    IndexID      uint32      // インデックスID
    IndexName    string      // インデックス名
    TableID      uint32      // テーブルID
    MetaPageID   disk.PageID // B+ツリーのメタページID
    IsUnique     bool        // ユニークインデックスかどうか
    ColumnIndices []int      // インデックスキーのカラム番号配列
}
```

##### CatalogManager

カタログテーブルを管理するマネージャーです。

```go
type CatalogManager struct {
    bufmgr *buffer.BufferPoolManager

    // カタログテーブル（システムテーブル）
    tablesCatalog  *table.Table  // テーブル情報
    columnsCatalog *table.Table  // カラム情報
    indexesCatalog *table.Table  // インデックス情報

    // 自動インクリメントID
    nextTableID  uint32
    nextIndexID  uint32

    // スキーマキャッシュ
    schemaCache map[string]*TableSchema
    mu          sync.RWMutex
}
```

#### 主要な関数・メソッド

##### NewCatalogManager

カタログマネージャーを作成します。カタログテーブルが存在しない場合は自動的に作成されます。

```go
func NewCatalogManager(bufmgr *buffer.BufferPoolManager) (*CatalogManager, error)
```

**動作:**
- カタログテーブル（tables_catalog, columns_catalog, indexes_catalog）を初期化
- 固定ページID（0, 1, 2）にカタログテーブルを配置
- スキーマキャッシュを初期化

##### CreateTable

新しいテーブルを作成し、カタログテーブルに登録します。

```go
func (cm *CatalogManager) CreateTable(tableName string, columns []ColumnDef) (*TableSchema, error)
```

**動作:**
1. テーブル名の重複チェック
2. テーブルIDを生成
3. B+ツリーを作成（実際のテーブル）
4. `tables_catalog`にテーブル情報を登録
5. `columns_catalog`に各カラム情報を登録
6. スキーマキャッシュに保存

**戻り値:**
- `*TableSchema`: 作成されたテーブルのスキーマ情報
- `error`: エラー（テーブルが既に存在する場合など）

##### GetTableSchema

テーブル名からスキーマ情報を取得します。

```go
func (cm *CatalogManager) GetTableSchema(tableName string) (*TableSchema, error)
```

**動作:**
1. スキーマキャッシュを確認
2. キャッシュにない場合、カタログテーブルから読み込み
3. `tables_catalog`からテーブル情報を検索
4. `columns_catalog`からカラム情報を読み込み
5. `indexes_catalog`からインデックス情報を読み込み
6. スキーマキャッシュに保存

**戻り値:**
- `*TableSchema`: テーブルのスキーマ情報
- `error`: エラー（テーブルが見つからない場合など）

##### CreateIndex

テーブルにインデックスを作成し、カタログテーブルに登録します。

```go
func (cm *CatalogManager) CreateIndex(tableName string, indexName string, columnIndices []int, isUnique bool) (*IndexDef, error)
```

**動作:**
1. テーブルスキーマを取得
2. インデックスIDを生成
3. B+ツリーを作成（インデックス用）
4. `indexes_catalog`にインデックス情報を登録
5. スキーマキャッシュを更新

**戻り値:**
- `*IndexDef`: 作成されたインデックスの定義
- `error`: エラー

#### カタログテーブルの構造

カタログテーブルは通常のテーブルとして実装されており、B+ツリーを使用してデータを格納します。

**tables_catalogのタプル構造:**
```
[table_id (4 bytes), table_name (可変長), meta_page_id (8 bytes), num_key_elems (4 bytes)]
```

**columns_catalogのタプル構造:**
```
[table_id (4 bytes), column_index (4 bytes), column_name (可変長),
 column_type (4 bytes), column_size (4 bytes), nullable (1 byte), is_primary_key (1 byte)]
```

**indexes_catalogのタプル構造:**
```
[index_id (4 bytes), index_name (可変長), table_id (4 bytes),
 meta_page_id (8 bytes), is_unique (1 byte), column_indices (可変長)]
```

#### スキーマ情報の読み込み

カタログマネージャーは、カタログテーブルからスキーマ情報を読み込む際に、B+ツリーの検索機能を使用します。

**テーブル検索の流れ:**
1. `tables_catalog`のB+ツリーを先頭からスキャン
2. 各タプルの`table_name`を確認
3. 一致するテーブルが見つかったら、`table_id`、`meta_page_id`、`num_key_elems`を取得

**カラム読み込みの流れ:**
1. `columns_catalog`のB+ツリーを`table_id`で検索
2. 該当するテーブルのカラム情報をすべて取得
3. `column_index`でソートして返す

**インデックス読み込みの流れ:**
1. `indexes_catalog`のB+ツリーを先頭からスキャン
2. `table_id`が一致するインデックスをすべて取得

#### 使用例

```go
// カタログマネージャーを作成
cm, err := catalog.NewCatalogManager(bufmgr)
if err != nil {
    log.Fatal(err)
}

// テーブルを作成
columns := []catalog.ColumnDef{
    {Name: "id", Type: catalog.ColumnTypeInt, Nullable: false, IsPrimaryKey: true},
    {Name: "name", Type: catalog.ColumnTypeVarchar, Size: 50, Nullable: false, IsPrimaryKey: false},
    {Name: "email", Type: catalog.ColumnTypeVarchar, Size: 100, Nullable: true, IsPrimaryKey: false},
}

schema, err := cm.CreateTable("users", columns)
if err != nil {
    log.Fatal(err)
}

// インデックスを作成
indexDef, err := cm.CreateIndex("users", "idx_email", []int{2}, true)
if err != nil {
    log.Fatal(err)
}

// スキーマ情報を取得
retrievedSchema, err := cm.GetTableSchema("users")
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Table: %s\n", retrievedSchema.TableName)
fmt.Printf("Columns: %d\n", len(retrievedSchema.Columns))
fmt.Printf("Indexes: %d\n", len(retrievedSchema.Indexes))
```

#### メリット

カタログテーブル方式の主なメリット：

1. **既存実装の再利用**: 通常のテーブル実装をそのまま使用できる
2. **クエリ可能**: スキーマ情報をSQLで検索できる（将来的な拡張）
3. **拡張性**: 新しいメタデータを簡単に追加できる
4. **一貫性**: データテーブルと同じ方法で管理されるため、一貫性が保たれる

#### 今後の拡張

カタログテーブル方式により、以下の機能の実装が可能になります：

1. **ALTER TABLE**: カラムの追加・削除・変更
2. **DROP TABLE**: テーブルの削除
3. **CREATE INDEX / DROP INDEX**: インデックスの作成・削除
4. **スキーマバージョニング**: スキーマ変更の履歴管理
5. **権限管理**: テーブル・カラムレベルの権限情報の保存

### query - クエリ実行エンジン

クエリ実行プランを実装するパッケージです。

#### 概要

`query`パッケージは、`table`パッケージの上に構築され、クエリ実行プランを実装します。シーケンシャルスキャン、インデックススキャン、フィルタ、インデックスオンリースキャンなどの操作を提供します。クエリはイテレータパターンで実装され、タプルを1つずつ返すことで、メモリ効率的な処理を実現します。

#### 主要な型

##### 基本型

- **`Tuple`**: データベースタプル（バイトスライスのスライス）
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
  - プライマリキーでテーブルを検索してタプルを取得
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

##### Project（射影）

- **`Project`**: 指定された列のみを選択するプラン（SELECT句の列選択に対応）
  - `InnerPlan`: 内部プラン（射影を適用するプラン）
  - `ColumnIndices`: 選択する列のインデックス（0ベース）

- **`Start(bufmgr *buffer.BufferPoolManager) (Executor, error)`**: 射影を開始
  - 内部プランを開始し、`ExecProject`を返す

- **`ExecProject`**: 射影の実行器
  - `innerIter`: 内部プランの実行器
  - `columnIndices`: 選択する列のインデックス

- **`Next(bufmgr *buffer.BufferPoolManager) (Tuple, bool, error)`**: 次のタプルを返す
  - 内部実行器からタプルを取得
  - 指定された列インデックスのみを選択して新しいタプルとして返す
  - 列インデックスが範囲外の場合は空のバイトスライスを返す
  - 列の順序は`ColumnIndices`の順序に従う（元の順序とは異なる順序でも可）

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

// Project（射影）
project := &query.Project{
    InnerPlan: seqScanPlan,
    ColumnIndices: []int{1, 2}, // 列1と列2のみを選択
}

// FilterとProjectの組み合わせ
filteredProject := &query.Project{
    InnerPlan: &query.Filter{
        InnerPlan: seqScanPlan,
        Cond: func(tuple query.TupleSlice) bool {
            return len(tuple[0]) > 5
        },
    },
    ColumnIndices: []int{1, 2}, // フィルタ後のタプルから列1と列2を選択
}
```

## データフローの例

### タプルの挿入

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

### タプルの検索

```
1. query.SeqScan.Start() が呼ばれる
2. btree.Search() でB+ツリーを検索
3. リーフノードを見つける
4. slotted.Data() でタプルを取得
5. tuple.Decode() でデコード
6. 条件に合致するタプルを返す
```

## メモリ管理

### バッファプールの役割

- 頻繁にアクセスされるページをメモリにキャッシュ
- ディスクI/Oを削減
- Clock置換アルゴリズムで効率的にページを管理

### ページのライフサイクル

1. **作成**: `CreateBuffer()`で新しいページを割り当て
2. **読み込み**: `FetchBuffer()`でディスクから読み込み（必要に応じて）
3. **変更**: バッファ内のページを変更（`IsDirty = true`）
4. **フラッシュ**: `Flush()`で変更をディスクに書き込み

## パフォーマンスの考慮事項

### B+ツリーの利点

- バランスされたツリー構造により、検索・挿入がO(log n)
- リーフノードがリンクリストで接続されているため、範囲検索が効率的
- ページ単位でディスクI/Oを行うため、ディスクアクセスが最適化される

### スロッテッドページの利点

- 可変長タプルを効率的に格納
- タプルの削除・サイズ変更時のデータ移動を最小化
- ポインタ配列により、タプルの順序変更が容易

## トランザクション機能

### 概要

`transaction`パッケージは、Go実装に新しく追加されたトランザクション管理機能です。
Rust版には存在しない機能で、ACID特性（Atomicity、Consistency、Isolation、Durability）を実装しています。

### 主要コンポーネント

#### 1. Transaction Manager（トランザクションマネージャー）

トランザクションの開始・コミット・アボートを管理します。

**主要な型:**
- `Transaction`: トランザクションを表す構造体
- `TransactionManager`: トランザクションのライフサイクルを管理

**使用例:**
```go
tm := transaction.NewTransactionManager()

// トランザクションを開始
txn := tm.Begin()

// データベース操作を実行
// ...

// コミット
if err := tm.Commit(txn); err != nil {
    // エラーハンドリング
}
```

#### 2. Lock Manager（ロックマネージャー）

Two-Phase Locking（2PL）プロトコルを実装し、並行性制御を提供します。

**主要な型:**
- `LockManager`: ロックの取得・解放を管理
- `LockMode`: ロックの種類（Shared/Exclusive）
- `RID`: Tuple ID（ページID + スロットID）

**機能:**
- 共有ロック（Shared Lock）: 読み取り用
- 排他ロック（Exclusive Lock）: 書き込み用
- デッドロック検出: Wait-forグラフを使用

**使用例:**
```go
lm := transaction.NewLockManager()
rid := transaction.RID{PageID: disk.PageId(1), SlotID: 0}

// 共有ロックを取得
if err := lm.LockShared(txn, rid); err != nil {
    // エラーハンドリング
}

// 操作を実行
// ...

// ロックを解放
lm.Unlock(txn, rid)
```

#### 3. Log Manager（ログマネージャー）

Write-Ahead Logging（WAL）を実装し、トランザクションの永続性を保証します。

**主要な型:**
- `LogManager`: WALの管理
- `LogRecord`: ログレコード
- `LogRecordType`: ログレコードの種類（Update、Commit、Abort等）

**機能:**
- ログレコードの記録
- ログの永続化（ディスクへの書き込み）
- ログの読み取り

**ログファイルの構造:**

ログファイルはバイナリ形式で、複数のログレコードが順番に格納されます。
各ログレコードは以下の構造を持ちます：

```
+------------------+------------------+------------------+------------------+
| LSN (8 bytes)    | RecordSize (4)   | Type (4 bytes)   | TxnID (8 bytes)  |
+------------------+------------------+------------------+------------------+
| PageID (8 bytes) | Offset (4 bytes) | OldValueLen (4)  | OldValue (可変)  |
+------------------+------------------+------------------+------------------+
| NewValueLen (4)  | NewValue (可変)   |
+------------------+------------------+
```

**フィールドの説明:**

- **LSN (Log Sequence Number)**: 8バイト、uint64、Big-Endian
  - ログレコードの一意な識別子。1から始まり、インクリメントされる。
- **RecordSize**: 4バイト、uint32、Big-Endian
  - レコードデータのサイズ（LSNとRecordSizeフィールドを除く）。
  - 後方から読み取る際にレコードをスキップするために使用される。
- **Type**: 4バイト、uint32、Big-Endian
  - ログレコードの種類（LogRecordType）:
    - `0` = LogRecordTypeUpdate（更新操作）
    - `1` = LogRecordTypeCommit（コミット操作）
    - `2` = LogRecordTypeAbort（アボート操作）
    - `3` = LogRecordTypeBegin（開始操作）
    - `4` = LogRecordTypeCheckpoint（チェックポイント）
- **TxnID**: 8バイト、uint64、Big-Endian
  - このログレコードが属するトランザクションID。
- **PageID**: 8バイト、uint64、Big-Endian
  - 変更されたページID（Updateレコードの場合）。
- **Offset**: 4バイト、uint32、Big-Endian
  - ページ内の変更が発生したバイトオフセット（Updateレコードの場合）。
- **OldValueLen**: 4バイト、uint32、Big-Endian
  - OldValueフィールドの長さ（バイト単位）。
- **OldValue**: 可変長、[]byte
  - 変更前の値（Updateレコードの場合）。
- **NewValueLen**: 4バイト、uint32、Big-Endian
  - NewValueフィールドの長さ（バイト単位）。
- **NewValue**: 可変長、[]byte
  - 変更後の値（Updateレコードの場合）。

すべてのマルチバイト整数は移植性のためにBig-Endian形式で格納されます。

**ログファイルの特性:**

- **追記専用（Append-Only）**: ログファイルは追記専用です。レコードは一度書き込まれると変更や削除されません。
- **順次読み取り**: リカバリは先頭から順番にレコードを読み取ることで行われます。
- **永続性の保証**: `AppendLog`メソッドは各レコード書き込み後に`Sync()`を呼び出し、ディスクへの永続化を保証します。

**使用例:**
```go
lm, err := transaction.NewLogManager("/path/to/log")
if err != nil {
    // エラーハンドリング
}
defer lm.Close()

record := &transaction.LogRecord{
    Type:     transaction.LogRecordTypeUpdate,
    TxnID:    txn.ID,
    PageID:   pageID,
    Offset:   offset,
    OldValue: oldValue,
    NewValue: newValue,
}

if err := lm.AppendLog(record); err != nil {
    // エラーハンドリング
}
```

#### 4. Recovery Manager（リカバリマネージャー）

トランザクションのロールバックとシステムリカバリを実装します。

**主要な型:**
- `RecoveryManager`: リカバリ処理を管理

**機能:**
- トランザクションのロールバック
- システムリカバリ（ARIESアルゴリズムの簡易版）
  - Analysis Phase: アクティブなトランザクションを特定
  - Redo Phase: コミットされたトランザクションを再実行
  - Undo Phase: 未コミットのトランザクションを元に戻す

**使用例:**
```go
rm := transaction.NewRecoveryManager(logManager, bufferPoolManager)

// トランザクションをロールバック
if err := rm.Rollback(txn); err != nil {
    // エラーハンドリング
}

// システムリカバリ
if err := rm.Recover(); err != nil {
    // エラーハンドリング
}
```

### ACID特性の実装

#### Atomicity（原子性）

- **実装**: WALとリカバリマネージャー
- **保証**: トランザクションはすべて実行されるか、すべて実行されないか

#### Consistency（一貫性）

- **実装**: アプリケーションレベルで保証
- **保証**: データベースの整合性制約が維持される

#### Isolation（独立性）

- **実装**: Lock ManagerとTwo-Phase Locking
- **保証**: 同時実行されるトランザクションが互いに影響しない

#### Durability（永続性）

- **実装**: WALとログの永続化
- **保証**: コミットされた変更は永続的

### トランザクションの状態遷移

```
開始（Begin）
  ↓
実行中（Active）
  ↓
  ├─→ コミット（Committed）
  │     ↓
  │   完了（Terminated）
  │
  └─→ 失敗（Failed）
        ↓
     ロールバック（Aborted）
        ↓
     完了（Terminated）
```

### デッドロック検出

Wait-forグラフを使用してデッドロックを検出します。

**アルゴリズム:**
1. 各トランザクションが待機しているトランザクションを記録
2. DFS（深さ優先探索）でサイクルを検出
3. サイクルが検出された場合、1つのトランザクションをアボート

### 今後の拡張

現在の実装は基本的なトランザクション機能を提供していますが、以下の拡張が可能です：

1. **分離レベルの実装**: Read Committed、Repeatable Read、Serializable
2. **タイムスタンプ順序付け**: タイムスタンプベースの並行性制御
3. **MVCC**: Multi-Version Concurrency Controlの実装
4. **チェックポイント**: 定期的なチェックポイントの作成
5. **ログ圧縮**: 古いログの圧縮とアーカイブ

## まとめ

gorellyは、リレーショナルデータベースの基本的な機能を実装した学習用のRDBMSです。
各パッケージは明確に分離されており、それぞれが特定の責任を持っています。
この実装を通じて、RDBMSの内部構造と動作原理を理解することができます。
