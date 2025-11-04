package gorelly

import (
	"encoding/binary"
	"errors"
	// "fmt"
)

type Pair struct {
	key   []uint8
	value []uint8
}

// ToBytes: Pair をバイト列にエンコード（keyLen | key | valueLen | value）
func (p Pair) ToBytes() []byte {
	// 最大サイズ見積もりで一度だけ確保（Uvarint は最大10バイト）
	buf := make([]byte, binary.MaxVarintLen64*2+len(p.key)+len(p.value))
	i := 0

	i += binary.PutUvarint(buf[i:], uint64(len(p.key)))
	copy(buf[i:], p.key)
	i += len(p.key)

	i += binary.PutUvarint(buf[i:], uint64(len(p.value)))
	copy(buf[i:], p.value)
	i += len(p.value)

	return buf[:i]
}

// FromBytes: バイト列から1件読み取り、pにセットして「消費したバイト数」を返す。
// 失敗時はエラー。複数連結されている場合は、戻り値nで次の位置に進めます。
func (p *Pair) FromBytes(b []byte) (n int, err error) {
	i := 0

	// key 長
	keyLen, m := binary.Uvarint(b[i:])
	if m <= 0 {
		return 0, errors.New("invalid varint for key length")
	}
	i += m
	if keyLen > uint64(len(b)-i) {
		return 0, errors.New("short buffer for key")
	}
	p.key = make([]byte, int(keyLen))
	copy(p.key, b[i:i+int(keyLen)])
	i += int(keyLen)

	// value 長
	valLen, m := binary.Uvarint(b[i:])
	if m <= 0 {
		return 0, errors.New("invalid varint for value length")
	}
	i += m
	if valLen > uint64(len(b)-i) {
		return 0, errors.New("short buffer for value")
	}
	p.value = make([]byte, int(valLen))
	copy(p.value, b[i:i+int(valLen)])
	i += int(valLen)

	return i, nil
}
