package branch

import (
	"encoding/binary"
)

type Pair struct {
	Key   []byte
	Value []byte
}

func (p *Pair) ToBytes() []byte {
	// Format: [key_len:4][key:key_len][value_len:4][value:value_len]
	buff := make([]byte, 0, 8+len(p.Key)+len(p.Value))

	keyLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyLenBytes, uint32(len(p.Key)))
	buff = append(buff, keyLenBytes...)
	buff = append(buff, p.Key...)

	valueLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(valueLenBytes, uint32(len(p.Value)))
	buff = append(buff, valueLenBytes...)
	buff = append(buff, p.Value...)

	return buff
}

func PairFromBytes(data []byte) *Pair {
	if len(data) < 8 {
		panic("pair data too short")
	}
	keyLen := binary.LittleEndian.Uint32(data[0:4])
	if len(data) < int(8+keyLen) {
		panic("pair data too short for key")
	}
	key := make([]byte, keyLen)
	copy(key, data[4:4+keyLen])
	valueLen := binary.LittleEndian.Uint32(data[4+keyLen : 8+keyLen])
	if len(data) < int(8+keyLen+valueLen) {
		panic("pair data too short for value")
	}
	value := make([]byte, valueLen)
	copy(value, data[8+keyLen:8+keyLen+valueLen])
	return &Pair{Key: key, Value: value}
}
