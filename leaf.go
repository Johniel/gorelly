package gorelly

import (
	"bytes"
	"fmt"
	"unsafe"
)

type LeafHeader struct {
	PrevLeafID PageID
	NextLeafID PageID
}

type Leaf struct {
	header *LeafHeader
	body   *Slotted
}

func (l *Leaf) PrevLeafID() PageID {
	return l.header.PrevLeafID
}

func (l *Leaf) NextLeafID() PageID {
	return l.header.NextLeafID
}

func (l *Leaf) NumPairs() uint16 {
	return l.body.NumSlots()
}

func (l *Leaf) SearchSlotID(key []uint8) (int, error) {
	for i := 0; i < int(l.NumPairs()); i += 1 {
		if bytes.Equal(l.PairAt(i).key, key) {
			return i, nil
		}
	}
	return 0, fmt.Errorf("")
}

func (l *Leaf) PairAt(idx int) *Pair {
	p := Pair{}
	p.FromBytes(l.body.Index(idx))
	return &p
}

func (l *Leaf) Insert(slotID int, key []uint8, value []uint8) error {
	return nil
}

func (l *Leaf) MaxPairSize() uint8 {
	return uint8(l.body.Capacity()/2 - uint16(unsafe.Sizeof(Pointer{})))
}

func (l *Leaf) IsHalfFull() bool {
	return l.body.FreeSpace() < l.body.Capacity()
}

func (l *Leaf) SplitInsert(newLeaf *Leaf, newKey []uint8, newValue []uint8) []uint8 {
	for {
		if newLeaf.IsHalfFull() {
			idx, err := l.SearchSlotID(newKey)
			if err != nil {
				panic(err.Error())
			}
			err = l.Insert(idx, newKey, newValue)
			if err != nil {
				panic(err.Error())
			}
			break
		}
		if bytes.Compare(l.PairAt(0).key, newKey) == -1 {
			l.Transfer(newLeaf)
		} else {
			newLeaf.Insert(int(newLeaf.NumPairs()), newKey, newValue)
			for newLeaf.IsHalfFull() {
				l.Transfer(newLeaf)
			}
			break
		}
	}
	return l.PairAt(0).key
}

func (l *Leaf) Transfer(dst *Leaf) {
	nextIndex := dst.NumPairs()
	copy(dst.body.Index(int(nextIndex)), l.body.Index(0))
	l.body.Remove(0)
}
