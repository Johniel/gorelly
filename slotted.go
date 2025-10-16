package gorelly

import (
	"unsafe"
)

type Pointer struct {
	offset uint16
	len    uint16
}

func (p *Pointer) Range() (begin, end uint16) {
	return p.offset, p.offset + p.len
}

type SlottedHeader struct {
	NumSlots        uint16
	FreeSpaceOffset uint16
	_pad            uint32
}

type Slotted struct {
	header *SlottedHeader
	body   []byte
}

func (s *Slotted) Capacity() int {
	return len(s.body)
}

func (s *Slotted) NumSlots() uint16 {
	return s.header.NumSlots
}

func (s *Slotted) FreeSpace() uint16 {
	return s.header.FreeSpaceOffset - s.PointersSize()
}

func (s *Slotted) PointersSize() uint16 {
	return uint16(unsafe.Sizeof(Pointer{})) * s.NumSlots()
}

func (s *Slotted) Pointers() []*Pointer {
	pointers := []*Pointer{}
	for i := uint16(0); i < s.PointersSize(); i += 1 {
	}
	return pointers
}

func (s *Slotted) Data(pointer *Pointer) []byte {
	begin, end := pointer.Range()
	return s.body[begin:end]
}

func (s *Slotted) Insert() {}

func (s *Slotted) Remove(index int) {

}

func (s *Slotted) Resize() {}
