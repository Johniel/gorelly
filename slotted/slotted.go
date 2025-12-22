// Package slotted provides a slotted page structure for storing variable-length tuples.
// Slotted pages organize data with a header, pointer array, and data area.
package slotted

import (
	"encoding/binary"
	"unsafe"
)

// PointerSize is the size of a pointer entry (2 bytes offset + 2 bytes length).
const PointerSize = 4

// HeaderSize is the size of the slotted page header (2 bytes NumSlots + 2 bytes FreeSpaceOffset + 4 bytes Pad).
const HeaderSize = 8

// Header contains metadata for a slotted page.
type Header struct {
	NumSlots        uint16 // Number of slots (tuples) in the page
	FreeSpaceOffset uint16 // Offset to the start of free space
	Pad             uint32 // Padding for alignment
}

// Pointer points to a tuple in the data area.
// It stores the offset and length of the tuple.
type Pointer struct {
	Offset uint16 // Offset from the start of the body
	Len    uint16 // Length of the tuple
}

func (p *Pointer) Range(bodyLen int) (start, end int) {
	start = int(p.Offset)
	end = start + int(p.Len)
	if end > bodyLen {
		end = bodyLen
	}
	return start, end
}

// Slotted represents a slotted page structure.
// It manages variable-length tuples stored in a page with a header and pointer array.
// Tuples are stored from the end of the page towards the beginning,
// while pointers are stored from the beginning after the header.
//
// Body structure:
//   - body[0:pointersSize]: Pointer array (each pointer is 4 bytes: offset + len)
//   - body[pointersSize:FreeSpaceOffset]: Free space (unused area)
//   - body[FreeSpaceOffset:end]: Data tuples (variable-length tuples stored backwards)
//
// Example layout (body size = 100, 2 tuples: "hello" and "world"):
//
//	body[0:8]     → Pointers array (2 pointers = 8 bytes)
//	body[8:90]    → Free space (82 bytes)
//	body[90:95]   → "world" data (5 bytes)
//	body[95:100]  → "hello" data (5 bytes)
//
// Relationship between body and pointers:
//   - body[0:pointersSize] stores pointers as raw bytes (binary format)
//   - pointers slice is a Go struct representation of the same data
//   - NewSlotted reads from body[0:pointersSize] to populate pointers slice
//   - updatePointersInBody() writes pointers slice back to body[0:pointersSize]
//   - This dual representation allows efficient manipulation in Go while maintaining
//     the binary format required for disk storage
type Slotted struct {
	header   *Header
	body     []byte    // Body contains: [pointers array][free space][data tuples]
	pointers []Pointer // Go struct representation of pointers (synced with body[0:pointersSize])
}

func NewSlotted(bytes []byte) *Slotted {
	if len(bytes) < HeaderSize {
		panic("slotted header must fit")
	}
	header := (*Header)(unsafe.Pointer(&bytes[0]))
	body := bytes[HeaderSize:]

	// Read pointers
	numPointers := int(header.NumSlots)
	pointers := make([]Pointer, numPointers)
	if numPointers > 0 {
		pointersSize := numPointers * PointerSize
		if len(body) < pointersSize {
			panic("pointers data too short")
		}
		pointersData := body[:pointersSize]
		for i := 0; i < numPointers; i++ {
			offset := binary.LittleEndian.Uint16(pointersData[i*PointerSize:])
			len := binary.LittleEndian.Uint16(pointersData[i*PointerSize+2:])
			pointers[i] = Pointer{Offset: offset, Len: len}
		}
	}

	return &Slotted{
		header:   header,
		body:     body,
		pointers: pointers,
	}
}

func (s *Slotted) Capacity() int {
	return len(s.body)
}

func (s *Slotted) NumSlots() int {
	return int(s.header.NumSlots)
}

func (s *Slotted) FreeSpace() int {
	return int(s.header.FreeSpaceOffset) - s.PointersSize()
}

func (s *Slotted) PointersSize() int {
	return PointerSize * s.NumSlots()
}

func (s *Slotted) Data(index int) []byte {
	if index >= len(s.pointers) {
		return nil
	}
	start, end := s.pointers[index].Range(len(s.body))
	if start >= len(s.body) || end > len(s.body) {
		return nil
	}
	return s.body[start:end]
}

func (s *Slotted) Initialize() {
	s.header.NumSlots = 0
	s.header.FreeSpaceOffset = uint16(len(s.body))
	s.pointers = s.pointers[:0]
	s.updatePointersInBody()
}

func (s *Slotted) Insert(index int, dataLen int) bool {
	if s.FreeSpace() < PointerSize+dataLen {
		return false
	}

	// Update header
	s.header.FreeSpaceOffset -= uint16(dataLen)
	s.header.NumSlots++

	// Expand pointers slice if needed
	if cap(s.pointers) < s.NumSlots() {
		newPointers := make([]Pointer, s.NumSlots(), s.NumSlots()*2)
		copy(newPointers, s.pointers)
		s.pointers = newPointers
	} else {
		s.pointers = s.pointers[:s.NumSlots()]
	}

	// Shift pointers
	copy(s.pointers[index+1:], s.pointers[index:])
	s.pointers[index] = Pointer{
		Offset: s.header.FreeSpaceOffset,
		Len:    uint16(dataLen),
	}

	// Update pointers in body
	s.updatePointersInBody()

	return true
}

func (s *Slotted) Remove(index int) {
	s.Resize(index, 0)
	copy(s.pointers[index:], s.pointers[index+1:])
	s.header.NumSlots--
	s.pointers = s.pointers[:s.NumSlots()]
	s.updatePointersInBody()
}

func (s *Slotted) Resize(index int, newLen int) bool {
	if index >= len(s.pointers) {
		return false
	}

	oldLen := int(s.pointers[index].Len)
	lenIncr := newLen - oldLen

	if lenIncr == 0 {
		return true
	}

	if lenIncr > s.FreeSpace() {
		return false
	}

	freeSpaceOffset := int(s.header.FreeSpaceOffset)
	oldOffset := int(s.pointers[index].Offset)
	shiftRangeStart := freeSpaceOffset
	shiftRangeEnd := oldOffset
	newFreeSpaceOffset := freeSpaceOffset - lenIncr

	// Shift data
	copy(s.body[newFreeSpaceOffset:], s.body[shiftRangeStart:shiftRangeEnd])

	// Update pointers
	for i := range s.pointers {
		if int(s.pointers[i].Offset) <= oldOffset {
			s.pointers[i].Offset = uint16(int(s.pointers[i].Offset) - lenIncr)
		}
	}

	s.pointers[index].Len = uint16(newLen)
	if newLen == 0 {
		s.pointers[index].Offset = uint16(newFreeSpaceOffset)
	}

	s.header.FreeSpaceOffset = uint16(newFreeSpaceOffset)
	s.updatePointersInBody()

	return true
}

func (s *Slotted) updatePointersInBody() {
	pointersSize := s.PointersSize()
	if len(s.body) < pointersSize {
		return
	}
	pointersData := s.body[:pointersSize]
	for i := 0; i < len(s.pointers) && i*PointerSize+4 <= len(pointersData); i++ {
		ptr := s.pointers[i]
		binary.LittleEndian.PutUint16(pointersData[i*PointerSize:], ptr.Offset)
		binary.LittleEndian.PutUint16(pointersData[i*PointerSize+2:], ptr.Len)
	}
}
