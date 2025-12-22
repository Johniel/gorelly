package slotted

import (
	"reflect"
	"testing"
)

func TestSlotted(t *testing.T) {
	pageData := make([]byte, 128)
	slotted := NewSlotted(pageData)

	insert := func(s *Slotted, index int, buf []byte) {
		if !s.Insert(index, len(buf)) {
			t.Fatalf("failed to insert at index %d", index)
		}
		copy(s.Data(index), buf)
	}

	push := func(s *Slotted, buf []byte) {
		index := s.NumSlots()
		insert(s, index, buf)
	}

	slotted.Initialize()
	push(slotted, []byte("hello"))
	push(slotted, []byte("world"))

	if !reflect.DeepEqual(slotted.Data(0), []byte("hello")) {
		t.Errorf("slot 0: expected 'hello', got %v", slotted.Data(0))
	}
	if !reflect.DeepEqual(slotted.Data(1), []byte("world")) {
		t.Errorf("slot 1: expected 'world', got %v", slotted.Data(1))
	}

	insert(slotted, 1, []byte(", "))
	push(slotted, []byte("!"))

	if !reflect.DeepEqual(slotted.Data(0), []byte("hello")) {
		t.Errorf("slot 0: expected 'hello', got %v", slotted.Data(0))
	}
	if !reflect.DeepEqual(slotted.Data(1), []byte(", ")) {
		t.Errorf("slot 1: expected ', ', got %v", slotted.Data(1))
	}
	if !reflect.DeepEqual(slotted.Data(2), []byte("world")) {
		t.Errorf("slot 2: expected 'world', got %v", slotted.Data(2))
	}
	if !reflect.DeepEqual(slotted.Data(3), []byte("!")) {
		t.Errorf("slot 3: expected '!', got %v", slotted.Data(3))
	}
}
