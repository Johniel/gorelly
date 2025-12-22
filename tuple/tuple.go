// Package tuple provides encoding and decoding of tuples (records) as byte sequences.
// Tuples are encoded using memcmpable encoding to preserve ordering.
package tuple

import (
	"fmt"

	"github.com/Johniel/gorelly/btree/memcmpable"
)

// Encode encodes a tuple (slice of byte slices) into a single byte sequence.
// Each element is encoded using memcmpable encoding to preserve ordering.
func Encode(elems [][]byte, bytes *[]byte) {
	for _, elem := range elems {
		memcmpable.Encode(elem, bytes)
	}
}

// Decode decodes a byte sequence back into a tuple (slice of byte slices).
func Decode(bytes []byte, elems *[][]byte) {
	rest := bytes
	for len(rest) > 0 {
		var elem []byte
		memcmpable.Decode(&rest, &elem)
		*elems = append(*elems, elem)
	}
}

// Pretty formats a tuple for human-readable display.
// It shows string representations for valid UTF-8 sequences and hex for binary data.
func Pretty(elems [][]byte) string {
	result := "Tuple("
	for i, elem := range elems {
		if i > 0 {
			result += ", "
		}
		if str := string(elem); isValidUTF8(str) {
			result += fmt.Sprintf("%q %x", str, elem)
		} else {
			result += fmt.Sprintf("%x", elem)
		}
	}
	result += ")"
	return result
}

func isValidUTF8(s string) bool {
	for _, r := range s {
		if r == 0xFFFD {
			return false
		}
	}
	return true
}
