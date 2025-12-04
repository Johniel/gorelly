// Package memcmpable provides encoding/decoding functions for creating
// memcmp-comparable byte sequences. This allows byte sequences to be
// compared using standard byte comparison while preserving ordering.
package memcmpable

// EscapeLength is the length of escape sequences used in encoding.
const EscapeLength = 9

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// EncodedSize calculates the size needed to encode a byte sequence of the given length.
func EncodedSize(len int) int {
	return (len + (EscapeLength - 1)) / (EscapeLength - 1) * EscapeLength
}

// Encode encodes a byte sequence into a memcmp-comparable format.
// The encoded data can be compared byte-by-byte while preserving the original ordering.
func Encode(src []byte, dst *[]byte) {
	for len(src) > 0 {
		copyLen := min(EscapeLength-1, len(src))
		*dst = append(*dst, src[0:copyLen]...)
		src = src[copyLen:]
		if len(src) == 0 {
			padSize := EscapeLength - 1 - copyLen
			if padSize > 0 {
				*dst = append(*dst, make([]byte, padSize)...)
			}
			*dst = append(*dst, byte(copyLen))
			break
		}
		*dst = append(*dst, EscapeLength)
	}
}

// Decode decodes a memcmp-comparable byte sequence back to its original form.
// The src slice is consumed during decoding (modified in place).
func Decode(src *[]byte, dst *[]byte) {
	for len(*src) > 0 {
		extra := (*src)[EscapeLength-1]
		len := min(EscapeLength-1, int(extra))
		*dst = append(*dst, (*src)[:len]...)
		*src = (*src)[EscapeLength:]
		if extra < EscapeLength {
			break
		}
	}
}
