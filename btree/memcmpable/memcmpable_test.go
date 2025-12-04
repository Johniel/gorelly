package memcmpable

import (
	"reflect"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	org1 := []byte("helloworld!memcmpable")
	org2 := []byte("foobarbazhogehuga")

	var enc []byte
	Encode(org1, &enc)
	Encode(org2, &enc)

	rest := enc

	var dec1 []byte
	Decode(&rest, &dec1)
	if !reflect.DeepEqual(org1, dec1) {
		t.Errorf("dec1: expected %v, got %v", org1, dec1)
	}

	var dec2 []byte
	Decode(&rest, &dec2)
	if !reflect.DeepEqual(org2, dec2) {
		t.Errorf("dec2: expected %v, got %v", org2, dec2)
	}
}
