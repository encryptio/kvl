package keys

import (
	"bytes"
	"testing"
)

func TestLexNext(t *testing.T) {
	tests := []struct {
		In  []byte
		Out []byte
	}{
		{nil, []byte{0xFF}},
		{[]byte{}, []byte{0xFF}},
		{[]byte{4}, []byte{4, 0xFF}},
	}

	for _, test := range tests {
		if !bytes.Equal(LexNext(test.In), test.Out) {
			t.Errorf("LexNext(%v) = %v, wanted %v", test.In, LexNext(test.In), test.Out)
		}
	}
}

func TestPrefixNext(t *testing.T) {
	tests := []struct {
		In  []byte
		Out []byte
	}{
		{nil, nil},
		{[]byte{0xFF}, nil},
		{[]byte{0x00}, []byte{0x01}},
		{[]byte{0x50, 0x80}, []byte{0x50, 0x81}},
		{[]byte{0x50, 0xFF}, []byte{0x51}},
	}

	for _, test := range tests {
		if !bytes.Equal(PrefixNext(test.In), test.Out) {
			t.Errorf("PrefixNext(%v) = %v, wanted %v", test.In, PrefixNext(test.In), test.Out)
		}
	}
}
