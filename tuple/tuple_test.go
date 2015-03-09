package tuple

import (
	"bytes"
	"math/big"
	"math/rand"
	"testing"
	"time"
)

func mustBigInt(s string, base int) *big.Int {
	n := &big.Int{}
	_, ok := n.SetString(s, base)
	if !ok {
		panic("couldn't make big int from " + s)
	}
	return n
}

func TestFormat(t *testing.T) {
	tests := []struct {
		Value   interface{}
		Encoded []byte
	}{
		{nil, []byte{0}},
		{false, []byte{2}},
		{true, []byte{3}},
		{int64(-9223372036854775808), []byte{0x48, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{int64(-9223372036854775807), []byte{0x48, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{int64(-1000000000000000000), []byte{0x48, 0xf2, 0x1f, 0x49, 0x4c, 0x58, 0x9b, 0xff, 0xff}},
		{int64(-100000000000000000), []byte{0x48, 0xfe, 0x9c, 0xba, 0x87, 0xa2, 0x75, 0xff, 0xff}},
		{int64(-10000000000000000), []byte{0x49, 0xdc, 0x79, 0x0d, 0x90, 0x3e, 0xff, 0xff}},
		{int64(-1000000000000000), []byte{0x49, 0xfc, 0x72, 0x81, 0x5b, 0x39, 0x7f, 0xff}},
		{int64(-100000000000000), []byte{0x4a, 0xa5, 0x0c, 0xef, 0x85, 0xbf, 0xff}},
		{int64(-10000000000000), []byte{0x4a, 0xf6, 0xe7, 0xb1, 0x8d, 0x5f, 0xff}},
		{int64(-1000000000000), []byte{0x4b, 0x17, 0x2b, 0x5a, 0xef, 0xff}},
		{int64(-100000000000), []byte{0x4b, 0xe8, 0xb7, 0x89, 0x17, 0xff}},
		{int64(-10000000000), []byte{0x4b, 0xfd, 0xab, 0xf4, 0x1b, 0xff}},
		{-1000000000, []byte{0x4c, 0xc4, 0x65, 0x35, 0xff}},
		{-100000000, []byte{0x4c, 0xfa, 0x0a, 0x1e, 0xff}},
		{-10000000, []byte{0x4d, 0x67, 0x69, 0x7f}},
		{-1000000, []byte{0x4d, 0xf0, 0xbd, 0xbf}},
		{-100000, []byte{0x4d, 0xfe, 0x79, 0x5f}},
		{-10000, []byte{0x4e, 0xd8, 0xef}},
		{-1000, []byte{0x4e, 0xfc, 0x17}},
		{-1, []byte{0x5e}},
		{0, []byte{0x60}},
		{1, []byte{0x61}},
		{2, []byte{0x62}},
		{3, []byte{0x63}},
		{15, []byte{0x6f}},
		{16, []byte{0x70, 0x10}},
		{255, []byte{0x70, 0xff}},
		{511, []byte{0x71, 0x01, 0xff}},
		{1023, []byte{0x71, 0x03, 0xff}},
		{1024, []byte{0x71, 0x04, 0x00}},
		{2048, []byte{0x71, 0x08, 0x00}},
		{100000, []byte{0x72, 0x01, 0x86, 0xa0}},
		{1000000, []byte{0x72, 0x0f, 0x42, 0x40}},
		{100000000, []byte{0x73, 0x05, 0xf5, 0xe1, 0x00}},
		{uint(100000000), []byte{0x73, 0x05, 0xf5, 0xe1, 0x00}},
		{uint32(100000000), []byte{0x73, 0x05, 0xf5, 0xe1, 0x00}},
		{uint64(100000000), []byte{0x73, 0x05, 0xf5, 0xe1, 0x00}},
		{mustBigInt("abcdef123456789abcdef", 16), []byte{0x7a, 0x0a, 0xbc, 0xde, 0xf1, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}},
		{mustBigInt("123456789012345678901234567890123456789012345678901234567890", 16), []byte{0x7f, 0x1d, 0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34, 0x56, 0x78, 0x90}},
		{"", []byte{0x80, 0}},
		{"hello", []byte{0x80, 'h', 'e', 'l', 'l', 'o', 0}},
		{[]byte("hello"), []byte{0x80, 'h', 'e', 'l', 'l', 'o', 0}},
		{"one\x01zero\x00", []byte{0x80, 'o', 'n', 'e', 1, 1, 'z', 'e', 'r', 'o', 1, 0, 0}},
		{[4]byte{1, 2, 3, 4}, []byte{0x80, 1, 1, 2, 3, 4, 0}},
		{[16]byte{5, 6, 7, 8, 5, 6, 7, 8, 5, 6, 7, 8, 9, 10, 11, 12}, []byte{0x80, 5, 6, 7, 8, 5, 6, 7, 8, 5, 6, 7, 8, 9, 10, 11, 12, 0}},
	}

	for _, test := range tests {
		out, err := appendElement(nil, test.Value)
		if err != nil {
			t.Errorf("Couldn't encode %#v: %v", test.Value, err)
			continue
		}

		if !bytes.Equal(out, test.Encoded) {
			t.Errorf("Encode(%#v) = %v, wanted %v", test.Value, out, test.Encoded)
		}
	}
}

func TestArrays(t *testing.T) {
	value := [4]byte{1, 2, 3, 4}
	out := MustAppend(nil, value)

	var decValue [4]byte
	err := UnpackInto(out, &decValue)
	if err != nil {
		t.Errorf("UnpackInto returned unexpected error %v", err)
	}
	if decValue != value {
		t.Errorf("UnpackInto set decValue to %v, but wanted %v",
			decValue, value)
	}

	var decValueBad [8]byte
	err = UnpackInto(out, &decValueBad)
	if _, ok := err.(ArrayLengthError); !ok {
		t.Errorf("UnpackInto returned error %v, but wanted an ArrayLengthError",
			err)
	}
}

func TestInts(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for bits := 1; bits < 1000; bits++ {
		var limit, half big.Int

		limit.SetUint64(1).Lsh(&limit, uint(bits))
		half.SetUint64(1).Lsh(&limit, uint(bits-1))

		var n, m big.Int
		for i := 0; i < 3; i++ {
			n.Rand(rng, &limit).Sub(&n, &half)

			data, err := appendElement(nil, &n)
			if err != nil {
				t.Errorf("Couldn't serialize int %v: %v", &n, err)
				continue
			}

			eaten, out, err := decodeElement(data)
			if err != nil {
				t.Errorf("Couldn't deserialize int %v from data %v: %v", &n, data, err)
				continue
			}

			if eaten != len(data) {
				t.Errorf("Didn't eat all the data for int %v (used %v bytes, wanted %v)", &n, eaten, len(data))
			}

			switch out := out.(type) {
			case int64:
				m.SetInt64(out)
			case *big.Int:
				m.Set(out)
			default:
				t.Errorf("Got unexpected type %T", out)
				continue
			}

			if n.Cmp(&m) != 0 {
				t.Errorf("Got integer %v from decode of int encoded from %v", &m, &n)
			}
		}
	}
}

func TestCompareInts(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for bits := 1; bits < 1000; bits++ {
		var limit, half, n1, n2 big.Int

		limit.SetUint64(1).Lsh(&limit, uint(bits))
		half.SetUint64(1).Lsh(&limit, uint(bits-1))

		for i := int64(0); i < 3; i++ {
			n1.Rand(rng, &limit).Sub(&n1, &half)
			n2.Rand(rng, &limit).Sub(&n2, &half)

			cmp := n1.Cmp(&n2)

			e1, err := appendElement(nil, &n1)
			if err != nil {
				t.Fatalf("Couldn't encode %v: %v", n1, err)
			}
			e2, err := appendElement(nil, &n2)
			if err != nil {
				t.Fatalf("Couldn't encode %v: %v", n2, err)
			}

			bcmp := bytes.Compare(e1, e2)
			if bcmp != cmp {
				t.Errorf("Encode(%v) = %v, Encode(%v) = %v, comparisons differ (%v != %v)", &n1, e1, &n2, e2, cmp, bcmp)
			}
		}
	}
}

func TestEncDecMany(t *testing.T) {
	data, err := Append(nil, 4, true, "asdf")
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, []byte{0x64, 0x03, 0x80, 'a', 's', 'd', 'f', 0}) {
		t.Fatalf("encoded incorrect data, got %v", data)
	}

	var i int
	var b bool
	var s string
	err = UnpackInto(data, &i, &b, &s)
	if err != nil {
		t.Fatal(err)
	}

	if i != 4 {
		t.Errorf("wanted 4, got %v", i)
	}
	if b != true {
		t.Errorf("wanted true, got %v", b)
	}
	if s != "asdf" {
		t.Errorf("wanted asdf, got %v", s)
	}
}

func BenchmarkEncodeInt0Byte(b *testing.B) {
	var data []byte
	var err error
	for i := 0; i < b.N; i++ {
		data, err = appendElement(data, int(0))
		if err != nil {
			b.Fatal(err)
		}
		data = data[:0]
	}
}

func BenchmarkEncodeInt1Byte(b *testing.B) {
	var data []byte
	var err error
	for i := 0; i < b.N; i++ {
		data, err = appendElement(data, int(30))
		if err != nil {
			b.Fatal(err)
		}
		data = data[:0]
	}
}

func BenchmarkEncodeInt2Byte(b *testing.B) {
	var data []byte
	var err error
	for i := 0; i < b.N; i++ {
		data, err = appendElement(data, int(10000))
		if err != nil {
			b.Fatal(err)
		}
		data = data[:0]
	}
}

func BenchmarkEncodeInt3Byte(b *testing.B) {
	var data []byte
	var err error
	for i := 0; i < b.N; i++ {
		data, err = appendElement(data, int(1000000))
		if err != nil {
			b.Fatal(err)
		}
		data = data[:0]
	}
}

func BenchmarkEncodeInt4Byte(b *testing.B) {
	var data []byte
	var err error
	for i := 0; i < b.N; i++ {
		data, err = appendElement(data, int(200000000))
		if err != nil {
			b.Fatal(err)
		}
		data = data[:0]
	}
}
