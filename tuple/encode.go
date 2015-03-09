package tuple

import (
	"errors"
	"math/big"
	"reflect"
)

// Tuples are byte strings of this form:
//     nil:
//         0b00000000
//     booleans:
//         0b0000001B -> value is B
//     ints:
//         ints must use the shortest representation of their value.
//         0b01PESSSS ...?
//             P -> if false, flip all successive bits of the int (including extra bytes) and return 0-decoded.
//             E -> if 0, then the int is internal and SSSS is the bit representation of the int.
//                  if 1, then SSSS is the number of bytes after this one that define the int, minus one.
//                        unless SSSS is 1111, in which case the "big int" mode is switched on; see below.
//         bigint mode:
//             starting at one byte, the length of the int (in bytes) is written. if the length does not fit
//             in one byte minus one, 0xff is written and the length is decoded at twice the byte size.
//             so a 100000-byte integer would have the following header:
//                 0xff 0xffff 0x000186a0
//             followed by 100000 bytes defining the integer.
//     bytestrings:
//         0b10000000 ...
//             escape nulls and 0x01 with 0x01 (e.g. "x\x00y\x01z" becomes "x\x01\x00y\x01\x01z")
//             ends in null

var ErrUnsupportedType = errors.New("unsupported type for tuple operation")

func MustAppend(t []byte, vs ...interface{}) []byte {
	ret, err := Append(t, vs...)
	if err != nil {
		panic(err)
	}
	return ret
}

func Append(t []byte, vs ...interface{}) ([]byte, error) {
	for _, v := range vs {
		var err error
		t, err = appendElement(t, v)
		if err != nil {
			return nil, err
		}
	}
	return t, nil
}

func appendElement(t []byte, v interface{}) ([]byte, error) {
	switch v := v.(type) {
	case nil:
		return append(t, 0), nil
	case bool:
		b := byte(0x02)
		if v {
			b = 0x03
		}
		return append(t, b), nil
	case int:
		return appendInt(t, int64(v)), nil
	case int8:
		return appendInt(t, int64(v)), nil
	case int16:
		return appendInt(t, int64(v)), nil
	case int32:
		return appendInt(t, int64(v)), nil
	case int64:
		return appendInt(t, v), nil
	case uint:
		return appendUint(t, uint64(v)), nil
	case uint8:
		return appendUint(t, uint64(v)), nil
	case uint16:
		return appendUint(t, uint64(v)), nil
	case uint32:
		return appendUint(t, uint64(v)), nil
	case uint64:
		return appendUint(t, v), nil
	case *big.Int:
		return appendBigInt(t, v), nil
	case string:
		return appendString(t, v), nil
	case []byte:
		return appendBytes(t, v), nil
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Array &&
			rv.Type().Elem().Kind() == reflect.Uint8 {

			slice := make([]byte, rv.Len())
			reflect.Copy(reflect.ValueOf(slice), rv)
			return appendBytes(t, slice), nil
		}
		return nil, ErrUnsupportedType
	}
}

func appendInt(t []byte, i int64) []byte {
	if i >= 0 {
		if i <= 15 {
			return append(t, 0x60|byte(i))
		} else if i <= 255 {
			return append(t, 0x70, byte(i))
		} else if i <= 65535 {
			return append(t, 0x71, byte(i>>8), byte(i))
		} else if i <= 16777215 {
			return append(t, 0x72, byte(i>>16), byte(i>>8), byte(i))
		} else if i <= 4294967295 {
			return append(t, 0x73, byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
		}
	} else {
		if i >= -15 {
			return append(t, 0x50|(byte(-i)^0x0F))
		} else if i >= -255 {
			return append(t, 0x4F, ^byte(-i))
		} else if i >= -65535 {
			return append(t, 0x4E, ^byte((-i)>>8), ^byte(-i))
		} else if i >= -16777215 {
			return append(t, 0x4D, ^byte((-i)>>16), ^byte((-i)>>8), ^byte(-i))
		} else if i >= -4294967295 {
			return append(t, 0x4C, ^byte((-i)>>24), ^byte((-i)>>16), ^byte((-i)>>8), ^byte(-i))
		}
	}

	var n big.Int
	n.SetInt64(i)
	return appendBigInt(t, &n)
}

func appendUint(t []byte, i uint64) []byte {
	if i <= 15 {
		return append(t, 0x60|byte(i))
	} else if i <= 255 {
		return append(t, 0x70, byte(i))
	} else if i <= 65535 {
		return append(t, 0x71, byte(i>>8), byte(i))
	} else if i <= 16777215 {
		return append(t, 0x72, byte(i>>16), byte(i>>8), byte(i))
	} else if i <= 4294967295 {
		return append(t, 0x73, byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	}

	var n big.Int
	n.SetUint64(i)
	return appendBigInt(t, &n)
}

func appendBigInt(t []byte, n *big.Int) []byte {
	negHeaderMask := byte(0)
	negDataMask := byte(0)
	if n.Sign() < 0 {
		negHeaderMask = 0x3F // includes negation bit
		negDataMask = 0xFF
		o := &big.Int{}
		o.Neg(n)
		n = o
	}

	bl := n.BitLen()

	if bl <= 4 {
		return append(t, (0x60|byte(n.Uint64()))^negHeaderMask)
	}

	byteLen := (bl + 7) / 8
	byteLenHeader := byteLen - 1

	if byteLenHeader < 15 {
		t = append(t, (0x70|byte(byteLenHeader))^negHeaderMask)
	} else {
		t = append(t, 0x7F^negHeaderMask)
		if byteLenHeader <= 254 {
			t = append(t, byte(byteLenHeader)^negDataMask)
		} else if byteLenHeader <= 65534 {
			t = append(t,
				0xFF^negDataMask,
				byte(byteLenHeader>>8)^negDataMask,
				byte(byteLenHeader)^negDataMask)
		} else if byteLenHeader <= 1000000000 {
			t = append(t,
				0xFF^negDataMask,
				0xFF^negDataMask,
				0xFF^negDataMask,
				byte(byteLenHeader>>24)^negDataMask,
				byte(byteLenHeader>>16)^negDataMask,
				byte(byteLenHeader>>8)^negDataMask)
		} else {
			panic("holy shit that's a huge int")
		}
	}

	data := n.Bytes()
	if byteLen != len(data) {
		panic("byteLen != len(n.Bytes())")
	}

	if negDataMask != 0 {
		for i := 0; i < len(data); i++ {
			data[i] ^= negDataMask
		}
	}

	t = append(t, data...)

	return t
}

func appendString(t []byte, s string) []byte {
	t = append(t, 0x80)
	for i := 0; i < len(s); i++ {
		if s[i] <= 1 {
			t = append(t, 1)
		}
		t = append(t, s[i])
	}
	return append(t, 0)
}

func appendBytes(t []byte, s []byte) []byte {
	t = append(t, 0x80)
	for i := 0; i < len(s); i++ {
		if s[i] <= 1 {
			t = append(t, 1)
		}
		t = append(t, s[i])
	}
	return append(t, 0)
}
