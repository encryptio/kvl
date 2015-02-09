package tuple

import (
	"errors"
	"math/big"
)

var (
	ErrTooShort        = errors.New("data too short")
	ErrBadTupleData    = errors.New("bad tuple data")
	ErrTooManyElements = errors.New("too many tuple elements for output slice")
	ErrTooFewElements  = errors.New("too few tuple elements for output slice")
	ErrIntTooBig       = errors.New("int too big")
)

func UnpackInto(t []byte, vs ...interface{}) error {
	left, err := UnpackIntoPartial(t, vs...)
	if err == nil && len(left) > 0 {
		return ErrTooFewElements
	}
	return err
}

func UnpackIntoPartial(t []byte, vs ...interface{}) ([]byte, error) {
	for _, v := range vs {
		if len(t) == 0 {
			return t, nil
		}

		i, err := UnpackElement(t, v)
		if err != nil {
			return t, err
		}

		t = t[i:]
	}

	return t, nil
}

func UnpackElement(t []byte, v interface{}) (int, error) {
	n, v2, err := decodeElement(t)
	if err != nil {
		return 0, err
	}
	err = convertAssign(v, v2)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func decodeElement(t []byte) (int, interface{}, error) {
	if len(t) == 0 {
		return 0, nil, ErrTooShort
	}

	if t[0] == 0 {
		return 1, nil, nil
	} else if t[0] == 2 {
		return 1, false, nil
	} else if t[0] == 3 {
		return 1, true, nil
	} else if (t[0] & 0xc0) == 0x40 {
		return decodeInt(t)
	} else if t[0] == 0x80 {
		return decodeByteString(t)
	} else {
		return 0, nil, ErrBadTupleData
	}
}

func decodeInt(t []byte) (int, interface{}, error) {
	eaten := 1
	header := t[0]
	dataMask := byte(0)
	t = t[1:]

	p := (header & 0x20) == 0x20
	if !p {
		header ^= 0x1F
		dataMask = 0xFF
	}

	e := (header & 0x10) == 0x10

	if !e {
		// inline int in header

		v := int64(header & 0x0F)
		if !p {
			v = -v
		}
		return 1, v, nil
	}

	byteLen := int(header&0x0F) + 1
	if byteLen == 16 {
		// bigint mode

		lenLen := 1
		for {
			if len(t) < lenLen {
				return 0, nil, ErrBadTupleData
			}

			foundNonFF := false
			for i := 0; i < lenLen; i++ {
				if t[i]^dataMask != 0xff {
					foundNonFF = true
				}
			}

			if foundNonFF {
				break
			}

			eaten += lenLen
			t = t[lenLen:]

			lenLen *= 2

			if lenLen > 4 {
				return 0, nil, ErrIntTooBig
			}
		}

		byteLen = 0

		for i := 0; i < lenLen; i++ {
			byteLen = (byteLen << 8) + int(t[i]^dataMask)
		}

		byteLen++

		eaten += lenLen
		t = t[lenLen:]
	}

	if len(t) < byteLen {
		return 0, nil, ErrBadTupleData
	}

	if byteLen <= 7 {
		// fits in int64 for sure

		v := int64(0)

		for i := 0; i < byteLen; i++ {
			v = (v << 8) + int64(t[i]^dataMask)
		}

		if !p {
			v = -v
		}

		return eaten + byteLen, v, nil
	}

	// can't guarantee the integer defined fits in an int64 easily
	// big.Int will work for anything

	n := &big.Int{}
	b := &big.Int{}

	for i := 0; i < byteLen; i++ {
		b.SetUint64(uint64(t[i] ^ dataMask))
		n.Lsh(n, 8).Add(n, b)
	}

	if !p {
		b.SetInt64(0)
		n.Sub(b, n)
	}

	return eaten + byteLen, n, nil
}

func decodeByteString(t []byte) (int, interface{}, error) {
	eaten := 1
	t = t[1:]

	out := make([]byte, 0, 4)
	for len(t) > 0 {
		eaten++
		b := t[0]
		t = t[1:]

		if b == 0 {
			return eaten, out, nil
		} else if b == 1 {
			if len(t) == 0 {
				// escape at end of data
				return 0, nil, ErrBadTupleData
			}

			eaten++
			out = append(out, t[0])
			t = t[1:]
		} else {
			out = append(out, b)
		}
	}

	// unclosed string
	return 0, nil, ErrBadTupleData
}
