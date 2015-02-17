package tuple

import (
	"fmt"
	"math/big"
	"reflect"
)

type CannotConvertError struct {
	From, To interface{}
}

func (e CannotConvertError) Error() string {
	return fmt.Sprintf("cannot convert %T to %T", e.From, e.To)
}

type IntRangeError struct {
	From, To interface{}
}

func (e IntRangeError) Error() string {
	return fmt.Sprintf("integer %v is out of range of destination type %T", e.From, e.To)
}

type ArrayLengthError struct {
	From, To       interface{}
	FromLen, ToLen int
}

func (e ArrayLengthError) Error() string {
	return fmt.Sprintf("length of target %v does not match length of source %v",
		e.FromLen, e.ToLen)
}

var maxInt64, maxUint64, minInt64 big.Int

func init() {
	maxInt64.SetInt64(1<<63 - 1)
	minInt64.SetInt64(-(1 << 63))
	maxUint64.SetUint64(1<<64 - 1)
}

func convertAssign(to, from interface{}) error {
	if to, ok := to.(*interface{}); ok {
		*to = from
		return nil
	}

	switch from := from.(type) {
	case []byte:
		switch to := to.(type) {
		case *string:
			*to = string(from)
			return nil
		case *[]byte:
			*to = from
			return nil
		default:
			rv := reflect.ValueOf(to)
			if rv.Kind() == reflect.Ptr &&
				rv.Elem().Kind() == reflect.Array &&
				rv.Elem().Type().Elem().Kind() == reflect.Uint8 {

				if len(from) != rv.Elem().Len() {
					return ArrayLengthError{
						From:    from,
						To:      to,
						FromLen: len(from),
						ToLen:   rv.Elem().Len(),
					}
				}

				reflect.Copy(rv.Elem(), reflect.ValueOf(from))
				return nil
			}
		}
	case bool:
		to, ok := to.(*bool)
		if ok {
			*to = from
			return nil
		}
	case int64:
		switch to.(type) {
		case *big.Int:
			to.(*big.Int).SetInt64(from)
			return nil
		case *int64, *int32, *int16, *int8, *int:
			var verify int64

			switch to := to.(type) {
			case *int64:
				*to = from
				verify = *to
			case *int32:
				*to = int32(from)
				verify = int64(*to)
			case *int16:
				*to = int16(from)
				verify = int64(*to)
			case *int8:
				*to = int8(from)
				verify = int64(*to)
			case *int:
				*to = int(from)
				verify = int64(*to)
			default:
				panic("not reached")
			}

			if verify != from {
				return IntRangeError{from, to}
			}
			return nil

		case *uint64, *uint32, *uint16, *uint8, *uint:
			if from > 1<<63-1 {
				return IntRangeError{from, to}
			}
			input := uint64(from)

			var verify uint64

			switch to := to.(type) {
			case *uint64:
				*to = input
				verify = *to
			case *uint32:
				*to = uint32(input)
				verify = uint64(*to)
			case *uint16:
				*to = uint16(input)
				verify = uint64(*to)
			case *uint8:
				*to = uint8(input)
				verify = uint64(*to)
			case *uint:
				*to = uint(input)
				verify = uint64(*to)
			default:
				panic("not reached")
			}

			if verify != input {
				return IntRangeError{input, to}
			}
			return nil
		}
	case *big.Int:
		switch to.(type) {
		case *big.Int:
			to.(*big.Int).Set(from)
			return nil
		case *int64, *int32, *int16, *int8, *int:
			if from.Cmp(&maxInt64) > 0 || from.Cmp(&minInt64) < 0 {
				return IntRangeError{from, to}
			}
			i := from.Int64()

			var verify int64

			switch to := to.(type) {
			case *int64:
				*to = i
				verify = *to
			case *int32:
				*to = int32(i)
				verify = int64(*to)
			case *int16:
				*to = int16(i)
				verify = int64(*to)
			case *int8:
				*to = int8(i)
				verify = int64(*to)
			case *int:
				*to = int(i)
				verify = int64(*to)
			default:
				panic("not reached")
			}

			if verify != i {
				return IntRangeError{from, to}
			}
			return nil

		case *uint64, *uint32, *uint16, *uint8, *uint:
			if from.Cmp(&big.Int{}) < 0 || from.Cmp(&maxUint64) > 0 {
				return IntRangeError{from, to}
			}
			i := from.Uint64()

			var verify uint64

			switch to := to.(type) {
			case *uint64:
				*to = i
				verify = *to
			case *uint32:
				*to = uint32(i)
				verify = uint64(*to)
			case *uint16:
				*to = uint16(i)
				verify = uint64(*to)
			case *uint8:
				*to = uint8(i)
				verify = uint64(*to)
			case *uint:
				*to = uint(i)
				verify = uint64(*to)
			default:
				panic("not reached")
			}

			if verify != i {
				return IntRangeError{from, to}
			}
			return nil
		}
	}

	return CannotConvertError{from, to}
}
