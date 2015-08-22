// for use with github.com/dvyukov/go-fuzz
// +build gofuzz

package tuple

import (
	"bytes"
	"fmt"
)

func unpackInterface(data []byte) ([]interface{}, error) {
	values := []interface{}{}
	for len(data) > 0 {
		var value interface{}
		left, err := UnpackIntoPartial(data, &value)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
		data = left
	}
	return values, nil
}

func Fuzz(data []byte) int {
	values, err := unpackInterface(data)
	if err != nil {
		if values != nil {
			panic("values != nil on err")
		}
		return 0
	}

	data2 := MustAppend(nil, values...)
	if !bytes.Equal(data, data2) {
		panic(fmt.Sprintf("round-tripped data %#v was not original data %#v", data2, data))
	}

	return 1
}
