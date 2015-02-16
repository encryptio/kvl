package kvl

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	ErrNotFound = errors.New("key not found")
)

type Tx func(Ctx) (interface{}, error)

type DB interface {
	RunTx(Tx) (interface{}, error)
	Close()
}

type Pair struct {
	Key, Value []byte
}

func (p Pair) IsZero() bool {
	return len(p.Key) == 0 && len(p.Value) == 0
}

func (p Pair) Equal(q Pair) bool {
	return bytes.Equal(p.Key, q.Key) && bytes.Equal(p.Value, q.Value)
}

func (p Pair) String() string {
	return fmt.Sprintf("Pair{%#v -> %#v}", string(p.Key), string(p.Value))
}

type RangeQuery struct {
	Low, High  []byte
	Limit      int
	Descending bool
}

type RCtx interface {
	Get(key []byte) (Pair, error)
	Range(query RangeQuery) ([]Pair, error)
}

type Ctx interface {
	RCtx
	Set(p Pair) error
	Delete(key []byte) error
}
