package kvl

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	ErrNotFound   = errors.New("key not found")
	ErrReadOnlyTx = errors.New("transaction not opened for writing")
)

// A Tx is a serializable transactional operation.
//
// If a Tx returns a non-nil error, the transaction will be rolled back. If the
// database engine has detected a serializability conflict, the transaction
// will be rolled back. If neither of these happen (no database conflict and a
// nil error return from the Tx), the transaction is committed.
//
// Txs should not cause sideeffects, because they may be called multiple times
// before RunTx or RunReadTx return, regardless of if any operation on the Ctx
// returns an error.
//
// Use closures to pass extra values into a transaction and/or return them.
// Note that because transactions may fail midway through and be retried, you
// may need to initialize your closed-over return variables at the start of the
// transaction.
type Tx func(Ctx) error

// A DB allows access to serializable transactions.
//
// During RunTx and RunReadTx, if the transaction has any serializability
// conflicts, an operation may (but is not required to) return a non-nil error,
// and the Tx will be called again until it succeeds.
type DB interface {
	// RunTx starts a read/write transaction.
	RunTx(Tx) error

	// RunReadTx starts a read-only transaction. Attempted write operations will
	// return ErrReadOnlyTx.
	RunReadTx(Tx) error

	// Close the DB. Concurrently executing transactions' behavior is not
	// defined, and should be avoided.
	//
	// The DB should not be used after Close is called.
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

type Ctx interface {
	Get(key []byte) (Pair, error)
	Range(query RangeQuery) ([]Pair, error)
	Set(p Pair) error
	Delete(key []byte) error
}
