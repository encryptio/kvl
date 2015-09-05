package kvl

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	ErrNotFound         = errors.New("key not found")
	ErrReadOnlyTx       = errors.New("transaction not opened for writing")
	ErrWatchUnsupported = errors.New("watch operations not supported on this database")
)

// A Tx is a serializable transactional operation.
//
// If a Tx returns a non-nil error, the transaction will be rolled back. If the
// database engine has detected a serializability conflict, the transaction
// will be rolled back and then retried. If neither of these happen (no database
// conflict and a nil error return from the Tx), the transaction is committed.
//
// Txs should not cause sideeffects, because they may be called multiple times
// before RunTx, RunReadTx, or WatchTx return, regardless of if any operation on
// the Ctx returns an error.
//
// Use closures to pass extra values into a transaction and/or return them.
// Note that because transactions may fail midway through and be retried, you
// may need to initialize your closed-over return variables at the start of the
// transaction.
type Tx func(Ctx) error

// A DB allows access to serializable transactions.
//
// During RunTx, RunReadTx, and WatchTx if the transaction has any consistency
// conflicts, an operation may (but is not required to) return a non-nil error,
// and the Tx will be called again until it succeeds. Note that even read-only
// transactions may be retried.
type DB interface {
	// RunTx starts a read/write transaction.
	RunTx(Tx) error

	// RunReadTx starts a read-only transaction. Attempted write operations will
	// return ErrReadOnlyTx.
	RunReadTx(Tx) error

	// WatchTx runs a read-only transaction once, like RunReadTx, but
	// additionally, atomically watches for changes in the underlying database to
	// the keys/ranges read by the Tx.
	//
	// Precisely one of the return values will be non-nil.
	//
	// If an error is returned from the Tx, it is passed up to the caller of
	// WatchTx.
	//
	// The caller of WatchTx is responsible for calling Close on the WatchResult
	// (if returned.)
	//
	// If watching is not supported by the backend, the transaction is not run and
	// ErrWatchUnsupported is returned.
	WatchTx(Tx) (WatchResult, error)

	// Close the DB. Concurrently executing transactions' and watches' behavior is
	// not defined.
	//
	// The DB should not be used after Close is called.
	Close()
}

// A WatchResult allows you to wait for changes to the keys/ranges used in a
// read query called with DB.WatchTx().
type WatchResult interface {
	// Done returns the channel that will be closed after the keys/ranges that
	// were queried in the WatchTx call change, or if an error occurs while
	// waiting for that notification.
	//
	// In some backend implementations, the Done channel might occasionally be
	// closed when the keys/ranges that were queried have not changed.
	Done() <-chan struct{}

	// Error returns a non-nil error after the Done channel has been closed if
	// an error occured while waiting for updates on the database.
	//
	// Depending on your use case, it may be okay to ignore errors during watches,
	// and simply re-execute WatchTx.
	Error() error

	// Close closes the Done channel and releases the resources held by the watch.
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
