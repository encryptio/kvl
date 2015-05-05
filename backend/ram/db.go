package ram

import (
	"sync"
	"sync/atomic"

	"git.encryptio.com/kvl"
)

type DB struct {
	mu       sync.RWMutex
	headData *data
}

func New() *DB {
	return &DB{
		headData: &data{make(map[string]*string, 0), 0, nil},
	}
}

func (db *DB) Close() {
}

func (db *DB) RunTx(tx kvl.Tx) (interface{}, error) {
	for {
		data, err, again := db.tryTx(tx, false)
		if !again {
			return data, err
		}
	}
}

func (db *DB) RunReadTx(tx kvl.Tx) (interface{}, error) {
	for {
		data, err, again := db.tryTx(tx, true)
		if !again {
			return data, err
		}
	}
}

func (db *DB) tryTx(tx kvl.Tx, readonly bool) (interface{}, error, bool) {
	db.mu.Lock()
	myData := db.headData
	myData.refcount++
	db.mu.Unlock()

	ctx := newCtx(myData, &db.mu, readonly)
	ret, err := tx(ctx)

	db.mu.Lock()
	if !ctx.aborted && err == nil {
		// want to commit
		// see if anything we depend on has changed
		conflicting := false

		newData := db.headData
	OUTER:
		for newData != myData {
			for _, k := range ctx.lockKeys {
				_, found := newData.contents[k]
				if found {
					conflicting = true
					break OUTER
				}
			}

			for _, r := range ctx.lockRanges {
				for k := range newData.contents {
					if k >= r.low && (r.high == "" || k < r.high) {
						conflicting = true
						break OUTER
					}
				}
			}

			newData = newData.inner
		}

		if conflicting {
			ctx.aborted = true
		} else {
			if len(ctx.toCommit) > 0 {
				db.headData = &data{ctx.toCommit, 0, db.headData}
			}
		}
	}
	myData.refcount--
	db.tryMerge()
	db.mu.Unlock()

	if ctx.aborted {
		atomic.AddUint64(&theCounters.Aborts, 1)
	} else if err != nil {
		atomic.AddUint64(&theCounters.Errors, 1)
	} else {
		atomic.AddUint64(&theCounters.Commits, 1)
	}

	return ret, err, ctx.aborted
}

func (db *DB) tryMerge() {
	// assumes mu.Lock is held
	for db.tryMergeOne() {
	}
}

func (db *DB) tryMergeOne() bool {
	// assumes mu.Lock is held

	if db.headData == nil {
		return false
	}
	if db.headData.inner == nil {
		return false
	}

	// go to the last two links in the chain
	second := db.headData
	last := second.inner
	for last.inner != nil {
		last = last.inner
		second = second.inner
	}

	if last.refcount != 0 || second.refcount != 0 {
		// one of them is in use, can't change them
		return false
	}

	// merge last into second
	for k, v := range last.contents {
		_, found := second.contents[k]
		if !found {
			second.contents[k] = v
		}
	}

	// remove last from the chain
	second.inner = nil

	// clean out any deletions from second, they cannot mask anything anymore
	for k, v := range second.contents {
		if v == nil {
			delete(second.contents, k)
		}
	}

	return true
}
