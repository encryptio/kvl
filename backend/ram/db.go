package ram

import (
	"sync"
	"sync/atomic"

	"github.com/encryptio/kvl"
)

type DB struct {
	mu       sync.RWMutex
	headData *data
	watches  []*watcher
}

func New() kvl.DB {
	return &DB{
		headData: &data{make(map[string]*string, 0), 0, nil},
	}
}

func (db *DB) Close() {
}

func (db *DB) RunTx(tx kvl.Tx) error {
	for {
		err, _, again := db.tryTx(tx, false, false)
		if !again {
			return err
		}
	}
}

func (db *DB) RunReadTx(tx kvl.Tx) error {
	for {
		err, _, again := db.tryTx(tx, true, false)
		if !again {
			return err
		}
	}
}

func (db *DB) WatchTx(tx kvl.Tx) (kvl.WatchResult, error) {
	for {
		err, wr, again := db.tryTx(tx, true, true)
		if !again {
			return wr, err
		}
	}
}

func (db *DB) tryTx(tx kvl.Tx, readonly bool, setupWatch bool) (error, kvl.WatchResult, bool) {
	var wr kvl.WatchResult

	db.mu.Lock()
	myData := db.headData
	myData.refcount++
	db.mu.Unlock()

	ctx := newCtx(myData, &db.mu, readonly)
	err := tx(ctx)

	db.mu.Lock()

	if !ctx.aborted && err == nil {
		// want to commit
		// see if anything we depend on has changed
		conflicting := false

		newData := db.headData
		for newData != myData {
			if ctx.locks.conflicts(newData.contents) {
				conflicting = true
				break
			}

			newData = newData.inner
		}

		if conflicting {
			ctx.aborted = true
		} else {
			// commit!

			if len(ctx.toCommit) > 0 {
				db.headData = &data{ctx.toCommit, 0, db.headData}

				for i := 0; i < len(db.watches); i++ {
					if db.watches[i].locks.conflicts(ctx.toCommit) {
						db.watches[i].trigger()
						db.watches = append(db.watches[:i], db.watches[i+1:]...)
						i--
					}
				}
			}

			if setupWatch {
				watcher := db.newWatcher(ctx.locks)
				db.watches = append(db.watches, watcher)
				wr = watcher
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

	return err, wr, ctx.aborted
}

func (db *DB) removeWatcher(w *watcher) {
	db.mu.Lock()
	for i := 0; i < len(db.watches); i++ {
		if db.watches[i] == w {
			db.watches = append(db.watches[:i], db.watches[i+1:]...)
			i--
		}
	}
	db.mu.Unlock()
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
