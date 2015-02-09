package ram

import (
	"sync/atomic"
)

// updated with atomic ops
var theCounters Counters

// Counters keep track of operation counts on all ram DBs.
//
// The global counters are kept using atomic operations and thus are valid in
// the face of parallel transactions, but the GetCounters and ResetCounters
// functions do not operate on the set of all counters atomically.
type Counters struct {
	Aborts uint64
	Errors uint64
	Commits uint64
}

func GetCounters() Counters {
	c := Counters{}
	c.Aborts = atomic.LoadUint64(&theCounters.Aborts)
	c.Errors = atomic.LoadUint64(&theCounters.Errors)
	c.Commits = atomic.LoadUint64(&theCounters.Commits)
	return c
}

func ResetCounters() {
	atomic.StoreUint64(&theCounters.Aborts, 0)
	atomic.StoreUint64(&theCounters.Errors, 0)
	atomic.StoreUint64(&theCounters.Commits, 0)
}
