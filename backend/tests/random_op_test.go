package tests

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"git.encryptio.com/kvl"
	"git.encryptio.com/kvl/backend/ram"
)

const (
	opTypeGet = iota
	opTypeRange
	opTypeSet
	opTypeDelete
)

type randOp struct {
	Type       int
	Key, Value []byte
	Range      kvl.RangeQuery
}

func genRandByteSlice(r *rand.Rand) []byte {
	s := make([]byte, r.Intn(2)+1)
	for i := range s {
		s[i] = byte(r.Intn(256))
	}
	return s
}

func genRandOp(r *rand.Rand) randOp {
	var op randOp

	if r.Intn(10) == 0 {
		op.Type = r.Intn(2) + opTypeSet
	} else {
		op.Type = r.Intn(2)
	}

	switch op.Type {
	case opTypeGet:
		op.Key = genRandByteSlice(r)
	case opTypeRange:
		op.Range.Descending = r.Intn(2) == 0
		op.Range.Limit = r.Intn(20) - 5
		// NB: about half of these ranges will be malformed
		if r.Intn(8) == 0 {
			op.Range.Low = nil
		} else {
			op.Range.Low = genRandByteSlice(r)
		}
		if r.Intn(8) == 0 {
			op.Range.High = nil
		} else {
			op.Range.High = genRandByteSlice(r)
		}
	case opTypeSet:
		op.Key = genRandByteSlice(r)
		op.Value = genRandByteSlice(r)
	case opTypeDelete:
		op.Key = genRandByteSlice(r)
	default:
		panic("not reached")
	}

	return op
}

type opResult struct {
	Return interface{}
	Error  error
}

func (op randOp) Do(ctx kvl.Ctx) opResult {
	switch op.Type {
	case opTypeGet:
		p, err := ctx.Get(op.Key)
		return opResult{p, err}
	case opTypeRange:
		ps, err := ctx.Range(op.Range)
		return opResult{ps, err}
	case opTypeSet:
		err := ctx.Set(kvl.Pair{op.Key, op.Value})
		return opResult{nil, err}
	case opTypeDelete:
		err := ctx.Delete(op.Key)
		return opResult{nil, err}
	default:
		panic("bad op type")
	}
}

func genOpTrace(r *rand.Rand, length int) []randOp {
	ops := make([]randOp, length)
	for i := range ops {
		ops[i] = genRandOp(r)
	}
	return ops
}

func runOpTrace(ops []randOp, db kvl.DB) []opResult {
	results := make([]opResult, len(ops))
	db.RunTx(func(ctx kvl.Ctx) error {
		for i, op := range ops {
			results[i] = op.Do(ctx)
		}
		return nil
	})
	return results
}

func testRandomOpConsistencyWithRAM(t *testing.T, db kvl.DB) {
	seed := time.Now().UnixNano()
	t.Logf("seed = %v", seed)

	for i := 0; i < 150; i++ {
		ram.ResetCounters()
		ramDB := ram.New()

		err := clearDB(db)
		if err != nil {
			t.Fatalf("Couldn't clear DB: %v", err)
		}

		src := rand.NewSource(seed + int64(i))
		r := rand.New(src)

		ops := genOpTrace(r, 10+i/10)
		resultsRAM := runOpTrace(ops, ramDB)
		resultsDB := runOpTrace(ops, db)

		compareTraces(t, ops, resultsRAM, resultsDB)
	}
}

func compareTraces(t *testing.T, ops []randOp, ramTrace, dbTrace []opResult) {
	for i := range ops {
		if !reflect.DeepEqual(ramTrace[i], dbTrace[i]) {
			t.Logf("For operation list:")
			for j, op := range ops {
				if j > i {
					break
				}
				t.Logf("op[%v] = %+v", j, op)
			}
			t.Fatalf("At this operation, DB returned %+v and RAM returned %+v", dbTrace[i], ramTrace[i])
			return
		}
	}
}
