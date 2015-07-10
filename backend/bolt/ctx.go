package bolt

import (
	"bytes"

	"git.encryptio.com/kvl"
	"github.com/boltdb/bolt"
)

type ctx struct {
	bucket   *bolt.Bucket // if nil, assume empty db. Only possible if readonly is true.
	readonly bool
}

func dupBytes(s []byte) []byte {
	n := make([]byte, len(s))
	copy(n, s)
	return n
}

func (ctx ctx) Get(key []byte) (kvl.Pair, error) {
	if ctx.bucket == nil {
		return kvl.Pair{}, kvl.ErrNotFound
	}

	val := ctx.bucket.Get(key)
	if val == nil {
		return kvl.Pair{}, kvl.ErrNotFound
	} else {
		return kvl.Pair{dupBytes(key), val}, nil
	}
}

func (ctx ctx) Range(query kvl.RangeQuery) ([]kvl.Pair, error) {
	if ctx.bucket == nil {
		return nil, nil
	}

	cur := ctx.bucket.Cursor()
	ret := []kvl.Pair{}
	if query.Descending {
		var k, v []byte
		if len(query.High) > 0 {
			k, v = cur.Seek(query.High)
			if k == nil {
				k, v = cur.Last()
			}
		} else {
			k, v = cur.Last()
		}

		for ; k != nil && (len(query.Low) == 0 || bytes.Compare(k, query.Low) >= 0); k, v = cur.Prev() {
			if len(query.High) == 0 || bytes.Compare(k, query.High) < 0 {
				ret = append(ret, kvl.Pair{dupBytes(k), dupBytes(v)})
			}
			if query.Limit > 0 && len(ret) >= query.Limit {
				break
			}
		}
	} else {
		// ascending
		var k, v []byte
		if len(query.Low) > 0 {
			k, v = cur.Seek(query.Low)
		} else {
			k, v = cur.First()
		}

		for ; k != nil && (len(query.High) == 0 || bytes.Compare(k, query.High) < 0); k, v = cur.Next() {
			ret = append(ret, kvl.Pair{dupBytes(k), dupBytes(v)})
			if query.Limit > 0 && len(ret) >= query.Limit {
				break
			}
		}
	}

	return ret, nil
}

func (ctx ctx) Set(p kvl.Pair) error {
	if ctx.readonly {
		return kvl.ErrReadOnlyTx
	}

	return ctx.bucket.Put(p.Key, p.Value)
}

func (ctx ctx) Delete(key []byte) error {
	if ctx.readonly {
		return kvl.ErrReadOnlyTx
	}

	data := ctx.bucket.Get(key)
	if data == nil {
		return kvl.ErrNotFound
	}

	return ctx.bucket.Delete(key)
}
