package kvl

import (
	"bytes"
)

type subDB struct {
	db DB
	prefix []byte
}

// SubDB returns a DB whose RunTx returns a SubCtx with the given prefix.
func SubDB(db DB, prefix []byte) DB {
	if otherSub, ok := db.(subDB); ok {
		// chained subDBs are equivalent to a single subDB with a combined
		// prefix, but the single one has less key copy operations.
		return SubDB(otherSub.db, prependCopy(otherSub.prefix, prefix))
	}

	return subDB{db, prefix}
}

func (s subDB) RunTx(tx Tx) (interface{}, error) {
	return s.db.RunTx(func (ctx Ctx) (interface{}, error) {
		return tx(SubCtx(ctx, s.prefix))
	})
}

// Close operations are ignored on SubDBs. You must close the inner DB yourself
// at an appropriate time.
func (s subDB) Close() {
	// do nothing.
}

type subCtx struct {
	ctx Ctx
	prefix []byte
}

func SubCtx(ctx Ctx, prefix []byte) Ctx {
	if otherSub, ok := ctx.(subCtx); ok {
		// same chaining logic as SubDB
		return SubCtx(otherSub.ctx, prependCopy(otherSub.prefix, prefix))
	}

	return subCtx{ctx, prefix}
}

func prependCopy(a, b []byte) []byte {
	c := make([]byte, len(a) + len(b))
	copy(c, a)
	copy(c[len(a):], b)
	return c
}

func (s subCtx) Get(key []byte) (Pair, error) {
	p, err := s.ctx.Get(prependCopy(s.prefix, key))
	p.Key = bytes.TrimPrefix(p.Key, s.prefix)
	return p, err
}

func (s subCtx) Set(p Pair) error {
	return s.ctx.Set(Pair{prependCopy(s.prefix, p.Key), p.Value})
}

func (s subCtx) Delete(key []byte) error {
	return s.ctx.Delete(prependCopy(s.prefix, key))
}

func (s subCtx) Range(query RangeQuery) ([]Pair, error) {
	ps, err := s.ctx.Range(RangeQuery{
		Low: prependCopy(s.prefix, query.Low),
		High: prependCopy(s.prefix, query.High),
		Limit: query.Limit,
		Descending: query.Descending,
	})
	for i := range ps {
		ps[i].Key = bytes.TrimPrefix(ps[i].Key, s.prefix)
	}
	return ps, err
}