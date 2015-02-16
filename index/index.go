package index

import (
	"bytes"
	"errors"
	"fmt"

	"git.encryptio.com/kvl"
	"git.encryptio.com/kvl/tuple"
)

var (
	ErrUnexpectedlyMissingEntry = errors.New("an index entry was unexpectedly missing")
	ErrUnexpectedlyPresentEntry = errors.New("an index entry was unexpectedly present")
)

type Indexer func(kvl.Pair) []kvl.Pair

type Index struct {
	dataCtx  kvl.Ctx
	indexCtx kvl.Ctx
	fn       Indexer
}

type ctxWrap struct {
	*Index
}

var (
	dataPrefix  = tuple.MustAppend(nil, "data")
	indexPrefix = tuple.MustAppend(nil, "index")
)

func Open(ctx kvl.Ctx, fn Indexer) (kvl.Ctx, *Index, error) {
	vals := fn(kvl.Pair{})
	if len(vals) > 0 {
		return nil, nil, fmt.Errorf(
			"indexer function must return an empty list " +
				"when called with a zero Pair")
	}

	dataCtx := kvl.SubCtx(ctx, dataPrefix)
	indexCtx := kvl.SubCtx(ctx, indexPrefix)

	index := &Index{
		dataCtx:  dataCtx,
		indexCtx: indexCtx,
		fn:       fn,
	}

	return ctxWrap{index}, index, nil
}

func (i *Index) Get(key []byte) (kvl.Pair, error) {
	return i.indexCtx.Get(key)
}

func (i *Index) Range(query kvl.RangeQuery) ([]kvl.Pair, error) {
	return i.indexCtx.Range(query)
}

func (w ctxWrap) Get(key []byte) (kvl.Pair, error) {
	return w.dataCtx.Get(key)
}

func (w ctxWrap) Range(query kvl.RangeQuery) ([]kvl.Pair, error) {
	return w.dataCtx.Range(query)
}

func (w ctxWrap) Set(newP kvl.Pair) error {
	oldP, err := w.dataCtx.Get(newP.Key)
	if err != nil && err != kvl.ErrNotFound {
		return err
	}

	err = w.switchIndexValues(oldP, newP)
	if err != nil {
		return err
	}

	return w.dataCtx.Set(newP)
}

func (w ctxWrap) Delete(key []byte) error {
	oldP, err := w.dataCtx.Get(key)
	if err != nil {
		return err
	}

	err = w.switchIndexValues(oldP, kvl.Pair{})
	if err != nil {
		return err
	}

	return w.dataCtx.Delete(key)
}

func (w ctxWrap) switchIndexValues(oldP, newP kvl.Pair) error {
	oldI := w.fn(oldP)
	newI := w.fn(newP)

	// TODO: replace this O(n*m) algorithm with an O(n+m) algorithm

	// search for index pairs to remove
	for _, p := range oldI {
		found := false
		for _, p2 := range newI {
			// NB: if values differ on a matching key, handle as Set
			if bytes.Equal(p.Key, p2.Key) {
				found = true
				break
			}
		}

		if !found {
			err := w.indexCtx.Delete(p.Key)
			if err != nil {
				if err == kvl.ErrNotFound {
					return ErrUnexpectedlyMissingEntry
				}
				return err
			}
		}
	}

	// search for index pairs to add
	for _, p := range newI {
		found := false
		for _, p2 := range oldI {
			if p.Equal(p2) {
				found = true
				break
			}
		}

		if !found {
			_, err := w.indexCtx.Get(p.Key)
			if err != kvl.ErrNotFound {
				if err != nil {
					return err
				}
				return ErrUnexpectedlyPresentEntry
			}

			err = w.indexCtx.Set(p)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
