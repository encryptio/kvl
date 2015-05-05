package ram

import (
	"bytes"
	"sort"
	"sync"

	"git.encryptio.com/kvl"
)

type keyRange struct{ low, high string }

type ctx struct {
	mu         *sync.RWMutex
	data       *data
	toCommit   map[string]*string
	lockKeys   []string
	lockRanges []keyRange
	aborted    bool
	readonly   bool
}

func newCtx(head *data, mu *sync.RWMutex, readonly bool) *ctx {
	return &ctx{
		mu:       mu,
		data:     head,
		toCommit: make(map[string]*string),
		readonly: readonly,
	}
}

func (c *ctx) Get(key []byte) (kvl.Pair, error) {
	sKey := string(key)
	v, ok := c.toCommit[string(sKey)]
	if !ok {
		c.mu.RLock()
		v = c.data.get(sKey)
		c.mu.RUnlock()
	}

	if v != nil {
		return kvl.Pair{[]byte(string(key)), []byte(*v)}, nil
	}

	return kvl.Pair{}, kvl.ErrNotFound
}

func (c *ctx) Set(p kvl.Pair) error {
	if c.readonly {
		return kvl.ErrReadOnlyTx
	}

	sKey := string(p.Key)
	sValue := string(p.Value)

	c.lockKeys = append(c.lockKeys, sKey)
	c.toCommit[sKey] = &sValue
	return nil
}

func (c *ctx) Delete(key []byte) error {
	if c.readonly {
		return kvl.ErrReadOnlyTx
	}

	sKey := string(key)

	c.lockKeys = append(c.lockKeys, sKey)
	c.toCommit[sKey] = nil
	return nil
}

func (c *ctx) Range(query kvl.RangeQuery) ([]kvl.Pair, error) {
	kr := keyRange{string(query.Low), string(query.High)}
	c.lockRanges = append(c.lockRanges, kr)

	c.mu.RLock()
	mapParts := c.data.getRange(kr)
	c.mu.RUnlock()

	for k, v := range c.toCommit {
		if k >= kr.low && (kr.high == "" || k < kr.high) {
			mapParts[k] = v
		}
	}

	sliceParts := make([]kvl.Pair, 0, len(mapParts))
	for k, v := range mapParts {
		if v == nil {
			continue
		}

		sliceParts = append(sliceParts, kvl.Pair{[]byte(k), []byte(*v)})
	}

	if query.Descending {
		sort.Sort(sort.Reverse(pairSlice(sliceParts)))
	} else {
		sort.Sort(pairSlice(sliceParts))
	}

	if query.Limit > 0 && len(sliceParts) > query.Limit {
		sliceParts = sliceParts[:query.Limit]
	}

	return sliceParts, nil
}

type pairSlice []kvl.Pair

func (s pairSlice) Len() int      { return len(s) }
func (s pairSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s pairSlice) Less(i, j int) bool {
	return bytes.Compare(s[i].Key, s[j].Key) < 0
}
