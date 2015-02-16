package index

import (
	"fmt"
	"strconv"
	"testing"

	"git.encryptio.com/kvl"
	"git.encryptio.com/kvl/backend/ram"
)

func TestReindex(t *testing.T) {
	const dbSize = 1915
	onlyOddFlipIndex := func(p kvl.Pair) []kvl.Pair {
		if p.IsZero() {
			return nil
		}
		last := p.Value[len(p.Value)-1]
		if (last-'0')%2 == 0 {
			return nil
		}
		return []kvl.Pair{{p.Value, p.Key}}
	}
	flipIndex := func(p kvl.Pair) []kvl.Pair {
		if p.IsZero() {
			return nil
		}
		return []kvl.Pair{{p.Value, p.Key}}
	}

	db := ram.New()

	_, err := db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		inner, _, err := Open(ctx, onlyOddFlipIndex)
		if err != nil {
			return nil, err
		}

		for i := 0; i < dbSize; i++ {
			key := []byte(strconv.FormatInt(int64(i), 10))
			value := []byte(strconv.FormatInt(int64(i+dbSize), 10))

			err := inner.Set(kvl.Pair{key, value})
			if err != nil {
				return nil, err
			}
		}

		return nil, nil
	})
	if err != nil {
		t.Fatalf("Couldn't insert initial data: %v", err)
	}

	stats, err := Reindex(db, flipIndex, nil)
	t.Logf("Reindex for flipIndex complete: %v", stats)
	if err != nil {
		t.Fatalf("Couldn't reindex to flipIndex: %v", err)
	}

	_, err = db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		_, index, err := Open(ctx, flipIndex)
		if err != nil {
			return nil, err
		}

		for i := 0; i < dbSize; i++ {
			value := []byte(strconv.FormatInt(int64(i), 10))
			key := []byte(strconv.FormatInt(int64(i+dbSize), 10))

			p, err := index.Get(key)
			if err != nil {
				return nil, err
			}

			if !p.Equal(kvl.Pair{key, value}) {
				return nil, fmt.Errorf("index pairs not equal %#v != %#v",
					p, kvl.Pair{key, value})
			}
		}

		return nil, nil
	})
	if err != nil {
		t.Fatalf("Couldn't check data after flipIndex reindex: %v", err)
	}

	stats, err = Reindex(db, onlyOddFlipIndex, nil)
	t.Logf("Reindex for onlyOddFlipIndex complete: %v", stats)
	if err != nil {
		t.Fatalf("Couldn't reindex to onlyOddFlipIndex: %v", err)
	}

	found := 0
	_, err = db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		found = 0

		_, index, err := Open(ctx, onlyOddFlipIndex)
		if err != nil {
			return nil, err
		}

		for i := 0; i < dbSize; i++ {
			value := []byte(strconv.FormatInt(int64(i), 10))
			key := []byte(strconv.FormatInt(int64(i+dbSize), 10))

			p, err := index.Get(key)
			if err == kvl.ErrNotFound {
				continue
			}
			if err != nil {
				return nil, err
			}
			found++

			if !p.Equal(kvl.Pair{key, value}) {
				return nil, fmt.Errorf("index pairs not equal %#v != %#v",
					p, kvl.Pair{key, value})
			}
		}

		return nil, nil
	})
	if err != nil {
		t.Fatalf("Couldn't check data after onlyOddFlipIndex reindex: %v", err)
	}

	t.Logf("Found after reindex to onlyOddFlipIndex: %v", found)

	if found < dbSize/2 {
		t.Fatalf("Found too few index pairs, wanted at least %v, got %v",
			dbSize/2, found)
	}
	if found > dbSize*6/10 {
		t.Fatalf("Found too many index pairs, wanted at most %v, got %v",
			dbSize*6/10, found)
	}
}
