package index

import (
	"fmt"
	"testing"

	"git.encryptio.com/kvl"
	"git.encryptio.com/kvl/backend/ram"
)

func TestIndexBasics(t *testing.T) {
	db := ram.New()

	flipIndexer := func(p kvl.Pair) []kvl.Pair {
		if p.IsZero() {
			return nil
		}
		return []kvl.Pair{kvl.Pair{p.Value, p.Key}}
	}

	_, err := db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		inner, _, err := Open(ctx, flipIndexer)
		if err != nil {
			return nil, err
		}

		err = inner.Set(kvl.Pair{[]byte("hello"), []byte("world")})
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	if err != nil {
		t.Fatalf("Couldn't insert data: %v", err)
	}

	_, err = db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		inner, index, err := Open(ctx, flipIndexer)
		if err != nil {
			return nil, err
		}

		p, err := inner.Get([]byte("hello"))
		if err != nil {
			return nil, err
		}

		if string(p.Value) != "world" {
			return nil, fmt.Errorf(
				"wrong value in inner Get, wanted \"world\" but got %#v",
				string(p.Value))
		}

		p, err = index.Get([]byte("world"))
		if err != nil {
			return nil, err
		}

		if string(p.Value) != "hello" {
			return nil, fmt.Errorf(
				"wrong value in index Get, wanted \"hello\" but got %#v",
				string(p.Value))
		}

		return nil, nil
	})
	if err != nil {
		t.Fatalf("Couldn't read data after first write: %v", err)
	}

	_, err = db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		inner, _, err := Open(ctx, flipIndexer)
		if err != nil {
			return nil, err
		}

		err = inner.Set(kvl.Pair{[]byte("hello"), []byte("there")})
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	if err != nil {
		t.Fatalf("Couldn't edit data: %v", err)
	}

	_, err = db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		_, index, err := Open(ctx, flipIndexer)
		if err != nil {
			return nil, err
		}

		p, err := index.Get([]byte("there"))
		if err != nil {
			return nil, err
		}

		if string(p.Value) != "hello" {
			return nil, fmt.Errorf(
				"wrong value in index Get, wanted \"hello\" but got %#v",
				string(p.Value))
		}

		p, err = index.Get([]byte("world"))
		if err != kvl.ErrNotFound {
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("Didn't get expected ErrNotFound")
		}

		return nil, nil
	})
	if err != nil {
		t.Fatalf("Couldn't read data after second write: %v", err)
	}
}
