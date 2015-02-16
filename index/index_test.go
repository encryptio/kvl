package index

import (
	"fmt"
	"reflect"
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

func TestIndexDelete(t *testing.T) {
	db := ram.New()

	flipIndexer := func(p kvl.Pair) []kvl.Pair {
		if p.IsZero() {
			return nil
		}
		return []kvl.Pair{kvl.Pair{p.Value, p.Key}}
	}

	_, err := db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		inner, index, err := Open(ctx, flipIndexer)
		if err != nil {
			return nil, err
		}

		err = inner.Set(kvl.Pair{[]byte("a"), []byte("b")})
		if err != nil {
			return nil, err
		}

		err = inner.Set(kvl.Pair{[]byte("c"), []byte("d")})
		if err != nil {
			return nil, err
		}

		innerPairs, err := inner.Range(kvl.RangeQuery{})
		if err != nil {
			return nil, err
		}
		wantInnerPairs := []kvl.Pair{
			kvl.Pair{[]byte("a"), []byte("b")},
			kvl.Pair{[]byte("c"), []byte("d")},
		}
		if !reflect.DeepEqual(innerPairs, wantInnerPairs) {
			return nil, fmt.Errorf("After inserting data, wanted innerPairs = %v, but got %v",
				wantInnerPairs, innerPairs)
		}

		indexPairs, err := index.Range(kvl.RangeQuery{})
		if err != nil {
			return nil, err
		}
		wantIndexPairs := []kvl.Pair{
			kvl.Pair{[]byte("b"), []byte("a")},
			kvl.Pair{[]byte("d"), []byte("c")},
		}
		if !reflect.DeepEqual(indexPairs, wantIndexPairs) {
			return nil, fmt.Errorf("After inserting data, wanted indexPairs = %v, but got %v",
				wantIndexPairs, indexPairs)
		}

		err = inner.Delete([]byte("a"))
		if err != nil {
			return nil, err
		}

		indexPairs, err = index.Range(kvl.RangeQuery{})
		if err != nil {
			return nil, err
		}
		wantIndexPairs = []kvl.Pair{
			kvl.Pair{[]byte("d"), []byte("c")},
		}
		if !reflect.DeepEqual(indexPairs, wantIndexPairs) {
			return nil, fmt.Errorf("After deleting one pair, wanted indexPairs = %v, but got %v",
				wantIndexPairs, indexPairs)
		}

		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexDuplicates(t *testing.T) {
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

		err = inner.Set(kvl.Pair{[]byte("a"), []byte("b")})
		if err != nil {
			return nil, err
		}

		err = inner.Set(kvl.Pair{[]byte("c"), []byte("b")})
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	if err != ErrUnexpectedlyPresentEntry {
		t.Fatalf("Wanted %v, got err = %v", ErrUnexpectedlyPresentEntry, err)
	}
}
