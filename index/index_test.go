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

	err := db.RunTx(func(ctx kvl.Ctx) error {
		inner, _, err := Open(ctx, flipIndexer)
		if err != nil {
			return err
		}

		err = inner.Set(kvl.Pair{[]byte("hello"), []byte("world")})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Couldn't insert data: %v", err)
	}

	err = db.RunTx(func(ctx kvl.Ctx) error {
		inner, index, err := Open(ctx, flipIndexer)
		if err != nil {
			return err
		}

		p, err := inner.Get([]byte("hello"))
		if err != nil {
			return err
		}

		if string(p.Value) != "world" {
			return fmt.Errorf(
				"wrong value in inner Get, wanted \"world\" but got %#v",
				string(p.Value))
		}

		p, err = index.Get([]byte("world"))
		if err != nil {
			return err
		}

		if string(p.Value) != "hello" {
			return fmt.Errorf(
				"wrong value in index Get, wanted \"hello\" but got %#v",
				string(p.Value))
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Couldn't read data after first write: %v", err)
	}

	err = db.RunTx(func(ctx kvl.Ctx) error {
		inner, _, err := Open(ctx, flipIndexer)
		if err != nil {
			return err
		}

		err = inner.Set(kvl.Pair{[]byte("hello"), []byte("there")})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Couldn't edit data: %v", err)
	}

	err = db.RunTx(func(ctx kvl.Ctx) error {
		_, index, err := Open(ctx, flipIndexer)
		if err != nil {
			return err
		}

		p, err := index.Get([]byte("there"))
		if err != nil {
			return err
		}

		if string(p.Value) != "hello" {
			return fmt.Errorf(
				"wrong value in index Get, wanted \"hello\" but got %#v",
				string(p.Value))
		}

		p, err = index.Get([]byte("world"))
		if err != kvl.ErrNotFound {
			if err != nil {
				return err
			}
			return fmt.Errorf("Didn't get expected ErrNotFound")
		}

		return nil
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

	err := db.RunTx(func(ctx kvl.Ctx) error {
		inner, index, err := Open(ctx, flipIndexer)
		if err != nil {
			return err
		}

		err = inner.Set(kvl.Pair{[]byte("a"), []byte("b")})
		if err != nil {
			return err
		}

		err = inner.Set(kvl.Pair{[]byte("c"), []byte("d")})
		if err != nil {
			return err
		}

		innerPairs, err := inner.Range(kvl.RangeQuery{})
		if err != nil {
			return err
		}
		wantInnerPairs := []kvl.Pair{
			kvl.Pair{[]byte("a"), []byte("b")},
			kvl.Pair{[]byte("c"), []byte("d")},
		}
		if !reflect.DeepEqual(innerPairs, wantInnerPairs) {
			return fmt.Errorf("After inserting data, wanted innerPairs = %v, but got %v",
				wantInnerPairs, innerPairs)
		}

		indexPairs, err := index.Range(kvl.RangeQuery{})
		if err != nil {
			return err
		}
		wantIndexPairs := []kvl.Pair{
			kvl.Pair{[]byte("b"), []byte("a")},
			kvl.Pair{[]byte("d"), []byte("c")},
		}
		if !reflect.DeepEqual(indexPairs, wantIndexPairs) {
			return fmt.Errorf("After inserting data, wanted indexPairs = %v, but got %v",
				wantIndexPairs, indexPairs)
		}

		err = inner.Delete([]byte("a"))
		if err != nil {
			return err
		}

		indexPairs, err = index.Range(kvl.RangeQuery{})
		if err != nil {
			return err
		}
		wantIndexPairs = []kvl.Pair{
			kvl.Pair{[]byte("d"), []byte("c")},
		}
		if !reflect.DeepEqual(indexPairs, wantIndexPairs) {
			return fmt.Errorf("After deleting one pair, wanted indexPairs = %v, but got %v",
				wantIndexPairs, indexPairs)
		}

		return nil
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

	err := db.RunTx(func(ctx kvl.Ctx) error {
		inner, _, err := Open(ctx, flipIndexer)
		if err != nil {
			return err
		}

		err = inner.Set(kvl.Pair{[]byte("a"), []byte("b")})
		if err != nil {
			return err
		}

		err = inner.Set(kvl.Pair{[]byte("c"), []byte("b")})
		if err != nil {
			return err
		}

		return nil
	})
	if err != ErrUnexpectedlyPresentEntry {
		t.Fatalf("Wanted %v, got err = %v", ErrUnexpectedlyPresentEntry, err)
	}
}
