package tests

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"git.encryptio.com/kvl"
)

func testShuffleShardedIncrement(t *testing.T, s kvl.DB) {
	// This test runs several goroutines which try to increment a random
	// key in the db (possibly swapping it with another random key at the
	// same time).
	//
	// After that happens, it ensures that the sum of all values is the
	// total number of increments.

	const (
		transactionsPerGoroutine = 100
		parallelism              = 10
		rowCount                 = 3000
	)

	err := clearDB(s)
	if err != nil {
		t.Fatalf("Couldn't clear DB: %v", err)
	}

	err = s.RunTx(func(ctx kvl.Ctx) error {
		zero := []byte("0")
		for i := 0; i < rowCount; i++ {
			key := []byte(fmt.Sprintf("%v", i))
			err := ctx.Set(kvl.Pair{key, zero})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Couldn't add testing rows: %v", err)
	}

	errCh := make(chan error, parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			for j := 0; j < transactionsPerGoroutine; j++ {
				err := s.RunTx(func(ctx kvl.Ctx) error {
					// pick two random rows
					var idA, idB int
					for idA == idB {
						idA = rand.Intn(rowCount)
						idB = rand.Intn(rowCount)
					}
					keyA := []byte(fmt.Sprintf("%v", idA))
					keyB := []byte(fmt.Sprintf("%v", idB))

					// read them
					pairA, err := ctx.Get(keyA)
					if err != nil {
						return err
					}
					pairB, err := ctx.Get(keyB)
					if err != nil {
						return err
					}

					// maybe swap their contents
					if rand.Intn(4) == 0 {
						pairA, pairB = pairB, pairA
					}

					// increment one of them
					num, err := strconv.ParseInt(string(pairA.Value), 10, 0)
					if err != nil {
						return err
					}
					num++
					pairA.Value = []byte(strconv.FormatInt(num, 10))

					// write both back
					err = ctx.Set(pairA)
					if err != nil {
						return err
					}
					err = ctx.Set(pairB)
					if err != nil {
						return err
					}

					return nil
				})
				if err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}()
	}

	for i := 0; i < parallelism; i++ {
		err := <-errCh
		if err != nil {
			t.Fatalf("Couldn't run incrementer transaction: %v = %#v", err, err)
		}
	}

	var total int
	err = s.RunReadTx(func(ctx kvl.Ctx) error {
		total = 0

		pairs, err := ctx.Range(kvl.RangeQuery{})
		if err != nil {
			return err
		}

		for _, pair := range pairs {
			val, err := strconv.ParseInt(string(pair.Value), 10, 0)
			if err != nil {
				return err
			}
			total += int(val)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Couldn't run total transaction: %v", err)
	}

	err = clearDB(s)
	if err != nil {
		t.Fatalf("Couldn't clear DB: %v", err)
	}

	if total != parallelism*transactionsPerGoroutine {
		t.Errorf("shuffle sharded increment got %v at end, wanted %v",
			total, parallelism*transactionsPerGoroutine)
	}
}

func testRangeMaxRandomReplacement(t *testing.T, s kvl.DB) {
	// This test runs several goroutines which each read the entire
	// keyspace, find the maximum value, then add one to the max and store
	// that into a random location in the keyspace.
	//
	// Afterwards, it find the maximum and ensures that it is equal to the
	// number of increments done.

	const (
		transactionsPerGoroutine = 20
		parallelism              = 4
		rowCount                 = 3000
	)

	err := clearDB(s)
	if err != nil {
		t.Fatalf("Couldn't clear DB: %v", err)
	}

	err = s.RunTx(func(ctx kvl.Ctx) error {
		zero := []byte("0")
		for i := 0; i < rowCount; i++ {
			key := []byte(fmt.Sprintf("%v", i))
			err := ctx.Set(kvl.Pair{key, zero})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Couldn't add testing rows: %v", err)
	}

	errCh := make(chan error, parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			for j := 0; j < transactionsPerGoroutine; j++ {
				err := s.RunTx(func(ctx kvl.Ctx) error {
					// find the max value of all pairs
					var max int64
					pairs, err := ctx.Range(kvl.RangeQuery{})
					if err != nil {
						return err
					}
					for _, pair := range pairs {
						num, err := strconv.ParseInt(string(pair.Value), 10, 0)
						if err != nil {
							return err
						}
						if num > max {
							max = num
						}
					}

					// increment
					max++

					// write to a random pair
					var pair kvl.Pair
					id := int64(rand.Intn(rowCount))
					pair.Key = []byte(strconv.FormatInt(id, 10))
					pair.Value = []byte(strconv.FormatInt(max, 10))

					err = ctx.Set(pair)
					if err != nil {
						return err
					}

					return nil
				})
				if err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}()
	}

	for i := 0; i < parallelism; i++ {
		err := <-errCh
		if err != nil {
			t.Fatalf("Couldn't run incrementer transaction: %v = %#v", err, err)
		}
	}

	var max int64
	err = s.RunReadTx(func(ctx kvl.Ctx) error {
		max = 0

		pairs, err := ctx.Range(kvl.RangeQuery{})
		if err != nil {
			return err
		}

		for _, pair := range pairs {
			val, err := strconv.ParseInt(string(pair.Value), 10, 0)
			if err != nil {
				return err
			}
			if val > max {
				max = val
			}
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Couldn't run total transaction: %v", err)
	}

	err = clearDB(s)
	if err != nil {
		t.Fatalf("Couldn't clear DB: %v", err)
	}

	if max != parallelism*transactionsPerGoroutine {
		t.Errorf("shuffle sharded increment got %v at end, wanted %v",
			max, parallelism*transactionsPerGoroutine)
	}
}
