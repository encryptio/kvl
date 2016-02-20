package index

import (
	"fmt"

	"github.com/encryptio/kvl"
	"github.com/encryptio/kvl/keys"
)

const (
	bloomSize              = 1024 * 1024 * 8 * 64 // 64MiB of bits
	reindexChunkSize       = 100
	reindexDeleteChunkSize = 1000
	reindexDeleteKeyMemory = 1024 * 1024 * 128 // 128MiB of expired keys
)

type ReindexStats struct {
	DataRowsChecked  uint64
	IndexRowsChecked uint64

	Created      uint64
	Edited       uint64
	Deleted      uint64
	Transactions uint64

	// The expected fraction of index entries that should be deleted but were
	// missed. Zero unless REINDEX_DELETE is passed.
	DeletionMissRate float64
}

func (rs ReindexStats) String() string {
	var deletedClause string
	if rs.DeletionMissRate != 0 || rs.Deleted != 0 {
		deletedClause = fmt.Sprintf(", deleted %v (miss rate %.3f%%)",
			rs.Deleted, rs.DeletionMissRate*100)
	}

	var indexRowsClause string
	if rs.IndexRowsChecked != 0 {
		indexRowsClause = fmt.Sprintf(" and %v index rows", rs.IndexRowsChecked)
	}

	return fmt.Sprintf(
		"checked %v data rows%v, created %v, edited %v%v index pairs in %v transactions",
		rs.DataRowsChecked, indexRowsClause, rs.Created, rs.Edited,
		deletedClause, rs.Transactions)
}

func (rs ReindexStats) addTo(r2 *ReindexStats) {
	r2.DataRowsChecked += rs.DataRowsChecked
	r2.IndexRowsChecked += rs.IndexRowsChecked
	r2.Created += rs.Created
	r2.Edited += rs.Edited
	r2.Deleted += rs.Deleted
	r2.Transactions += rs.Transactions
}

// Reindex checks all data pairs in the database given and makes sure the index
// entries for all data pairs are consistent with the Indexer function given.
//
// Extraneous index entries might be missed for deletion; you should repeatedly
// call Reindex until the Deleted field of the ReindexStats returned is low
// enough for your liking.
//
// Note that there are race conditions if you are repeatedly adding and removing
// the same index entry during Reindexing; it may be unexpectedly removed if
// you're unlucky.
//
// The progress channel passed, if any, will be sent partial ReindexStats as
// the reindex occurs.
func Reindex(db kvl.DB, fn Indexer, progress chan<- ReindexStats) (ReindexStats, error) {
	var stats ReindexStats
	bloom := newBloom(bloomSize)

	// 1st pass: Search for missing index entries and build bloom filter of
	// index keys.
	var from []byte
	var done bool
	for !done {
		var txStats ReindexStats
		err := db.RunTx(func(ctx kvl.Ctx) error {
			done = false
			txStats = ReindexStats{}

			dataCtx := kvl.SubCtx(ctx, dataPrefix)
			indexCtx := kvl.SubCtx(ctx, indexPrefix)

			ps, err := dataCtx.Range(kvl.RangeQuery{
				Low:   from,
				Limit: reindexChunkSize,
			})
			if err != nil {
				return err
			}

			for _, p := range ps {
				txStats.DataRowsChecked++
				indexPairs := fn(p)

				for _, ip := range indexPairs {
					ipDB, err := indexCtx.Get(ip.Key)
					if err != nil && err != kvl.ErrNotFound {
						return err
					}
					if !ip.Equal(ipDB) {
						if err == kvl.ErrNotFound {
							txStats.Created++
						} else {
							txStats.Edited++
						}
						err = indexCtx.Set(ip)
						if err != nil {
							return err
						}
					}

					bloom.Set(ip.Key)
				}
			}

			if len(ps) < reindexChunkSize {
				done = true
			} else {
				from = keys.LexNext(ps[len(ps)-1].Key)
			}

			txStats.Transactions++

			return nil
		})
		if err != nil {
			return stats, err
		}

		txStats.addTo(&stats)

		if progress != nil {
			progress <- stats
		}
	}

	// 2nd pass: Search index keys for things missing in the bloom filter and
	// collect them in a map.
	from = nil
	done = false
	stats.DeletionMissRate = bloom.Fullness()
	wantRemove := make(map[string]struct{}, 100)
	wantRemoveSize := 0
	for !done {
		var thisRemove map[string]struct{}
		var txStats ReindexStats
		err := db.RunTx(func(ctx kvl.Ctx) error {
			done = false
			txStats = ReindexStats{}

			thisRemove = make(map[string]struct{}, 100)
			indexCtx := kvl.SubCtx(ctx, indexPrefix)

			ps, err := indexCtx.Range(kvl.RangeQuery{
				Low:   from,
				Limit: reindexDeleteChunkSize,
			})
			if err != nil {
				return err
			}

			for _, p := range ps {
				txStats.IndexRowsChecked++
				if !bloom.Test(p.Key) {
					thisRemove[string(p.Key)] = struct{}{}
				}
			}

			if len(ps) < reindexDeleteChunkSize {
				done = true
			} else {
				from = keys.LexNext(ps[len(ps)-1].Key)
			}

			txStats.Transactions++

			return nil
		})
		if err != nil {
			return stats, err
		}

		for k := range thisRemove {
			wantRemove[k] = struct{}{}
			wantRemoveSize += len(k) + 8
		}

		txStats.addTo(&stats)

		if progress != nil {
			progress <- stats
		}

		if wantRemoveSize > reindexDeleteKeyMemory {
			break
		}
	}

	if wantRemoveSize > 0 {
		// 3rd pass: Scan data again to ensure the index keys we want to remove
		// are not newly added.
		from = nil
		done = false
		for !done {
			var txStats ReindexStats
			err := db.RunTx(func(ctx kvl.Ctx) error {
				done = false
				txStats = ReindexStats{}

				dataCtx := kvl.SubCtx(ctx, dataPrefix)

				ps, err := dataCtx.Range(kvl.RangeQuery{
					Low:   from,
					Limit: reindexChunkSize,
				})
				if err != nil {
					return err
				}

				for _, p := range ps {
					txStats.DataRowsChecked++
					indexPairs := fn(p)

					for _, ip := range indexPairs {
						delete(wantRemove, string(ip.Key))
					}
				}

				if len(ps) < reindexChunkSize {
					done = true
				} else {
					from = keys.LexNext(ps[len(ps)-1].Key)
				}

				txStats.Transactions++

				return nil
			})
			if err != nil {
				return stats, err
			}

			txStats.addTo(&stats)

			if progress != nil {
				progress <- stats
			}
		}

		// 4th pass: remove index entries we want to remove.
		for len(wantRemove) > 0 {
			var txStats ReindexStats
			err := db.RunTx(func(ctx kvl.Ctx) error {
				indexCtx := kvl.SubCtx(ctx, indexPrefix)
				txStats = ReindexStats{}

				for k := range wantRemove {
					delete(wantRemove, k)

					err := indexCtx.Delete([]byte(k))
					if err != nil {
						return err
					}

					txStats.Deleted++
					if txStats.Deleted > reindexChunkSize {
						break
					}
				}

				txStats.Transactions++

				return nil
			})
			if err != nil {
				return stats, err
			}

			txStats.addTo(&stats)

			if progress != nil {
				progress <- stats
			}
		}
	}

	return stats, nil
}
