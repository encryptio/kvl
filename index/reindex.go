package index

import (
	"fmt"

	"git.encryptio.com/kvl"
	"git.encryptio.com/kvl/keys"
)

const (
	REINDEX_DELETE = 1 << iota

	bloomSize              = 1024 * 1024 * 8 * 8 // 8MiB of bits
	reindexChunkSize       = 100
	reindexDeleteChunkSize = 1000
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
// If the REINDEX_DELETE flag is given, Reindex also probabalistically removes
// old index entries. It is not guaranteed to remove all old index entries.
//
// Passing REINDEX_DELETE is may cause inconsistent indexes if parallel writes
// are occurring, because the reindexing process is spread across many
// transactions. If temporary index inconsistency is acceptable, run Reindex
// with the REINDEX_DELETE flag and then immediately run it again without that
// flag to add the incorrectly removed Pairs back to the database.
//
// The progress channel passed, if any, will be sent partial ReindexStats as
// the reindex occurs.
func Reindex(db kvl.DB, fn Indexer, options uint64,
	progress chan<- ReindexStats) (ReindexStats, error) {

	deleteFlag := (options & REINDEX_DELETE) != 0

	var stats ReindexStats

	var bloom *bloom
	if deleteFlag {
		bloom = newBloom(bloomSize)
	}

	var from []byte
	done := false
	for !done {
		ret, err := db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
			dataCtx := kvl.SubCtx(ctx, dataPrefix)
			indexCtx := kvl.SubCtx(ctx, indexPrefix)

			var stats ReindexStats

			ps, err := dataCtx.Range(kvl.RangeQuery{
				Low:   from,
				Limit: reindexChunkSize,
			})
			if err != nil {
				return nil, err
			}

			for _, p := range ps {
				stats.DataRowsChecked++
				indexPairs := fn(p)

				for _, ip := range indexPairs {
					ipDB, err := indexCtx.Get(ip.Key)
					if err != nil && err != kvl.ErrNotFound {
						return nil, err
					}
					if !ip.Equal(ipDB) {
						if err == kvl.ErrNotFound {
							stats.Created++
						} else {
							stats.Edited++
						}
						err = indexCtx.Set(ip)
						if err != nil {
							return nil, err
						}
					}

					if deleteFlag {
						bloom.Set(ip.Key)
					}
				}
			}

			if len(ps) < reindexChunkSize {
				done = true
			} else {
				from = keys.LexNext(ps[len(ps)-1].Key)
			}

			stats.Transactions++

			return stats, nil
		})
		if err != nil {
			return stats, err
		}

		newStats := ret.(ReindexStats)
		newStats.addTo(&stats)

		if progress != nil {
			progress <- stats
		}
	}

	if deleteFlag {
		stats.DeletionMissRate = bloom.Fullness()

		from = nil
		done = false
		for !done {
			ret, err := db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
				indexCtx := kvl.SubCtx(ctx, indexPrefix)

				var stats ReindexStats

				ps, err := indexCtx.Range(kvl.RangeQuery{
					Low:   from,
					Limit: reindexDeleteChunkSize,
				})
				if err != nil {
					return nil, err
				}

				for _, p := range ps {
					stats.IndexRowsChecked++
					if !bloom.Test(p.Key) {
						err := indexCtx.Delete(p.Key)
						if err != nil {
							return nil, err
						}
						stats.Deleted++
					}
				}

				if len(ps) < reindexDeleteChunkSize {
					done = true
				} else {
					from = keys.LexNext(ps[len(ps)-1].Key)
				}

				stats.Transactions++

				return stats, nil
			})
			if err != nil {
				return stats, err
			}

			newStats := ret.(ReindexStats)
			newStats.addTo(&stats)

			if progress != nil {
				progress <- stats
			}
		}
	}

	return stats, nil
}
