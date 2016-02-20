package bolt

import (
	"github.com/encryptio/kvl"
	"github.com/boltdb/bolt"
)

func init() {
	kvl.RegisterBackend("bolt", Open)
}

var bucketName = []byte("kvl")

type db struct {
	b *bolt.DB
}

func Open(dsn string) (kvl.DB, error) {
	b, err := bolt.Open(dsn, 0666, nil)
	if err != nil {
		return nil, err
	}

	return db{b: b}, nil
}

func (db db) Close() {
	db.b.Close()
}

func (db db) RunTx(tx kvl.Tx) error {
	return db.b.Update(func(btx *bolt.Tx) error {
		b, err := btx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}

		return tx(ctx{bucket: b, readonly: false})
	})
}

func (db db) RunReadTx(tx kvl.Tx) error {
	return db.b.View(func(btx *bolt.Tx) error {
		// NB: may be nil
		b := btx.Bucket(bucketName)

		return tx(ctx{bucket: b, readonly: true})
	})
}

func (db db) WatchTx(tx kvl.Tx) (kvl.WatchResult, error) {
	return nil, kvl.ErrWatchUnsupported
}
