package bolt

import (
	"git.encryptio.com/kvl"
	"github.com/boltdb/bolt"
)

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

func (db db) RunTx(tx kvl.Tx) (interface{}, error) {
	var ret interface{}
	err := db.b.Update(func(btx *bolt.Tx) error {
		ret = nil

		b, err := btx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}

		ret, err = tx(ctx{bucket: b, readonly: false})
		return err
	})
	return ret, err
}

func (db db) RunReadTx(tx kvl.Tx) (interface{}, error) {
	var ret interface{}
	err := db.b.View(func(btx *bolt.Tx) error {
		ret = nil

		b := btx.Bucket(bucketName)

		var err error
		ret, err = tx(ctx{bucket: b, readonly: true})
		return err
	})
	return ret, err
}
