package psql

import (
	"database/sql"

	_ "github.com/lib/pq"

	"git.encryptio.com/kvl"
)

type DB struct {
	sqlDB *sql.DB
}

func Open(dsn string) (*DB, error) {
	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	err = sqlDB.Ping()
	if err != nil {
		sqlDB.Close()
		return nil, err
	}

	db := &DB{sqlDB: sqlDB}

	err = db.ensureTable()
	if err != nil {
		sqlDB.Close()
		return nil, err
	}

	return db, nil
}

func (db *DB) Close() {
	db.sqlDB.Close()
}

func (db *DB) ensureTable() error {
	_, err := db.sqlDB.Exec(
		"CREATE TABLE IF NOT EXISTS " +
			"data (" +
			"    key bytea not null primary key," +
			"    value bytea not null" +
			") " +
			"WITH ( OIDS=FALSE, fillfactor=90 )")
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) RunTx(tx kvl.Tx) (interface{}, error) {
	for {
		ret, err, again := db.tryTx(tx, false)
		if !again {
			return ret, err
		}
	}
}

func (db *DB) RunReadTx(tx kvl.Tx) (interface{}, error) {
	for {
		ret, err, again := db.tryTx(tx, true)
		if !again {
			return ret, err
		}
	}
}

func (db *DB) tryTx(tx kvl.Tx, readonly bool) (interface{}, error, bool) {
	sqlTx, err := db.sqlDB.Begin()
	if err != nil {
		return nil, err, false
	}

	var roClause string
	if readonly {
		roClause = ", READ ONLY"
	}

	_, err = sqlTx.Exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE" + roClause)
	if err != nil {
		return nil, err, false
	}

	ctx := &ctx{sqlTx: sqlTx, readonly: readonly}

	ret, err := tx(ctx)
	if err != nil {
		ctx.checkErr(err)
		err2 := sqlTx.Rollback()
		ctx.checkErr(err2)
		// err2 is not returned; the first error is probably more important
		return nil, err, ctx.needsRetry
	}

	err = sqlTx.Commit()
	if err != nil {
		ctx.checkErr(err)
		return nil, err, ctx.needsRetry
	}

	return ret, nil, ctx.needsRetry
}
