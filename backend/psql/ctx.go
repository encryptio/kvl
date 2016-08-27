package psql

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"

	"github.com/encryptio/kvl"
)

type ctx struct {
	sqlTx      *sql.Tx
	needsRetry bool
	readonly   bool
}

func (c *ctx) checkErr(err error) {
	if pgErr, ok := err.(*pq.Error); ok {
		switch pgErr.Code {
		case "40001": // serialization_failure
			c.needsRetry = true
		case "23505": // unique_violation; occurs when key creation races with itself
			c.needsRetry = true
		case "40P01": // deadlock_detected
			c.needsRetry = true
		}
	}
}

func (c *ctx) Get(key []byte) (kvl.Pair, error) {
	var p kvl.Pair

	row := c.sqlTx.QueryRow("SELECT key, value FROM data WHERE key = $1", key)
	err := row.Scan(&p.Key, &p.Value)
	if err != nil {
		c.checkErr(err)
		if err == sql.ErrNoRows {
			return p, kvl.ErrNotFound
		}
		return p, err
	}

	return p, nil
}

func (c *ctx) Set(p kvl.Pair) error {
	if c.readonly {
		return kvl.ErrReadOnlyTx
	}

	// Upsert
	_, err := c.sqlTx.Exec(
		"WITH "+
			"upsert AS ("+
			"    UPDATE data SET value = $2 WHERE key = $1 RETURNING *"+
			") "+
			"INSERT INTO data (key, value) SELECT $1, $2 "+
			"    WHERE NOT EXISTS (SELECT * FROM upsert)", p.Key, p.Value)
	if err != nil {
		c.checkErr(err)
		return err
	}

	return nil
}

func (c *ctx) Delete(key []byte) error {
	if c.readonly {
		return kvl.ErrReadOnlyTx
	}

	res, err := c.sqlTx.Exec("DELETE FROM data WHERE key = $1", key)
	if err != nil {
		c.checkErr(err)
		return err
	}

	count, err := res.RowsAffected()
	if err != nil {
		c.checkErr(err)
		return err
	}

	if count == 0 {
		return kvl.ErrNotFound
	}

	if count > 1 {
		panic("single deletion matched multiple rows")
	}

	return nil
}

func (c *ctx) Range(q kvl.RangeQuery) ([]kvl.Pair, error) {
	params := make([]interface{}, 0, 2)
	query := "SELECT key, value FROM data WHERE TRUE"
	if len(q.Low) > 0 {
		query += fmt.Sprintf(" AND key >= $%v", len(params)+1)
		params = append(params, q.Low)
	}
	if len(q.High) > 0 {
		query += fmt.Sprintf(" AND KEY < $%v", len(params)+1)
		params = append(params, q.High)
	}
	if q.Descending {
		query += " ORDER BY key DESC"
	} else {
		query += " ORDER BY key ASC"
	}
	if q.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %v", q.Limit)
	}

	rows, err := c.sqlTx.Query(query, params...)
	if err != nil {
		c.checkErr(err)
		return nil, err
	}
	defer rows.Close()

	pairs := make([]kvl.Pair, 0, 16)
	for rows.Next() {
		var k, v []byte
		err = rows.Scan(&k, &v)
		if err != nil {
			c.checkErr(err)
			return nil, err
		}

		pairs = append(pairs, kvl.Pair{k, v}) // TODO: needs copy?
	}
	err = rows.Err()
	if err != nil {
		c.checkErr(err)
		return nil, err
	}

	return pairs, nil
}
