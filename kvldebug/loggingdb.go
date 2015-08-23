package kvldebug

import (
	"log"

	"git.encryptio.com/kvl"
)

var _ kvl.DB = &LoggingDB{}

type LoggingDB struct {
	Inner kvl.DB
}

func (l *LoggingDB) RunTx(tx kvl.Tx) error {
	return l.Inner.RunTx(func(ctx kvl.Ctx) (err error) {
		logCtx := &LoggingCtx{ctx}
		log.Printf("%p.RunTx(%p) starting as %p", l, tx, ctx)
		defer log.Printf("%p.RunTx(%p) returning %v", l, tx, err)
		err = tx(logCtx)
		return
	})
}

func (l *LoggingDB) RunReadTx(tx kvl.Tx) error {
	return l.Inner.RunReadTx(func(ctx kvl.Ctx) (err error) {
		logCtx := &LoggingCtx{ctx}
		log.Printf("%p.RunReadTx(%p) starting as %p", l, tx, ctx)
		defer log.Printf("%p.RunReadTx(%p) returning %v", l, tx, err)
		err = tx(logCtx)
		return
	})
}

func (l *LoggingDB) Close() {
	l.Inner.Close()
	log.Printf("%p.Close()", l)
}

type LoggingCtx struct {
	Inner kvl.Ctx
}

func (l *LoggingCtx) Get(key []byte) (kvl.Pair, error) {
	p, err := l.Inner.Get(key)
	log.Printf("%p.Get(%#v) -> (%v, %v)", l, string(key), p, err)
	return p, err
}

func (l *LoggingCtx) Range(query kvl.RangeQuery) ([]kvl.Pair, error) {
	ps, err := l.Inner.Range(query)
	log.Printf("%p.Range(%#v) -> (%v, %v)", l, query, ps, err)
	return ps, err
}

func (l *LoggingCtx) Set(p kvl.Pair) error {
	err := l.Inner.Set(p)
	log.Printf("%p.Set(%#v) -> %v", l, p, err)
	return err
}

func (l *LoggingCtx) Delete(key []byte) error {
	err := l.Inner.Delete(key)
	log.Printf("%p.Delete(%v) -> %v", l, string(key), err)
	return err
}
