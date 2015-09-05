package tests

import (
	"testing"
	"time"

	"git.encryptio.com/kvl"
)

func skipWatchIfUnsupported(t *testing.T, db kvl.DB) {
	wr, err := db.WatchTx(func(kvl.Ctx) error { return nil })
	if err == kvl.ErrWatchUnsupported {
		t.Skipf("Watch unsupported on %T", db)
	}
	wr.Close()
}

func testWatchBasic(t *testing.T, db kvl.DB) {
	skipWatchIfUnsupported(t, db)

	wr, err := db.WatchTx(func(ctx kvl.Ctx) error {
		_, err := ctx.Get([]byte("asdf"))
		if err == kvl.ErrNotFound {
			err = nil
		}
		return err
	})
	if err != nil {
		t.Fatalf("Couldn't watch: %v", err)
	}

	var gotErr error
	timedOut := false
	done := make(chan struct{})
	go func() {
		select {
		case <-wr.Done():
			gotErr = wr.Error()
		case <-time.After(time.Second):
			timedOut = true
		}
		wr.Close()
		close(done)
	}()

	err = db.RunTx(func(ctx kvl.Ctx) error {
		return ctx.Set(kvl.Pair{[]byte("asdf"), []byte("value")})
	})
	if err != nil {
		t.Errorf("Couldn't set value: %v", err)
	}

	<-done

	if timedOut {
		t.Errorf("Timed out while waiting for WatchTx result")
	}
	if gotErr != nil {
		t.Errorf("Got error from WatchResult: %v", gotErr)
	}
}
