package tests

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"git.encryptio.com/kvl"
	"git.encryptio.com/kvl/backend/bolt"
)

func openBolt(t *testing.T) (string, kvl.DB) {
	dir, err := ioutil.TempDir("", "kvl_bolt_test")
	if err != nil {
		t.Fatalf("Couldn't create temporary dir: %v", err)
	}

	db, err := bolt.Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("Couldn't open bolt driver: %v", err)
	}

	return dir, db
}

func TestBoltShuffleShardedIncrement(t *testing.T) {
	dir, db := openBolt(t)
	defer os.RemoveAll(dir)
	defer db.Close()
	testShuffleShardedIncrement(t, db)
}

func TestBoltRangeMaxRandomReplacement(t *testing.T) {
	dir, db := openBolt(t)
	defer os.RemoveAll(dir)
	defer db.Close()
	testRangeMaxRandomReplacement(t, db)
}

func TestBoltConsistencyWithRAM(t *testing.T) {
	dir, db := openBolt(t)
	defer os.RemoveAll(dir)
	defer db.Close()
	testRandomOpConsistencyWithRAM(t, db)
}

func TestBoltWatchBasic(t *testing.T) {
	dir, db := openBolt(t)
	defer os.RemoveAll(dir)
	defer db.Close()
	testWatchBasic(t, db)
}
