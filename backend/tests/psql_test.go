package tests

import (
	"os"
	"testing"

	"github.com/encryptio/kvl"
	"github.com/encryptio/kvl/backend/psql"
)

func openPSQL(t *testing.T) kvl.DB {
	dsn := os.Getenv("PSQL_DSN")
	if dsn == "" {
		t.Skip("Set PSQL_DSN to enable PostgreSQL tests")
	}

	psqlDB, err := psql.Open(dsn)
	if err != nil {
		t.Fatalf("Couldn't open psql driver: %v", err)
	}
	return psqlDB
}

func TestPSQLShuffleShardedIncrement(t *testing.T) {
	s := openPSQL(t)
	defer s.Close()
	testShuffleShardedIncrement(t, s)
}

func TestPSQLRangeMaxRandomReplacement(t *testing.T) {
	s := openPSQL(t)
	defer s.Close()
	testRangeMaxRandomReplacement(t, s)
}

func TestPSQLConsistencyWithRAM(t *testing.T) {
	s := openPSQL(t)
	defer s.Close()
	testRandomOpConsistencyWithRAM(t, s)
}

func TestPSQLWatchBasic(t *testing.T) {
	s := openPSQL(t)
	defer s.Close()
	testWatchBasic(t, s)
}
