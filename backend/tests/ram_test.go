package tests

import (
	"testing"

	"github.com/encryptio/kvl/backend/ram"
)

func TestRAMShuffleShardedIncrement(t *testing.T) {
	s := ram.New()
	testShuffleShardedIncrement(t, s)
}

func TestRAMRangeMaxRandomReplacement(t *testing.T) {
	s := ram.New()
	testRangeMaxRandomReplacement(t, s)
}

func TestRAMConsistencyWithRAM(t *testing.T) {
	s := ram.New()
	testRandomOpConsistencyWithRAM(t, s)
}

func TestRAMWatchBasic(t *testing.T) {
	s := ram.New()
	testWatchBasic(t, s)
}
