package tests

import (
	"testing"

	"git.encryptio.com/kvl/backend/ram"
)

func TestRAMShuffleShardedIncrement(t *testing.T) {
	ram.ResetCounters()
	s := ram.New()
	testShuffleShardedIncrement(t, s)
	t.Logf("Counters = %#v", ram.GetCounters())
}

func TestRAMRangeMaxRandomReplacement(t *testing.T) {
	ram.ResetCounters()
	s := ram.New()
	testRangeMaxRandomReplacement(t, s)
	t.Logf("Counters = %#v", ram.GetCounters())
}

func TestRAMConsistencyWithRAM(t *testing.T) {
	ram.ResetCounters()
	s := ram.New()
	testRandomOpConsistencyWithRAM(t, s)
	t.Logf("Counters = %#v", ram.GetCounters())
}
