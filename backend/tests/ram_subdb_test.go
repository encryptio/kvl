package tests

import (
	"testing"

	"git.encryptio.com/kvl"
	"git.encryptio.com/kvl/backend/ram"
)

func TestSubDBShuffleShardedIncrement(t *testing.T) {
	s := ram.New()
	subdb := kvl.SubDB(s, []byte("some\x00prefix"))
	testShuffleShardedIncrement(t, subdb)
}

func TestSubDBRangeMaxRandomReplacement(t *testing.T) {
	s := ram.New()
	subdb := kvl.SubDB(s, []byte("some\x00prefix"))
	testRangeMaxRandomReplacement(t, subdb)
}

func TestSubDBConsistencyWithRAM(t *testing.T) {
	s := ram.New()
	subdb := kvl.SubDB(s, []byte("some\x00prefix"))
	testRandomOpConsistencyWithRAM(t, subdb)
}

func TestSubDBWatchBasic(t *testing.T) {
	s := ram.New()
	subdb := kvl.SubDB(s, []byte("some\x00prefix"))
	testWatchBasic(t, subdb)
}
