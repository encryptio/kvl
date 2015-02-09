package index

import (
	"hash"
	"hash/fnv"
	"math/rand"
)

type bloom struct {
	vals []byte
	seed [8]byte
	h    hash.Hash64
}

func newBloom(size uint64) *bloom {
	b := &bloom{
		vals: make([]byte, (size+7)/8),
		h:    fnv.New64a(),
	}
	for i := 0; i < 8; i++ {
		b.seed[i] = byte(rand.Int31())
	}
	return b
}

func (b *bloom) hash(k []byte) uint64 {
	b.h.Reset()
	b.h.Write(b.seed[:])
	b.h.Write(k)
	return b.h.Sum64() % uint64(len(b.vals)*8)
}

func (b *bloom) Set(k []byte) {
	idx := b.hash(k)
	b.vals[idx/8] |= 1 << (idx & 7)
}

func (b *bloom) Test(k []byte) bool {
	idx := b.hash(k)
	return (b.vals[idx/8] & (1 << (idx & 7))) != 0
}

func (b *bloom) Fullness() float64 {
	totalSet := uint64(0)
	for _, byt := range b.vals {
		totalSet += uint64(bitsSetIn[byt])
	}
	return float64(totalSet) / float64(len(b.vals)*8)
}

var bitsSetIn [256]byte

func init() {
	for i := range bitsSetIn {
		count := byte(0)
		n := i
		for n > 0 {
			if n&1 == 1 {
				count++
			}
			n >>= 1
		}

		bitsSetIn[i] = count
	}
}
