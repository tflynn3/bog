package main

import "hash/crc32"

// A fixed 8 Kibit Bloom filter over overlapping four-byte windows, reset at
// each line boundary. Index bytes alone can exclude a block, never match a row.
type summary [1024]byte

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func gramBits(gram string) (uint32, uint32) {
	x := uint32(gram[0]) | uint32(gram[1])<<8 | uint32(gram[2])<<16 | uint32(gram[3])<<24
	x ^= x >> 16
	x *= 0x7feb352d
	x ^= x >> 15
	x *= 0x846ca68b
	x ^= x >> 16
	return x, (x >> 16) | 1
}

func (s *summary) add(line string) {
	for i := 0; i+4 <= len(line); i++ {
		start, step := gramBits(line[i : i+4])
		for j := uint32(0); j < 3; j++ {
			bit := (start + j*step) % (8 * uint32(len(s)))
			s[bit/8] |= 1 << (bit % 8)
		}
	}
}

func (s *summary) mayContain(literal string) bool {
	for i := 0; i+4 <= len(literal); i++ {
		start, step := gramBits(literal[i : i+4])
		for j := uint32(0); j < 3; j++ {
			bit := (start + j*step) % (8 * uint32(len(s)))
			if s[bit/8]&(1<<(bit%8)) == 0 {
				return false
			}
		}
	}
	return true
}
