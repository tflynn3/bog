package store

import "math/bits"

// QueryMemory accounts for variable-sized backing arrays during a match scan.
// The caller owns the budget and reserves bounded setup separately. A failed
// reservation leaves the meter unchanged. A successful capture transfers its
// ordinal charge to the caller, which releases it when the capture is dropped.
type QueryMemory interface {
	ReserveQueryBytes(uint64) error
	ReleaseQueryBytes(uint64)
}

const queryIntBytes = bits.UintSize / 8

// growQueryInts reserves the whole replacement while the old array is still
// charged. The limit is bounded by the published rows or blocks.
func growQueryInts(values *[]int, limit int, memory QueryMemory) error {
	old := *values
	if memory == nil || len(old) < cap(old) {
		return nil
	}
	capacity := cap(old) + min(max(1, cap(old)), limit-cap(old))
	if err := memory.ReserveQueryBytes(uint64(capacity) * queryIntBytes); err != nil {
		return err
	}
	next := make([]int, len(old), capacity)
	copy(next, old)
	*values = next
	releaseQueryInts(memory, cap(old))
	return nil
}

func releaseQueryInts(memory QueryMemory, capacity int) {
	if memory != nil && capacity != 0 {
		memory.ReleaseQueryBytes(uint64(capacity) * queryIntBytes)
	}
}
