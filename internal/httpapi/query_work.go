package httpapi

import (
	"math"

	"bog/internal/store"
)

// Snapshot HTTP work is charged in blocks. Carry unused bytes across rows
// and scans so fixed per-row rounding does not dominate small pages. Charge
// before doing the covered work, even for a partially filled block.
const snapshotWorkBlock = 1024

type snapshotWork struct {
	work  *store.QueryWork
	spare uint64
}

func (m *snapshotWork) charge(bytes uint64) error {
	if bytes <= m.spare {
		if err := m.work.Charge(); err != nil {
			return err
		}
		m.spare -= bytes
		return nil
	}
	bytes -= m.spare
	blocks := (bytes-1)/snapshotWorkBlock + 1
	if err := m.work.Charge(blocks); err != nil {
		return err
	}
	m.spare = (snapshotWorkBlock - bytes%snapshotWorkBlock) % snapshotWorkBlock
	return nil
}

func (m *snapshotWork) reserveRecord(line string) error {
	// Cover the fixed timestamp and envelope, two reads of the input and
	// the escaped bytes in scratch, the response buffer and delivery.
	const fixed, perByte = 64, 24
	if uint64(len(line)) > (math.MaxUint64-fixed)/perByte {
		return store.ErrQueryWorkExhausted
	}
	return m.charge(fixed + perByte*uint64(len(line)))
}

// Reserve bounded token authentication and encoding together so issuance
// pays at least the same cursor work as an arbitrary resume.
func (m *snapshotWork) reserveCursor(literal string) error {
	const fixed = 384
	return m.charge(fixed + uint64(len(literal)))
}
