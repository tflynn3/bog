package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"strings"
	"time"
)

const (
	tailOnlyParityCase = "tail_only_two_cycle_parity"
	tailParityCycles   = 2
	walMagicBytes      = 8
	walFrameHeader     = 16
)

type tailParityPoint struct {
	Generation       string `json:"generation"`
	CoveredOffset    int64  `json:"covered_offset"`
	NextOrdinal      int    `json:"next_ordinal"`
	CommittedEnd     int64  `json:"committed_end"`
	CommittedOrdinal int    `json:"committed_ordinal"`
	SuffixSHA256     string `json:"suffix_sha256"`
}

type tailParityCycle struct {
	Input               tailParityPoint `json:"input"`
	Export              tailParityPoint `json:"export"`
	ConsumedSHA256      string          `json:"consumed_sha256"`
	TailBase            int64           `json:"tail_base"`
	TailBytes           int64           `json:"tail_bytes"`
	AppendStatuses      [2]int          `json:"append_statuses"`
	HeldRows            int             `json:"held_rows"`
	FreshRows           int             `json:"fresh_rows"`
	RestartRows         int             `json:"restart_rows"`
	RestoredRows        int             `json:"restored_rows"`
	RestartAppendStatus int             `json:"restart_append_status"`
	RestartNextOrdinal  int             `json:"restart_next_ordinal"`
}

type tailParityProof struct {
	Cycles [tailParityCycles]tailParityCycle `json:"cycles"`
}

func fixtureTime(second int) time.Time {
	return time.Date(2020, time.January, 1, 0, 0, second, 0, time.UTC)
}

func fixtureWireRow(second int, line string) row {
	return row{fixtureTime(second).Format(time.RFC3339Nano), line}
}

type parityFixtureRow struct {
	second int
	line   string
}

func parityFixtureData() [parityFixtureRows]parityFixtureRow {
	return [parityFixtureRows]parityFixtureRow{
		{2, "checkpoint " + strings.Repeat("x", 110)},
		{1, "duplicate [match]"},
		{1, "duplicate [match]"},
		{1, "tail " + strings.Repeat("y", 110)},
		{0, "backfill [match]"},
		{1, "duplicate [match]"},
	}
}

type tailCycleFixture struct {
	batches  [2][]row
	input    tailParityPoint
	export   tailParityPoint
	consumed []byte
	suffix   []byte
}

// Calculate logical bounds from the separately framed request bodies. The
// first advance consumes the v1 suffix and one new batch. The next advance
// consumes the previous export's suffix and one new batch at its new base.
func tailParityFixtures() ([tailParityCycles]tailCycleFixture, error) {
	var fixtures [tailParityCycles]tailCycleFixture
	data := parityFixtureData()
	rows := make([]row, 0, len(data))
	for _, r := range data {
		rows = append(rows, fixtureWireRow(r.second, r.line))
	}
	seed, err := fixtureFrame(rows[:paritySeedRows])
	if err != nil {
		return fixtures, err
	}
	carried, err := fixtureFrame(rows[paritySeedRows:])
	if err != nil {
		return fixtures, err
	}
	input := tailParityPoint{
		CoveredOffset: walMagicBytes + int64(len(seed)), NextOrdinal: paritySeedRows,
		CommittedEnd: walMagicBytes + int64(len(seed)+len(carried)), CommittedOrdinal: len(rows),
		SuffixSHA256: parityDigest(carried),
	}
	for i := range fixtures {
		batches := [2][]row{
			{fixtureWireRow(0, fmt.Sprintf("cycle %d backfill [match]", i+1)), rows[1]},
			{rows[1], fixtureWireRow(1, fmt.Sprintf("cycle %d carried [match]%s", i+1, strings.Repeat("z", i*17)))},
		}
		advance, err := fixtureFrame(batches[0])
		if err != nil {
			return fixtures, err
		}
		suffix, err := fixtureFrame(batches[1])
		if err != nil {
			return fixtures, err
		}
		cut := input.CommittedEnd + int64(len(advance))
		next := input.CommittedOrdinal + len(batches[0])
		export := tailParityPoint{
			CoveredOffset: cut, NextOrdinal: next, CommittedEnd: cut + int64(len(suffix)),
			CommittedOrdinal: next + len(batches[1]), SuffixSHA256: parityDigest(suffix),
		}
		fixtures[i] = tailCycleFixture{batches, input, export, append(bytes.Clone(carried), advance...), suffix}
		input, carried = export, suffix
	}
	return fixtures, nil
}

// Encode the documented v1 WAL frame independently of the runtime writer.
func fixtureFrame(rows []row) ([]byte, error) {
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return nil, err
		}
	}
	frame := make([]byte, walFrameHeader, walFrameHeader+body.Len())
	size := uint32(body.Len())
	table := crc32.MakeTable(crc32.Castagnoli)
	binary.LittleEndian.PutUint32(frame[0:4], size)
	binary.LittleEndian.PutUint32(frame[4:8], ^size)
	binary.LittleEndian.PutUint32(frame[8:12], crc32.Checksum(body.Bytes(), table))
	binary.LittleEndian.PutUint32(frame[12:16], crc32.Checksum(frame[:12], table))
	return append(frame, body.Bytes()...), nil
}

func parityDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func validParityGeneration(id string) bool {
	return len(id) == 2*sha256.Size && strings.Trim(id, "0123456789abcdef") == "" &&
		id != strings.Repeat("0", 2*sha256.Size)
}

func checkTailParityPoint(got, want tailParityPoint) error {
	if !validParityGeneration(got.Generation) {
		return fmt.Errorf("invalid generation digest %q", got.Generation)
	}
	if want.Generation == "" {
		want.Generation = got.Generation
	}
	if got != want {
		return fmt.Errorf("recovery point %+v differs from expected %+v", got, want)
	}
	return nil
}

func validateTailOnlyParity(raw jsontext.Value) error {
	var cycles [tailParityCycles]jsontext.Value
	if _, err := decodeFields(raw, map[string]any{"cycles": &cycles}); err != nil {
		return fmt.Errorf("evidence: %w", err)
	}
	fixtures, err := tailParityFixtures()
	if err != nil {
		return err
	}
	var firstGeneration string
	var priorExport tailParityPoint
	for i, fixture := range fixtures {
		var cycle tailParityCycle
		if _, err := decodeFields(cycles[i], map[string]any{
			"input": &cycle.Input, "export": &cycle.Export, "consumed_sha256": &cycle.ConsumedSHA256,
			"tail_base": &cycle.TailBase, "tail_bytes": &cycle.TailBytes, "append_statuses": &cycle.AppendStatuses,
			"held_rows": &cycle.HeldRows, "fresh_rows": &cycle.FreshRows, "restart_rows": &cycle.RestartRows,
			"restored_rows": &cycle.RestoredRows, "restart_append_status": &cycle.RestartAppendStatus,
			"restart_next_ordinal": &cycle.RestartNextOrdinal,
		}); err != nil {
			return fmt.Errorf("evidence: cycle %d: %w", i+1, err)
		}
		if i == 0 {
			firstGeneration = cycle.Input.Generation
		} else {
			fixture.input = priorExport
		}
		if err := validateTailParityCycle(cycle, fixture, firstGeneration); err != nil {
			return fmt.Errorf("evidence: cycle %d: %w", i+1, err)
		}
		priorExport = cycle.Export
	}
	return nil
}

func validateTailParityCycle(cycle tailParityCycle, fixture tailCycleFixture, firstGeneration string) error {
	if err := checkTailParityPoint(cycle.Input, fixture.input); err != nil {
		return fmt.Errorf("input: %w", err)
	}
	if err := checkTailParityPoint(cycle.Export, fixture.export); err != nil {
		return fmt.Errorf("export: %w", err)
	}
	if cycle.Export.Generation == cycle.Input.Generation || cycle.Export.Generation == firstGeneration {
		return errors.New("export generation did not advance")
	}
	if cycle.ConsumedSHA256 != parityDigest(fixture.consumed) || cycle.TailBase != fixture.export.CoveredOffset ||
		cycle.TailBytes != int64(len(fixture.suffix)) {
		return fmt.Errorf("invalid consumed_sha256, tail_base or tail_bytes: %+v", cycle)
	}
	if cycle.AppendStatuses != ([2]int{http.StatusNoContent, http.StatusNoContent}) ||
		cycle.RestartAppendStatus != http.StatusNoContent || cycle.RestartNextOrdinal != cycle.Export.CommittedOrdinal+1 {
		return fmt.Errorf("invalid append_statuses, restart_append_status or restart_next_ordinal: %+v", cycle)
	}
	if cycle.HeldRows != cycle.Input.CommittedOrdinal || cycle.FreshRows != cycle.Export.CommittedOrdinal ||
		cycle.RestartRows != cycle.FreshRows || cycle.RestoredRows != cycle.FreshRows {
		return fmt.Errorf("invalid held_rows, fresh_rows, restart_rows or restored_rows: %+v", cycle)
	}
	return nil
}
