package fuzz

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"maps"
	"math"
	"slices"
	"testing"

	"bog/internal/checkpoint"
)

// Selectors distinguish raw corruption from input with repaired outer
// checksums. They map only to known fixture filenames.
type target uint8

const (
	rawMetadata target = iota
	signedMetadata
	rawTail
	boundTail
	rawManifest
	boundManifestPayload
	rawSegment
	boundSegment
	targetCount
)

type mutation struct {
	name   string
	target target
	data   []byte
}

type manifestEnvelope struct {
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

func decodeEnvelope(t testing.TB, b []byte) manifestEnvelope {
	t.Helper()
	var env manifestEnvelope
	must(t, json.Unmarshal(b, &env))
	if digest(env.Payload) != env.SHA256 {
		t.Fatal("fixture manifest payload has incorrect checksum")
	}
	return env
}

func decodeManifest(t testing.TB, payload []byte) checkpoint.Manifest {
	t.Helper()
	var m checkpoint.Manifest
	must(t, json.Unmarshal(payload, &m))
	return m
}

func manifestPayload(t testing.TB, m checkpoint.Manifest) []byte {
	t.Helper()
	payload, err := json.Marshal(m)
	must(t, err)
	return payload
}

func putManifest(image files, payload []byte) {
	// Preserve the candidate payload bytes so whitespace, invalid JSON and
	// duplicate fields can reach the public parsers.
	sum := sha256.Sum256(payload)
	image[manifestFile] = []byte(`{"sha256":"` + hex.EncodeToString(sum[:]) + `","payload":` + string(payload) + `}`)
	meta := slices.Clone(image[metadataFile])
	copy(meta[8:40], sum[:])
	var boundary struct {
		CoveredOffset int64  `json:"covered_offset"`
		NextOrdinal   uint64 `json:"next_ordinal"`
	}
	if err := json.Unmarshal(payload, &boundary); err == nil {
		le.PutUint64(meta[40:48], uint64(boundary.CoveredOffset))
		le.PutUint64(meta[48:56], boundary.NextOrdinal)
	}
	signMetadata(meta)
	image[metadataFile] = meta
}

func mutateImage(t testing.TB, f fixture, which target, data []byte) files {
	t.Helper()
	image := maps.Clone(f.image)
	switch which {
	case rawMetadata:
		image[metadataFile] = data
	case signedMetadata:
		if len(data) != 128 {
			// Short and long forms still exercise framing validation.
			image[metadataFile] = data
			break
		}
		meta := slices.Clone(data)
		signMetadata(meta)
		image[metadataFile] = meta
	case rawTail:
		image[tailFile] = data
	case boundTail:
		meta := slices.Clone(image[metadataFile])
		le.PutUint64(meta[56:64], uint64(f.point.CoveredOffset)+uint64(len(data)))
		sum := sha256.Sum256(data)
		copy(meta[64:96], sum[:])
		signMetadata(meta)
		image[tailFile], image[metadataFile] = data, meta
	case rawManifest:
		image[manifestFile] = data
	case boundManifestPayload:
		putManifest(image, data)
	case rawSegment:
		image[f.segment] = data
	case boundSegment:
		m := decodeManifest(t, decodeEnvelope(t, image[manifestFile]).Payload)
		m.Segments[0].Size = int64(len(data))
		m.Segments[0].SHA256 = digest(data)
		putManifest(image, manifestPayload(t, m))
		image[f.segment] = data
	default:
		t.Fatalf("unknown mutation target %d", which)
	}
	return image
}

func changed(b []byte, edit func([]byte)) []byte {
	result := slices.Clone(b)
	edit(result)
	return result
}

func flip(b []byte, i int) []byte {
	return changed(b, func(b []byte) { b[i] ^= 1 })
}

func blockEdit(b []byte, edit func([]byte)) []byte {
	return changed(b, func(b []byte) {
		edit(b)
		le.PutUint32(b[36:40], crc32.Checksum(b[blockHeader:], crcTable))
		le.PutUint32(b[40:44], crc32.Checksum(b[:40], crcTable))
	})
}

func backupMutations(f fixture) []mutation {
	meta, tail := f.image[metadataFile], f.image[tailFile]
	secondFrame := frameHeader + int(le.Uint32(tail[:4]))
	return []mutation{
		{"metadata/truncated", rawMetadata, meta[:127]},
		{"metadata/extra_byte", rawMetadata, append(slices.Clone(meta), 0)},
		{"metadata/checksum", rawMetadata, flip(meta, 127)},
		{"metadata/magic", signedMetadata, flip(meta, 0)},
		{"metadata/generation", signedMetadata, flip(meta, 8)},
		{"metadata/next_ordinal_mismatch", signedMetadata, flip(meta, 48)},
		{"metadata/end_before_covered", signedMetadata, changed(meta, func(b []byte) {
			le.PutUint64(b[56:64], uint64(f.point.CoveredOffset-1))
		})},
		{"metadata/end_out_of_range", signedMetadata, changed(meta, func(b []byte) {
			le.PutUint64(b[56:64], math.MaxUint64)
		})},
		{"metadata/huge_declared_tail", signedMetadata, changed(meta, func(b []byte) {
			le.PutUint64(b[56:64], math.MaxInt64)
		})},
		{"tail/size_mismatch", rawTail, tail[:len(tail)-1]},
		{"tail/digest", signedMetadata, flip(meta, 64)},
		{"tail/partial_first_header", boundTail, tail[:frameHeader-1]},
		{"tail/partial_second_header", boundTail, tail[:secondFrame+frameHeader-1]},
		{"tail/partial_second_payload", boundTail, tail[:len(tail)-1]},
		{"tail/zero_frame_length", boundTail, frameLength(tail, 0)},
		{"tail/huge_frame_length", boundTail, frameLength(tail, math.MaxUint32)},
		{"tail/huge_second_frame_length", boundTail, changed(tail, func(b []byte) {
			copy(b[secondFrame:], frameLength(b[secondFrame:], math.MaxUint32))
		})},
		{"tail/frame_length_past_end", boundTail, frameLength(tail, uint32(len(tail)))},
		{"tail/frame_length_complement", boundTail, changed(tail, func(b []byte) {
			b[4] ^= 1
			fixFrameHeader(b)
		})},
		{"tail/frame_header_checksum", boundTail, flip(tail, 12)},
		{"tail/frame_payload_checksum", boundTail, flip(tail, frameHeader)},
		{"tail/invalid_batch_with_valid_crcs", boundTail, changed(tail, func(b []byte) {
			b[frameHeader] = '!'
			le.PutUint32(b[8:12], crc32.Checksum(b[frameHeader:secondFrame], crcTable))
			fixFrameHeader(b)
		})},
	}
}

func historyMutations(t testing.TB, f fixture) []mutation {
	t.Helper()
	manifest, block := f.image[manifestFile], f.image[f.segment]
	payload := decodeEnvelope(t, manifest).Payload
	env := decodeEnvelope(t, manifest)
	env.SHA256 = digest(nil)
	badEnvelope, err := json.Marshal(env)
	must(t, err)

	editManifest := func(edit func(*checkpoint.Manifest)) []byte {
		m := decodeManifest(t, payload)
		edit(&m)
		return manifestPayload(t, m)
	}

	firstLineLen := int(le.Uint32(block[blockHeader+20 : blockHeader+rowHeader]))
	secondRow := blockHeader + rowHeader + firstLineLen
	secondLineLen := int(le.Uint32(block[secondRow+20 : secondRow+rowHeader]))
	thirdRow := secondRow + rowHeader + secondLineLen
	count := le.Uint32(block[4:8])
	return []mutation{
		{"manifest/truncated_envelope", rawManifest, manifest[:len(manifest)-1]},
		{"manifest/oversized", rawManifest, bytes.Repeat([]byte{' '}, maxArtifactSize)},
		{"manifest/checksum", rawManifest, badEnvelope},
		{"manifest/wrong_payload_type", boundManifestPayload, []byte(`[]`)},
		{"manifest/version", boundManifestPayload, editManifest(func(m *checkpoint.Manifest) { m.Version++ })},
		{"manifest/impossible_ordinal_count", boundManifestPayload, editManifest(func(m *checkpoint.Manifest) { m.NextOrdinal = math.MaxUint64 })},
		{"manifest/inconsistent_ordinal_count", boundManifestPayload, editManifest(func(m *checkpoint.Manifest) { m.NextOrdinal++ })},
		{"manifest/negative_segment_size", boundManifestPayload, editManifest(func(m *checkpoint.Manifest) { m.Segments[0].Size = -1 })},
		{"manifest/huge_segment_size", boundManifestPayload, editManifest(func(m *checkpoint.Manifest) { m.Segments[0].Size = math.MaxInt64 })},
		{"manifest/missing_referenced_segment", boundManifestPayload, editManifest(func(m *checkpoint.Manifest) {
			m.Segments[0].Name = "bog-segment-00000000000000000000000000000000"
		})},
		{"manifest/duplicate_segment", boundManifestPayload, editManifest(func(m *checkpoint.Manifest) {
			m.Segments = append(m.Segments, m.Segments[0])
		})},
		{"manifest/too_many_segments", boundManifestPayload, editManifest(func(m *checkpoint.Manifest) {
			m.Segments = slices.Repeat(m.Segments[:1], checkpoint.MaxSegments+1)
		})},
		{"segment/truncated_header", boundSegment, block[:blockHeader-1]},
		{"segment/truncated_payload_size_mismatch", rawSegment, block[:len(block)-1]},
		{"segment/digest", rawSegment, flip(block, len(block)-1)},
		{"segment/header_checksum", boundSegment, flip(block, 40)},
		{"segment/payload_checksum", boundSegment, flip(block, blockHeader+rowHeader)},
		{"segment/payload_length", boundSegment, blockEdit(block, func(b []byte) { le.PutUint32(b[8:12], math.MaxUint32) })},
		{"segment/huge_row_count", boundSegment, blockEdit(block, func(b []byte) { le.PutUint32(b[4:8], math.MaxUint32) })},
		{"segment/short_row_count", boundSegment, blockEdit(block, func(b []byte) { le.PutUint32(b[4:8], count-1) })},
		{"segment/truncated_row_body", boundSegment, blockEdit(block[:len(block)-1], func(b []byte) {
			le.PutUint32(b[8:12], uint32(len(b)-blockHeader))
		})},
		{"segment/huge_line_length", boundSegment, blockEdit(block, func(b []byte) {
			le.PutUint32(b[blockHeader+20:blockHeader+rowHeader], math.MaxUint32)
		})},
		{"segment/duplicate_ordinal", boundSegment, blockEdit(block, func(b []byte) {
			copy(b[thirdRow+12:thirdRow+20], b[blockHeader+12:blockHeader+20])
		})},
		{"segment/invalid_utf8", boundSegment, blockEdit(block, func(b []byte) { b[blockHeader+rowHeader] = 0xff })},
	}
}
