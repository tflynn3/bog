package main

import (
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func testTailParityEvidence(t *testing.T, edit func(*tailParityProof)) jsontext.Value {
	t.Helper()
	fixtures, err := tailParityFixtures()
	if err != nil {
		t.Fatal(err)
	}
	var proof tailParityProof
	for i, f := range fixtures {
		f.input.Generation = strings.Repeat(fmt.Sprint(i+1), 64)
		f.export.Generation = strings.Repeat(fmt.Sprint(i+2), 64)
		proof.Cycles[i] = tailParityCycle{
			Input: f.input, Export: f.export, ConsumedSHA256: parityDigest(f.consumed),
			TailBase: f.export.CoveredOffset, TailBytes: int64(len(f.suffix)),
			AppendStatuses: [2]int{http.StatusNoContent, http.StatusNoContent},
			HeldRows:       f.input.CommittedOrdinal, FreshRows: f.export.CommittedOrdinal,
			RestartRows: f.export.CommittedOrdinal, RestoredRows: f.export.CommittedOrdinal,
			RestartAppendStatus: http.StatusNoContent, RestartNextOrdinal: f.export.CommittedOrdinal + 1,
		}
	}
	if edit != nil {
		edit(&proof)
	}
	raw, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestValidateTailOnlyParity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		edit func(*tailParityProof)
		want string
	}{
		{"complete", nil, ""},
		{"missing cycle", func(p *tailParityProof) { p.Cycles[1] = tailParityCycle{} }, "cycle 2"},
		{"stale predecessor", func(p *tailParityProof) { p.Cycles[1].Input = p.Cycles[0].Input }, "cycle 2: input"},
		{"unlinked generation", func(p *tailParityProof) { p.Cycles[1].Input.Generation = strings.Repeat("4", 64) }, "cycle 2: input"},
		{"unlinked carried suffix", func(p *tailParityProof) { p.Cycles[1].Input.SuffixSHA256 = p.Cycles[1].Export.SuffixSHA256 }, "cycle 2: input"},
		{"stale generation", func(p *tailParityProof) { p.Cycles[1].Export.Generation = p.Cycles[0].Export.Generation }, "did not advance"},
		{"reused v1 generation", func(p *tailParityProof) { p.Cycles[1].Export.Generation = p.Cycles[0].Input.Generation }, "did not advance"},
		{"malformed generation", func(p *tailParityProof) { p.Cycles[0].Export.Generation = strings.Repeat("G", 64) }, "generation digest"},
		{"all zero generation", func(p *tailParityProof) { p.Cycles[0].Export.Generation = strings.Repeat("0", 64) }, "generation digest"},
		{"partial frame cutoff", func(p *tailParityProof) { p.Cycles[0].Export.CoveredOffset++ }, "cycle 1: export"},
		{"wrong cutoff ordinal", func(p *tailParityProof) { p.Cycles[1].Export.NextOrdinal-- }, "cycle 2: export"},
		{"empty carried suffix", func(p *tailParityProof) { p.Cycles[1].Export.CommittedEnd = p.Cycles[1].Export.CoveredOffset }, "cycle 2: export"},
		{"wrong committed ordinal", func(p *tailParityProof) { p.Cycles[0].Export.CommittedOrdinal++ }, "cycle 1: export"},
		{"wrong suffix digest", func(p *tailParityProof) { p.Cycles[1].Export.SuffixSHA256 = p.Cycles[1].Input.SuffixSHA256 }, "cycle 2: export"},
		{"wrong consumed digest", func(p *tailParityProof) { p.Cycles[0].ConsumedSHA256 = p.Cycles[0].Export.SuffixSHA256 }, "consumed_sha256"},
		{"stale tail base", func(p *tailParityProof) { p.Cycles[1].TailBase = p.Cycles[0].TailBase }, "tail_base"},
		{"wrong physical length", func(p *tailParityProof) { p.Cycles[0].TailBytes++ }, "tail_bytes"},
		{"unacknowledged carried write", func(p *tailParityProof) { p.Cycles[0].AppendStatuses[1] = 503 }, "append_statuses"},
		{"held walk leaked new rows", func(p *tailParityProof) { p.Cycles[1].HeldRows = p.Cycles[1].FreshRows }, "held_rows"},
		{"fresh walk lost rows", func(p *tailParityProof) { p.Cycles[0].FreshRows-- }, "fresh_rows"},
		{"restart walk lost rows", func(p *tailParityProof) { p.Cycles[0].RestartRows-- }, "restart_rows"},
		{"restored walk lost rows", func(p *tailParityProof) { p.Cycles[1].RestoredRows-- }, "restored_rows"},
		{"restart append refused", func(p *tailParityProof) { p.Cycles[1].RestartAppendStatus = 503 }, "restart_append_status"},
		{"restart reused ordinal", func(p *tailParityProof) { p.Cycles[0].RestartNextOrdinal-- }, "restart_next_ordinal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateTailOnlyParity(testTailParityEvidence(t, tc.edit))
			if tc.want == "" && err == nil {
				return
			}
			if tc.want == "" || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validation = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestTailParityOutcomeRequiresExplicitEvidence(t *testing.T) {
	t.Parallel()
	valid := testTailParityEvidence(t, nil)
	var proof struct {
		Cycles []jsontext.Value `json:"cycles"`
	}
	if err := json.Unmarshal(valid, &proof); err != nil {
		t.Fatal(err)
	}
	cycle := string(proof.Cycles[0])
	for _, tc := range []struct {
		name     string
		status   string
		evidence string
		want     string
	}{
		{"complete", "pass", string(valid), ""},
		{"failed", "fail", string(valid), "expected pass"},
		{"skipped", "skip", string(valid), "expected pass"},
		{"status only", "pass", "", "evidence"},
		{"wrong evidence shape", "pass", "[]", "malformed JSON object"},
		{"null cycles", "pass", `{"cycles":null}`, "cycles"},
		{"one cycle", "pass", `{"cycles":[` + cycle + `]}`, "cycles"},
		{"three cycles", "pass", `{"cycles":[` + cycle + "," + cycle + "," + cycle + `]}`, "cycles"},
		{"null acknowledgment", "pass", strings.Replace(string(valid), `"restart_append_status":204`, `"restart_append_status":null`, 1), "restart_append_status"},
		{"duplicate cutoff", "pass", strings.Replace(string(valid), `"next_ordinal":3`, `"next_ordinal":3,"next_ordinal":3`, 1), "duplicate"},
		{"missing ordinal", "pass", strings.Replace(string(valid), `"next_ordinal":3,`, "", 1), "input"},
		{"wrong key spelling", "pass", strings.Replace(string(valid), `"tail_bytes"`, `"TAIL_BYTES"`, 1), "tail_bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data, err := json.Marshal(outcome{Status: tc.status, Evidence: jsontext.Value(tc.evidence)})
			if err != nil {
				t.Fatal(err)
			}
			err = validateOutcome(data, tailOnlyParityCase, true, validationOptions{requireCutover: true})
			if tc.want == "" && err == nil {
				return
			}
			if tc.want == "" || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("strict validation = %v, want %q", err, tc.want)
			}
		})
	}
}
