package capacityevidence

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

func cutoverTimeline(pattern string, held, requested, final int, predecessor, generation int) string {
	sample := func(event, state string, elapsed, sent, generation, covered, committed int, pins string) string {
		return fmt.Sprintf(`{"event":%q,"elapsed_ns":%d,"request_sent_ns":%d,
			"status":{"state":%q,"generation":%d,"covered_rows":%d,"committed_rows":%d},
			"candidate_status_metrics":{"retired_generation_pins":%s}}`,
			event, elapsed, sent, state, generation, covered, committed, pins)
	}
	samples := []string{
		sample("compaction_start", "running", 25, 20, predecessor, held, requested, "null"),
		sample("overlap", "running", 35, 30, predecessor, held, requested+BatchRows, "null"),
	}
	lastAck, publishedRows, lagLower := 29, final, 10
	if pattern == "scattered" {
		lastAck, publishedRows, lagLower = 58, requested+BatchRows, 20
		samples = append(samples, sample("status_poll", "running", 45, 40, predecessor, held, publishedRows, "null"))
	}
	samples = append(samples, sample("publication", "complete", 55, 50, generation, requested, publishedRows, "2"))
	if publishedRows != final {
		samples = append(samples, sample("final_cutoff", "complete", 65, 60, generation, requested, final, "2"))
	}
	samples = append(samples,
		`{"event":"held_exhausted","elapsed_ns":70,"status":null}`,
		`{"event":"terminal_retried","elapsed_ns":72,"status":null}`,
		sample("before_retirement", "complete", 75, 73, generation, requested, final, "2"),
		`{"event":"capture_retired","elapsed_ns":80,"status":null}`,
		sample("after_retirement", "complete", 85, 81, generation, requested, final, "1"),
		sample("drain_poll", "complete", 100000090, 100000080, generation, requested, final, "0"),
		`{"event":"fresh_visible","elapsed_ns":100000110,"status":null}`,
	)
	return fmt.Sprintf(`{"pattern":%q,"status":"pass","held_cutoff":%d,"requested_cutoff":%d,"expected_committed_rows":%d,
		"held_rows_checked":3,"held_cursor_exhausted":true,"terminal_page_retried":true,"capture_retired":true,"fresh_backfill_visible":true,"fresh_walk_exact":true,"fresh_rows_checked":%d,
		"acknowledged_backfill":{"rows":%d,"batches":%d,"first_ack_ns":18,"last_ack_ns":%d,
			"earliest_row_ts":"2020-01-01T00:00:00Z","latest_row_ts":"2020-01-01T00:00:01Z"},
		"publication":{"generation":%d,"observed_lag_lower_bound_ns":%d,"observed_lag_upper_bound_ns":35},
		"pin_drain":{"status":"observed","retired_pins_while_held":2,"retired_pins_after_exhaustion":0},
		"samples":[%s]}`,
		pattern, held, requested, final, final, final-held, (final-held)/BatchRows, lastAck, generation, lagLower, strings.Join(samples, ","))
}

func TestValidateCutoverTimelineSequence(t *testing.T) {
	t.Parallel()

	seed := SmallScale
	small, large := observationRecord(t, seed), observationRecord(t, LargeScale)
	sampleIndex := func(b *backfillEvidence, event string) int {
		return slices.IndexFunc(b.Samples, func(s backfillSample) bool { return s.Event == event })
	}
	sample := func(b *backfillEvidence, event string) *backfillSample {
		return &b.Samples[sampleIndex(b, event)]
	}
	remove := func(b *backfillEvidence, event string) {
		i := sampleIndex(b, event)
		b.Samples = slices.Delete(b.Samples, i, i+1)
	}

	for _, tc := range []struct {
		name    string
		pattern string
		mutate  func(*backfillEvidence)
		problem string
	}{
		{
			name: "premature completion labeled as a running poll", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				poll := sample(b, "status_poll")
				poll.Compaction = &compactionEvidence{"complete", b.Publication.Generation, b.Cutoff, b.FinalCutoff}
			},
			problem: "running",
		},
		{
			name: "null running poll status", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { sample(b, "status_poll").Compaction = nil },
			problem: "status_poll sample",
		},
		{
			name: "scattered predecessor predates seed checkpoint", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				for i := range b.Samples {
					if c := b.Samples[i].Compaction; c != nil && c.State == "running" {
						c.Generation = 40 // The clustered publication still covers seed minus a batch.
					}
				}
			},
			problem: "seed checkpoint",
		},
		{
			name: "repeated running poll with no new committed rows", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				i := sampleIndex(b, "status_poll")
				poll := b.Samples[i]
				poll.SentNS, poll.ElapsedNS, poll.Memory = new(int64(45)), 46, nil
				b.Samples = slices.Insert(b.Samples, i+1, poll)
				b.Publication.LowerNS = 25
			},
		},
		{
			name: "running poll overlaps prior reply", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				s := sample(b, "status_poll")
				s.Memory, s.SentNS = nil, new(int64(34))
			},
			problem: "status_poll sample",
		},
		{
			name: "running poll regresses committed rows", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { sample(b, "status_poll").Compaction.CommittedRows-- },
			problem: "same running compaction",
		},
		{
			name: "publication regresses committed rows", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { sample(b, "publication").Compaction.CommittedRows-- },
			problem: "publication at the requested cutoff",
		},
		{
			name: "publication bounds include the last running request", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { b.Publication.LowerNS-- },
			problem: "publication lag bounds",
		},
		{
			name: "memory read starts before prior running reply", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { sample(b, "status_poll").Memory.ReadStart = 34 },
			problem: "mis-timed or excessive memory.current samples",
		},
		{
			name: "optional unavailable staging observation", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				sample(b, "overlap").Filesystem = &filesystemObservation{Unavailable: map[string]string{
					"checkpoint_staging_apparent_bytes": "scan failed",
				}}
			},
		},
		{
			name: "timed staging, runtime and memory bytes are independent", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				s := sample(b, "overlap")
				s.Filesystem = &filesystemObservation{ObservedNS: new(int64(36)), StagingBytes: new(int64(8192))}
				s.Metrics.StagingUsed = new(uint64(0))
				sample(b, "publication").Filesystem = &filesystemObservation{ObservedNS: new(int64(57)), StagingBytes: new(int64(0))}
			},
		},
		{
			name: "staging bytes missing observation time", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				sample(b, "overlap").Filesystem = &filesystemObservation{StagingBytes: new(int64(0))}
			},
			problem: "filesystem staging observation timing",
		},
		{
			name: "staging observation before its reply", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				sample(b, "overlap").Filesystem = &filesystemObservation{ObservedNS: new(int64(34)), StagingBytes: new(int64(0))}
			},
			problem: "filesystem staging observation timing",
		},
		{
			name: "next memory read starts before staging scan", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				sample(b, "overlap").Filesystem = &filesystemObservation{ObservedNS: new(int64(37)), StagingBytes: new(int64(0))}
			},
			problem: "mis-timed or excessive memory.current samples",
		},
		{
			name: "negative staged bytes", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				sample(b, "overlap").Filesystem = &filesystemObservation{ObservedNS: new(int64(36)), StagingBytes: new(int64(-1))}
			},
			problem: "checkpoint_staging_apparent_bytes",
		},
		{name: "clustered final count in publication", pattern: "clustered"},
		{name: "scattered publication before last acknowledgement", pattern: "scattered"},
		{
			name: "last acknowledgment at fresh visibility", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				b.Acknowledged.Last = sample(b, "fresh_visible").ElapsedNS
			},
		},
		{
			name: "last acknowledgment after fresh visibility", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				b.Acknowledged.Last = sample(b, "fresh_visible").ElapsedNS + 1
				b.Samples = append(b.Samples, backfillSample{Event: "fresh_walk_complete", ElapsedNS: b.Acknowledged.Last + 1})
			},
			problem: "last backfill acknowledgment must occur by the fresh visibility observation",
		},
		{
			name: "scattered final count in publication", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				sample(b, "publication").Compaction.CommittedRows = b.FinalCutoff
				remove(b, "final_cutoff")
			},
		},
		{
			name: "missing final cutoff", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { remove(b, "final_cutoff") },
			problem: "requires a prior final cutoff observation",
		},
		{
			name: "final cutoff after exhaustion", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				i := sampleIndex(b, "final_cutoff")
				b.Samples[i].ElapsedNS, b.Samples[i].SentNS = 71, new(int64(71))
				b.Samples[i], b.Samples[i+1] = b.Samples[i+1], b.Samples[i]
			},
			problem: "requires a prior final cutoff observation",
		},
		{
			name: "final cutoff still missing rows", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { sample(b, "final_cutoff").Compaction.CommittedRows-- },
			problem: "final_cutoff sample must confirm its cutoff",
		},
		{
			name: "final cutoff observes different coverage", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { sample(b, "final_cutoff").Compaction.CoveredRows-- },
			problem: "final_cutoff sample did not observe publication",
		},
		{
			name: "final cutoff request failed", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				s := sample(b, "final_cutoff")
				s.Compaction, s.Metrics.RetiredPins = nil, nil
				s.StatusError = "connection reset"
			},
			problem: "final_cutoff sample failed",
		},
		{
			name: "status poll error", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				s := sample(b, "status_poll")
				s.Compaction = nil
				s.StatusError = context.DeadlineExceeded.Error()
			},
			problem: "status_poll sample failed",
		},
		{
			name: "missing terminal replay event", pattern: "clustered",
			mutate:  func(b *backfillEvidence) { remove(b, "terminal_retried") },
			problem: "after terminal page replay",
		},
		{
			name: "held observation request precedes replay", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { sample(b, "before_retirement").SentNS = new(int64(71)) },
			problem: "after terminal page replay",
		},
		{
			name: "publication pins cannot replace before-retirement sample", pattern: "clustered",
			mutate:  func(b *backfillEvidence) { remove(b, "before_retirement") },
			problem: "requires a prior before_retirement pin observation",
		},
		{
			name: "before retirement has no positive pin", pattern: "clustered",
			mutate:  func(b *backfillEvidence) { sample(b, "before_retirement").Metrics.RetiredPins = new(uint64(0)) },
			problem: "observe the positive held pin count",
		},
		{
			name: "missing capture retirement event", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { remove(b, "capture_retired") },
			problem: "did not follow explicit capture retirement",
		},
		{
			name: "drain poll lacks initial observation", pattern: "clustered",
			mutate:  func(b *backfillEvidence) { remove(b, "after_retirement") },
			problem: "must start with after_retirement",
		},
		{
			name: "drain succeeds on first observation", pattern: "clustered",
			mutate: func(b *backfillEvidence) {
				sample(b, "after_retirement").Metrics.RetiredPins = new(uint64(0))
				remove(b, "drain_poll")
			},
		},
		{
			name: "retry missing counter", pattern: "clustered",
			mutate: func(b *backfillEvidence) { sample(b, "after_retirement").Metrics.RetiredPins = nil },
		},
		{
			name: "retry status request deadline", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				s := sample(b, "after_retirement")
				s.Compaction, s.Metrics.RetiredPins = nil, nil
				s.StatusError = context.DeadlineExceeded.Error()
				s.ElapsedNS = *s.SentNS + int64(250*time.Millisecond)
				poll := sample(b, "drain_poll")
				poll.SentNS, poll.ElapsedNS = new(s.ElapsedNS+1), s.ElapsedNS+5
				sample(b, "fresh_visible").ElapsedNS = poll.ElapsedNS + 1
			},
		},
		{
			name: "deadline cannot attest zero", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				s := sample(b, "drain_poll")
				s.Compaction, s.Metrics.RetiredPins = nil, nil
				s.StatusError = context.DeadlineExceeded.Error()
			},
			problem: "fresh visibility sample must follow observed pin drain",
		},
		{
			name: "drain cannot retry arbitrary error", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				s := sample(b, "after_retirement")
				s.Compaction, s.Metrics.RetiredPins = nil, nil
				s.StatusError = "connection reset"
			},
			problem: "after_retirement sample failed",
		},
		{
			name: "zero at the drain budget boundary", pattern: "clustered",
			mutate: func(b *backfillEvidence) {
				poll := sample(b, "drain_poll")
				poll.ElapsedNS = sample(b, "capture_retired").ElapsedNS + int64(DrainBudget)
				poll.SentNS = new(poll.ElapsedNS - 1)
				sample(b, "fresh_visible").ElapsedNS = poll.ElapsedNS + 1
			},
		},
		{
			name: "drain retry cannot overlap the previous request", pattern: "clustered",
			mutate: func(b *backfillEvidence) {
				sample(b, "drain_poll").SentNS = new(sample(b, "after_retirement").ElapsedNS - 1)
			},
			problem: "request overlapped a previous observation",
		},
		{
			name: "late zero exceeds producer drain budget", pattern: "clustered",
			mutate: func(b *backfillEvidence) {
				poll := sample(b, "drain_poll")
				poll.ElapsedNS = sample(b, "capture_retired").ElapsedNS + int64(DrainBudget) + 1
				poll.SentNS = new(poll.ElapsedNS - 1)
				sample(b, "fresh_visible").ElapsedNS = poll.ElapsedNS + 1
			},
			problem: "exceeded the drain budget",
		},
		{
			name: "delay before first poll counts toward drain budget", pattern: "clustered",
			mutate: func(b *backfillEvidence) {
				first := sample(b, "after_retirement")
				first.ElapsedNS = sample(b, "capture_retired").ElapsedNS + int64(DrainBudget) + 1
				first.SentNS = new(first.ElapsedNS - 1)
				first.Metrics.RetiredPins = new(uint64(0))
				remove(b, "drain_poll")
				sample(b, "fresh_visible").ElapsedNS = first.ElapsedNS + 1
			},
			problem: "exceeded the drain budget",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var r record
			if err := json.Unmarshal(small, &r); err != nil {
				t.Fatal(err)
			}
			phase := 0
			if tc.pattern == "scattered" {
				phase = 1
			}
			if tc.mutate != nil {
				tc.mutate(&r.Diagnostics.Timelines[phase])
			}
			data, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			checkWireProof(t, data, large, tc.problem)
		})
	}
}

func TestRuntimeObservationPresence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		wire       string
		oldestKind jsontext.Kind
		pending    *uint64
		reason     string
	}{
		{wire: `{}`},
		{wire: `{"pending_compactions":0,"oldest_pinned_generation": null }`, oldestKind: jsontext.KindNull, pending: new(uint64(0))},
		{wire: `{"pending_compactions":null,"oldest_pinned_generation":null,"unavailable":{"oldest_pinned_generation":"withheld"}}`,
			oldestKind: jsontext.KindNull, reason: "withheld"},
	} {
		var m runtimeObservations
		if err := json.Unmarshal([]byte(tc.wire), &m); err != nil {
			t.Fatal(err)
		}
		if got := m.OldestPinnedGeneration.Kind(); got != tc.oldestKind {
			t.Errorf("%s: oldest generation kind = %v, want %v", tc.wire, got, tc.oldestKind)
		}
		if m.PendingCompactions == nil && tc.pending != nil ||
			m.PendingCompactions != nil && (tc.pending == nil || *m.PendingCompactions != *tc.pending) {
			t.Errorf("%s: unexpected pending compactions %v", tc.wire, m.PendingCompactions)
		}
		if got := m.Unavailable["oldest_pinned_generation"]; got != tc.reason {
			t.Errorf("%s: oldest generation reason = %q, want %q", tc.wire, got, tc.reason)
		}
	}
}

func TestValidateCapacityObservations(t *testing.T) {
	t.Parallel()
	id := fmt.Sprintf("%q", strings.Repeat("0123456789abcdef", 4))
	small, large := observationRecord(t, SmallScale), observationRecord(t, LargeScale)
	for _, tc := range []struct {
		name, event, metrics, statusError, problem string
		omitStatus                                 bool
	}{
		{name: "omitted running poll status", event: "status_poll", omitStatus: true, problem: "compaction state for status_poll sample"},
		{name: "historical omission", event: "compaction_start"},
		{name: "numeric zero and empty live pin set", event: "compaction_start",
			metrics: `{"pending_compactions":0,"oldest_pinned_generation":null}`},
		{name: "uint64 count and live pin", event: "overlap",
			metrics: `{"pending_compactions":18446744073709551615,"oldest_pinned_generation":` + id + `}`},
		{name: "oldest live pin may survive predecessor drain", event: "drain_poll",
			metrics: `{"retired_generation_pins":0,"oldest_pinned_generation":` + id + `}`},
		{name: "both diagnostics unavailable", event: "status_poll",
			metrics: `{"pending_compactions":null,"oldest_pinned_generation":null,"unavailable":{"pending_compactions":"missing","oldest_pinned_generation":"missing"}}`},
		{name: "missing value with a reason", event: "status_poll",
			metrics: `{"unavailable":{"oldest_pinned_generation":"missing"}}`},
		{name: "unrelated reason and future diagnostic", event: "publication",
			metrics: `{"pending_compactions":0,"future_metric":{"value":true},"unavailable":{"oldest_pinned_generation":"missing"}}`},
		{name: "historical skipped status placeholder", event: "capture_retired",
			metrics: `{"oldest_pinned_generation":null}`},
		{name: "retry unavailable snapshot", event: "after_retirement", statusError: context.DeadlineExceeded.Error(),
			metrics: `{"oldest_pinned_generation":null,"unavailable":{"oldest_pinned_generation":"timeout","retired_generation_pins":"timeout"}}`},
		{name: "pending zero contradicts reason", event: "compaction_start",
			metrics: `{"pending_compactions":0,"unavailable":{"pending_compactions":"missing"}}`,
			problem: "compaction_start sample: pending_compactions contradicts unavailable status"},
		{name: "staging zero contradicts reason", event: "overlap",
			metrics: `{"staging_used_bytes":0,"unavailable":{"staging_used_bytes":"missing"}}`,
			problem: "overlap sample: staging_used_bytes contradicts unavailable status"},
		{name: "retired zero contradicts reason during running poll", event: "status_poll",
			metrics: `{"retired_generation_pins":0,"unavailable":{"retired_generation_pins":"missing"}}`,
			problem: "status_poll sample: retired_generation_pins contradicts unavailable status"},
		{name: "retired zero contradicts reason during drain", event: "drain_poll",
			metrics: `{"retired_generation_pins":0,"unavailable":{"retired_generation_pins":"missing"}}`,
			problem: "drain_poll sample: retired_generation_pins contradicts unavailable status"},
		{name: "generation contradicts reason", event: "publication",
			metrics: `{"oldest_pinned_generation":` + id + `,"unavailable":{"oldest_pinned_generation":"missing"}}`,
			problem: "publication sample: oldest_pinned_generation contradicts unavailable status"},
		{name: "zero on failed request", event: "after_retirement", statusError: context.DeadlineExceeded.Error(),
			metrics: `{"retired_generation_pins":0}`, problem: "retired_generation_pins requires a successful candidate status snapshot"},
		{name: "generation on skipped request", event: "capture_retired",
			metrics: `{"oldest_pinned_generation":` + id + `}`, problem: "oldest_pinned_generation requires a successful candidate status snapshot"},
		{name: "negative pending count", event: "status_poll",
			metrics: `{"pending_compactions":-1}`, problem: "pending_compactions"},
		{name: "overflowing pending count", event: "status_poll",
			metrics: `{"pending_compactions":18446744073709551616}`, problem: "pending_compactions"},
		{name: "fractional pending count", event: "status_poll",
			metrics: `{"pending_compactions":0.5}`, problem: "pending_compactions"},
		{name: "quoted pending count", event: "status_poll",
			metrics: `{"pending_compactions":"0"}`, problem: "pending_compactions"},
		{name: "numeric generation", event: "publication",
			metrics: `{"oldest_pinned_generation":0}`, problem: "oldest_pinned_generation must be a lowercase hexadecimal SHA-256 digest or null"},
		{name: "short generation", event: "publication",
			metrics: `{"oldest_pinned_generation":"abcd"}`, problem: "oldest_pinned_generation must be a lowercase hexadecimal SHA-256 digest or null"},
		{name: "uppercase generation", event: "publication",
			metrics: `{"oldest_pinned_generation":` + strings.ToUpper(id) + `}`, problem: "oldest_pinned_generation must be a lowercase hexadecimal SHA-256 digest or null"},
		{name: "nonhex generation", event: "publication",
			metrics: fmt.Sprintf(`{"oldest_pinned_generation":%q}`, strings.Repeat("g", 64)),
			problem: "oldest_pinned_generation must be a lowercase hexadecimal SHA-256 digest or null"},
		{name: "empty live pin set cannot prove retired zero", event: "drain_poll",
			metrics: `{"oldest_pinned_generation":null}`, problem: "fresh visibility sample must follow observed pin drain"},
		{name: "pending zero cannot prove retired zero", event: "drain_poll",
			metrics: `{"pending_compactions":0,"retired_generation_pins":null}`, problem: "fresh visibility sample must follow observed pin drain"},
		{name: "live pin ID cannot prove positive held count", event: "before_retirement",
			metrics: `{"oldest_pinned_generation":` + id + `}`, problem: "before_retirement must observe the positive held pin count"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			type object = map[string]any
			var wire object
			if err := json.Unmarshal(small, &wire); err != nil {
				t.Fatal(err)
			}
			timeline := wire["diagnostics"].(object)["backfill_timelines"].([]any)[0].(object)
			samples := timeline["samples"].([]any)
			i := slices.IndexFunc(samples, func(raw any) bool { return raw.(object)["event"] == tc.event })
			if i == -1 {
				t.Fatalf("missing %s sample", tc.event)
			}
			s := samples[i].(object)
			if tc.metrics != "" {
				s["candidate_status_metrics"] = jsontext.Value(tc.metrics)
			}
			if tc.omitStatus {
				delete(s, "status")
			}
			if tc.statusError != "" {
				s["status_error"], s["status"] = tc.statusError, nil
			}

			data, err := json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			checkWireProof(t, data, large, tc.problem)
		})
	}
}

func checkWireProof(t *testing.T, data, large []byte, problem string) {
	t.Helper()
	envelope := fmt.Appendf(nil, `{"schema_version":1,"fixture":%q,"mode":"live","cases":{%q:{"status":"pass","evidence":[%s,%s]}}}`,
		FixtureID, Case, data, large)
	for _, result := range []struct {
		entry string
		err   error
	}{
		{"ValidateRecord", ValidateRecord(data)},
		{"ValidateReport", ValidateReport(envelope)},
	} {
		if problem == "" && result.err != nil ||
			problem != "" && (result.err == nil || !strings.Contains(result.err.Error(), problem)) {
			t.Errorf("%s error = %v, want %q", result.entry, result.err, problem)
		}
	}
}

func observationRecord(t *testing.T, seed int) []byte {
	t.Helper()
	d := &diagnostics{
		MemoryEvents: map[string]*uint64{"oom_kill": new(uint64(0))},
		Candidates: []lifecycleEvidence{
			{Lifetime: "startup", Ready: 1, Reaped: true, ExitCode: new(int(0))},
			{Lifetime: "restart", Ready: 1, Reaped: true, ExitCode: new(int(0))},
		},
	}
	for _, pattern := range [...]string{"clustered", "scattered"} {
		predecessor, generation := 38, 40
		held, requested, final := seed-2*BatchRows, seed-BatchRows, seed
		if pattern == "scattered" {
			held, requested, final = seed, seed+BatchRows, seed+TailRows
			predecessor, generation = 47, 52
		}
		var b backfillEvidence
		if err := json.Unmarshal([]byte(cutoverTimeline(pattern, held, requested, final, predecessor, generation)), &b); err != nil {
			t.Fatal(err)
		}
		start, overlap := b.Samples[0], &b.Samples[1]
		firstRead, lastRead := int64(25), int64(39)
		overlap.Memory = &memoryCurrentSample{ReadStart: 26, ReadEnd: 29, Bytes: new(uint64(1024))}
		if pattern == "clustered" {
			start.Event, start.SentNS, start.ElapsedNS = "status_poll", new(int64(27)), 28
			start.Memory = &memoryCurrentSample{ReadStart: firstRead, ReadEnd: 26, Bytes: new(uint64(1024))}
			overlap.Memory.ReadStart, lastRead = 28, 29
			b.Samples = slices.Insert(b.Samples, 1, start)
		} else {
			firstRead = overlap.Memory.ReadStart
			b.Samples[2].Memory = &memoryCurrentSample{ReadStart: 36, ReadEnd: lastRead, Bytes: new(uint64(1024))}
		}
		b.Memory = &memoryObservation{Samples: 2, Span: lastRead - firstRead, Min: 1024, Max: 1024}
		d.Timelines = append(d.Timelines, b)
	}
	data, err := json.Marshal(record{
		Fixture: fixture{seed, TailRows, LineBytes, BatchRows, MemoryLimit},
		Status:  "pass", Workload: "pass", Phase: "complete", TimedOut: new(false), RecoveredRows: seed + TailRows,
		Compaction:  compactionEvidence{"complete", 52, seed + BatchRows, seed + TailRows},
		Diagnostics: d,
		Cleanup:     cleanupEvidence{Status: "pass", CandidateReaped: true, SubtreeEmpty: true, SubtreeRemoved: true, StoreRemoved: true},
		CgroupLimit: new(uint64(MemoryLimit)), CgroupSwap: new(uint64(0)), CgroupOOM: new(uint64(0)), CgroupPeak: 2048,
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
