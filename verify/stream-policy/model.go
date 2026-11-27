package streampolicy

import (
	"cmp"
	"net/http"
	"slices"
	"strings"
)

const (
	backfillKey             = "fixture-backfill-key"
	rollbackKey             = "fixture-before-commit-key"
	interruptedKey          = "fixture-after-commit-key"
	rawBackfill             = "fixture-raw-backfill"
	rawInterrupted          = "fixture-raw-interrupted"
	maskInputSecret         = "fixture-rotating-secret"
	backfillMarker          = "fixture-retained-backfill"
	maskedMarker            = "fixture-mask-subject"
	beforeCommitMarker      = "fixture-retained-before-commit"
	afterCommitMarker       = "fixture-retained-after-commit"
	freshPolicyMarker       = "fixture-retained-fresh-policy"
	maskResult              = "[masked]"
	changedRedactionVersion = "fixture-policy-a-v2"
	changedReplacement      = "[redacted-v2]"
)

type modeledRecord struct {
	Record
	version string
}

// referenceModel advances from fixture expectations and scheduled policy
// changes, never from rows, retry records, or response bytes under test.
// Its append order is the tie breaker for both queries and replay.
type referenceModel struct {
	streams  map[string]Scope
	grants   map[Target]Grant
	versions map[Scope]string
	masking  map[Scope]MaskingChange
	log      []modeledRecord
	retries  []RetryRecord
}

func newReferenceModel(f Fixture, initial []appendCase) referenceModel {
	m := referenceModel{
		streams: make(map[string]Scope), grants: make(map[Target]Grant),
		versions: make(map[Scope]string), masking: make(map[Scope]MaskingChange),
	}
	for _, stream := range f.Streams {
		m.streams[stream.Selector] = stream.Scope
		m.versions[stream.Scope] = stream.PolicyVersion
	}
	for _, grant := range f.Grants {
		m.grants[grant.Target] = grant
	}
	for _, batch := range initial {
		m.append(batch)
	}
	return m
}

func (m *referenceModel) append(batch appendCase) {
	scope := m.streams[batch.request.Selector]
	version := m.versions[scope]
	for _, row := range batch.rows {
		m.log = append(m.log, modeledRecord{Record{scope, row}, version})
	}
	m.retries = append(m.retries, RetryRecord{scope, batch.request.Principal, batch.request.Key, version, http.StatusNoContent})
}

func (m *referenceModel) records() []Record {
	records := make([]Record, 0, len(m.log))
	for _, entry := range m.log {
		records = append(records, entry.Record)
	}
	return records
}

func (m *referenceModel) query(r QueryRequest) []Row {
	if !m.grants[r.Target].Query {
		return nil
	}
	scope := m.streams[r.Selector]
	// All fixture timestamps and bounds are canonical UTC whole seconds.
	// Comparing them lexically gives timestamp order. A stable sort preserves
	// the append order of tied rows and retains duplicates.
	var rows []Row
	for _, entry := range m.log {
		if entry.Scope != scope {
			continue
		}
		row := entry.Row
		if rule := m.masking[scope].Replacement; rule.Forbidden != "" {
			row.Line = strings.ReplaceAll(row.Line, rule.Forbidden, rule.Replacement)
		}
		if r.Start <= row.Timestamp && row.Timestamp < r.End && strings.Contains(row.Line, r.Contains) {
			rows = append(rows, row)
		}
	}
	slices.SortStableFunc(rows, func(a, b Row) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
	return rows
}

func (m *referenceModel) export(target Target) ([]Record, []string) {
	var records []Record
	var versions []string
	if !m.grants[target].Export {
		return records, versions
	}
	for _, entry := range m.log {
		if entry.Scope == m.streams[target.Selector] {
			records = append(records, entry.Record)
			if !slices.Contains(versions, entry.version) {
				versions = append(versions, entry.version)
			}
		}
	}
	slices.Sort(versions)
	return records, versions
}

func compareRetryRecord(a, b RetryRecord) int {
	for _, order := range []int{
		cmp.Compare(a.Scope.Tenant, b.Scope.Tenant), cmp.Compare(a.Scope.Stream, b.Scope.Stream),
		cmp.Compare(a.Principal, b.Principal), cmp.Compare(a.Key, b.Key),
		cmp.Compare(a.PolicyVersion, b.PolicyVersion), cmp.Compare(a.Status, b.Status),
	} {
		if order != 0 {
			return order
		}
	}
	return 0
}

type rowInput struct{ timestamp, retained, raw string }

func additionalBatch(target Target, key, replacement string, inputs ...rowInput) appendCase {
	batch := appendCase{request: IngestRequest{Target: target, Key: key}}
	for _, input := range inputs {
		batch.request.Body += ndjson(input.timestamp, input.retained+" "+input.raw)
		batch.rows = append(batch.rows, Row{input.timestamp, input.retained + " " + replacement})
	}
	return batch
}

func backfillBatch(f Fixture) appendCase {
	// The exact duplicate ties with a seed row. The last canonical row sits
	// at the query's exclusive upper bound and still belongs in the export.
	tied := rowInput{"2026-01-01T00:00:01Z", backfillMarker, rawBackfill + " " + plain}
	return additionalBatch(Target{alice, f.Streams[0].Selector}, backfillKey, replacement,
		tied, tied, rowInput{f.QueryStart, maskedMarker, escaped},
		rowInput{f.QueryEnd, "fixture-excluded-upper-bound", plain})
}
