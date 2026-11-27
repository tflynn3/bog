package streampolicy

import (
	"strconv"
	"strings"
)

// Grant lists the independent permissions to provision on the fixture target.
type Grant struct {
	Target
	Ingest, Query, Export bool
}

type Stream struct {
	Selector      string
	Scope         Scope
	PolicyVersion string
}

type Replacement struct{ Forbidden, Replacement string }

// Fixture defines the setup and synthetic inputs for Run. Selectors are
// fixture routing handles, independent of the canonical tenant and stream IDs.
// The primary principal has explicit access to both fixture tenants.
type Fixture struct {
	QueryStart, QueryEnd string // Half-open UTC interval covering the fixture rows.
	Streams              [3]Stream
	Grants               []Grant
	Replacements         []Replacement
	// FailOn simulates unavailability of redaction on a row containing it.
	FailOn string
}

const (
	alice       = "fixture-alice"
	bob         = "fixture-bob"
	exporter    = "fixture-export-only"
	unknown     = "fixture-unknown-selector"
	key         = "fixture-shared-key"
	plain       = "fixture-secret-plain"
	escaped     = "fixture-secret-\"\\-雪🦉"
	failed      = "fixture-failed-redaction"
	uncommitted = "fixture-uncommitted-line"
	replacement = "[redacted]"
)

// ReferenceFixture returns fresh setup data for a single run. The pairs of
// scopes differ by stream alone and tenant alone relative to the first scope.
func ReferenceFixture() Fixture {
	return Fixture{
		QueryStart: "2026-01-01T00:00:00Z",
		QueryEnd:   "2026-01-01T00:01:00Z",
		Streams: [3]Stream{
			{"fixture-a", Scope{"fixture-tenant-one", "fixture-stream-one"}, "fixture-policy-a-v1"},
			{"fixture-b", Scope{"fixture-tenant-one", "fixture-stream-two"}, "fixture-policy-b-v1"},
			{"fixture-c", Scope{"fixture-tenant-two", "fixture-stream-one"}, "fixture-policy-c-v1"},
		},
		Grants: []Grant{
			{Target{alice, "fixture-a"}, true, true, true},
			{Target{alice, "fixture-b"}, true, true, true},
			{Target{alice, "fixture-c"}, true, true, true},
			{Target{bob, "fixture-a"}, true, true, false},
			{Target{bob, "fixture-b"}, false, false, true},
			{Target{exporter, "fixture-a"}, false, false, true},
		},
		Replacements: []Replacement{{plain, replacement}, {escaped, replacement}},
		FailOn:       failed,
	}
}

type appendCase struct {
	request IngestRequest
	records []Record
}

func fixtureAppends(f Fixture) []appendCase {
	return []appendCase{
		fixtureAppend(f.Streams[0], alice, "retained-alice-a", plain, "2026-01-01T00:00:01Z"),
		fixtureAppend(f.Streams[1], alice, "retained-alice-b", escaped, "2026-01-01T00:00:02Z", "2026-01-01T00:00:03Z"),
		fixtureAppend(f.Streams[2], alice, "retained-alice-c", plain, "2026-01-01T00:00:04Z", "2026-01-01T00:00:05Z"),
		fixtureAppend(f.Streams[0], bob, "retained-bob-a", escaped, "2026-01-01T00:00:06Z"),
	}
}

func fixtureAppend(stream Stream, principal, prefix, secret string, timestamps ...string) appendCase {
	entry := appendCase{request: IngestRequest{Target: Target{principal, stream.Selector}, Key: key}}
	for i, timestamp := range timestamps {
		line := prefix
		if i > 0 {
			line += "-" + strconv.Itoa(i+1)
		}
		entry.request.Body += ndjson(timestamp, line+" "+secret)
		entry.records = append(entry.records, Record{stream.Scope, Row{timestamp, line + " " + replacement}})
	}
	return entry
}

func ndjson(timestamp, line string) string {
	// The fixture strings use only JSON-compatible Go escapes. Encode the
	// Unicode marker with both a BMP code unit and a surrogate pair.
	quoted := strings.ReplaceAll(strconv.Quote(line), "雪🦉", `\u96ea\ud83e\udd89`)
	return `{"ts":` + strconv.Quote(timestamp) + `,"line":` + quoted + "}\n"
}
