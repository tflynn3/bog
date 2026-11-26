package main

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestLedgerReconcile(t *testing.T) {
	t.Parallel()
	ordered := fixtureRows("z-ordered", 2, 2, "")
	newer := fixtureRows("newer", 1, 3, "")
	backfill := fixtureRows("a-backfill", 1, 2, "")
	invalid := fixtureRows("rejected", 1, 2, "")
	inFlight := fixtureRows("unknown", 2, 2, "")
	l := ledger{{ordered, acknowledged}, {newer, acknowledged}, {backfill, acknowledged}, {invalid, rejected}, {inFlight, unknown}}
	without := slices.Concat(ordered, backfill, newer)
	with := slices.Concat(ordered, backfill, inFlight, newer)
	wrongTime := without[0]
	wrongTime.TS = wrongTime.TS.Add(time.Second)
	equivalentTime := without[0]
	equivalentTime.TS = equivalentTime.TS.In(time.FixedZone("offset", -7*60*60))

	for _, tc := range []struct {
		name      string
		rows      []row
		recovered int
		wantErr   string
	}{
		{name: "unknown absent", rows: without},
		{name: "unknown whole", rows: with, recovered: 2},
		{name: "offset equivalent", rows: slices.Concat([]row{equivalentTime}, without[1:])},
		{name: "unknown partial", rows: slices.Concat(without[:3], inFlight[:1], newer), wantErr: "recovered 1 of 2"},
		{name: "acknowledged missing", rows: without[1:], wantErr: "expected 4"},
		{name: "acknowledged duplicate", rows: slices.Concat(without, without[:1]), wantErr: "duplicate"},
		{name: "rejected leaked", rows: slices.Concat(without[:3], invalid, newer), wantErr: "expected 4"},
		{name: "rejected replaced acknowledged", rows: slices.Concat(without[:2], invalid, newer), wantErr: "row 2"},
		{name: "tie reordered", rows: slices.Concat(backfill, ordered, newer), wantErr: "row 0"},
		{name: "timestamp changed", rows: slices.Concat([]row{wrongTime}, without[1:]), wantErr: "row 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			proof, err := l.reconcile(tc.rows)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("got %v, expected %q", err, tc.wantErr)
				}
				return
			}
			want := reconciliation{4, 1, 2, tc.recovered}
			if err != nil || proof != want {
				t.Fatalf("reconcile = (%+v, %v), expected %+v", proof, err, want)
			}
		})
	}
}
