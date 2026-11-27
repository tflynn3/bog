package querymodel

import "testing"

func TestQueryCostsPartialObservation(t *testing.T) {
	t.Parallel()
	scanned := QueryCostCounter{Source: "input bytes", Value: 41}
	zero := QueryCostCounter{Source: "fixture charges"}
	work := QueryCostCounter{Source: "fixture charges", Value: 8}
	pages := []QueryCostPage{
		{QueryScanObservation: QueryScanObservation{ScannedBytes: &scanned, ChargedWorkUnits: &zero}},
		{QueryScanObservation: QueryScanObservation{ChargedWorkUnits: &work}},
	}
	var cost QueryCostWalk
	if err := cost.summarize(pages, WireStats{Pages: 2, Rows: 2}); err != nil {
		t.Fatal(err)
	}
	if cost.ScannedBytes != nil || cost.ScannedBytesPerReturnedRow != nil || cost.Unavailable["scanned_bytes"] == "" ||
		cost.PageObservations[0].Unavailable["scanned_bytes"] != "" || cost.PageObservations[1].Unavailable["scanned_bytes"] == "" {
		t.Fatalf("partial byte count reported as a complete walk: %+v", cost)
	}
	if cost.ChargedWorkUnits == nil || *cost.ChargedWorkUnits != work || cost.Unavailable["charged_work_units"] != "" ||
		cost.PageObservations[0].ChargedWorkUnits == nil || *cost.PageObservations[0].ChargedWorkUnits != zero {
		t.Fatalf("charged work lost an observed zero or total: %+v", cost)
	}
}
