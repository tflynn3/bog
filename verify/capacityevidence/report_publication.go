package capacityevidence

import (
	"encoding/json"
	"fmt"

	"bog/verify/reportpublication"
)

type ReportPublicationState = reportpublication.State

const (
	ReportUnpublished         = reportpublication.Unpublished
	ReportDurabilityUncertain = reportpublication.DurabilityUncertain
	ReportDurablyPublished    = reportpublication.DurablyPublished
)

type ReportPublicationError = reportpublication.Error
type ReportDirectory = reportpublication.OpenedDirectory
type ReportStagingFile = reportpublication.StagingFile

// ReportPublisher retains the standalone JSON replacement API for existing
// consumers, including the nested segment-layout benchmark.
type ReportPublisher struct {
	OpenDir func(string) (ReportDirectory, error)
}

func (p ReportPublisher) WriteReport(path string, results any) error {
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return &ReportPublicationError{Path: path, State: ReportUnpublished, Err: fmt.Errorf("encode report: %w", err)}
	}
	return reportpublication.WriteFile(path, append(data, '\n'), p.OpenDir)
}

// OpenReportDirectory pins the resolved parent for all publication operations.
func OpenReportDirectory(path string) (ReportDirectory, error) {
	return reportpublication.OpenDirectory(path)
}
