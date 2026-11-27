package main

import "bog/verify/capacityevidence"

const capacityFixtureID = capacityevidence.FixtureID

func validateCapacityReport(data []byte, requireCutover bool) error {
	if requireCutover {
		return capacityevidence.ValidateReleaseCutoverReport(data)
	}
	return capacityevidence.ValidateHistoricalReport(data)
}
