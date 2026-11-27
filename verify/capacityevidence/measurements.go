package capacityevidence

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type measurements struct {
	StartReady    int64                         `json:"start_to_ready_ns"`
	RestartReady  int64                         `json:"restart_to_ready_ns"`
	IngestP99     int64                         `json:"ingest_p99_ns"`
	QueryP99      int64                         `json:"backfill_query_p99_ns"`
	IngestSamples int                           `json:"ingest_samples"`
	QuerySamples  int                           `json:"backfill_query_samples"`
	PeakRSS       uint64                        `json:"peak_rss_bytes"`
	PeakFDs       uint64                        `json:"peak_descriptors"`
	InitialFree   uint64                        `json:"initial_disk_free_bytes"`
	MinFree       uint64                        `json:"minimum_disk_headroom_bytes"`
	DeviceIO      map[string]map[string]*uint64 `json:"device_io"`
	Unavailable   map[string]string             `json:"unavailable"`
}

func (r measurements) validate() error {
	if r.StartReady <= 0 || r.RestartReady <= 0 || r.IngestP99 <= 0 || r.QueryP99 <= 0 ||
		r.IngestSamples != TailRows/BatchRows || r.QuerySamples <= 0 {
		return errors.New("missing or invalid timing measurements or sample counts")
	}
	if r.PeakRSS == 0 || r.PeakFDs == 0 ||
		r.MinFree == 0 || r.InitialFree < r.MinFree || len(r.Unavailable) != 0 {
		return errors.New("missing or invalid resource measurements")
	}
	if len(r.DeviceIO) == 0 {
		return errors.New("missing device I/O measurements")
	}
	for device, counters := range r.DeviceIO {
		major, minor, _ := strings.Cut(device, ":")
		_, majorErr := strconv.ParseUint(major, 10, 32)
		_, minorErr := strconv.ParseUint(minor, 10, 32)
		if majorErr != nil || minorErr != nil || counters["rbytes"] == nil || counters["wbytes"] == nil {
			return fmt.Errorf("invalid I/O measurements for device %q", device)
		}
		for name, value := range counters {
			if name == "" || value == nil {
				return fmt.Errorf("invalid I/O counter %q for device %q", name, device)
			}
		}
	}
	return nil
}
