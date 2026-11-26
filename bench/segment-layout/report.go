package main

import "time"

const reportVersion = 3
const fixtureVersion = "-log-v3"
const cachePolicy = "FADV_RANDOM on open, FADV_DONTNEED before each timed query"

type generationReport struct {
	InputSHA256               string        `json:"input_sha256"`
	Rows                      int           `json:"rows"`
	Blocks                    int           `json:"blocks"`
	RetainedBytes             int64         `json:"retained_bytes"`
	BloomMetadataBytes        int64         `json:"bloom_metadata_bytes"`
	BloomBytesPerRetainedByte float64       `json:"bloom_bytes_per_retained_byte"`
	Queries                   []measurement `json:"queries"`
}

type backfillReport struct {
	Scenario     string    `json:"scenario"`
	AppendedRows int       `json:"appended_rows"`
	Earliest     time.Time `json:"earliest_appended_timestamp"`
	Latest       time.Time `json:"latest_appended_timestamp"`
	RewriteBytes int64     `json:"rewrite_bytes"`
	generationReport
}

type layoutReport struct {
	Name string `json:"layout"`
	generationReport
	RewriteBytes int64            `json:"rewrite_bytes"` // Clustered rewrite, also listed in Backfills.
	Backfills    []backfillReport `json:"backfills"`
}

type fixtureReport struct {
	Name            string `json:"name"`
	Scale           string `json:"scale"`
	Seed            int    `json:"seed"`
	Batches         int    `json:"batches"`
	BatchPattern    []int  `json:"batch_size_pattern"`
	Rows            int    `json:"rows"`
	RewrittenRows   int    `json:"rewritten_rows"`
	TargetBlockSize int    `json:"target_block_bytes"`
	SamplesPerQuery int    `json:"samples_per_query"`
	MaxLineBytes    int    `json:"max_line_bytes"`
}

type report struct {
	SchemaVersion int               `json:"schema_version"`
	Revision      string            `json:"revision"`
	Fixture       fixtureReport     `json:"fixture"`
	Environment   reportEnvironment `json:"environment"`
	Layouts       []layoutReport    `json:"layouts"`
}

type reportEnvironment struct {
	OS                   string `json:"os"`
	Arch                 string `json:"arch"`
	Machine              string `json:"kernel_machine"`
	GoVersion            string `json:"go"`
	BuildRevision        string `json:"build_revision"`
	BinarySHA256         string `json:"binary_sha256"`
	CPUs                 int    `json:"cpus"`
	GOMAXPROCS           int    `json:"gomaxprocs"`
	Kernel               string `json:"kernel"`
	FilesystemMagic      string `json:"filesystem_magic"`
	FilesystemBlockBytes int64  `json:"filesystem_block_bytes"`
	CachePolicy          string `json:"cache_policy"`
	WorkDirectory        string `json:"work_directory"`
	ZstdLevel            string `json:"zstd_level"`
	ZstdWindowBytes      int    `json:"zstd_window_bytes"`
}
