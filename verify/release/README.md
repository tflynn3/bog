# Linux release verification

Dispatch the **Linux release verification** workflow with the full commit SHA.
The workflow builds the candidate once with Go 1.27.0 on Ubuntu 24.04 amd64 and
uses the resulting absolute binary path for every verifier. Download the
`linux-release-evidence` artifact for the revision, SHA-256, build metadata,
fixture, reports, producer exit statuses, and logs.

All three producers take `-binary PATH -revision SHA -fixture ID -report PATH`.
The release fixture ID is `linux-release-v1`. The workflow checks the
candidate against `binary.sha256` before each producer and again before final
validation. It runs `go run ./verify/release` from the workflow checkout and
`go run ./verify/durability` and `go run ./bench/retained-history` from the
candidate checkout. Durability also takes `-sha256 SHA256` from the build record
and verifies the digest and clean Git metadata of its private executable copy.
Dispatch the gate against a revision containing both producer commands.
Missing commands, failed cases, and missing reports fail the gate. Logs
and available reports are uploaded even when a producer fails.

The [durability fixture](../durability/README.md) requires a local temporary
filesystem with room for a 32 MiB image, Linux loop devices, `mkfs.ext4`,
`mount`, `umount`, and permission to mount and unmount the loopback filesystem
either directly or through passwordless `sudo -n`.

Reports have integer `schema_version` 1, string `revision`, lowercase hexadecimal
`binary_sha256`, string `fixture`, and a `cases` object. Each case has a string
`status` (`pass`, `fail`, or `skip`) and may include a string `detail` and
additional evidence. The following cases must pass.

| Producer | Required cases |
| --- | --- |
| Admission | `one_slot_saturation`, `ingest_during_saturation`, `cursor_resume`, `work_budget` |
| Durability | `sigkill`, `recovery`, `enospc` |
| Capacity | `retained_history_capacity` |

The durability report's `enospc` case requires the following `evidence` object.

```json
{
  "errno": "ENOSPC",
  "disposable_local_filesystem": true,
  "observed_enospc": true,
  "recovered_after_space_freed": true
}
```

## Admission fixture

The probe ingests 320 timestamped rows one second apart from
2020-01-01T00:00:00Z in 32-row batches. Each line contains an ordinal and
32 KiB of padding. With `-query-concurrency=1`, it captures a one-row snapshot
page and starts a large continuation with the opaque cursor. The slow TCP
reader uses a 4096-byte receive buffer and stops reading after parsing the
response headers. The probe checks the Linux TCP persist timer and queued
bytes before and after a competing continuation and an ingest attempt.
The competing response must complete within two seconds and have HTTP 503,
zero NDJSON rows, and no next cursor. Ingest must return HTTP 204.

After disconnecting the slow reader, the probe replays the original cursor
and checks every remaining row, in order, through the terminal page. A
fresh query confirms that the acknowledged concurrent backfill, timestamped
inside the original snapshot range, is visible. A separate server with
`-query-work-budget=1` receives 32 fixture rows and must return a clean HTTP
503 on a scan for an absent string.

Run the report tests independently with `go test -race ./verify/release`.
