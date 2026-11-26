# Linux durability verifier

Run from the verifier checkout with a candidate built from a clean Git checkout
using `go build -trimpath -buildvcs=true -o /absolute/path/to/bog ./cmd/bog`.

```sh
go run ./verify/durability \
  -binary /absolute/path/to/bog \
  -revision "$REVISION" \
  -sha256 "$BINARY_SHA256" \
  -fixture linux-release-v1 \
  -report /absolute/path/to/durability.json
```

Set `BINARY_SHA256` to the trusted SHA-256 recorded when the candidate was built.
Before execution, the verifier compares this required digest with the SHA-256 of
a private copy and checks that copy's Git build metadata.
For the disk fixture it needs Linux loop devices, `mkfs.ext4`, `mount` and
`umount`, and either mount privileges or passwordless `sudo -n`. The verifier
creates its own 32 MiB ext4 image under the system temporary directory.

The fixture compares complete cursor walks with an ordered row oracle. It
checks acknowledged and rejected batches across restarts, accepts only a whole
batch or its absence when a SIGKILL interrupts an ingest without a final
response, and exercises torn headers and payloads, committed checksum damage,
tied backfill, and stale cursors. The disk case requires kernel ENOSPC and
checks rollback, a fresh durable ingest after freeing space, and recovery on
restart.

The version-1 report includes the six fixture outcomes and the release gate's
`sigkill`, `recovery` and `enospc` cases. A failed fixture produces a report
and a nonzero exit. The workflow entry point in the base revision is
`./verify/linux`. Use `./verify/durability` for this producer.

```sh
go vet ./verify/durability
go test -race ./verify/durability
BOG_DURABILITY_BINARY=/absolute/path/to/bog BOG_DURABILITY_REVISION="$REVISION" \
  BOG_DURABILITY_SHA256="$BINARY_SHA256" \
  go test -race ./verify/durability
```
