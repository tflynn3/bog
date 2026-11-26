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
creates its own 32 MiB ext4 image under the system temporary directory. Use a
local filesystem with room for the image and the private executable copy.

The fixture compares complete cursor walks with an ordered row oracle. It
checks acknowledged and rejected batches across restarts, accepts only a whole
batch or its absence when a SIGKILL interrupts an ingest without a final
response, and checks that an incomplete HTTP request never publishes rows.
Recovery exercises torn headers and payloads, byte-for-byte preservation
after startup rejects committed corruption, tied backfill, and stale cursors.
The disk case requires kernel ENOSPC and checks rollback, a fresh durable
ingest after freeing space, and recovery on restart.

The version-1 report includes the six fixture outcomes and the release gate's
`sigkill`, `recovery` and `enospc` cases. A failed fixture produces a report
and a nonzero exit. The tail of each candidate process's output is kept in the
verifier log. The Linux PR workflow runs vet, race tests, and the live
fixtures against the PR checkout's pinned build.

```sh
go vet ./verify/durability
go test -race ./verify/durability
BOG_DURABILITY_BINARY=/absolute/path/to/bog BOG_DURABILITY_REVISION="$REVISION" \
  BOG_DURABILITY_SHA256="$BINARY_SHA256" \
  go test -race -count=1 -timeout=3m ./verify/durability
```

## Historical pin and benchmark

The live correctness suite passed on `f727b8f676845a8058d1ecf114442d8963cbbbae`
and release base `3c0a586e241b75a64094b4275edfe278b196763c` in an Ubuntu arm64
Lima VM (four vCPUs, 5 GiB RAM, kernel `6.8.0-100-generic`). The data directory
for the benchmark was on the guest's local ext4 `/dev/vda1` with mount options
`rw,relatime,discard,errors=remount-ro,commit=30`. The disk-full fixture used
a separate disposable ext4 loop image.

Go 1.27.0, Linux arm64, `CGO_ENABLED=0`, `-trimpath -buildvcs=true` produced
these binary SHA-256 values.

| Revision | Binary SHA-256 |
| --- | --- |
| `f727b8f` | `31ff595e91af19fb39911df1e85ca08dfa51da314ac35a0df824e59f0dc49080` |
| `2c26412` | `558d8cfd090a44fcc9ed5946ee51293fd9da55f5a0a737ff7c183b332812806b` |
| `3c0a586` | `2a53ebd2a5b856389fa8c781ae7849e1827fa62d4ed24dbad2487fc8d99e2439` |

Each run used a fresh store and the same 128,000 rows in 1,000 sequential
batches of 128. The fixture used UTC timestamps in groups of four and backdated
every fourth batch by 64 seconds. Row IDs included 64-bit values from Python's
`random.Random(314159)`. Every eighth row matched `hit-`. The concatenated
NDJSON had SHA-256
`5cc0711db302b5f228d2adb526a9913abd9126df54a35ed2623b7f2565e96523`.
Each durable ingest was followed by a ten-row query for `hit-`. Throughput
uses time spent awaiting ingest responses. Latency percentiles use nearest
rank. Restart-to-health includes polling at 50 ms intervals. Peak RSS uses
Linux `VmHWM` across the initial and restarted processes. The post-restart
cursor walk matched all 128,000 rows in timestamp and append order on every
run.

| Pair | Revision | Durable rows/s | Ingest p99 (ms) | Query p95 (ms) | Restart-to-health (ms) | Peak RSS (KiB) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | `f727b8f` | 44,809.53 | 7.572 | 0.560 | 152.390 | 43,164 |
| 1 | `2c26412` | 45,997.77 | 8.746 | 0.727 | 258.499 | 42,876 |
| 2 | `f727b8f` | 20,626.05 | 32.518 | 2.023 | 154.647 | 43,084 |
| 2 | `2c26412` | 24,698.18 | 29.824 | 1.614 | 207.379 | 43,412 |
| 3 | `f727b8f` | 46,290.54 | 5.425 | 0.681 | 204.229 | 43,192 |
| 3 | `2c26412` | 60,054.71 | 5.845 | 0.470 | 153.056 | 42,972 |

Throughput varied 2.2× on `f727b8f` and 2.4× on `2c26412` between the
slowest and fastest runs. Query p95 varied 3.6× and 3.4× respectively.
These runs measure the legacy retained-history path. Run the same fixture
on a candidate exposing disk-backed retained history for that comparison.
