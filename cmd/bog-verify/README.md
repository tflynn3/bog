# bog-verify

Build the command with `go build ./cmd/bog-verify`.

```sh
bog-verify verify -backup /srv/backup/bog-snapshot

bog-verify compare -backup /srv/backup/bog-snapshot \
  -url http://127.0.0.1:8080 \
  -start 2026-01-01T00:00:00Z -end 2026-01-02T00:00:00Z \
  -contains 'request completed' -limit 100

bog-verify drill -backup /srv/backup/bog-snapshot \
  -url http://127.0.0.1:8080 \
  -start 2026-01-01T00:00:00Z -end 2026-01-02T00:00:00Z \
  -contains 'request completed' -limit 100 -max-retries 3
```

Keep the reference backup immutable for the entire operation. For comparison,
restore a separate copy and point `-url` at a running Bog server that can serve
the restored image. Keep the restored server free of writes before the first
query if you expect an exact match to the backup's recovery point.

All commands verify the backup's integrity and report the recovery point on
success. Comparison verifies the reference before issuing a query. It applies
the same half-open time range and case-sensitive substring to both sides, then
compares decoded timestamps and lines in order, including tied timestamps and
duplicates. The page boundaries may differ. A successful comparison reports
the matched row count and both page counts after consuming both terminal pages.
Bog emits at least one row on each page carrying a continuation. The Go client
also accepts fully framed empty pages with fresh cursors, and comparison
follows them within the same page budget.

## Restore drill

Point `-url` at the restored instance directly, with writes paused for the
entire drill. Wait for the verifier's restart prompt after the first
comparison. Stop and restart the server on the same restored data directory
and URL. Once the server is ready, run the prompt's `kill -USR1 PID` command
against the verifier process. The signal acknowledges the operator's restart.

The drill reopens and verifies the immutable reference, checks its recovery
point, and starts a new query snapshot at page one. Each phase has its own
page budget. The verifier reports the phase and the first differing row, or
the matched row and page counts for each completed phase.

`-max-retries` defaults to three extra attempts per live page and accepts
zero for a single attempt. `-retry-delay` defaults to 100 ms between attempts.
On HTTP 503 or interrupted page delivery the verifier retries the same
request, including its cursor. Empty and terminal pages count once when
accepted. Capture expiry (HTTP 410), invalid cursors (HTTP 400), and retry
exhaustion produce distinct incomplete diagnostics. The overall
`-timeout` covers both comparisons, retry waits, and the restart boundary.

On native Linux, run the fixture-backed integration test with a built #53
candidate for the runner's architecture. The test restores a disposable
copy and runs the read-only drill, checking exact parity with the immutable
bundle before and after restart. Once the drill completes, the test harness
posts a batch of backfilled rows with tied timestamps and requires HTTP 204.
It restarts the same binary on the same data directory and listener address,
compares the full ordered history with the original fixture and appended rows
through the terminal page, and reverifies the reference bundle.

```sh
BOG_RESTORE_DRILL_BINARY=/absolute/path/to/bog \
  go test -count=1 ./cmd/bog-verify -run '^TestRestoreDrillShippedServer$' -v
```

The test skips when `BOG_RESTORE_DRILL_BINARY` is unset. A supplied candidate
must pass every phase. The CLI's `drill` remains read-only. Release parity is
qualified by [`verify/release`](../../verify/release/README.md).

## Limits and exit statuses

`-limit` defaults to 100 rows per page and accepts 1 through 1000.
`-max-pages` defaults to 100,000 pages per stream, including empty and terminal
pages.
`-max-page-bytes` defaults to 33,554,432 bytes (32 MiB) per live HTTP page.
If the server sends larger pages, raise the ceiling to fit the encoded page
or lower `-limit` to request fewer rows. The comparison holds up to one live
page and one byte-bounded reference page at a time.
The reference walk uses private files under the system temporary directory
to sort matching committed-tail rows, so allow temporary disk space for
those rows and their merge output. The command removes its scratch files
when the walk succeeds, fails, or is canceled.
`-timeout` defaults to five minutes and covers verification and the full walk.
If a walk fails or exceeds its budget, the command reports the failing side
and row, with the page position for a failed fetch. A new invocation starts
a fresh snapshot.

| Exit status | Meaning |
| --- | --- |
| 0 | Backup verified, comparison matched, or both drill phases matched |
| 2 | Invalid command or options |
| 3 | Backup verification failed |
| 4 | Comparison incomplete or result could not be reported |
| 5 | First ordering, timestamp, line, or length mismatch found |

Parity diagnostics use one-based row numbers. Line mismatches include the
one-based byte offset and quoted previews around it.
