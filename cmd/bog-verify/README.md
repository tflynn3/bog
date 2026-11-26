# bog-verify

Build the command with `go build ./cmd/bog-verify`.

```sh
bog-verify verify -backup /srv/backup/bog-snapshot

bog-verify compare -backup /srv/backup/bog-snapshot \
  -url http://127.0.0.1:8080 \
  -start 2026-01-01T00:00:00Z -end 2026-01-02T00:00:00Z \
  -contains 'request completed' -limit 100
```

Keep the reference backup immutable for the entire operation. For comparison,
restore a separate copy and point `-url` at a running Bog server that can serve
the restored image. Keep the restored server free of writes before the first
query if you expect an exact match to the backup's recovery point.

Both commands verify the backup's integrity and report the recovery point on
success. Comparison verifies the reference before issuing a query. It applies
the same half-open time range and case-sensitive substring to both sides, then
compares decoded timestamps and lines in order, including tied timestamps and
duplicates. The page boundaries may differ. A successful comparison reports
the matched row count and both page counts after consuming both terminal pages.
Bog emits at least one row on each page carrying a continuation. The Go client
also accepts fully framed empty pages with fresh cursors, and comparison
follows them within the same page budget.

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
| 0 | Backup verified, or both query streams exhausted with identical rows |
| 2 | Invalid command or options |
| 3 | Backup verification failed |
| 4 | Comparison incomplete or result could not be reported |
| 5 | First ordering, timestamp, line, or length mismatch found |

Parity diagnostics use one-based row numbers. Line mismatches include the
one-based byte offset and quoted previews around it.
