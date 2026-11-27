# bog-export

Run the exporter as a separate operator process against a running Bog server.

```sh
go run ./cmd/bog-export \
  -url http://127.0.0.1:8080 \
  -start 2025-01-01T00:00:00Z -end 2025-01-02T00:00:00Z \
  -contains 'job completed' -limit 100 -max-pages 1000 -timeout 2m \
  > rows.ndjson
```

The range includes `-start` and excludes `-end`. `-contains` matches a
case-sensitive literal substring. Rows use the query API's `ts` and `line`
fields and retain server order, including ties and duplicates. The page
budget includes the terminal page. `-max-page-bytes` defaults to 32 MiB.
Retryable page failures get up to three retries at 100 ms intervals by
default. The command uses the original continuation for each retry.

Exit status 0 indicates a complete export, including an empty result, or
successful help. Status 2 indicates invalid options. Status 1 indicates an
incomplete export or output failure. Help and errors go to stderr. Each
accepted row is written in a single write call. A failed write can leave a
partial final line after the complete rows. Check the status before treating
an output file as the complete result.

The timeout covers requests, retries, and output. SIGINT and SIGTERM cancel
the walk. Pollable stdout uses the same absolute deadline and a pending write
is interrupted on cancellation. On Linux, the command can make an inherited
stdout pipe pollable by reopening it through procfs. For destinations without
write deadlines, cancellation is checked between writes.

## Architecture handoff

The CLI component within Core Engineering owns the command. The standalone
operator root `bog/cmd/bog-export` has direct production imports
`bog/internal/client` and `bog/internal/store` on Linux amd64 and arm64. Its
query traffic uses the production client and the store import supplies the
shared RFC3339 parser. The server has no production path to the command.

In the ordered Architecture handoff before merge, register the root with the
fitness suite's verifier command role, document it as an operator command in
`docs/architecture/SYSTEM.md`, and add its two direct edges to the diagram.
Map the new root to the operator label in the role-parity check. Run
`go test -count=1 ./internal/archtest` on the composed change to verify both
Linux inventories.
