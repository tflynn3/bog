# bog-export

Run the exporter as a separate operator process against a running Bog server.

```sh
bog-export -url http://127.0.0.1:8080 \
  -start 2025-01-01T00:00:00Z -end 2025-01-02T00:00:00Z \
  -contains 'job completed' -limit 100 -max-pages 1000 -timeout 2m \
  > rows.ndjson
```

The range includes `-start` and excludes `-end`. `-contains` matches a
case-sensitive literal substring. NDJSON rows have `ts` and `line` fields
and retain server order, including ties and duplicates. The page budget
includes the terminal page. `-max-page-bytes` defaults to 32 MiB. HTTP 503
and interrupted delivery of otherwise valid pages get up to three retries
at 100 ms intervals by default. Each retry uses the same request cursor
and each page is validated in full before any of its rows are written.

Exit status 0 indicates a complete export or successfully written help.
Status 1 indicates an incomplete export or output failure. Invalid options
return status 2 even if stderr cannot drain. Help and diagnostics go to
stderr. A failed write can leave a partial final line after the complete
rows. Check the exit status before using the export as a complete result.

For a valid invocation, stdout and stderr workers start before the first
query. They use the original inherited descriptors and preserve their file
status flags. The export can continue if its executable is renamed after
startup, including while a query is in flight. The overall timeout covers
startup, requests, retries, and output. SIGINT and SIGTERM cancel that work.
Worker writes are interrupted and workers are reaped on exit. Usage and help
have a short reporting window. If the export is already canceled or timed
out, the command allows one brief final diagnostic attempt.

## Checkpointed export

Start a checkpointed export against a direct instance endpoint, or a route
that keeps continuations on the issuing process. Use a regular output file
opened for append and a separate checkpoint in a trusted local directory.

```sh
# Prepare an empty rows.ndjson at a durable location before the first run.
bog-export -url http://127.0.0.1:8080 \
  -start 2025-01-01T00:00:00Z -end 2025-01-02T00:00:00Z \
  -checkpoint export.progress >> rows.ndjson

# After interruption, reopen the same output and checkpoint.
bog-export -url http://127.0.0.1:8080 \
  -start 2025-01-01T00:00:00Z -end 2025-01-02T00:00:00Z \
  -checkpoint export.progress -resume >> rows.ndjson
```

Use the same URL, time bounds, literal filter, and row limit on resume.
`-max-pages` counts pages acknowledged in earlier invocations, and you can
increase it on resume. A completed checkpoint resumes successfully without
issuing a query or writing rows. If the first invocation failed before
creating a checkpoint, rerun the start command. Starting with an existing
checkpoint requires `-resume`.

After writing all rows of a page, the exporter syncs stdout before the
client saves the page acknowledgment. The output file's directory entry
must already be durable. Use a local filesystem that supports file and
directory sync and atomic checkpoint replacement. Keep the same output
file, reserve it for this export, and direct stderr elsewhere. Keep the
checkpoint directory stable while the command runs. The checkpoint can
contain sensitive opening-page rows. Names starting with
`.export-progress-stage-` and `.bog-export-lock-` are reserved in that
directory. Keep the lock file in place so concurrent commands contend on the
same lock.

Resume starts at the last durably recorded acknowledgment. The opening page
is journaled before delivery so it can replay without creating a new
snapshot. If output is written ahead of a saved acknowledgment, a restart
can append the unacknowledged page again. It can also follow an incomplete
record from the previous run. Reconcile that suffix before resuming if you
need a single copy of each page and complete NDJSON framing. The checkpoint
tracks pages and cursors without storing output offsets.

A saved cursor belongs to the original Bog process and snapshot. If the
process restarts, the snapshot expires or is retired, or routing sends the
cursor to another instance, resume can fail. If the original snapshot is
gone, use a new checkpoint and output file to start a fresh export.

## Architecture

The CLI component within Core Engineering owns this operator command.
Its only direct root-module production import is `bog/internal/client`.
The client owns HTTP query framing, cursors, export sessions, and
checkpoint persistence. It uses `bog/internal/store` for shared row
types. The server has no production path to this command.
