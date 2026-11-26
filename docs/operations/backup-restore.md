# Single-node backup and restore

Stop Bog cleanly and wait for it to exit before copying its data directory. Copy
`bog.wal` and `bog.commit` together as one consistent backup; copying either
file while Bog is writing does not provide a consistent snapshot. Preserve the
backup separately and verify it by restoring to a different directory. Never
run two processes against the same data directory.

For restore, stop Bog and restore both files into an existing, trusted directory
on Linux storage meeting the durability requirements in the main README. Ensure
ownership, permissions and link counts meet those requirements, then start Bog
with that directory. Wait for `/healthz` after WAL replay and verify expected
queries before directing traffic to it. Preserve the old directory until the
restore is verified. A restore returns to the backup's committed point and does
not recover later writes.

`bogctl ingest` reads NDJSON from standard input, sends whole-line batches of at
most 4 MiB, and stops on the first error. Earlier batches may already be durable.
A lost reply leaves the current batch's outcome unknown; do not blindly retry it.
`bogctl query -start ... -end ...` streams a paginated snapshot to standard
output. Point it at a specific Bog process: cursors are process-local and become
invalid after restart, failover, or routing to another process. Query output may
already contain rows if a later page fails.
