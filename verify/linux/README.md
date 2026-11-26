# Linux durability and recovery verifier

Run on Linux with Python 3, Git, findmnt, and the Go version required by the checkout. Supply a separate clean checkout and its full commit ID. The verifier builds a fresh detached worktree at that revision with Go workspace and build-flag overrides disabled, removes the worktree, and checks the binary's embedded revision and `vcs.modified=false`; it never builds the verifier's own working tree. Use a local filesystem and storage stack meeting the durability requirements in the checkout's README.

The full run requires a **dedicated, disposable, capacity-limited filesystem** (at most 2 GiB, with at least 8 MiB free). It writes until the filesystem returns ENOSPC, so nothing else should use that filesystem. For example, on a Linux host with loop devices, ext4 tools, and mount privileges:

```sh
REV=015189e066b96a1c333c0436d9e1cef72677a626
git clone https://github.com/tflynn3/bog.git /tmp/bog-pinned
git -C /tmp/bog-pinned checkout --detach "$REV"
truncate -s 256M /tmp/bog-gate.img
mkfs.ext4 -F /tmp/bog-gate.img
sudo mkdir -p /mnt/bog-gate
sudo mount -o loop /tmp/bog-gate.img /mnt/bog-gate
sudo chown "$(id -u):$(id -g)" /mnt/bog-gate
python3 verify/linux/verify.py /tmp/bog-pinned --revision "$REV" --filesystem /mnt/bog-gate --rounds 5 --batches 100 --batch-size 100
sudo umount /mnt/bog-gate
rm /tmp/bog-gate.img
```

Unmount and remove the image even if the verifier fails. The verifier creates and removes its own disposable directories, filler file, and child processes. It requires a real ENOSPC, checks the server reports a WAL write ENOSPC and the rejected ingest returns the generic 503, frees space, appends successfully, and reconciles after reopening. It also checks repeated SIGKILL recovery with complete paginated reconciliation of acknowledged, rejected and unread-response batches, torn suffix truncation, committed corruption startup failure, ordered replay with ties, duplicates and late backfill, restart cursor rejection, bind failure, and graceful shutdown with an admitted request. A skipped or failed case fails the run.

The crash oracle uses a reproducible seed (default 314159; override with `--seed`). The JSON report records the seed, revision, host, filesystem, fixture settings and two independent runs: combined ingest/query workload throughput (rows per second), ingest request p99, query request p95, restart-to-health time, and peak process RSS. The workload performs one ten-row query after each sequential ingest batch; percentiles use the nearest-rank method. The restart metric includes process launch and health polling. Measurements depend on the host and fixture and are not power-loss qualification. The capacity test alone runs on the supplied filesystem; other disposable stores and performance measurements use the system temporary directory, so place `TMPDIR` on the intended test filesystem when appropriate (separate from the dedicated ENOSPC mount).
