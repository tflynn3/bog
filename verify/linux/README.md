# Linux crash and lifecycle verifier

On Linux, install Python 3, Git, and the Go version required by the pinned checkout. Supply a separate clean checkout at `f727b8f676845a8058d1ecf114442d8963cbbbae`, with no untracked files:

```sh
git clone https://github.com/tflynn3/bog.git /tmp/bog-pinned
git -C /tmp/bog-pinned checkout --detach f727b8f676845a8058d1ecf114442d8963cbbbae
python3 verify/linux/verify.py /tmp/bog-pinned --rounds 5
```

The verifier builds the binary, checks its embedded revision and clean build metadata, and creates disposable stores. It repeatedly kills and restarts the binary, checks acknowledged, rejected and unread-response batches, tests snapshot pagination and stale cursors, and exercises bind failure and SIGTERM with an admitted request. It cleans up child processes and stores on failure. A passing run prints `PASS`; failures exit nonzero. Run on a local filesystem with the durability properties described by bog. Power loss, torn suffixes, committed corruption, disk exhaustion, and performance are outside this verifier's scope.
