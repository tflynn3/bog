# Fresh-store pagination walkthrough

This example writes two matching rows in one batch, then retrieves them one at a time from a snapshot. It checks both HTTP status codes and exact NDJSON bodies, reads the actual `Bog-Next-Cursor` header without depending on its capitalization, and URL-encodes that value for the next request. The last page must not have a cursor. Use a fresh store: existing matching rows would change the result.

On Linux, from the repository root, install Go 1.27 and `curl`, then build and run the server. The data directory must be private even when the caller's umask is permissive:

```sh
go build -o /tmp/bog ./cmd/bog
umask 0002
data_dir=$(mktemp -d)
chmod 700 "$data_dir"
/tmp/bog -data-dir "$data_dir" -addr 127.0.0.1:18080 &
bog_pid=$!
```

Wait for readiness, then run the walkthrough (it needs `curl`, `awk`, and `cmp`):

```sh
curl --connect-timeout 3 --max-time 10 -f http://127.0.0.1:18080/healthz
BOG_URL=http://127.0.0.1:18080 docs/examples/pagination/walk.sh
```

A successful run under `umask 0002` prints:

```text
Pagination walkthrough passed: both rows returned, then no cursor.
```

Stop the server and remove the fresh data after the run:

```sh
kill "$bog_pid"
wait "$bog_pid" || :
rm -rf "$data_dir"
```

To see the missing-cursor failure, start the deliberately broken local endpoint (Python 3 required) in one terminal:

```sh
python3 docs/examples/pagination/no-cursor.py
```

In another terminal, run:

```sh
BOG_URL=http://127.0.0.1:18081 docs/examples/pagination/walk.sh
```

It exits nonzero with `pagination walkthrough: first page is missing a non-empty Bog-Next-Cursor header`. Stop the mock with Ctrl-C. The script bounds each HTTP request and removes its temporary files on exit. The mock only supplies the responses needed to demonstrate this failure; it is not a Bog server.

A root README link to this walkthrough is follow-up work after #19 lands.
