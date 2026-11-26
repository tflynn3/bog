# Pagination walkthrough

On Linux, install Go (the version in `go.mod`), curl, and lsof. Run the
walkthrough from the repository root.

```sh
docs/examples/pagination/walkthrough.sh
```

The script builds Bog, creates a fresh private temporary data directory under
`umask 0002`, and starts a local server. It checks the listening process before
sending requests, ingests two matching rows, and follows the cursor from a
one-row page with the same filters. It checks both rows and confirms the final
page has no cursor. The server and temporary files are removed on exit.

The default port is 18080. Set `BOG_PORT` to choose another port if it is in
use. The script fails if another process owns the selected port. All requests
bypass curl configuration files and proxies.

## Positive timezone offsets

Encode the plus sign in a positive RFC3339 timezone offset as `%2B`.
An unescaped plus sign is decoded as a space in the query string.
This request starts a walk over January 1, 2025 UTC.

```sh
curl -i 'http://127.0.0.1:18080/query?start=2025-01-01T05:30:00%2B05:30&end=2025-01-02T05:30:00%2B05:30&page=1&limit=1'
```
