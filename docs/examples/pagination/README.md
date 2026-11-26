# Pagination walkthrough

On Linux, install Go (the version in `go.mod`), curl, and lsof, then run:

```sh
docs/examples/pagination/walkthrough.sh
```

The script builds Bog, creates a fresh private temporary data directory under
`umask 0002`, and starts a local server. It checks the listening process before
sending requests, ingests two matching rows, and follows the cursor from a
one-row page with the same filters. It checks both rows and confirms the final
page has no cursor. The server and temporary files are removed on exit.

The default port is 18080; set `BOG_PORT` to choose another port if it is in
use. The script fails if another process owns the selected port. All requests
bypass curl configuration files and proxies.
