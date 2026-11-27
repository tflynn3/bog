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

## Optional bounded export check

On the composed #98 tree, set `BOG_EXPORT_BIN` to the absolute path of an
already built `bog-export` executable for the Linux host:

```sh
BOG_EXPORT_BIN=/absolute/path/to/bog-export docs/examples/pagination/walkthrough.sh
```

The check uses the same loopback server and temporary directory. It appends a
fixture with out-of-order timestamps and three tied rows, including two
identical rows separated in append order by a distinct row. The exporter
receives `-url`, `-start`, `-end`, `-contains`, `-limit 1`, `-max-pages`, and
`-timeout 10s`. With `-max-pages 4`, it must exit successfully and produce the
exact four NDJSON rows in query order through the terminal page. With
`-max-pages 1`, it must exit nonzero, write a diagnostic to stderr, and leave
exactly the first complete NDJSON row on stdout. Treat a nonzero exit as an
incomplete export even when stdout contains valid rows.
