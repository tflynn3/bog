# Pagination walkthrough

From the repository root, run `docs/examples/pagination/walkthrough.sh`. It
requires Go 1.27, curl, and standard POSIX utilities. Set `BOG_ADDR` to a free
loopback address (for example, `127.0.0.1:18081`) if the default port 18080 is
in use. The script builds Bog, starts it with a fresh private temporary data
directory under umask 0002, and cleans up the server and files on exit.

The script ingests two matching rows, requests a one-row snapshot page, and
extracts its `Bog-Next-Cursor` response header. It sends that actual cursor on
the next request with the same time bounds and `contains` filter, checks both
rows, and verifies that the final page has no continuation header. A missing
first-page cursor or an unexpected response fails the walkthrough. Cursors are
valid only for the running server; start a new walk after a restart.
