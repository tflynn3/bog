#!/bin/sh
# Run from any directory on a Linux host with Go, curl, and lsof installed.
set -eu
umask 0002

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
export_bin=${BOG_EXPORT_BIN:-}
case $export_bin in
    '') ;;
    /*)
        if [ ! -f "$export_bin" ] || [ ! -x "$export_bin" ]; then
            echo "BOG_EXPORT_BIN must name an executable file: $export_bin" >&2
            exit 1
        fi
        ;;
    *) echo "BOG_EXPORT_BIN must be an absolute path" >&2; exit 1 ;;
esac
port=${BOG_PORT:-18080}
case $port in
    ''|*[!0-9]*) echo "BOG_PORT must be a TCP port number" >&2; exit 1 ;;
esac
if [ "$port" -lt 1 ] || [ "$port" -gt 65535 ]; then
    echo "BOG_PORT must be between 1 and 65535" >&2; exit 1
fi
command -v lsof >/dev/null 2>&1 || { echo "lsof is required to verify the listener PID" >&2; exit 1; }
work=$(mktemp -d "${TMPDIR:-/tmp}/bog-pagination.XXXXXXXX")
pid=
cleanup() {
    if [ -n "$pid" ]; then
        kill "$pid" 2>/dev/null || :
        wait "$pid" 2>/dev/null || :
    fi
    rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
mkdir -m 700 "$work/data"
(cd "$root" && go build -o "$work/bog" ./cmd/bog)
"$work/bog" -data-dir "$work/data" -addr "127.0.0.1:$port" >"$work/server.log" 2>&1 &
pid=$!
base=http://127.0.0.1:$port

# Do not trust a health response until every listener reported for this port
# belongs to our child. This also catches concurrent walkthroughs on one port.
ready=0
attempt=0
while [ "$attempt" -lt 100 ]; do
    owners=$(lsof -nP -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null || :)
    for owner in $owners; do
        if [ "$owner" != "$pid" ]; then
            echo "Port $port is owned by PID $owner, not launched Bog PID $pid" >&2
            exit 1
        fi
    done
    if [ -n "$owners" ] && kill -0 "$pid" 2>/dev/null &&
        curl -q --noproxy '*' --max-time 1 -fsS "$base/healthz" >/dev/null; then
        ready=1
        break
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
        echo "Bog PID $pid exited before becoming ready:" >&2
        cat "$work/server.log" >&2
        exit 1
    fi
    attempt=$((attempt + 1))
    sleep 0.1
done
if [ "$ready" -ne 1 ]; then
    echo "Timed out waiting for Bog PID $pid to listen on port $port" >&2
    cat "$work/server.log" >&2
    exit 1
fi

cat >"$work/rows" <<'ROWS'
{"ts":"2025-01-01T00:00:00Z","line":"pagination first"}
{"ts":"2025-01-01T00:00:01Z","line":"pagination second"}
ROWS
curl -q --noproxy '*' --max-time 10 -fsS -X POST --data-binary @"$work/rows" "$base/ingest" >/dev/null
start=2025-01-01T00:00:00Z
end=2025-01-02T00:00:00Z
contains=pagination
curl -q --noproxy '*' --max-time 10 -fsS -G -D "$work/headers1" -o "$work/page1" \
    --data-urlencode "start=$start" --data-urlencode "end=$end" \
    --data-urlencode "contains=$contains" --data-urlencode 'limit=1' \
    --data-urlencode 'page=1' "$base/query"
cursor=$(awk 'BEGIN { IGNORECASE=1 } { sub(/\r$/, ""); if (tolower($0) ~ /^bog-next-cursor:/) { sub(/^[^:]*:[[:space:]]*/, ""); print } }' "$work/headers1")
if [ -z "$cursor" ]; then
    echo "First page did not return the expected Bog-Next-Cursor" >&2
    exit 1
fi
curl -q --noproxy '*' --max-time 10 -fsS -G -D "$work/headers2" -o "$work/page2" \
    --data-urlencode "start=$start" --data-urlencode "end=$end" \
    --data-urlencode "contains=$contains" --data-urlencode 'limit=1' \
    --data-urlencode "cursor=$cursor" "$base/query"
if awk 'tolower($0) ~ /^bog-next-cursor:/ { found=1 } END { exit !found }' "$work/headers2"; then
    echo "Final page unexpectedly returned Bog-Next-Cursor" >&2
    exit 1
fi
cat "$work/page1" "$work/page2" >"$work/actual"
if ! cmp -s "$work/rows" "$work/actual"; then
    echo "Pagination returned unexpected rows:" >&2
    cat "$work/actual" >&2
    exit 1
fi
printf '%s\n' 'First page:'
cat "$work/page1"
printf '%s\n' 'Second (final) page:'
cat "$work/page2"
printf '%s\n' 'Pagination completed without a further cursor.'

if [ -n "$export_bin" ]; then
    # Append out of timestamp order and repeat a tied row after a different tie.
    cat >"$work/export-ingest" <<'ROWS'
{"ts":"2025-01-01T00:00:03Z","line":"export match repeated"}
{"ts":"2025-01-01T00:00:02Z","line":"export match backfill"}
{"ts":"2025-01-01T00:00:03Z","line":"export match middle"}
{"ts":"2025-01-01T00:00:03Z","line":"export match repeated"}
ROWS
    curl -q --noproxy '*' --max-time 10 -fsS -X POST \
        --data-binary @"$work/export-ingest" "$base/ingest" >/dev/null
    cat >"$work/export-expected" <<'ROWS'
{"ts":"2025-01-01T00:00:02Z","line":"export match backfill"}
{"ts":"2025-01-01T00:00:03Z","line":"export match repeated"}
{"ts":"2025-01-01T00:00:03Z","line":"export match middle"}
{"ts":"2025-01-01T00:00:03Z","line":"export match repeated"}
ROWS
    head -n 1 "$work/export-expected" >"$work/export-first-row"

    run_export() {
        "$export_bin" -url "$base" -start "$start" -end "$end" \
            -contains 'export match' -limit 1 -max-pages "$1" -timeout 10s
    }

    if ! run_export 4 >"$work/export-actual" 2>"$work/export-stderr"; then
        echo "Export failed before completing the four-page walk" >&2
        cat "$work/export-stderr" >&2
        exit 1
    fi
    if ! cmp -s "$work/export-expected" "$work/export-actual"; then
        echo "Export returned unexpected NDJSON:" >&2
        cat "$work/export-actual" >&2
        exit 1
    fi
    if run_export 1 >"$work/export-actual" 2>"$work/export-stderr"; then
        echo "Export unexpectedly succeeded with a one-page budget" >&2
        exit 1
    fi
    if [ ! -s "$work/export-stderr" ]; then
        echo "Export exhausted its page budget without a diagnostic on stderr" >&2
        exit 1
    fi
    if ! cmp -s "$work/export-first-row" "$work/export-actual"; then
        echo "Export with a one-page budget did not emit exactly the first complete row:" >&2
        cat "$work/export-actual" >&2
        exit 1
    fi
    printf '%s\n' 'Export completed all four pages and preserved the first row on budget exhaustion.'
fi
