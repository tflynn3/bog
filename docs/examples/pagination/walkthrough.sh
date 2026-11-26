#!/bin/sh
# Run from any directory on a Linux host with Go, curl, and lsof installed.
set -eu
umask 0002

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
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
        curl -q --noproxy '*' -fsS "$base/healthz" >/dev/null; then
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
curl -q --noproxy '*' -fsS -X POST --data-binary @"$work/rows" "$base/ingest" >/dev/null
start=2025-01-01T00:00:00Z
end=2025-01-02T00:00:00Z
contains=pagination
curl -q --noproxy '*' -fsS -G -D "$work/headers1" -o "$work/page1" \
    --data-urlencode "start=$start" --data-urlencode "end=$end" \
    --data-urlencode "contains=$contains" --data-urlencode 'limit=1' \
    --data-urlencode 'page=1' "$base/query"
cursor=$(awk 'BEGIN { IGNORECASE=1 } { sub(/\r$/, ""); if (tolower($0) ~ /^bog-next-cursor:/) { sub(/^[^:]*:[[:space:]]*/, ""); print } }' "$work/headers1")
if [ -z "$cursor" ]; then
    echo "First page did not return the expected Bog-Next-Cursor" >&2
    exit 1
fi
curl -q --noproxy '*' -fsS -G -D "$work/headers2" -o "$work/page2" \
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
