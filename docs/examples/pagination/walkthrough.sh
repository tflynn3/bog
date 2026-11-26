#!/bin/sh
# Run from any directory; the temporary store and server are removed on exit.
set -eu
umask 0002

repo=$(CDPATH= cd "$(dirname "$0")/../../.." && pwd)
work=$(mktemp -d "${TMPDIR:-/tmp}/bog-pagination.XXXXXXXX")
pid=
cleanup() {
    if [ -n "$pid" ]; then
        kill "$pid" 2>/dev/null || :
        wait "$pid" 2>/dev/null || :
    fi
    rm -rf "$work"
}
trap cleanup 0
trap 'exit 1' HUP INT TERM

# mkdir -m matters under umask 0002: Bog rejects group-writable stores.
mkdir -m 700 "$work/data"
(cd "$repo" && go build -o "$work/bog" ./cmd/bog)
addr=${BOG_ADDR:-127.0.0.1:18080}
case $addr in
    127.0.0.1:*) port=${addr#127.0.0.1:} ;;
    *) echo 'BOG_ADDR must be 127.0.0.1 followed by a port' >&2; exit 1 ;;
esac
case $port in
    ''|*[!0-9]*) echo 'BOG_ADDR must include a numeric port' >&2; exit 1 ;;
esac
if [ "$port" -eq 0 ] || [ "$port" -gt 65535 ]; then
    echo 'BOG_ADDR port must be between 1 and 65535' >&2
    exit 1
fi
if ! command -v lsof > /dev/null 2>&1; then
    echo 'lsof is required to verify the Bog listener belongs to this run' >&2
    exit 1
fi
base="http://$addr"
if curl -fsS "$base/healthz" > /dev/null 2>&1; then
    echo "A server is already listening at $addr; choose a free BOG_ADDR" >&2
    exit 1
fi
"$work/bog" -data-dir "$work/data" -addr "$addr" >"$work/server.log" 2>&1 &
pid=$!

# A health response alone could come from a competing server. Require the
# launched process to own the exact listener before making any requests.
i=0
while ! lsof -nP -a -p "$pid" -iTCP@127.0.0.1:"$port" -sTCP:LISTEN -t > /dev/null 2>&1; do
    if ! kill -0 "$pid" 2>/dev/null || [ "$i" -ge 30 ]; then
        echo "Bog could not own the listener at $addr; it may already be in use. Server log follows:" >&2
        cat "$work/server.log" >&2
        exit 1
    fi
    i=$((i + 1))
    sleep 1
done
if ! curl -fsS "$base/healthz" > /dev/null; then
    echo "Bog at $addr failed its health check; server log follows:" >&2
    cat "$work/server.log" >&2
    exit 1
fi

printf '%s\n' \
    '{"ts":"2025-01-01T00:00:00Z","line":"match first"}' \
    '{"ts":"2025-01-01T00:00:01Z","line":"match second"}' > "$work/input"
curl -fsS -X POST --data-binary "@$work/input" "$base/ingest" > /dev/null

# --data-urlencode preserves the exact same filters on both requests.
query() {
    curl -fsS -G -D "$1" -o "$2" \
        --data-urlencode 'start=2025-01-01T00:00:00Z' \
        --data-urlencode 'end=2025-01-02T00:00:00Z' \
        --data-urlencode 'contains=match' \
        --data-urlencode 'limit=1' \
        --data-urlencode "$3" "$base/query"
}
cursor_from() {
    awk '{ sub(/\r$/, ""); if (tolower($0) ~ /^bog-next-cursor:/) { sub(/^[^:]*:[[:space:]]*/, ""); print; exit } }' "$1"
}
query "$work/headers1" "$work/page1" 'page=1'
cursor=$(cursor_from "$work/headers1")
if [ -z "$cursor" ]; then
    echo 'Expected Bog-Next-Cursor on the first page, but it was absent or empty' >&2
    exit 1
fi
query "$work/headers2" "$work/page2" "cursor=$cursor"
if awk '{ sub(/\r$/, ""); if (tolower($0) ~ /^bog-next-cursor:/) found=1 } END { exit !found }' "$work/headers2"; then
    echo 'Expected the final page to have no Bog-Next-Cursor header' >&2
    exit 1
fi
printf '%s\n' '{"ts":"2025-01-01T00:00:00Z","line":"match first"}' > "$work/expected1"
printf '%s\n' '{"ts":"2025-01-01T00:00:01Z","line":"match second"}' > "$work/expected2"
if ! cmp -s "$work/expected1" "$work/page1" || ! cmp -s "$work/expected2" "$work/page2"; then
    echo 'Unexpected pagination results' >&2
    cat "$work/page1" "$work/page2" >&2
    exit 1
fi
printf '%s\n' 'First page:'
cat "$work/page1"
printf '%s\n' 'Final page (no continuation cursor):'
cat "$work/page2"
