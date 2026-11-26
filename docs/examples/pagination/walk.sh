#!/bin/sh
# Run against a fresh Bog store. Override BOG_URL if the server uses another address.
set -eu

base=${BOG_URL:-http://127.0.0.1:8080}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' 0
trap 'exit 1' HUP INT TERM

fail() { printf 'pagination walkthrough: %s\n' "$*" >&2; exit 1; }

cat > "$tmp/batch" <<'ROWS'
{"ts":"2025-01-01T00:00:01Z","line":"pagination example first"}
{"ts":"2025-01-01T00:00:02Z","line":"pagination example second"}
ROWS
printf '%s\n' '{"ts":"2025-01-01T00:00:01Z","line":"pagination example first"}' > "$tmp/first"
printf '%s\n' '{"ts":"2025-01-01T00:00:02Z","line":"pagination example second"}' > "$tmp/second"

status=$(curl -sS --connect-timeout 3 --max-time 10 -o "$tmp/body" -w '%{http_code}' \
  -X POST --data-binary @"$tmp/batch" "$base/ingest") || fail 'ingest request failed'
[ "$status" = 204 ] || fail "ingest returned HTTP $status, expected 204"

query() {
  curl -sS --connect-timeout 3 --max-time 10 -G -D "$tmp/headers" -o "$tmp/body" -w '%{http_code}' \
    --data-urlencode 'start=2025-01-01T00:00:00Z' \
    --data-urlencode 'end=2025-01-02T00:00:00Z' \
    --data-urlencode 'contains=pagination example' \
    --data-urlencode 'limit=1' "$@" "$base/query"
}

status=$(query --data-urlencode 'page=1') || fail 'first page request failed'
[ "$status" = 200 ] || fail "first page returned HTTP $status, expected 200"
cmp -s "$tmp/first" "$tmp/body" || fail 'first page body did not match the expected row'
# Header names are case-insensitive; strip the HTTP line ending and surrounding whitespace.
cursor=$(awk '
  { sub(/\r$/, ""); colon = index($0, ":");
    if (colon && tolower(substr($0, 1, colon - 1)) == "bog-next-cursor") {
      value = substr($0, colon + 1); sub(/^[ \t]*/, "", value); sub(/[ \t]*$/, "", value); print value
    }
  }' "$tmp/headers")
[ -n "$cursor" ] || fail 'first page is missing a non-empty Bog-Next-Cursor header'

status=$(query --data-urlencode "cursor=$cursor") || fail 'second page request failed'
[ "$status" = 200 ] || fail "second page returned HTTP $status, expected 200"
cmp -s "$tmp/second" "$tmp/body" || fail 'second page body did not match the expected row'
if awk '{ colon = index($0, ":"); if (colon && tolower(substr($0, 1, colon - 1)) == "bog-next-cursor") found = 1 }
  END { exit !found }' "$tmp/headers"; then
  fail 'second page unexpectedly included a Bog-Next-Cursor header'
fi
printf '%s\n' 'Pagination walkthrough passed: both rows returned, then no cursor.'
