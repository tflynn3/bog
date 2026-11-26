#!/usr/bin/env bash
set -uo pipefail

# Output is outside the source tree during the clean build.
out="$RUNNER_TEMP/bog-release-evidence"
mkdir -p "$out" release-evidence
trap 'cp -R "$out"/. release-evidence/ 2>/dev/null || true' EXIT
exec > >(tee "$out/gate.log") 2>&1

fail() { echo "release gate: $*" >&2; exit 1; }
[[ "${REVISION:-}" =~ ^[0-9a-f]{40}$ ]] || fail 'invalid revision'
[[ "${BASELINE:-}" == '' || "$BASELINE" =~ ^[0-9a-f]{40}$ ]] || fail 'invalid baseline'
[[ "$(uname -s)" == Linux ]] || fail 'Linux is required'
[[ "$(git rev-parse HEAD)" == "$REVISION" ]] || fail 'checkout revision mismatch'
[[ -z "$(git status --porcelain --untracked-files=all -- . ':!release-evidence')" ]] || fail 'source tree is not clean'
[[ "$(go env GOVERSION)" == go1.27.0 ]] || fail 'unexpected Go toolchain'

binary="$RUNNER_TEMP/bog-candidate"
go build -buildvcs=true -o "$binary" ./cmd/bog || fail 'build failed'
go version -m "$binary" | tee "$out/build-info.txt"
grep -Fx $'\tbuild\tvcs=git' "$out/build-info.txt" >/dev/null || fail 'missing VCS metadata'
grep -Fx $'\tbuild\tvcs.revision='"$REVISION" "$out/build-info.txt" >/dev/null || fail 'binary revision mismatch'
grep -Fx $'\tbuild\tvcs.modified=false' "$out/build-info.txt" >/dev/null || fail 'binary was built from a modified tree'
sha256sum "$binary" | awk '{print $1}' > "$out/binary.sha256"
go version > "$out/toolchain.txt"
printf '%s\n' "$REVISION" > "$out/revision.txt"
printf '%s\n' "${BASELINE:-none}" > "$out/baseline.txt"

# These are explicit, repository-owned entry points; absence or failure is fatal.
# Each producer receives the same already-verified binary, and writes a report.
status=0
for spec in 'durability:bench/durability/verify-release' 'capacity:bench/retained-history/verify-release' 'admission:bench/query-admission/verify-release'; do
  name=${spec%%:*}
  entry=${spec#*:}
  if [[ ! -x "$entry" ]]; then
    echo "missing executable producer: $entry" | tee "$out/$name.log"
    status=1
    continue
  fi
  mkdir -p "$out/$name-artifacts"
  "$entry" --binary "$binary" --revision "$REVISION" --binary-sha256 "$(cat "$out/binary.sha256")" --output "$out/$name.json" --artifacts "$out/$name-artifacts" > "$out/$name.log" 2>&1 || status=1
done
python3 .github/workflows/validate_release_gate.py \
  --revision "$REVISION" --binary-sha256 "$(cat "$out/binary.sha256")" \
  --reports "$out" || status=1
exit "$status"
