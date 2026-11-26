#!/usr/bin/env bash
set -uo pipefail

gate_dir="$GITHUB_WORKSPACE/gate/.github/workflows"
cd "$GITHUB_WORKSPACE/candidate" || exit 1

# Write directly to the uploaded directory, outside the candidate checkout.
out="$GITHUB_WORKSPACE/release-evidence"
mkdir -p "$out" || exit 1
exec > "$out/gate.log" 2>&1 || exit 1

fail() { echo "release gate: $*" >&2; exit 1; }
[[ "${REVISION:-}" =~ ^[0-9a-f]{40}$ ]] || fail 'invalid revision'
[[ "${BASELINE:-}" == '' || "$BASELINE" =~ ^[0-9a-f]{40}$ ]] || fail 'invalid baseline'
[[ "$(uname -s)" == Linux && "$(uname -m)" == aarch64 ]] || fail 'Linux ARM64 is required'
[[ "$(git rev-parse HEAD)" == "$REVISION" ]] || fail 'checkout revision mismatch'
[[ -z "$(git status --porcelain --untracked-files=all)" ]] || fail 'source tree is not clean'
[[ "$(go env GOVERSION)" == go1.27.0 ]] || fail 'unexpected Go toolchain'

go version > "$out/toolchain.txt" || fail 'cannot record toolchain'
uname -a > "$out/platform.txt" || fail 'cannot record platform'
printf '%s\n' "$REVISION" > "$out/revision.txt" || fail 'cannot record revision'
printf '%s\n' "${BASELINE:-none}" > "$out/baseline.txt" || fail 'cannot record baseline'

binary="$RUNNER_TEMP/bog-candidate"
go build -buildvcs=true -o "$binary" ./cmd/bog || fail 'build failed'
go version -m "$binary" | tee "$out/build-info.txt" || fail 'cannot record build metadata'
grep -Fx $'\tbuild\tvcs=git' "$out/build-info.txt" >/dev/null || fail 'missing VCS metadata'
grep -Fx $'\tbuild\tvcs.revision='"$REVISION" "$out/build-info.txt" >/dev/null || fail 'binary revision mismatch'
grep -Fx $'\tbuild\tvcs.modified=false' "$out/build-info.txt" >/dev/null || fail 'binary was built from a modified tree'
digest=$(sha256sum "$binary") || fail 'cannot hash candidate binary'
readonly digest="${digest%% *}"
printf '%s\n' "$digest" > "$out/binary.sha256" || fail 'cannot record binary digest'
check_binary() {
  local actual
  actual=$(sha256sum "$binary") || return 1
  [[ "${actual%% *}" == "$digest" ]]
}
# These are explicit, repository-owned entry points; absence or failure is fatal.
# Each producer receives the same already-verified binary, and writes a report.
status=0
for spec in 'durability:bench/durability/verify-release' 'capacity:bench/retained-history/verify-release' 'admission:bench/query-admission/verify-release'; do
  name=${spec%%:*}
  entry=${spec#*:}
  if [[ ! -x "$entry" ]]; then
    echo "missing executable producer: $entry" | tee "$out/$name.log" || fail "cannot record $name log"
    status=1
    continue
  fi
  mkdir -p "$out/$name-artifacts" || fail "cannot create $name artifact directory"
  if ! check_binary; then
    echo "candidate binary changed before $name" | tee "$out/$name.log" || fail "cannot record $name log"
    status=1
    continue
  fi
  "$entry" --binary "$binary" --revision "$REVISION" --binary-sha256 "$digest" --output "$out/$name.json" --artifacts "$out/$name-artifacts" > "$out/$name.log" 2>&1 || status=1
  if ! check_binary; then
    echo "candidate binary changed after $name" | tee -a "$out/$name.log" || fail "cannot record $name log"
    status=1
  fi
done
python3 "$gate_dir/validate_release_gate.py" \
  --revision "$REVISION" --binary-sha256 "$digest" \
  --reports "$out" || status=1
exit "$status"
