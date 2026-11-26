#!/usr/bin/env bash
set -uo pipefail

# Output is outside the source tree during the clean build.
gate="$GITHUB_WORKSPACE/gate/.github/workflows"
cd "$GITHUB_WORKSPACE/candidate" || exit 1
out="$GITHUB_WORKSPACE/release-evidence"
mkdir -p "$out"
exec > >(tee "$out/gate.log") 2>&1

# Preserve the requested inputs and actual toolchain even if verification or build fails.
printf '%s\n' "${REVISION:-}" > "$out/revision.txt"
printf '%s\n' "${BASELINE:-none}" > "$out/baseline.txt"
go version > "$out/toolchain.txt" 2>&1

fail() { echo "release gate: $*" >&2; exit 1; }
[[ "${REVISION:-}" =~ ^[0-9a-f]{40}$ ]] || fail 'invalid revision'
[[ "${BASELINE:-}" == '' || "$BASELINE" =~ ^[0-9a-f]{40}$ ]] || fail 'invalid baseline'
[[ "$(uname -s)" == Linux && "$(uname -m)" == aarch64 ]] || fail 'Linux ARM64 is required'
[[ "$(git rev-parse HEAD)" == "$REVISION" ]] || fail 'checkout revision mismatch'
[[ -z "$(git status --porcelain --untracked-files=all)" ]] || fail 'source tree is not clean'
[[ "$(go env GOVERSION)" == go1.27.0 ]] || fail 'unexpected Go toolchain'

binary="$RUNNER_TEMP/bog-candidate"
go build -buildvcs=true -o "$binary" ./cmd/bog || fail 'build failed'
go version -m "$binary" | tee "$out/build-info.txt"
grep -Fx $'\tbuild\tvcs=git' "$out/build-info.txt" >/dev/null || fail 'missing VCS metadata'
grep -Fx $'\tbuild\tvcs.revision='"$REVISION" "$out/build-info.txt" >/dev/null || fail 'binary revision mismatch'
grep -Fx $'\tbuild\tvcs.modified=false' "$out/build-info.txt" >/dev/null || fail 'binary was built from a modified tree'
digest=$(sha256sum "$binary" | awk '{print $1}') || fail 'cannot hash binary'
printf '%s\n' "$digest" > "$out/binary.sha256"
goroot=$(go env GOROOT) || fail 'cannot locate Go toolchain'
command -v docker >/dev/null || fail 'Docker is required for producer isolation'

# Only the source, toolchain, and verified binary are mounted read-only. Each
# producer gets its own writable output and bounded disposable scratch filesystem.
# The gate checkout, host filesystem, and other producers' evidence are absent.
status=0
container="release-gate-${GITHUB_RUN_ID:-local}-$$"
cleanup() { docker rm -f "$container" >/dev/null 2>&1 || true; }
trap cleanup EXIT
for spec in 'durability:bench/durability/verify-release' 'capacity:bench/retained-history/verify-release' 'admission:bench/query-admission/verify-release'; do
  name=${spec%%:*}
  entry=${spec#*:}
  if [[ ! -x "$entry" ]]; then
    echo "missing executable producer: $entry" | tee "$out/$name.log"
    status=1
    continue
  fi
  mkdir -p "$out/$name/artifacts"
  # A deadline per producer leaves time to collect evidence and upload it.
  timeout --kill-after=30s 25m docker run --rm --name "$container" \
    --network none --read-only --cap-drop ALL --security-opt no-new-privileges \
    --user "$(id -u):$(id -g)" --workdir /source \
    --tmpfs /tmp:rw,nosuid,nodev,size=2g,mode=1777 \
    --mount "type=bind,src=$PWD,dst=/source,readonly" \
    --mount "type=bind,src=$binary,dst=/candidate-binary,readonly" \
    --mount "type=bind,src=$goroot,dst=/opt/go,readonly" \
    --mount "type=bind,src=$out/$name,dst=/output" \
    --env PATH=/opt/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    --env GOTOOLCHAIN=local --env HOME=/tmp --env TMPDIR=/tmp \
    ubuntu:24.04 "/source/$entry" --binary /candidate-binary --revision "$REVISION" \
    --binary-sha256 "$digest" --output /output/report.json --artifacts /output/artifacts \
    > "$out/$name.log" 2>&1 || status=1
  cleanup
  if [[ -f "$out/$name/report.json" && ! -L "$out/$name/report.json" ]]; then
    cp "$out/$name/report.json" "$out/$name.json" || status=1
  else
    echo "missing regular report for $name"; status=1
  fi
  [[ "$(sha256sum "$binary" | awk '{print $1}')" == "$digest" ]] || { echo "candidate binary changed after $name"; status=1; }
done
python3 "$gate/validate_release_gate.py" \
  --revision "$REVISION" --binary-sha256 "$digest" \
  --reports "$out" || status=1
exit "$status"
