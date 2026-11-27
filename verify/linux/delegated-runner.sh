#!/usr/bin/env bash
# Use the same systemd delegation and verifier limits for the prebuild
# probe and the live capacity invocation.
set -euo pipefail

runner_uid=$(id -u)
runner_gid=$(id -g)
mode=${2:?expected verifier subcommand}
case "$mode" in
  preflight) duration=20s ;;
  capacity) duration=24min ;;
  *) printf 'Unsupported delegated runner subcommand %s\n' "$mode" >&2; exit 1 ;;
esac
if sudo -n systemd-run --quiet --wait --pipe --collect --expand-environment=no \
  --uid="$runner_uid" --gid="$runner_gid" -p 'Delegate=memory io' -p DelegateSubgroup=runner \
  -p LimitAS=infinity -p KillMode=control-group -p "RuntimeMaxSec=$duration" -p TimeoutStopSec=30s \
  "$@"; then
  exit 0
else
  status=$?
  printf 'Delegated %s service exited with status %d\n' "$mode" "$status" >&2
  exit "$status"
fi
