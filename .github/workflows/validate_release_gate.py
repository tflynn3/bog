#!/usr/bin/env python3
"""Validate producer reports for one exact, independently verified candidate."""
import argparse
import json
import pathlib
import re
import sys

SHA = re.compile(r"[0-9a-f]{40}\Z")
DIGEST = re.compile(r"[0-9a-f]{64}\Z")
REQUIRED = {
    "durability": {
        "acknowledged_writes_survive_sigkill",
        "torn_suffix_recovery",
        "committed_corruption_blocks_startup",
        "backfill_cursor_walk",
        "stale_cursors_after_restart",
        "real_enospc_recovery",
    },
    "capacity": {"retained_history_capacity"},
    "admission": {"one_slot_admission"},
}


def validate(directory, revision, binary_sha256):
    errors = []
    if not SHA.fullmatch(revision):
        errors.append("invalid candidate revision")
    if not DIGEST.fullmatch(binary_sha256):
        errors.append("invalid candidate binary digest")
    for producer, required_cases in REQUIRED.items():
        path = directory / (producer + ".json")
        try:
            report = json.loads(path.read_text())
            if not isinstance(report, dict):
                raise ValueError("report must be an object")
        except (OSError, ValueError) as exc:
            errors.append(f"{producer}: missing or invalid report: {exc}")
            continue
        if report.get("schema_version") != 1 or report.get("producer") != producer:
            errors.append(f"{producer}: incorrect schema or producer")
        verifier = report.get("verifier")
        if not isinstance(verifier, dict) or not all(
            isinstance(verifier.get(key), str) and verifier[key].strip()
            for key in ("name", "version")
        ):
            errors.append(f"{producer}: missing verifier identity")
        if report.get("candidate_revision") != revision:
            errors.append(f"{producer}: candidate revision mismatch")
        if report.get("candidate_binary_sha256") != binary_sha256:
            errors.append(f"{producer}: candidate binary mismatch")
        fixture = report.get("fixture")
        if not isinstance(fixture, dict) or not isinstance(fixture.get("id"), str) or not fixture["id"].strip() or not isinstance(fixture.get("sha256"), str) or not DIGEST.fullmatch(fixture["sha256"]):
            errors.append(f"{producer}: missing fixture identity")
        cases = report.get("cases")
        if not isinstance(cases, dict):
            errors.append(f"{producer}: missing cases")
            continue
        for case in sorted(required_cases):
            result = cases.get(case)
            if not isinstance(result, dict) or result.get("status") != "pass":
                errors.append(f"{producer}: required case {case} did not pass")
                continue
            if case == "real_enospc_recovery":
                for key in ("disposable_local_filesystem", "observed_enospc", "recovered_after_space_freed"):
                    if result.get(key) is not True:
                        errors.append(f"{producer}: {case} missing {key}")
            if case == "one_slot_admission":
                for key in ("prompt_503", "zero_ndjson_rows", "no_cursor", "health_responsive_during_hold", "ingest_responsive_during_hold", "original_cursor_retry_succeeded_after_disconnect"):
                    if result.get(key) is not True:
                        errors.append(f"{producer}: {case} missing {key}")
    return errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--revision", required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--reports", required=True, type=pathlib.Path)
    args = parser.parse_args()
    errors = validate(args.reports, args.revision, args.binary_sha256)
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        return 1
    print("All required candidate reports passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
