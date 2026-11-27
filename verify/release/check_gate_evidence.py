"""Check producer provenance and required Go test results for the release gate."""

import argparse
import json
from collections import Counter


def check_producers(manifest, revision, arch, gate_sha, definitions):
    checkout_shas = {"gate": gate_sha, "candidate": revision}
    expected = {}
    for definition in definitions.splitlines():
        name, checkout = definition.split()
        expected[name] = (
            f"{name} revision={revision} arch={arch} exit=0 "
            f"checkout={checkout} checkout_sha={checkout_shas[checkout]}"
        )

    seen = set()
    try:
        with open(manifest, encoding="utf-8") as records:
            for number, raw in enumerate(records, 1):
                record = raw.rstrip("\n")
                name = record.partition(" ")[0]
                location = f"{manifest}:{number}"
                if name not in expected:
                    raise SystemExit(f"{location}: unexpected producer {name!r}")
                if name in seen:
                    raise SystemExit(f"{location}: duplicate producer {name!r}")
                if record != expected[name]:
                    raise SystemExit(f"{location}: expected {expected[name]!r}, got {record!r}")
                seen.add(name)
    except (OSError, UnicodeError) as error:
        raise SystemExit(f"{manifest}: cannot read producer manifest: {error}") from None

    missing = expected.keys() - seen
    if missing:
        raise SystemExit(f"{manifest}: missing producers: {', '.join(sorted(missing))}")
    print(f"Validated {len(seen)} producer records for {revision} on {arch}")


def check_test_events(path, package, test, children):
    required = ["", test, *(f"{test}/{child}" for child in children)]
    passes = Counter()
    with open(path, encoding="utf-8") as events:
        for line in events:
            event = json.loads(line)
            if event.get("Package") != package:
                continue
            name = event.get("Test", "")
            action = event.get("Action")
            if name == "" or name == test or name.startswith(test + "/"):
                if action in ("fail", "skip"):
                    raise SystemExit(f"{name or package} reported {action}")
                if action == "pass":
                    passes[name] += 1
    for name in required:
        if passes[name] != 1:
            raise SystemExit(f"{name or package} reported {passes[name]} passes, expected 1")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    checks = parser.add_subparsers(dest="check", required=True)
    producers = checks.add_parser("producers", help="validate producer manifest records")
    producers.add_argument("manifest")
    producers.add_argument("revision")
    producers.add_argument("arch")
    producers.add_argument("gate_sha")
    producers.add_argument("definitions", help="newline-separated producer and checkout pairs")
    events = checks.add_parser("test-events", help="validate required Go test results")
    events.add_argument("jsonl")
    events.add_argument("package")
    events.add_argument("test")
    events.add_argument("children", nargs="*", help="subtest names relative to the parent test")
    args = parser.parse_args()
    if args.check == "producers":
        check_producers(args.manifest, args.revision, args.arch, args.gate_sha, args.definitions)
    else:
        check_test_events(args.jsonl, args.package, args.test, args.children)


if __name__ == "__main__":
    main()
