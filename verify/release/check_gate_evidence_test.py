"""Exercise the same checker entry point as the native release workflow."""

import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest


CHECKER = Path(__file__).resolve().with_name("check_gate_evidence.py")
REVISION = "a" * 40
GATE_SHA = "b" * 40
PACKAGE = "bog/verify/release"
TEST = "TestReleaseFixedBudgetBackfill"


class GateEvidenceTest(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.fixture = Path(directory.name) / "evidence"

    def run_checker(self, *args):
        return subprocess.run(
            [sys.executable, str(CHECKER), *args],
            cwd=self.fixture.parent,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def assert_verdict(self, result, *, output="", error=""):
        self.assertEqual((result.returncode, result.stdout, result.stderr),
                         (int(bool(error)), output, error))

    def test_producer_manifest(self):
        admission = f"admission revision={REVISION} arch=arm64 exit=0 checkout=gate checkout_sha={GATE_SHA}"
        capacity = f"capacity revision={REVISION} arch=arm64 exit=0 checkout=candidate checkout_sha={REVISION}"
        for label, records, error in (
            ("valid", [capacity, admission], ""),
            ("missing", [capacity], f"{self.fixture}: missing producers: admission\n"),
            ("duplicate", [admission, admission, capacity], f"{self.fixture}:2: duplicate producer 'admission'\n"),
            ("unexpected", ["other", admission, capacity], f"{self.fixture}:1: unexpected producer 'other'\n"),
        ):
            with self.subTest(label=label):
                self.assert_verdict(
                    self.check_manifest(records),
                    output="" if error else f"Validated 2 producer records for {REVISION} on arm64\n",
                    error=error,
                )

        for label, expected, wrong in (
            ("gate SHA", admission, admission.replace(f"checkout_sha={GATE_SHA}", f"checkout_sha={REVISION}")),
            ("candidate SHA", capacity, capacity.replace(f"checkout_sha={REVISION}", f"checkout_sha={GATE_SHA}")),
            ("checkout", capacity, capacity.replace("checkout=candidate", "checkout=gate")),
            ("revision", capacity, capacity.replace(f"revision={REVISION}", f"revision={GATE_SHA}")),
            ("architecture", capacity, capacity.replace("arch=arm64", "arch=amd64")),
            ("producer failed", capacity, capacity.replace("exit=0", "exit=1")),
        ):
            with self.subTest(label=label):
                self.assert_verdict(
                    self.check_manifest([wrong]),
                    error=f"{self.fixture}:1: expected {expected!r}, got {wrong!r}\n",
                )

    def check_manifest(self, records):
        self.fixture.write_text("\n".join(records) + "\n", encoding="utf-8")
        return self.run_checker("producers", str(self.fixture), REVISION, "arm64", GATE_SHA,
                                "admission gate\ncapacity candidate")

    def check_events(self, events, test=TEST, children=("tied", "duplicates", "sparse")):
        self.fixture.write_text(
            "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8",
        )
        return self.run_checker("test-events", str(self.fixture), PACKAGE, test, *children)

    def test_required_test_pass_counts(self):
        required = (f"{TEST}/tied", f"{TEST}/duplicates", f"{TEST}/sparse", TEST, "")
        passes = [go_event(name) for name in required]
        self.assert_verdict(self.check_events(passes))
        for index, name in enumerate(required):
            for count in (0, 2):
                with self.subTest(name=name or PACKAGE, count=count):
                    events = passes[:index] + [passes[index]] * count + passes[index + 1:]
                    self.assert_verdict(
                        self.check_events(events),
                        error=f"{name or PACKAGE} reported {count} passes, expected 1\n",
                    )

    def test_test_failures_and_skips_override_passes(self):
        required = (f"{TEST}/tied", f"{TEST}/duplicates", f"{TEST}/sparse", TEST, "")
        passes = [go_event(name) for name in required]
        for name in (*required, f"{TEST}/extra/nested"):
            for action in ("fail", "skip"):
                with self.subTest(name=name or PACKAGE, action=action):
                    self.assert_verdict(
                        self.check_events(passes + [go_event(name, action)]),
                        error=f"{name or PACKAGE} reported {action}\n",
                    )

    def test_no_required_subtests_and_unrelated_events(self):
        parity = "TestReleaseRestoredParity"
        events = [
            go_event(parity, "pass", "bog/other"),
            go_event(parity + "Other/child", "skip"),
            go_event(TEST, "fail"),
            go_event(parity, "run"),
            go_event(parity, "output"),
            go_event(parity),
            go_event(),
        ]
        self.assert_verdict(self.check_events(events, parity, ()))
        self.assert_verdict(
            self.check_events(events[:-2] + [go_event()], parity, ()),
            error=f"{parity} reported 0 passes, expected 1\n",
        )


def go_event(name="", action="pass", package=PACKAGE):
    event = {"Package": package, "Action": action}
    if name:
        event["Test"] = name
    return event


if __name__ == "__main__":
    unittest.main()
