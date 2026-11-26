#!/usr/bin/env python3
"""Exercise the report contract without granting release approval."""
import copy
import json
import pathlib
import tempfile
import unittest

from validate_release_gate import validate

HERE = pathlib.Path(__file__).resolve().parent
FIXTURE = json.loads((HERE / "fixtures/pass.json").read_text())


class ValidatorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = pathlib.Path(self.temp.name)
        self.fixture = copy.deepcopy(FIXTURE)

    def reports(self):
        for name, cases in self.fixture["reports"].items():
            report = dict(self.fixture["common"], producer=name,
                          candidate_revision=self.fixture["revision"],
                          candidate_binary_sha256=self.fixture["binary_sha256"],
                          cases=cases)
            (self.directory / (name + ".json")).write_text(json.dumps(report))

    def errors(self):
        return validate(self.directory, self.fixture["revision"], self.fixture["binary_sha256"])

    def test_complete_pass(self):
        self.reports()
        self.assertEqual(self.errors(), [])

    def test_revision_mismatch(self):
        self.reports()
        path = self.directory / "durability.json"
        report = json.loads(path.read_text())
        report["candidate_revision"] = "0" * 40
        path.write_text(json.dumps(report))
        self.assertTrue(any("revision mismatch" in e for e in self.errors()))

    def test_binary_mismatch(self):
        self.reports()
        path = self.directory / "capacity.json"
        report = json.loads(path.read_text())
        report["candidate_binary_sha256"] = "0" * 64
        path.write_text(json.dumps(report))
        self.assertTrue(any("binary mismatch" in e for e in self.errors()))

    def test_missing_verifier(self):
        del self.fixture["common"]["verifier"]
        self.reports()
        self.assertTrue(any("verifier identity" in e for e in self.errors()))

    def test_missing_report(self):
        self.reports()
        (self.directory / "admission.json").unlink()
        self.assertTrue(any("missing or invalid report" in e for e in self.errors()))

    def test_required_skip(self):
        self.fixture["reports"]["capacity"]["retained_history_capacity"]["status"] = "skip"
        self.reports()
        self.assertTrue(any("did not pass" in e for e in self.errors()))

    def test_duplicate_status_rejected(self):
        self.reports()
        path = self.directory / "capacity.json"
        report = path.read_text().replace('"status": "pass"',
                                          '"status": "skip", "status": "pass"')
        path.write_text(report)
        self.assertTrue(any("duplicate JSON key: status" in e for e in self.errors()))

    def test_failed_case(self):
        self.fixture["reports"]["durability"]["torn_suffix_recovery"]["status"] = "fail"
        self.reports()
        self.assertTrue(any("did not pass" in e for e in self.errors()))

    def test_work_budget_is_required(self):
        del self.fixture["reports"]["admission"]["work_budget"]
        self.reports()
        self.assertTrue(any("work_budget" in e for e in self.errors()))

    def test_configuration_is_required(self):
        self.fixture["common"]["configuration"] = {}
        self.reports()
        self.assertTrue(any("missing configuration" in e for e in self.errors()))

    def test_additional_skipped_case_fails(self):
        self.fixture["reports"]["capacity"]["extra"] = {"status": "skipped"}
        self.reports()
        self.assertTrue(any("case extra did not pass" in e for e in self.errors()))

    def test_missing_enospc_and_admission_evidence(self):
        del self.fixture["reports"]["durability"]["real_enospc_recovery"]["observed_enospc"]
        del self.fixture["reports"]["admission"]["one_slot_admission"]["no_cursor"]
        self.reports()
        errors = self.errors()
        self.assertTrue(any("observed_enospc" in e for e in errors))
        self.assertTrue(any("no_cursor" in e for e in errors))


if __name__ == "__main__":
    unittest.main()
