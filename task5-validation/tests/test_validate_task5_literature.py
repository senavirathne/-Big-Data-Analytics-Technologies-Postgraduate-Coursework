from __future__ import annotations

import sys
import unittest
import urllib.error
from pathlib import Path
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
LOCAL_SCRIPT_DIRECTORY = REPOSITORY_ROOT / ".github" / "scripts"
SCRIPT_DIRECTORY = (
    LOCAL_SCRIPT_DIRECTORY if LOCAL_SCRIPT_DIRECTORY.is_dir() else Path("/opt/coursework")
)
sys.path.insert(0, str(SCRIPT_DIRECTORY))

import validate_task5_literature as validator  # noqa: E402


class FakeResponse:
    def __init__(self, status: int, url: str) -> None:
        self.status = status
        self._url = url

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def geturl(self) -> str:
        return self._url


def http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        "https://example.test/reference",
        code,
        "test",
        hdrs=None,
        fp=None,
    )


class ReferenceParsingTests(unittest.TestCase):
    def test_real_paper_exposes_every_doi(self) -> None:
        paper_path = (
            REPOSITORY_ROOT / "task5-literature-review.md"
            if (REPOSITORY_ROOT / "task5-literature-review.md").is_file()
            else Path("/coursework/task5-literature-review.md")
        )
        text = paper_path.read_text(encoding="utf-8")
        references = text.split("## References", 1)[1]
        entries = validator.split_reference_entries(references)
        records = validator.parse_doi_references(entries)

        self.assertEqual(22, len(entries))
        self.assertEqual(12, len(records))
        self.assertEqual(12, len({record.doi for record in records}))
        self.assertIn(
            "10.1007/s42484-024-00155-2",
            {record.doi for record in records},
        )
        self.assertTrue(any("csrc.nist.gov" in entry for entry in entries))

    def test_doi_normalisation_preserves_balanced_parentheses(self) -> None:
        self.assertEqual(
            "10.1234/example(test)",
            validator.normalise_doi("https://doi.org/10.1234/Example(Test))."),
        )


class MetadataIdentityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.reference = validator.ReferenceRecord(
            doi="10.1234/example",
            entry="Smith (2024) ‘A Useful Study’",
            title="A Useful Study",
            first_author="Smith",
            publication_year=2024,
            citation_year="2024",
        )
        self.matching = validator.DoiResult(
            doi="10.1234/example",
            status=validator.VERIFIED,
            detail="fetched",
            publication_years=(2024,),
            work_type="journal-article",
            title="A useful study",
            first_author="Smith",
            container_title="Journal of Tests",
        )

    def test_complete_identity_match_is_verified(self) -> None:
        result = validator.validate_doi_reference(self.reference, self.matching)
        self.assertEqual(validator.VERIFIED, result.status)

    def test_title_author_and_year_mismatches_are_each_rejected(self) -> None:
        variants = {
            "title": validator.DoiResult(
                **{**self.matching.__dict__, "title": "An Unrelated Paper"}
            ),
            "author": validator.DoiResult(
                **{**self.matching.__dict__, "first_author": "Jones"}
            ),
            "year": validator.DoiResult(
                **{**self.matching.__dict__, "publication_years": (2023,)}
            ),
        }
        for name, metadata in variants.items():
            with self.subTest(name=name):
                result = validator.validate_doi_reference(self.reference, metadata)
                self.assertEqual(validator.MISMATCH, result.status)
                self.assertIn(name if name != "author" else "first author", result.detail)

    def test_recency_is_fixed_to_five_inclusive_calendar_years(self) -> None:
        self.assertEqual(2022, validator.MINIMUM_RECENT_YEAR)
        self.assertEqual(2026, validator.ASSIGNMENT_AS_OF_YEAR)
        for year in (2022, 2026):
            with self.subTest(year=year):
                result = validator.DoiResult(
                    **{**self.matching.__dict__, "publication_years": (year,)}
                )
                self.assertTrue(validator.is_recent_scholarly(result))
        for year in (2021, 2027):
            with self.subTest(year=year):
                result = validator.DoiResult(
                    **{**self.matching.__dict__, "publication_years": (year,)}
                )
                self.assertFalse(validator.is_recent_scholarly(result))


class CitationPairTests(unittest.TestCase):
    def test_supported_author_year_forms(self) -> None:
        bodies = (
            "Smith (2024) reports the result.",
            "Smith *et al.* (2024) report the result.",
            "The result is established (Smith, 2024).",
            "Several results agree (Jones, 2023; Smith et al., 2024).",
        )
        for body in bodies:
            with self.subTest(body=body):
                self.assertTrue(
                    validator.has_author_year_citation(body, "Smith", "2024")
                )

    def test_separate_author_and_year_do_not_form_a_citation(self) -> None:
        body = "Smith discussed the design.\n\nAn unrelated source appeared in (2024)."
        self.assertFalse(validator.has_author_year_citation(body, "Smith", "2024"))
        self.assertFalse(
            validator.has_author_year_citation("Sunday (2024)", "Sun", "2024")
        )

    def test_year_suffix_is_not_interchangeable(self) -> None:
        body = "Smith (2024a) distinguishes the standards."
        self.assertTrue(validator.has_author_year_citation(body, "Smith", "2024a"))
        self.assertFalse(validator.has_author_year_citation(body, "Smith", "2024b"))


class LinkClassificationTests(unittest.TestCase):
    @mock.patch.object(validator.urllib.request, "urlopen")
    def test_success_is_verified(self, urlopen: mock.Mock) -> None:
        urlopen.return_value = FakeResponse(200, "https://example.test/reference")
        result = validator.fetch_link("https://example.test/reference")
        self.assertEqual(validator.VERIFIED, result.status)

    @mock.patch.object(validator.urllib.request, "urlopen")
    def test_head_rejection_can_fall_back_to_successful_get(self, urlopen: mock.Mock) -> None:
        urlopen.side_effect = [
            http_error(403),
            FakeResponse(200, "https://example.test/reference"),
        ]
        result = validator.fetch_link("https://example.test/reference")
        self.assertEqual(validator.VERIFIED, result.status)

    @mock.patch.object(validator.urllib.request, "urlopen")
    def test_repeated_access_denial_is_blocked(self, urlopen: mock.Mock) -> None:
        urlopen.side_effect = [http_error(403), http_error(403)]
        result = validator.fetch_link("https://example.test/reference")
        self.assertEqual(validator.BLOCKED, result.status)

    @mock.patch.object(validator.urllib.request, "urlopen")
    def test_confirmed_missing_endpoint_is_broken(self, urlopen: mock.Mock) -> None:
        urlopen.side_effect = [http_error(404), http_error(404)]
        result = validator.fetch_link("https://example.test/reference")
        self.assertEqual(validator.BROKEN, result.status)

    @mock.patch.object(validator.urllib.request, "urlopen")
    def test_transport_failure_is_unverifiable(self, urlopen: mock.Mock) -> None:
        urlopen.side_effect = urllib.error.URLError("offline")
        result = validator.fetch_link("https://example.test/reference")
        self.assertEqual(validator.UNVERIFIABLE, result.status)


class CheckPolicyTests(unittest.TestCase):
    def test_advisory_failure_does_not_fail_run(self) -> None:
        status, exit_code = validator.evaluation_status(
            [validator.CheckResult("style signal", False, "warning", validator.ADVISORY)]
        )
        self.assertEqual(("PASS", 0), (status, exit_code))

    def test_mandatory_failure_fails_run(self) -> None:
        status, exit_code = validator.evaluation_status(
            [validator.CheckResult("required", False, "failure")]
        )
        self.assertEqual(("FAIL", 1), (status, exit_code))

    def test_skipped_network_is_incomplete(self) -> None:
        status, exit_code = validator.evaluation_status([], incomplete=True)
        self.assertEqual(("INCOMPLETE", 2), (status, exit_code))


if __name__ == "__main__":
    unittest.main()
