#!/usr/bin/env python3
"""Validate Task 5 structure, coverage, references, and authoritative URLs."""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import re
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path


RECENT_WINDOW_YEARS = 5
AUTHORITATIVE_DOMAINS = {
    "atlas.apache.org",
    "doi.org",
    "docs.open-metadata.org",
    "eur-lex.europa.eu",
    "leginfo.legislature.ca.gov",
}
DOI_PATTERN = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.IGNORECASE)
URL_PATTERN = re.compile(r"https://[^\s<>]+")


@dataclass(frozen=True)
class LinkResult:
    url: str
    status: str
    detail: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument(
        "--skip-links",
        action="store_true",
        help="Run deterministic local checks without making network requests.",
    )
    return parser.parse_args()


def normalise_url(raw: str) -> str:
    return raw.rstrip(".,;:)]}\"")


def fetch_link(url: str) -> LinkResult:
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8",
        "User-Agent": "Coventry-Coursework-Link-Validator/1.0",
    }
    context = ssl.create_default_context()
    last_error = "unknown error"
    for method in ("HEAD", "GET"):
        request = urllib.request.Request(url, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=20, context=context) as response:
                status = response.status
                if 200 <= status < 400:
                    return LinkResult(url, "PASS", f"HTTP {status}: {response.geturl()}")
                last_error = f"HTTP {status}"
        except urllib.error.HTTPError as error:
            last_error = f"HTTP {error.code}"
            if error.code in {401, 403, 405, 429}:
                if method == "HEAD" and error.code == 405:
                    continue
                return LinkResult(
                    url,
                    "WARN",
                    f"{last_error}; the authoritative endpoint refused automated validation.",
                )
            if error.code in {404, 410}:
                return LinkResult(url, "FAIL", last_error)
        except (OSError, TimeoutError, urllib.error.URLError) as error:
            last_error = f"{type(error).__name__}: {error}"
        if method == "HEAD":
            continue
    return LinkResult(url, "WARN", f"Transient/unverifiable: {last_error}")


def section_between(text: str, start: str, end: str) -> str:
    start_index = text.find(start)
    end_index = text.find(end, start_index + len(start)) if start_index >= 0 else -1
    if start_index < 0 or end_index < 0:
        return ""
    return text[start_index + len(start) : end_index]


def has_terms(text: str, alternatives: tuple[str, ...]) -> bool:
    lowered = text.casefold()
    return any(term.casefold() in lowered for term in alternatives)


def main() -> int:
    args = parse_args()
    text = args.paper.read_text(encoding="utf-8")
    checks: list[tuple[str, bool, str]] = []
    warnings: list[str] = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append((name, passed, detail))

    headings = [
        "## 5. Literature Review",
        "### 5.1 Enterprise data governance, Data Mesh and regulatory lifecycle management",
        "### 5.2 Quantum and generative-AI directions for big-data engineering",
        "## References",
    ]
    heading_positions = [text.find(heading) for heading in headings]
    check(
        "Formal section structure",
        all(position >= 0 for position in heading_positions)
        and heading_positions == sorted(heading_positions),
        "Required 5, 5.1, 5.2, and References headings occur once and in order.",
    )
    for heading in headings:
        check(
            f"Single heading: {heading}",
            text.count(heading) == 1,
            f"Occurrences: {text.count(heading)}.",
        )

    section_51 = section_between(text, headings[1], headings[2])
    section_52 = section_between(text, headings[2], headings[3])
    references = text[text.find(headings[3]) + len(headings[3]) :] if headings[3] in text else ""
    body = text[: text.find(headings[3])] if headings[3] in text else text

    for number, section in (("5.1", section_51), ("5.2", section_52)):
        word_count = len(re.findall(r"\b[\w’'-]+\b", section))
        citation_count = len(re.findall(r"\((?:[^()]{0,100},\s*)?(?:19|20)\d{2}\)", section))
        check(
            f"Section {number} is substantive and cited",
            word_count >= 700 and citation_count >= 3,
            f"Words: {word_count}; parenthetical author-year citations: {citation_count}.",
        )

    placeholder_matches = re.findall(
        r"(?im)\b(?:TODO|TBC|FIXME|LOREM IPSUM|INSERT (?:CITATION|REFERENCE))\b",
        text,
    )
    check(
        "No drafting placeholders",
        not placeholder_matches,
        f"Placeholder matches: {len(placeholder_matches)}.",
    )

    required_51 = {
        "centralised lakes/warehouses and their limits": (
            ("centralised", "centralized"),
            ("data lake",),
            ("warehouse",),
            ("bottleneck", "limitation", "limit", "downstream effects"),
        ),
        "Data Mesh and Data Contracts": (("data mesh",), ("data contract",)),
        "GDPR/CCPA lifecycle controls": (
            ("gdpr",),
            ("ccpa",),
            ("ingestion",),
            ("processing",),
            ("sharing",),
            ("archiv", "retention"),
            ("deletion", "erasure"),
        ),
        "Atlas/OpenMetadata lineage and access controls": (
            ("apache atlas",),
            ("openmetadata",),
            ("lineage",),
            ("access control", "access controls", "policy"),
        ),
    }
    required_52 = {
        "QML, quantum databases, and NP-hard processing": (
            ("quantum machine learning", "qml"),
            ("quantum-enhanced database", "quantum database", "database evidence"),
            ("np-hard",),
        ),
        "signatures and multidimensional indexing": (
            ("cryptographic signature", "digital signature"),
            ("multidimensional index", "multidimensional indexing"),
        ),
        "LLM code-generation validation loop": (
            ("large language model", "llm"),
            ("generate sql", "generate code", "code-generation", "proposes code"),
            ("compile",),
            ("sandbox",),
            ("deterministic",),
        ),
        "predictive optimisation and self-healing ETL": (
            ("predictive optimisation", "predictive optimization"),
            ("self-healing", "self-repairing"),
            ("etl",),
        ),
        "text-to-SQL": (("text-to-sql",),),
        "nondeterministic LLMs with deterministic Spark/Flink": (
            ("nondeterminism", "non-determinism", "nondeterministic"),
            ("spark",),
            ("flink",),
            ("deterministic",),
        ),
    }
    for name, groups in required_51.items():
        missing = [" / ".join(group) for group in groups if not has_terms(section_51, group)]
        check(f"5.1 coverage: {name}", not missing, f"Missing term groups: {missing or 'none'}.")
    for name, groups in required_52.items():
        missing = [" / ".join(group) for group in groups if not has_terms(section_52, group)]
        check(f"5.2 coverage: {name}", not missing, f"Missing term groups: {missing or 'none'}.")

    reference_entries = [
        entry.strip()
        for entry in re.split(r"\n\s*\n", references.strip())
        if entry.strip()
    ]
    check(
        "Reference list is populated",
        len(reference_entries) >= 5,
        f"Reference entries: {len(reference_entries)}.",
    )

    current_year = dt.date.today().year
    minimum_recent_year = current_year - RECENT_WINDOW_YEARS
    recent_peer_reviewed: dict[str, tuple[int, str]] = {}
    for entry in reference_entries:
        dois = DOI_PATTERN.findall(entry)
        years = [int(year) for year in re.findall(r"\(((?:19|20)\d{2})\)", entry)]
        venue_signal = bool(
            re.search(
                r"(?i)(journal|transactions|conference|findings|communications|"
                r"machine learning|computational intelligence|ieee access|proceedings)",
                entry,
            )
        )
        if not years or not venue_signal:
            continue
        publication_year = years[0]
        if minimum_recent_year <= publication_year <= current_year:
            for raw_doi in dois:
                doi = raw_doi.rstrip(".,;:)]}").casefold()
                recent_peer_reviewed[doi] = (publication_year, entry.split(",", 1)[0])

    check(
        "At least five distinct recent peer-reviewed DOI references",
        len(recent_peer_reviewed) >= 5,
        (
            f"Found {len(recent_peer_reviewed)} distinct venue-qualified DOI references "
            f"published from {minimum_recent_year} through {current_year}."
        ),
    )

    uncited_recent = []
    for _, (year, first_author) in sorted(recent_peer_reviewed.items()):
        if first_author.casefold() not in body.casefold() or str(year) not in body:
            uncited_recent.append(f"{first_author} ({year})")
    check(
        "Recent DOI references are cited in the review",
        not uncited_recent,
        f"Uncited entries: {uncited_recent or 'none'}.",
    )

    urls = sorted({normalise_url(url) for url in URL_PATTERN.findall(references)})
    authoritative_urls = [
        url
        for url in urls
        if (urllib.parse.urlparse(url).hostname or "").casefold() in AUTHORITATIVE_DOMAINS
    ]
    check(
        "Authoritative DOI/regulatory/documentation URLs present",
        len(authoritative_urls) >= 5
        and any("doi.org" in url for url in authoritative_urls)
        and any("eur-lex.europa.eu" in url for url in authoritative_urls)
        and any("leginfo.legislature.ca.gov" in url for url in authoritative_urls),
        f"Authoritative URLs selected for validation: {len(authoritative_urls)}.",
    )

    link_results: list[LinkResult] = []
    if args.skip_links:
        warnings.append("Network link checking was skipped by command-line request.")
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            link_results = sorted(executor.map(fetch_link, authoritative_urls), key=lambda item: item.url)
        failed_links = [result.url for result in link_results if result.status == "FAIL"]
        warned_links = [result.url for result in link_results if result.status == "WARN"]
        check(
            "No authoritative URL returned 404/410",
            not failed_links,
            f"Definitively broken URLs: {failed_links or 'none'}.",
        )
        if warned_links:
            warnings.append(
                f"{len(warned_links)} authoritative endpoint(s) blocked or could not reliably complete automated validation."
            )

    passed = sum(1 for _, result, _ in checks if result)
    failed = len(checks) - passed
    lines = [
        "# Task 5 CI validation report",
        "",
        f"Overall: **{'PASS' if failed == 0 else 'FAIL'}** ({passed} passed, {failed} failed)",
        "",
        f"Paper: `{args.paper}`",
        f"Recent-publication window: {minimum_recent_year}–{current_year}",
        "",
        "## Deterministic content checks",
        "",
        "| Check | Status | Evidence |",
        "|---|---:|---|",
    ]
    for name, result, detail in checks:
        safe_detail = detail.replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {name} | {'PASS' if result else 'FAIL'} | {safe_detail} |")

    lines.extend(["", "## Authoritative link checks", ""])
    if link_results:
        lines.extend(["| URL | Status | Result |", "|---|---:|---|"])
        for result in link_results:
            detail = result.detail.replace("|", "\\|")
            lines.append(f"| <{result.url}> | {result.status} | {detail} |")
    else:
        lines.append("Link checks were not run.")

    if warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {warning}" for warning in warnings)

    lines.extend(
        [
            "",
            "Peer-review status is checked conservatively from the formal venue and DOI metadata",
            "present in each bibliography entry; the report does not infer evidential strength from DOI",
            "presence alone.",
            "",
        ]
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines), encoding="utf-8")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
