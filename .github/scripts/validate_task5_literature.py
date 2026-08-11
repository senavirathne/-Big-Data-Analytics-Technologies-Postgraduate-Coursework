#!/usr/bin/env python3
"""Validate Task 5 against the coursework brief and verify its references."""

from __future__ import annotations

import argparse
import concurrent.futures
import html
import json
import re
import ssl
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path


# "Recent" is evaluated at the assignment year, not the machine's wall clock.
# Five inclusive calendar years ending in 2026 are therefore 2022--2026.
ASSIGNMENT_AS_OF_YEAR = 2026
RECENT_WINDOW_YEARS = 5
MINIMUM_RECENT_YEAR = ASSIGNMENT_AS_OF_YEAR - RECENT_WINDOW_YEARS + 1

AUTHORITATIVE_DOMAINS = {
    "atlas.apache.org",
    "csrc.nist.gov",
    "doi.org",
    "docs.open-metadata.org",
    "eur-lex.europa.eu",
    "leginfo.legislature.ca.gov",
}
DOI_PATTERN = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.IGNORECASE)
URL_PATTERN = re.compile(r"https://[^\s<>]+")
YEAR_PATTERN = re.compile(r"\(((?:19|20)\d{2})([a-z]?)\)", re.IGNORECASE)
PEER_REVIEWED_CROSSREF_TYPES = {
    "journal-article",
    "proceedings-article",
}

MANDATORY = "MANDATORY"
ADVISORY = "ADVISORY"

VERIFIED = "VERIFIED"
BLOCKED = "BLOCKED"
UNVERIFIABLE = "UNVERIFIABLE"
BROKEN = "BROKEN"
INVALID = "INVALID"
MISMATCH = "MISMATCH"


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str
    level: str = MANDATORY


@dataclass(frozen=True)
class LinkResult:
    url: str
    status: str
    detail: str


@dataclass(frozen=True)
class ReferenceRecord:
    doi: str
    entry: str
    title: str | None
    first_author: str | None
    publication_year: int | None
    citation_year: str | None


@dataclass(frozen=True)
class DoiResult:
    doi: str
    status: str
    detail: str
    publication_years: tuple[int, ...] = ()
    work_type: str | None = None
    title: str | None = None
    first_author: str | None = None
    container_title: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument(
        "--skip-network",
        "--skip-links",
        dest="skip_network",
        action="store_true",
        help=(
            "Run local structural checks without URL or Crossref requests. "
            "The report is marked INCOMPLETE and the command exits 2."
        ),
    )
    parser.add_argument(
        "--strict-network",
        action="store_true",
        help=(
            "Require every DOI's Crossref identity metadata to be network-verifiable. "
            "Authoritative URL health remains an advisory signal."
        ),
    )
    return parser.parse_args()


def strip_unbalanced_closing_parenthesis(value: str) -> str:
    while value.endswith(")") and value.count("(") < value.count(")"):
        value = value[:-1]
    return value


def normalise_doi(raw: str) -> str:
    value = urllib.parse.unquote(raw.strip())
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if value.casefold().startswith(prefix):
            value = value[len(prefix) :]
            break
    value = value.rstrip(".,;:]}\"'")
    value = strip_unbalanced_closing_parenthesis(value)
    return value.casefold()


def normalise_url(raw: str) -> str:
    value = raw.rstrip(".,;:]}\"'")
    return strip_unbalanced_closing_parenthesis(value)


def normalise_words(value: str) -> str:
    value = html.unescape(re.sub(r"<[^>]+>", " ", value))
    decomposed = unicodedata.normalize("NFKD", value.casefold())
    without_marks = "".join(char for char in decomposed if not unicodedata.combining(char))
    return " ".join(re.findall(r"[\w]+", without_marks, flags=re.UNICODE))


def titles_match(expected: str, actual: str) -> bool:
    expected_normalised = normalise_words(expected)
    actual_normalised = normalise_words(actual)
    if not expected_normalised or not actual_normalised:
        return False
    if expected_normalised == actual_normalised:
        return True
    return SequenceMatcher(None, expected_normalised, actual_normalised).ratio() >= 0.92


def extract_reference_title(entry: str) -> str | None:
    for pattern in (r"[‘“](.+?)[’”]", r'"(.+?)"'):
        match = re.search(pattern, entry, flags=re.DOTALL)
        if match:
            return " ".join(match.group(1).split())
    return None


def extract_first_author(entry: str) -> str | None:
    prefix = entry.split(",", 1)[0].strip()
    prefix = re.sub(r"[*_`]", "", prefix)
    return prefix or None


def extract_reference_year(entry: str) -> tuple[int | None, str | None]:
    match = YEAR_PATTERN.search(entry)
    if not match:
        return None, None
    year = int(match.group(1))
    return year, f"{year}{match.group(2).casefold()}"


def split_reference_entries(references: str) -> list[str]:
    return [
        entry.strip()
        for entry in re.split(r"\n\s*\n", references.strip())
        if entry.strip()
    ]


def parse_doi_references(reference_entries: list[str]) -> list[ReferenceRecord]:
    records: list[ReferenceRecord] = []
    for entry in reference_entries:
        year, citation_year = extract_reference_year(entry)
        for raw_doi in DOI_PATTERN.findall(entry):
            records.append(
                ReferenceRecord(
                    doi=normalise_doi(raw_doi),
                    entry=entry,
                    title=extract_reference_title(entry),
                    first_author=extract_first_author(entry),
                    publication_year=year,
                    citation_year=citation_year,
                )
            )
    return records


def section_between(text: str, start: re.Match[str] | None, end: re.Match[str] | None) -> str:
    if start is None or end is None or start.end() > end.start():
        return ""
    return text[start.end() : end.start()]


def has_terms(text: str, alternatives: tuple[str, ...]) -> bool:
    lowered = text.casefold()
    return any(term.casefold() in lowered for term in alternatives)


def has_author_year_citation(
    body: str,
    first_author: str,
    citation_year: str | int,
) -> bool:
    normalised_body = unicodedata.normalize("NFKD", body.casefold())
    normalised_body = "".join(
        char for char in normalised_body if not unicodedata.combining(char)
    )
    normalised_author = normalise_words(first_author)
    if not normalised_author:
        return False

    author = rf"(?<!\w){re.escape(normalised_author)}(?!\w)"
    year = re.escape(str(citation_year).casefold())
    narrative = rf"{author}[^()\n]{{0,120}}\(\s*{year}\s*\)"
    parenthetical = rf"\([^()\n]{{0,240}}{author}[^()\n]{{0,160}}\b{year}\b[^()\n]*\)"
    return bool(
        re.search(narrative, normalised_body, flags=re.IGNORECASE)
        or re.search(parenthetical, normalised_body, flags=re.IGNORECASE)
    )


def fetch_link(url: str) -> LinkResult:
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8",
        "User-Agent": "Coventry-Coursework-Link-Validator/2.0",
    }
    context = ssl.create_default_context()
    last_error = "unknown error"
    for method in ("HEAD", "GET"):
        request = urllib.request.Request(url, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=20, context=context) as response:
                status = response.status
                final_url = response.geturl()
                final_host = (urllib.parse.urlparse(final_url).hostname or "").casefold()
                if 200 <= status < 400:
                    if final_host.endswith("perfdrive.com") or "captcha" in final_url.casefold():
                        return LinkResult(
                            url,
                            BLOCKED,
                            f"HTTP {status} redirected to an automation challenge: {final_url}",
                        )
                    return LinkResult(url, VERIFIED, f"HTTP {status}: {final_url}")
                last_error = f"HTTP {status}"
        except urllib.error.HTTPError as error:
            last_error = f"HTTP {error.code}"
            if method == "HEAD":
                # Some valid sites reject or misimplement HEAD; confirm with GET.
                continue
            if error.code in {401, 403, 429}:
                return LinkResult(
                    url,
                    BLOCKED,
                    f"{last_error}; endpoint refused or throttled automated GET.",
                )
            if error.code in {404, 410}:
                return LinkResult(url, BROKEN, last_error)
        except (OSError, TimeoutError, urllib.error.URLError) as error:
            last_error = f"{type(error).__name__}: {error}"
        if method == "HEAD":
            continue
    return LinkResult(url, UNVERIFIABLE, f"Could not verify endpoint: {last_error}")


def crossref_publication_years(message: dict) -> tuple[int, ...]:
    years: list[int] = []
    for key in ("published-print", "published-online", "published", "issued"):
        value = message.get(key)
        if not isinstance(value, dict):
            continue
        date_parts = value.get("date-parts")
        if (
            isinstance(date_parts, list)
            and date_parts
            and isinstance(date_parts[0], list)
            and date_parts[0]
            and isinstance(date_parts[0][0], int)
        ):
            year = date_parts[0][0]
            if year not in years:
                years.append(year)
    return tuple(years)


def fetch_doi_metadata(doi: str) -> DoiResult:
    encoded_doi = urllib.parse.quote(doi, safe="")
    url = f"https://api.crossref.org/works/{encoded_doi}"
    headers = {
        "Accept": "application/json",
        "User-Agent": "Coventry-Coursework-DOI-Validator/2.0",
    }
    context = ssl.create_default_context()
    last_error = "unknown error"
    for attempt in range(3):
        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=30, context=context) as response:
                payload = json.load(response)
            if not isinstance(payload, dict):
                return DoiResult(doi, INVALID, "Crossref returned a non-object payload")
            message = payload.get("message")
            if not isinstance(message, dict):
                return DoiResult(doi, INVALID, "Crossref returned no work metadata object")

            returned_doi = normalise_doi(str(message.get("DOI", "")))
            if returned_doi != doi:
                return DoiResult(
                    doi,
                    INVALID,
                    f"Crossref DOI mismatch: {returned_doi or 'missing'}",
                )

            titles = message.get("title", [])
            title = titles[0].strip() if isinstance(titles, list) and titles else None
            authors = message.get("author", [])
            first_author = None
            if isinstance(authors, list) and authors and isinstance(authors[0], dict):
                first_author = str(authors[0].get("family", "")).strip() or None
            containers = message.get("container-title", [])
            container_title = (
                containers[0].strip()
                if isinstance(containers, list) and containers
                else None
            )
            work_type = str(message.get("type", "")).strip() or None
            publication_years = crossref_publication_years(message)

            missing = []
            if not title:
                missing.append("title")
            if not first_author:
                missing.append("first author")
            if not publication_years:
                missing.append("publication year")
            if not work_type:
                missing.append("work type")
            if missing:
                return DoiResult(
                    doi,
                    INVALID,
                    f"Crossref metadata lacks: {', '.join(missing)}",
                    publication_years,
                    work_type,
                    title,
                    first_author,
                    container_title,
                )

            return DoiResult(
                doi,
                VERIFIED,
                f"Crossref {work_type}: {title}",
                publication_years,
                work_type,
                title,
                first_author,
                container_title,
            )
        except urllib.error.HTTPError as error:
            last_error = f"HTTP {error.code}"
            if error.code in {404, 410}:
                return DoiResult(doi, INVALID, last_error)
            if error.code == 429 and attempt < 2:
                time.sleep(2**attempt)
                continue
        except (
            OSError,
            TimeoutError,
            urllib.error.URLError,
            json.JSONDecodeError,
            UnicodeDecodeError,
        ) as error:
            last_error = f"{type(error).__name__}: {error}"
        if attempt < 2:
            time.sleep(2**attempt)
    return DoiResult(doi, UNVERIFIABLE, f"Crossref metadata unavailable: {last_error}")


def validate_doi_reference(reference: ReferenceRecord, result: DoiResult) -> DoiResult:
    if result.status != VERIFIED:
        return result

    mismatches: list[str] = []
    if not reference.title:
        mismatches.append("bibliography title could not be parsed")
    elif not result.title or not titles_match(reference.title, result.title):
        mismatches.append(
            f"title differs (cited: {reference.title!r}; Crossref: {result.title!r})"
        )

    if not reference.first_author:
        mismatches.append("bibliography first author could not be parsed")
    elif not result.first_author or (
        normalise_words(reference.first_author) != normalise_words(result.first_author)
    ):
        mismatches.append(
            "first author differs "
            f"(cited: {reference.first_author!r}; Crossref: {result.first_author!r})"
        )

    if reference.publication_year is None:
        mismatches.append("bibliography publication year could not be parsed")
    elif reference.publication_year not in result.publication_years:
        mismatches.append(
            "publication year differs "
            f"(cited: {reference.publication_year}; Crossref: {result.publication_years})"
        )

    if mismatches:
        return DoiResult(
            result.doi,
            MISMATCH,
            "; ".join(mismatches),
            result.publication_years,
            result.work_type,
            result.title,
            result.first_author,
            result.container_title,
        )

    return DoiResult(
        result.doi,
        VERIFIED,
        "DOI, title, first author, and publication year match the bibliography entry",
        result.publication_years,
        result.work_type,
        result.title,
        result.first_author,
        result.container_title,
    )


def is_recent_scholarly(result: DoiResult) -> bool:
    return (
        result.status == VERIFIED
        and result.work_type in PEER_REVIEWED_CROSSREF_TYPES
        and any(
            MINIMUM_RECENT_YEAR <= year <= ASSIGNMENT_AS_OF_YEAR
            for year in result.publication_years
        )
    )


def doi_from_url(url: str) -> str | None:
    parsed = urllib.parse.urlparse(url)
    if (parsed.hostname or "").casefold() != "doi.org":
        return None
    candidate = normalise_doi(parsed.path.lstrip("/"))
    return candidate if DOI_PATTERN.fullmatch(candidate) else None


def evaluation_status(checks: list[CheckResult], incomplete: bool = False) -> tuple[str, int]:
    if incomplete:
        return "INCOMPLETE", 2
    if any(not check.passed for check in checks if check.level == MANDATORY):
        return "FAIL", 1
    return "PASS", 0


def main() -> int:
    args = parse_args()
    text = args.paper.read_text(encoding="utf-8")
    checks: list[CheckResult] = []
    warnings: list[str] = []

    def check(
        name: str,
        passed: bool,
        detail: str,
        level: str = MANDATORY,
    ) -> None:
        checks.append(CheckResult(name, passed, detail, level))

    heading_patterns = {
        "5": re.compile(r"^##\s+5\.\s+Literature Review\s*$", re.MULTILINE | re.IGNORECASE),
        "5.1": re.compile(r"^###\s+5\.1\b.*$", re.MULTILINE),
        "5.2": re.compile(r"^###\s+5\.2\b.*$", re.MULTILINE),
        "references": re.compile(r"^##\s+References\s*$", re.MULTILINE | re.IGNORECASE),
    }
    heading_matches = {name: list(pattern.finditer(text)) for name, pattern in heading_patterns.items()}
    first_headings = {
        name: matches[0] if len(matches) == 1 else None
        for name, matches in heading_matches.items()
    }
    ordered_positions = [
        first_headings[name].start() if first_headings[name] else -1
        for name in ("5", "5.1", "5.2", "references")
    ]
    check(
        "Required Task 5 section structure",
        all(position >= 0 for position in ordered_positions)
        and ordered_positions == sorted(ordered_positions),
        "Sections 5, 5.1, 5.2, and References must each occur once and in order; "
        + ", ".join(f"{name}={len(matches)}" for name, matches in heading_matches.items()),
    )

    section_51 = section_between(text, first_headings["5.1"], first_headings["5.2"])
    section_52 = section_between(text, first_headings["5.2"], first_headings["references"])
    references_match = first_headings["references"]
    references = text[references_match.end() :] if references_match else ""
    body = text[: references_match.start()] if references_match else text

    for number, section in (("5.1", section_51), ("5.2", section_52)):
        word_count = len(re.findall(r"\b[\w’'-]+\b", section))
        citation_count = len(
            re.findall(r"\((?:[^()]{0,120},\s*)?(?:19|20)\d{2}[a-z]?\)", section)
        )
        check(
            f"Advisory depth signals for section {number}",
            word_count >= 700 and citation_count >= 3,
            f"Words: {word_count}; author-year citation markers: {citation_count}. "
            "The PDF specifies critical formal analysis but no numeric section quota.",
            ADVISORY,
        )

    placeholder_matches = re.findall(
        r"(?im)\b(?:TODO|TBC|FIXME|LOREM IPSUM|INSERT (?:CITATION|REFERENCE))\b",
        text,
    )
    check(
        "Formal submission contains no drafting placeholders",
        not placeholder_matches,
        f"Placeholder matches: {len(placeholder_matches)}.",
    )

    required_51 = {
        "centralised lakes/warehouses and their functional limitations": (
            ("centralised", "centralized", "central"),
            ("data lake",),
            ("warehouse",),
            ("bottleneck", "limitation", "limit", "constraint", "downstream effects"),
        ),
        "Data Mesh and Data Contracts": (("data mesh",), ("data contract",)),
        "GDPR/CCPA big-data lifecycle governance": (
            ("gdpr",),
            ("ccpa",),
            ("lifecycle", "ingestion"),
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
        "cryptographic signatures and multidimensional indexing": (
            ("cryptographic signature", "digital signature"),
            ("multidimensional index", "multidimensional indexing"),
        ),
        "LLM code-generation agents": (
            ("large language model", "llm"),
            ("generate sql", "generate code", "code-generation", "proposes code"),
        ),
        "predictive optimisation and self-healing ETL": (
            ("predictive optimisation", "predictive optimization"),
            ("self-healing", "self-repairing"),
            ("etl",),
        ),
        "structural text-to-SQL": (("text-to-sql",),),
        "nondeterministic LLMs with deterministic Spark/Flink": (
            ("nondeterminism", "non-determinism", "nondeterministic"),
            ("spark",),
            ("flink",),
            ("deterministic",),
            ("bottleneck", "constraint", "integration"),
        ),
    }
    for name, groups in required_51.items():
        missing = [" / ".join(group) for group in groups if not has_terms(section_51, group)]
        check(f"5.1 required coverage: {name}", not missing, f"Missing concepts: {missing or 'none'}.")
    for name, groups in required_52.items():
        missing = [" / ".join(group) for group in groups if not has_terms(section_52, group)]
        check(f"5.2 required coverage: {name}", not missing, f"Missing concepts: {missing or 'none'}.")

    reference_entries = split_reference_entries(references)
    doi_references = parse_doi_references(reference_entries)
    unique_dois = sorted({reference.doi for reference in doi_references})
    duplicate_dois = sorted(
        doi for doi in unique_dois if sum(reference.doi == doi for reference in doi_references) > 1
    )
    check(
        "Reference list is populated",
        len(reference_entries) >= 5,
        f"Reference entries: {len(reference_entries)}.",
    )
    check(
        "Advisory DOI uniqueness",
        not duplicate_dois,
        f"Repeated DOI values: {duplicate_dois or 'none'}.",
        ADVISORY,
    )

    doi_results: list[tuple[ReferenceRecord, DoiResult]] = []
    raw_doi_results: dict[str, DoiResult] = {}
    if args.skip_network:
        warnings.append(
            "Network verification was skipped; citation compliance is not established by this run."
        )
        check(
            "Crossref and authoritative-source verification executed",
            False,
            "Run without --skip-network to establish the PDF's citation requirement.",
        )
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            fetched = executor.map(fetch_doi_metadata, unique_dois)
            raw_doi_results = {result.doi: result for result in fetched}
        doi_results = [
            (reference, validate_doi_reference(reference, raw_doi_results[reference.doi]))
            for reference in doi_references
        ]

        conflicting_dois = sorted(
            reference.doi
            for reference, result in doi_results
            if result.status in {INVALID, MISMATCH}
        )
        check(
            "No bibliography DOI conflicts with Crossref identity metadata",
            not conflicting_dois,
            f"Invalid or mismatched DOI entries: {conflicting_dois or 'none'}.",
        )

        recent_scholarly = [
            (reference, result)
            for reference, result in doi_results
            if is_recent_scholarly(result)
        ]
        paired_recent = [
            (reference, result)
            for reference, result in recent_scholarly
            if reference.first_author
            and reference.citation_year
            and has_author_year_citation(
                body,
                reference.first_author,
                reference.citation_year,
            )
        ]
        check(
            "At least five recent scholarly papers are Crossref-verified",
            len({reference.doi for reference, _ in recent_scholarly}) >= 5,
            f"Verified Crossref journal/proceedings records from "
            f"{MINIMUM_RECENT_YEAR}–{ASSIGNMENT_AS_OF_YEAR}: "
            f"{len({reference.doi for reference, _ in recent_scholarly})}.",
        )
        check(
            "At least five verified recent papers have paired author-year citations",
            len({reference.doi for reference, _ in paired_recent}) >= 5,
            f"Paired in-text citations: {len({reference.doi for reference, _ in paired_recent})}.",
        )
        paired_recent_dois = {reference.doi for reference, _ in paired_recent}
        uncited_recent = sorted(
            f"{reference.first_author} ({reference.citation_year})"
            for reference, _result in recent_scholarly
            if reference.doi not in paired_recent_dois
        )
        check(
            "Advisory: every recent DOI reference is cited",
            not uncited_recent,
            f"Recent entries without a paired author-year occurrence: {uncited_recent or 'none'}.",
            ADVISORY,
        )

        unavailable = sorted(
            reference.doi
            for reference, result in doi_results
            if result.status == UNVERIFIABLE
        )
        if unavailable:
            warnings.append(f"Crossref could not verify {len(unavailable)} DOI record(s).")
            if args.strict_network:
                check(
                    "All DOI metadata was network-verifiable",
                    False,
                    f"Network-unverifiable DOI records: {unavailable}.",
                )
            else:
                check(
                    "Advisory network-unverifiable DOI metadata",
                    False,
                    f"Network-unverifiable DOI records: {unavailable}.",
                    ADVISORY,
                )

    urls = sorted({normalise_url(url) for url in URL_PATTERN.findall(references)})
    authoritative_urls = [
        url
        for url in urls
        if (urllib.parse.urlparse(url).hostname or "").casefold() in AUTHORITATIVE_DOMAINS
    ]
    hosts = {
        (urllib.parse.urlparse(url).hostname or "").casefold()
        for url in authoritative_urls
    }
    check(
        "Advisory official-source coverage",
        {
            "atlas.apache.org",
            "csrc.nist.gov",
            "docs.open-metadata.org",
            "eur-lex.europa.eu",
            "leginfo.legislature.ca.gov",
        }.issubset(hosts),
        f"Authoritative bibliography hosts: {sorted(hosts)}.",
        ADVISORY,
    )

    link_results: list[LinkResult] = []
    if not args.skip_network:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            link_results = sorted(
                executor.map(fetch_link, authoritative_urls),
                key=lambda item: item.url,
            )

        broken_links = [result.url for result in link_results if result.status == BROKEN]
        check(
            "Advisory authoritative bibliography URL health",
            not broken_links,
            f"HTTP 404/410 URLs: {broken_links or 'none'}.",
            ADVISORY,
        )

        unresolved_links: list[str] = []
        for result in link_results:
            if result.status not in {BLOCKED, UNVERIFIABLE}:
                continue
            doi = doi_from_url(result.url)
            doi_result = raw_doi_results.get(doi) if doi else None
            if doi_result is not None and doi_result.status == VERIFIED:
                continue
            unresolved_links.append(result.url)

        if unresolved_links:
            warnings.append(
                f"{len(unresolved_links)} authoritative non-DOI or independently-unverified "
                "endpoint(s) could not be fully checked."
            )
        check(
            "Advisory independent verification for blocked/unverifiable links",
            not unresolved_links,
            f"Unresolved authoritative URLs: {unresolved_links or 'none'}.",
            ADVISORY,
        )

    overall, exit_code = evaluation_status(checks, incomplete=args.skip_network)
    mandatory_checks = [check for check in checks if check.level == MANDATORY]
    advisory_checks = [check for check in checks if check.level == ADVISORY]
    mandatory_passed = sum(check.passed for check in mandatory_checks)
    advisory_passed = sum(check.passed for check in advisory_checks)
    network_mode = (
        "offline/incomplete"
        if args.skip_network
        else "strict"
        if args.strict_network
        else "network-enabled"
    )

    lines = [
        "# Task 5 CI validation report",
        "",
        f"Overall: **{overall}**",
        "",
        f"Paper: `{args.paper}`",
        f"Network mode: **{network_mode}**",
        f"Assignment as-of year: {ASSIGNMENT_AS_OF_YEAR}",
        f"Defined recent-publication window: {MINIMUM_RECENT_YEAR}–{ASSIGNMENT_AS_OF_YEAR} "
        f"({RECENT_WINDOW_YEARS} inclusive calendar years)",
        f"Bibliography DOI entries discovered: {len(doi_references)} "
        f"({len(unique_dois)} distinct)",
        "",
        "## PDF-mandated checks",
        "",
        f"Passed: {mandatory_passed}/{len(mandatory_checks)}",
        "",
        "| Check | Status | Evidence |",
        "|---|---:|---|",
    ]
    for result in mandatory_checks:
        detail = result.detail.replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {result.name} | {'PASS' if result.passed else 'FAIL'} | {detail} |")

    lines.extend(
        [
            "",
            "## Advisory quality signals",
            "",
            "These checks make useful review signals visible but do not add requirements to the PDF.",
            "",
            f"Passed: {advisory_passed}/{len(advisory_checks)}",
            "",
            "| Check | Status | Evidence |",
            "|---|---:|---|",
        ]
    )
    for result in advisory_checks:
        detail = result.detail.replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {result.name} | {'PASS' if result.passed else 'WARN'} | {detail} |")

    lines.extend(["", "## Crossref DOI identity checks", ""])
    if doi_results:
        lines.extend(
            [
                "| DOI | Status | Cited author/year | Crossref years/type | Crossref title | Result |",
                "|---|---:|---|---|---|---|",
            ]
        )
        for reference, result in sorted(doi_results, key=lambda item: item[0].doi):
            detail = result.detail.replace("|", "\\|").replace("\n", " ")
            title = (result.title or "").replace("|", "\\|")
            cited = f"{reference.first_author or ''} ({reference.citation_year or ''})"
            metadata = f"{','.join(map(str, result.publication_years))} / {result.work_type or ''}"
            lines.append(
                f"| `{reference.doi}` | {result.status} | {cited} | {metadata} | "
                f"{title} | {detail} |"
            )
    else:
        lines.append("DOI metadata checks were not run.")

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
            "Crossref journal-article and proceedings-article records are the automated",
            "scholarly-publication classification used for the five-paper minimum. Each counted",
            "record must also match the cited DOI, title, first author, and publication year and",
            "occur as a paired author-year citation in the review. Link status BLOCKED is never",
            "reported as VERIFIED; a blocked DOI landing page is tolerated only when the same",
            "DOI has independently verified Crossref metadata.",
            "",
        ]
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines), encoding="utf-8")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
