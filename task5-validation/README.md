# Task 5 containerized validation

Commit the Task 5 paper and validation files before generating evidence. The
following command must print no changes:

```sh
git status --short -- \
  task5-literature-review.md \
  .github/scripts/validate_task5_literature.py \
  .github/scripts/write_evidence_manifest.py \
  task5-validation
```

Then run the literature, Crossref-identity, paired-citation, and authoritative-
source validation without a native Python dependency. `--build` is required so
the image cannot silently reuse an older copy of the paper or validator:

```sh
COURSEWORK_COMMIT_SHA="$(git rev-parse HEAD)" \
  docker compose --file task5-validation/docker-compose.yml \
  run --rm --build literature-validation
```

The container writes `ci-evidence/task5/validation-report.md` and a SHA-256
evidence manifest bound to the supplied full Git commit. The manifest hashes the
paper, validator, manifest helper, container definitions, entrypoint, and test
suite as well as the report. It is produced with `PASS` or `FAIL` status even
when validation fails.

Network verification is strict. Every bibliography DOI must match Crossref's
title, first-author surname, and publication year. At least five matched
journal/proceedings papers from the fixed 2022–2026 assignment window must also
have paired author-year citations in the review. Link results distinguish
`VERIFIED`, `BLOCKED`, `UNVERIFIABLE`, and `BROKEN`; a blocked DOI landing page
is accepted only when Crossref independently verifies that DOI.

Run the mocked unit tests in their container with:

```sh
docker compose --file task5-validation/docker-compose.yml \
  run --rm --build literature-tests
```
