#!/usr/bin/env sh
set -eu

: "${COURSEWORK_COMMIT_SHA:?COURSEWORK_COMMIT_SHA is required}"

mkdir -p /evidence
rm -f /evidence/validation-report.md /evidence/evidence-manifest.json

validation_exit=0
python3 /opt/coursework/validate_task5_literature.py \
  --paper /coursework/task5-literature-review.md \
  --report /evidence/validation-report.md \
  --strict-network || validation_exit=$?

validation_status=PASS
if [ "${validation_exit}" -ne 0 ]; then
  validation_status=FAIL
fi

python3 /opt/coursework/write_evidence_manifest.py \
  --task task-5-literature \
  --commit-sha "${COURSEWORK_COMMIT_SHA}" \
  --evidence-dir /evidence \
  --output /evidence/evidence-manifest.json \
  --status "${validation_status}" \
  --source-file task5-literature-review.md=/coursework/task5-literature-review.md \
  --source-file .github/scripts/validate_task5_literature.py=/opt/coursework/validate_task5_literature.py \
  --source-file .github/scripts/write_evidence_manifest.py=/opt/coursework/write_evidence_manifest.py \
  --source-file task5-validation/Dockerfile=/opt/coursework/source/task5-validation/Dockerfile \
  --source-file task5-validation/docker-compose.yml=/opt/coursework/source/task5-validation/docker-compose.yml \
  --source-file task5-validation/run-validation.sh=/opt/coursework/run-validation.sh \
  --source-file task5-validation/tests/test_validate_task5_literature.py=/opt/coursework/tests/test_validate_task5_literature.py

exit "${validation_exit}"
