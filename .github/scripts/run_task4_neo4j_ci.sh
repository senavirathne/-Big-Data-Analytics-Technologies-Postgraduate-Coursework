#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
task_directory="${repository_root}/task4-neo4j"
compose_file="${task_directory}/docker-compose.yml"

compose() {
  docker compose --file "${compose_file}" --project-directory "${task_directory}" "$@"
}

cleanup() {
  exit_code=$?
  trap - EXIT
  set +e
  compose ps --all
  compose logs --no-color --timestamps
  compose down --remove-orphans
  exit "${exit_code}"
}
trap cleanup EXIT

# Every executable dependency after this point runs in a Compose service. The
# host script only sequences Docker Compose and propagates service failures.
compose config --quiet
compose pull --quiet --ignore-buildable
compose build --pull browser-evidence
compose run --rm --no-deps stage-evidence
compose up --detach --wait neo4j
compose run --rm --no-deps import-patents
compose run --rm --no-deps analyze-patents
compose run --rm --no-deps browser-evidence
compose run --rm --no-deps validate-evidence

echo "Task 4 runtime, PROFILE, scoped-import, browser, and commit-bound evidence validated."
