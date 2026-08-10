#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
task_directory="${repository_root}/task4-neo4j"
evidence_directory="${TASK4_EVIDENCE_DIR:-${repository_root}/ci-evidence/task4}"
compose_file="${task_directory}/docker-compose.yml"
chrome_profile=""

mkdir -p "${evidence_directory}"
cp "${task_directory}/cypher/import_patents.cypher" "${evidence_directory}/import_patents.cypher"
cp "${task_directory}/cypher/analysis_queries.cypher" "${evidence_directory}/analysis_queries.cypher"
cp "${task_directory}/scripts/prepare_patents.py" "${evidence_directory}/prepare_patents.py"

compose() {
  docker compose --file "${compose_file}" --project-directory "${task_directory}" "$@"
}

capture_compose_diagnostics() {
  compose ps --all >"${evidence_directory}/compose-status.txt" 2>&1 || true
  compose logs --no-color --timestamps >"${evidence_directory}/compose-logs.txt" 2>&1 || true
  docker system df >"${evidence_directory}/docker-disk-usage.txt" 2>&1 || true
}

cleanup() {
  exit_code=$?
  trap - EXIT
  set +e
  capture_compose_diagnostics
  compose down --volumes --remove-orphans \
    >>"${evidence_directory}/cleanup.log" 2>&1
  if [[ -n "${chrome_profile}" && -d "${chrome_profile}" ]]; then
    rm -rf -- "${chrome_profile}"
  fi
  exit "${exit_code}"
}
trap cleanup EXIT

if [[ ! -f "${compose_file}" ]]; then
  echo "Missing Task 4 Compose file: ${compose_file}" >&2
  exit 1
fi

docker version >"${evidence_directory}/docker-version.txt"
docker compose version >>"${evidence_directory}/docker-version.txt"
compose config --quiet

profile_count="$(grep -Ec '^[[:space:]]*PROFILE[[:space:]]*$' "${task_directory}/cypher/analysis_queries.cypher")"
if [[ "${profile_count}" != "3" ]]; then
  echo "Expected exactly three PROFILE statements; found ${profile_count}." >&2
  exit 1
fi

if ! grep -Fq 'https://snap.stanford.edu/data/cit-Patents.txt.gz' \
  "${task_directory}/scripts/prepare_patents.py"; then
  echo "Task 4 is not configured to stream the official SNAP patent citation data." >&2
  exit 1
fi

df -h "${repository_root}" >"${evidence_directory}/disk-before.txt"
compose pull --quiet >"${evidence_directory}/compose-pull.txt" 2>&1
compose up --detach >"${evidence_directory}/compose-up.txt" 2>&1

deadline=$((SECONDS + 15 * 60))
analysis_container="$(compose ps --all --quiet analyze-patents)"
if [[ -z "${analysis_container}" ]]; then
  echo "Compose did not create the analyze-patents container." >&2
  exit 1
fi

while true; do
  for service in prepare-patents import-patents analyze-patents; do
    container_id="$(compose ps --all --quiet "${service}")"
    if [[ -z "${container_id}" ]]; then
      continue
    fi
    state="$(docker inspect --format '{{.State.Status}}' "${container_id}")"
    if [[ "${state}" == "exited" ]]; then
      service_exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${container_id}")"
      if [[ "${service_exit_code}" != "0" ]]; then
        echo "${service} exited with code ${service_exit_code}." >&2
        exit 1
      fi
    elif [[ "${state}" == "dead" ]]; then
      echo "${service} entered the dead container state." >&2
      exit 1
    fi
  done

  analysis_state="$(docker inspect --format '{{.State.Status}}' "${analysis_container}")"
  if [[ "${analysis_state}" == "exited" ]]; then
    analysis_exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${analysis_container}")"
    if [[ "${analysis_exit_code}" != "0" ]]; then
      echo "analyze-patents exited with code ${analysis_exit_code}." >&2
      exit 1
    fi
    break
  fi
  if (( SECONDS >= deadline )); then
    echo "Timed out after 15 minutes waiting for Task 4 analysis." >&2
    exit 1
  fi
  sleep 5
done

compose exec --no-TTY neo4j cypher-shell \
  --address bolt://localhost:7687 \
  --username neo4j \
  --password coursework2026 \
  --format plain \
  'MATCH ()-[citation:CITES]->() RETURN count(citation) AS directed_cites;' \
  >"${evidence_directory}/relationship-count.txt"

relationship_count="$(tail -n 1 "${evidence_directory}/relationship-count.txt" | tr -d '[:space:]\r\"')"
if [[ "${relationship_count}" != "5000" ]]; then
  echo "Expected exactly 5,000 directed CITES relationships; found ${relationship_count}." >&2
  exit 1
fi

compose exec --no-TTY neo4j cypher-shell \
  --address bolt://localhost:7687 \
  --username neo4j \
  --password coursework2026 \
  --format plain \
  'MATCH ()-[relationship]->() RETURN count(relationship) AS total_relationships, collect(DISTINCT type(relationship)) AS relationship_types;' \
  >"${evidence_directory}/relationship-types.txt"

cp "${task_directory}/import/patent_edges_5000.csv" \
  "${evidence_directory}/patent_edges_5000.csv"
cp "${task_directory}/results/query-execution.txt" \
  "${evidence_directory}/query-execution.txt"

browser_url="http://127.0.0.1:7474/browser/"
curl --fail --silent --show-error --location \
  --retry 5 --retry-all-errors --retry-delay 2 \
  --connect-timeout 5 --max-time 30 \
  --dump-header "${evidence_directory}/neo4j-browser-headers.txt" \
  --output "${evidence_directory}/neo4j-browser-http.html" \
  "${browser_url}"

chrome_binary=""
for candidate in google-chrome google-chrome-stable chromium chromium-browser; do
  if command -v "${candidate}" >/dev/null 2>&1; then
    chrome_binary="$(command -v "${candidate}")"
    break
  fi
done
if [[ -z "${chrome_binary}" ]]; then
  echo "No Chromium-family browser is installed on the runner." >&2
  exit 1
fi

chrome_profile="$(mktemp -d)"
timeout --signal=TERM --kill-after=5s 45s "${chrome_binary}" \
  --headless=new \
  --no-sandbox \
  --disable-dev-shm-usage \
  --disable-gpu \
  --hide-scrollbars \
  --window-size=1440,1200 \
  --virtual-time-budget=10000 \
  --user-data-dir="${chrome_profile}" \
  --dump-dom \
  "${browser_url}" \
  >"${evidence_directory}/neo4j-browser-dom.html" \
  2>"${evidence_directory}/neo4j-browser-console.log"

if ! grep -Eiq 'neo4j|neo4j browser' "${evidence_directory}/neo4j-browser-dom.html"; then
  echo "The browser-rendered page did not identify itself as Neo4j." >&2
  exit 1
fi

timeout --signal=TERM --kill-after=5s 45s "${chrome_binary}" \
  --headless=new \
  --no-sandbox \
  --disable-dev-shm-usage \
  --disable-gpu \
  --hide-scrollbars \
  --window-size=1440,1200 \
  --virtual-time-budget=10000 \
  --user-data-dir="${chrome_profile}" \
  --screenshot="${evidence_directory}/neo4j-browser.png" \
  "${browser_url}" \
  >>"${evidence_directory}/neo4j-browser-console.log" 2>&1

python3 "${repository_root}/.github/scripts/validate_task4_evidence.py" \
  --task-directory "${task_directory}" \
  --evidence-directory "${evidence_directory}" \
  --report "${evidence_directory}/validation-report.md"

capture_compose_diagnostics
df -h "${repository_root}" >"${evidence_directory}/disk-after.txt"
echo "Task 4 runtime, query-plan, relationship, and browser evidence validated."
