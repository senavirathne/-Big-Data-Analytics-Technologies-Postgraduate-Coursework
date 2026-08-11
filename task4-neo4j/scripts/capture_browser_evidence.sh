#!/usr/bin/env sh
set -eu

browser_url="${NEO4J_BROWSER_URL:-http://neo4j:7474/browser/}"
browser_binary="$(command -v chromium || command -v chromium-browser)"
profile_directory="/tmp/neo4j-browser-profile"

mkdir -p /evidence "${profile_directory}"

curl --fail --silent --show-error --location \
  --retry 10 --retry-all-errors --retry-delay 2 \
  --connect-timeout 5 --max-time 60 \
  --dump-header /evidence/neo4j-browser-headers.txt \
  --output /evidence/neo4j-browser-http.html \
  "${browser_url}"

"${browser_binary}" \
  --headless \
  --no-sandbox \
  --disable-dev-shm-usage \
  --disable-gpu \
  --hide-scrollbars \
  --window-size=1440,1200 \
  --virtual-time-budget=10000 \
  --user-data-dir="${profile_directory}" \
  --dump-dom \
  "${browser_url}" \
  > /evidence/neo4j-browser-dom.html \
  2> /evidence/neo4j-browser-console.log

"${browser_binary}" \
  --headless \
  --no-sandbox \
  --disable-dev-shm-usage \
  --disable-gpu \
  --hide-scrollbars \
  --window-size=1440,1200 \
  --virtual-time-budget=10000 \
  --user-data-dir="${profile_directory}" \
  --screenshot=/evidence/neo4j-browser.png \
  "${browser_url}" \
  >> /evidence/neo4j-browser-console.log 2>&1

test -s /evidence/neo4j-browser-http.html
test -s /evidence/neo4j-browser-dom.html
test -s /evidence/neo4j-browser.png
grep -Eiq 'neo4j' /evidence/neo4j-browser-dom.html

echo "Neo4j Browser HTTP response, rendered DOM, console log, and screenshot captured in-container"
