#!/bin/sh
set -eu

# Uses only the dedicated tmpfs test stack, never developer database volumes.
cd "$(dirname "$0")/.."
test_project="${TEST_PROJECT:-linkpulse-integration}"

cleanup() {
    docker compose -p "$test_project" -f docker-compose.test.yml down --remove-orphans
}
trap cleanup EXIT INT TERM

docker compose -p "$test_project" -f docker-compose.test.yml up --build --wait postgres clickhouse analytics-service link-service
docker compose -p "$test_project" -f docker-compose.test.yml run --build --rm tests
python3 scripts/smoke.py --project "$test_project" --port "${TEST_HTTP_PORT:-18080}"
