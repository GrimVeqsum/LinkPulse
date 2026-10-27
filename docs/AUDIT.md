# LinkPulse engineering audit

Date: 2026-10-27. Baseline: `298a146` (MVP). This initial audit and repair plan was recorded before implementation changes. The complete source tree, generated protocol, module dependencies, migrations and container configuration were inspected.

## Findings at baseline

| Area | Finding | Planned correction |
| --- | --- | --- |
| Redirect delivery (critical) | One detached goroutine per redirect; events disappear on RPC failure or process exit; concurrency is unbounded. | Persist immutable events in a PostgreSQL outbox before returning 302; one bounded worker with leases, deadlines and retry backoff. Return 503 without redirect if persistence fails. |
| Analytics correctness (critical) | No event identity or deduplication; retrying inserts would inflate counts. | Add UUID event identity to protobuf; store new events separately from unchanged legacy data; use exact distinct event counts at query time, independently of background merges. |
| UTC | `today_clicks` uses server-local start of day and has no upper bound. | Explicit half-open UTC calendar-day interval, including midnight tests. |
| HTTP validation | Unbounded JSON; accepts unknown fields/trailing JSON; weak URL checks (credentials, empty hostname and invalid ports); no code validation. | Bound body and URL size; reject malformed/ambiguous input; preserve the four existing endpoints and their success payloads. |
| Timeouts | HTTP server has no header/read/write/idle limits; database and startup calls can wait indefinitely. | Bounded request, database, startup and RPC contexts plus configured HTTP timeouts and header limit. |
| gRPC | Timestamp/code validation is incomplete; backend failures all become Internal; transport is plaintext. | Validate immutable event payload and timestamp; map cancellation/deadlines and dependency failures; standard health service. Keep plaintext internal to Compose and document TLS boundary. |
| Lifecycle | No signal handling or graceful HTTP/gRPC drain; `log.Fatal` bypasses deferred cleanup. | Signal-driven shutdown, drain HTTP, cancel/join worker, bounded gRPC stop, then close clients/pools. |
| Readiness | `/health` is unconditional; dependencies are not checked at runtime; `grpc.NewClient` is lazy. | Keep liveness; add PostgreSQL-based redirect readiness and analytics dependency health. Analytics outages must still permit queued redirects. |
| Migrations | Goose package-global filesystem/dialect state can race if both migrators run in one process. | Instance-based providers; preserve every existing SQL migration and add new ones. |
| Configuration/security | Demo credentials embedded in Compose; all database/gRPC ports publicly exposed; no ignore files; tracked Windows executables. | Demo `.env.example`, private internal ports, nonroot runtime, ignore artifacts and secrets, remove tracked binaries without rewriting history. |
| Containers | Runtime runs as root, no app health checks or graceful-stop settings; broad build context. | Multi-stage reproducible module build, nonroot user, minimal context, dependency health checks and stop grace period. |
| Tests/CI/docs | No tests or CI; README is UTF-16 and contains only a title. | Meaningful unit/race tests, isolated real-database integration Compose, separate CI integration job, complete technical English README. |

## Implementation sequence

1. Establish shared immutable event contract and additive database migrations.
2. Implement durable outbox with atomic leased claims, ownership-checked acknowledgement and capped retry backoff. Persist before redirect. No in-memory queue or per-click goroutine.
3. Implement gRPC event identity, exact ClickHouse read-time deduplication, UTC-day counting and dependency health.
4. Harden handlers, service validation, deadlines, configuration and process shutdown.
5. Add isolated integration tests, unit tests, container/CI improvements and operational documentation.
6. Run formatting, vet, unit/race tests, builds, Compose validation, image builds and isolated end-to-end/outage-recovery checks where tools permit; record actual results and limitations.

## Chosen guarantees and compatibility

- A successful redirect requires a committed outbox event. Failed persistence returns HTTP 503 and no Location header. A committed event can exist even if the client disconnects before receiving the response; this measures accepted redirect requests, not completed browser navigation.
- Delivery is **at least once**, conditional on eventual recovery, valid events and available storage. A crash after ClickHouse commit but before acknowledgement can cause redelivery.
- New-event statistics count distinct immutable UUIDs at query time, so duplicate physical rows do not inflate API counts. This is not an exactly-once transport guarantee. Raw `count()` on new rows is not a supported click count.
- The original `click_events` data is retained and counted as legacy history; historical rows lack identities and cannot be retroactively deduplicated reliably. New writes use `click_events_v2`.
- `today_clicks` means `[00:00 UTC, next 00:00 UTC)` by original event time, including delayed delivery. Statistics are eventually consistent with the outbox.
- Existing HTTP endpoint paths, 201 creation payload, 302 redirect and stats payload remain. Invalid requests are rejected more strictly. `/ready` is additive.

## Initial environment observations

- Go 1.26.8 and Docker Compose v5.6.1 are installed.
- Initial Docker daemon access from the sandbox reports permission denied; elevated verification will be attempted. No claim of container validation is made at this stage.
- `protoc` is absent; protobuf generation needs temporary tooling, without adding runtime dependencies for code generation.

## Verification results

Completed on 2026-10-27 using Go 1.26.8, Docker Engine 29.9.2 and Compose v5.6.1. Docker required elevated execution outside the initial sandbox; it was available and the following container checks actually ran. Go caches and downloaded protobuf tooling were placed under `/tmp`, avoiding writes to the user's read-only module cache.

| Check | Actual result |
| --- | --- |
| `gofmt -l` over every Go file, including new and generated files | Clean. |
| `go vet ./...` | Passed. |
| `go test ./...` | Passed. |
| `go test -race ./...` | Passed, no reported races. |
| `go build ./...` | Passed. |
| `go mod tidy` / `go mod verify` | Tidy succeeded; all modules verified. UUID was already in the module graph and is now a direct dependency. |
| `git diff --check` | Passed. Both original `001` migrations have no diff. |
| Both Compose files: `config --quiet` | Passed. |
| Main Compose: `docker compose -p linkpulse-codex-images -f docker-compose.yml build --quiet` | Both application images built successfully. No main-stack database was started or altered. |
| Test Compose: `up --build --wait postgres clickhouse analytics-service link-service` | All four real services became healthy in isolated project `linkpulse-codex-check`; databases used tmpfs. |
| `docker compose -p linkpulse-codex-check -f docker-compose.test.yml run --build --rm tests` | Final run: all six real-database integration tests passed (`1.852s` test execution, excluding build/compile). |
| `python3 scripts/smoke.py --project linkpulse-codex-check --port 18080` | Passed on final application images: one initial click, eight durable clicks while actual Analytics container was stopped, nine clicks after restart, zero pending events. HTTP health/readiness stayed 200 and stats returned 503 during the outage. |
| Runtime identity | Both applications reported `uid=10001(app) gid=10001(app)`. |
| Actual SIGTERM shutdown through Compose | Both applications exited with code 0 and `OOMKilled=false`. Worker cancellation is also exercised in unit tests. |
| README encoding/structure | Valid UTF-8, no NUL characters, all 13 requested sections. |

The integration suite verifies:

1. HTTP link creation, redirect, committed outbox row before worker startup, statistics, actual gRPC unavailability, persisted retry state and recovery.
2. Simulated death after remote ClickHouse commit but before outbox acknowledgement: the same immutable event is physically inserted twice and counted once; stale lease acknowledgement is rejected.
3. Twenty-four concurrent HTTP redirects and two concurrent PostgreSQL claimers with no overlapping claimed event IDs.
4. UTC midnight minus 1 ms, exactly midnight, and next midnight, with duplicate events and retained legacy history. The ClickHouse session is explicitly `Asia/Tokyo`; persisted epoch milliseconds are checked directly.
5. A real closed PostgreSQL outbox pool makes the redirect return 503 with no Location and no queued event.
6. Full deployed binaries: health/readiness, HTTP creation, 302 and eventual statistics through the actual process wiring.

Both migrations are also reapplied in each fixture to verify restart with existing schema/history. Integration fixtures create and clean only their generated schemas/databases inside the dedicated tmpfs test stack. No external destination is fetched. GitHub-hosted CI itself has not been triggered; its individual checks were executed locally.

## Additional defects found during implementation

- The first real integration run failed when ClickHouse used a non-UTC session timezone: the Go driver's positional `time.Time` binding could emit a timezone-less SQL literal and default to second precision. Merely expressing the day boundary in UTC did not fix the stored instant. New inserts now bind integer Unix milliseconds and explicitly call `fromUnixTimestamp64Milli(..., 'UTC')`. The regression verifies exact stored milliseconds as well as counts. The failed run is not counted as successful verification; the corrected suite was rebuilt and passed.
- The original generator could produce reserved code `health`, making a newly created link unreachable. Generation now skips that code within the existing bounded attempt budget.
- Invalid UTF-8 metadata could become a permanently undeliverable protobuf event, and Go JSON decoding could silently replace malformed URL bytes. Validation now rejects both before persistence.
- CPU-based default PostgreSQL pool sizing could exceed the database connection budget on large hosts. Link Service now caps the pool at 16; Analytics caps ClickHouse connections at 10.
- Unique violations unrelated to the code constraint should not trigger new-code retries. The PostgreSQL adapter now maps only `links_code_key` violations to `ErrCodeAlreadyExists`.

## Remaining limitations and operational risks

- Delivery is at least once and statistics are eventually consistent. Duplicate raw v2 rows remain on disk; exact query aggregation consumes memory proportional to unique IDs.
- Legacy rows have no event ID and retain their original counting semantics. Historical duplicate detection cannot be guaranteed.
- Permanent RPC errors have no dead-letter path or retry limit. Backlog growth, age, errors and database capacity need operational monitoring; no metrics/alerting service was added.
- Analytics bootstrap migrations require a single active runner. PostgreSQL migration locking is implemented; ClickHouse cross-process DDL/version locking is not.
- The deployment is single-node and has no replication, backups, public-user authentication, rate limiting or TLS termination. Database storage loss remains outside delivery guarantees. These features are explicitly absent from the README rather than represented as implemented.
- Event identity assumes trusted internal producers preserve the immutable payload. Reusing the same UUID with a different link/time is unsupported.
- Initial checked-in Windows executables are deleted from the working tree and ignored going forward. No Git history rewrite, commit, or push was performed. Existing user data and Docker volumes were not deleted.

## Changed files

- `.dockerignore`
- `.env.example`
- `.github/workflows/ci.yml`
- `.gitignore`
- `Dockerfile`
- `README.md`
- `analytics-service.exe` (deleted binary)
- `cmd/analytics-service/main.go`
- `cmd/analytics-service/main_test.go`
- `cmd/link-service/main.go`
- `docker-compose.test.yml`
- `docker-compose.yml`
- `docs/AUDIT.md`
- `gen/analytics/v1/analytics.pb.go`
- `gen/analytics/v1/analytics_grpc.pb.go`
- `go.mod`
- `internal/analytics/client/client.go`
- `internal/analytics/client/client_test.go`
- `internal/analytics/grpc/server.go`
- `internal/analytics/grpc/server_test.go`
- `internal/analytics/repository/clickhouse.go`
- `internal/config/config.go`
- `internal/config/config_test.go`
- `internal/event/event.go`
- `internal/link/handler/http.go`
- `internal/link/handler/http_test.go`
- `internal/link/repository/postgres.go`
- `internal/link/service/service.go`
- `internal/link/service/service_test.go`
- `internal/outbox/worker.go`
- `internal/outbox/worker_test.go`
- `link-service.exe` (deleted binary)
- `migrations/clickhouse/002_create_click_events_v2.sql`
- `migrations/migrations.go`
- `migrations/postgres/002_create_click_outbox.sql`
- `proto/analytics.proto`
- `scripts/integration.sh`
- `scripts/smoke.py`
- `tests/integration/pipeline_test.go`
