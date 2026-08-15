# Aero Arc Conformance

Aero Arc Conformance is the continuous flight-monitoring data plane. It joins
immutable mission assignments with normalized UAV telemetry, evaluates lateral,
vertical, temporal, and telemetry-availability rules, publishes current state,
and records replayable incident evidence.

> [!IMPORTANT]
> This repository begins as a bounded prototype. The evaluator, fenced lease
> store, and forward-contract InfluxDB reader are real and tested. The binary
> now serves assignment lifecycle gRPC and delivers committed live projections
> to Registry through a leased PostgreSQL outbox. It does not yet run the
> telemetry claim/poll/evaluate loop.

![Aero Arc Conformance data flow from mission assignment and telemetry through evaluation into live and durable state](docs/images/conformance-data-flow.svg)

## Why a separate service?

The API owns request/response mission workflows. Conformance owns an always-on
workload: ordered telemetry evaluation, timers, hysteresis, assignment leases,
incident state, replay, and independent scaling. A Conformance outage must not
interrupt Relay telemetry acknowledgement or storage.

## Prototype contents

- Pure deterministic evaluator with separate incident state per violation type.
- Numbered PostgreSQL migrations for assignments, inbox, fenced leases,
  checkpoints, incidents, immutable transition events, summaries, and outbox.
- Atomic assignment application and atomic fenced evaluation commits.
- Idempotent assignment prepare/cancel/cutover gRPC plus exact-generation reads.
- Independently leased Registry outbox publication with exact evaluation acknowledgement.
- Blue-green assignment preparation: current and armed generations coexist,
  then an explicit event-time cutover atomically transfers authority.
- Forward-contract InfluxDB 3 reader with bounded aircraft batches, time
  windows, stable-frame deduplication, strict decoding, and overflow rejection.
- Liveness, PostgreSQL-aware readiness, baseline Prometheus process metrics,
  structured
  logging, strict YAML configuration, and bounded shutdown.
- Real Postgres and InfluxDB integration coverage through Testcontainers.

The reader deliberately fails with `ErrWALIdentityUnavailable` against today’s
merged Relay schema, because that schema does not yet carry `wal_id`. This is a
deployment gate, not a legacy mode that can quietly weaken replay correctness.

## Quick start

Requirements: Go 1.24 or newer and Docker for integration tests.

```bash
cp configs/config.yaml.example configs/config.yaml
go test ./...
go test -race ./...
go test -tags=integration -timeout=10m ./internal/integration
go run ./cmd/aero-arc-conformance --config-path configs/config.yaml
```

`go run` accepts assignment lifecycle commands and publishes any committed
Registry outbox rows. It will not evaluate telemetry until the worker runtime
slice lands and Relay supplies the required `wal_id` contract.

Assignment gRPC defaults to `:50052`. Management endpoints default to:

- `GET /healthz` — process liveness.
- `GET /readyz` — initialized process with reachable PostgreSQL.
- `GET /metrics` — Prometheus metrics.

## Read next

- [Conformance design](docs/conformance-design.md)
- [Blue-green assignment cutover](docs/assignment-cutover.md)
- [Development and validation](docs/development.md)
- [Configuration](docs/configuration.md)
- [Deployment](docs/deployment.md)
- [Agent guidance](AGENTS.md)

## License

Mozilla Public License 2.0. See [LICENSE](LICENSE).
