# Development

The prototype targets Go 1.24 and follows the single-binary conventions used by
Aero Arc Relay while improving strict configuration, lifecycle ownership,
management server timeouts, and race coverage. The binary uses `urfave/cli/v3`
for its command and flag contract.

## Checks

```bash
make build
make test
make test-race
make vet
make integration
```

Integration tests start pinned Postgres and InfluxDB containers on dynamically
mapped ports. They apply production migrations, write real normalized telemetry,
claim and reclaim real assignment rows, evaluate observations, and verify stale
worker fencing. Docker availability is the only reason they may skip locally.

## Package boundaries

- `internal/domain`: stable in-process types.
- `internal/evaluator`: pure deterministic evaluator.
- `internal/telemetry/influx`: bounded observation reader.
- `internal/transport/grpc`: assignment lifecycle transport adapter.
- `internal/projection/registry`: leased durable Registry publication.
- `internal/store/postgres`: migrations and atomic persistence operations.
- `internal/conformance`: Conformance service lifecycle and management endpoints.
- `internal/integration`: multi-dependency behavior tests.
- `internal/testsupport`: integration-only container ownership.

Do not add a public `pkg/` tree before a concrete external Go consumer exists.
Cross-service contracts belong in `aero-arc-protos` after the prototype validates
their invariants.
