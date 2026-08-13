# AGENTS.md

## Purpose

This repository owns continuous flight conformance evaluation. Preserve these
boundaries unless an accepted design change explicitly moves them:

- API owns mission lifecycle, DSS coordination, and assignment generation.
- Agent and Relay own telemetry capture, delivery, attribution, and storage.
- InfluxDB is replayable telemetry evidence, not assignment authority.
- Conformance owns evaluation, hysteresis, checkpoints, and incident lifecycle.
- PostgreSQL is Conformance's durable authority.
- Registry may hold an expiring live projection; it is not incident history.
- Ops consumes API-composed state rather than reaching into service stores.

## Invariants

- Assignment generation, worker lease generation, and evaluation revision are
  different fences. Do not collapse them.
- A received or armed candidate never authorizes telemetry and is never
  claimable. Only an explicit cutover can activate it.
- At an assignment cutover, old authority is `[old_from, cutover)` and new
  authority is `[cutover, new_until)`. Never union candidate and current
  geometry, and resolve delayed observations by event time.
- Preparing or cancelling a candidate must not change the current generation;
  cutover must atomically fence every lease held by the superseded generation.
- Historical reconciliation must be fenced by the current generation's lease,
  remain scoped to the superseded authority interval, and never publish a live
  Registry projection or revive a superseded lease.
- Reject a cutover at or before the current generation's committed evaluation
  watermark; never delete or reassign evidence whose outbox may be delivered.
- Incident transitions own their WAL cursor and stable opening-frame occurrence
  identity. Immutable event payloads must not contain mutable batch-final state.
- Claim and renewal use PostgreSQL time. An expired lease must never be revived.
- Summary, incidents, transition events, checkpoint, and delivery outbox commit
  atomically behind the current assignment lease.
- Events use deterministic IDs and immutable insert semantics.
- Telemetry windows are bounded. A saturated query is split or rejected; it is
  never silently treated as complete.
- Stable `frame_id` provides idempotency. A future `wal_id` plus WAL sequence
  provides ordering within one Agent WAL generation.
- Sequence gaps are normal because the Agent WAL contains all MAVLink messages.
- A late observation may amend history but must not roll current live state back.
- Unknown or incompatible altitude reference is not a vertical violation.
- Relay acknowledgement must never depend synchronously on Conformance.
- Ops must distinguish condition, monitoring freshness, and recording status.

## Current prototype limitations

- Merged telemetry does not yet contain `wal_id`.
- Relay currently acknowledges queue admission before durable InfluxDB flush;
  audit-grade conformance requires that acknowledged-loss gap to be closed.
- The runtime claim/poll loop and external Protobuf contracts are the next slice.
- The reader is forward-contract-only and must fail loudly until Agent and Relay
  provide `wal_id`; do not add a silent sequence-only fallback.
- Reader overlap currently deduplicates within a returned window. Persistent
  overlap replay semantics must be finalized before production checkpointing.
- The real integration proves checkpoint restoration and fenced suffix takeover,
  but does not yet prove delayed Influx visibility or same-timestamp pagination.

## Validation

Run all relevant checks before handoff:

```bash
go test ./...
go test -race ./...
go vet ./...
go test -tags=integration -timeout=10m ./internal/integration
git diff --check
```

Integration tests must perform real database operations. Container readiness
alone is not meaningful coverage.

## Repository conventions

- Use numbered SQL migrations; do not mutate an already released migration.
- Keep implementation packages under `internal/` until another repository has
  a demonstrated Go API need. Cross-service contracts belong in Protobuf.
- Add MPL 2.0 headers to Go source files.
- Update design/configuration docs whenever a contract or invariant changes.
- Commits must include a matching `Signed-off-by` line for DCO.
