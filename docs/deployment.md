# Deployment

The prototype initializes numbered migrations and clients, serves management
HTTP and assignment lifecycle gRPC, runs the live telemetry claim/evaluate
worker, and drains committed Registry projection outbox messages.
The tested store is designed for multiple replicas: advisory-locked migrations,
revision-fenced commits, and assignment leases prevent competing commits.

Deployments should expose `/healthz`, `/readyz`, and `/metrics` only on a
management network. Readiness requires initialized migrations and reachable
PostgreSQL. Future production readiness will also include Influx query health,
Registry projection delivery health, and a running telemetry claim loop.

Current graceful shutdown cancels new telemetry claims and in-flight reads and
renewals before stopping assignment gRPC, Registry delivery, management HTTP,
and clients within a bounded timeout. Any uncommitted claim remains fenced until
its PostgreSQL lease expires.

Agent and Relay must deploy the `wal_id` field before this binary is considered
ready for live telemetry. A missing field terminates the evaluator worker and
therefore the service instead of silently advancing a sequence-only checkpoint.

The assignment gRPC listener requires a server certificate/private key and a
client CA. Assignment lifecycle callers, including the coordinator permitted to
arm validated candidates, must present a certificate chaining to that CA;
startup fails if any credential is absent or invalid. Use a CA dedicated to
authorized control-plane clients, rotate credentials through mounted secrets
plus a controlled restart, and retain network policy as defense in depth. Never
treat the caller-provided assignment `source` as workload identity.

Assignment replacements roll out through
`candidate_received → candidate_armed → active`.
Deploy the Conformance schema/store support across the fleet before a control
plane begins emitting candidate, arm, or cutover commands. Candidate-specific
state names prevent the previous binary from claiming them after an accidental
rollback.
The operational rollback order is still: stop candidate emission, cancel and
verify all candidates, then roll back Conformance. Active authority intervals
and superseded history remain durable throughout that sequence.

Do not place Conformance in Relay's acknowledgement path. A Conformance rollout
or outage must not interrupt telemetry capture and delivery.
