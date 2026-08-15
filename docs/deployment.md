# Deployment

The prototype initializes numbered migrations and clients, serves management
HTTP and assignment lifecycle gRPC, and drains committed Registry projection
outbox messages. It still has no telemetry claim/evaluate worker loop.
The tested store is designed for multiple replicas: advisory-locked migrations,
revision-fenced commits, and assignment leases prevent competing commits.

Deployments should expose `/healthz`, `/readyz`, and `/metrics` only on a
management network. Readiness requires initialized migrations and reachable
PostgreSQL. Future production readiness will also include Influx query health,
Registry projection delivery health, and a running telemetry claim loop.

Current graceful shutdown stops readiness, assignment gRPC, Registry delivery,
management HTTP, and clients within a bounded timeout. The worker slice must
extend this to stop new telemetry claims and fence active evaluation work.

Assignment replacements roll out through
`candidate_received → candidate_armed → active`.
Deploy the Conformance schema/store support across the fleet before an API
begins emitting candidate or cutover commands. Candidate-specific state names
prevent the previous binary from claiming them after an accidental rollback.
The operational rollback order is still: stop candidate emission, cancel and
verify all candidates, then roll back Conformance. Active authority intervals
and superseded history remain durable throughout that sequence.

Do not place Conformance in Relay's acknowledgement path. A Conformance rollout
or outage must not interrupt telemetry capture and delivery.
