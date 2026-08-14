# Deployment

The prototype runs one intentionally inert process plus PostgreSQL and InfluxDB.
The current process initializes numbered migrations and clients, then serves
management HTTP. It has no assignment ingress, claim worker, or gRPC server yet.
The tested store is designed for multiple replicas: advisory-locked migrations,
revision-fenced commits, and assignment leases prevent competing commits.

Deployments should expose `/healthz`, `/readyz`, and `/metrics` only on a
management network. Readiness requires initialized migrations and reachable
PostgreSQL. Future production readiness will also include assignment ingress,
Influx query health, Registry projection delivery, and a running claim loop.

Current graceful shutdown stops readiness, shuts down management HTTP, and
closes clients within a bounded timeout. The worker slice must extend this to
stop new claims, fence active work, drain outbox delivery, and stop assignment
gRPC before production deployment.

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
