# Durable history reads

`ConformanceService.ListConformanceEvents` reads persisted incident transitions
without running evaluation or changing incidents, checkpoints, or Registry state.
It uses the existing gRPC server and its configured transport security.

An assignment ID is required. Generation 0 selects all generations; nonzero is an
exact filter. Optional `from`/`until` are inclusive/exclusive event-time bounds.
The default page is 50 events (maximum 200). Ordering is descending event time,
then event ID, with a matching keyset cursor. Tokens bind assignment, generation,
and time filters, but are not authentication credentials. This is not a snapshot;
refresh page one to discover late inserts. Unknown assignments produce empty reads.

Events expose immutable transition evidence and assignment metadata, not the
mutable current incident summary. Spatial `deviation_m` preserves measured zero;
temporal/telemetry-loss events omit it. Optional `planned_start_at` and
`planned_end_at` come from the minimum start and maximum end of the matching
immutable assignment generation's volume windows. Missing/incomplete windows
omit both. These are not monitoring authority bounds, actual completion, or a
guarantee of continuous authorization between windows. Consumers can compare the
event observation time to these bounds; no temporal meter value is invented.
PostgreSQL timestamp precision is retained in pagination; the event frame ID is
available to locate original telemetry evidence.

Migration 002 adds an assignment/event-time/event-ID read index. It does not
rewrite history. Existing evaluation, lease, authority and outbox behavior is
unchanged. The API owns intent lookup and its normal authorization boundary;
Ops accesses this data only through the API, never through PostgreSQL.

The integration test starts PostgreSQL with the existing Testcontainers fixture,
loads baseline schema and event evidence, upgrades, and proves generation/scope
isolation, equal-time pagination, half-open time bounds, zero/absent measurements,
and preservation after reopen. Adapter validation uses unit tests, without an
unnecessary external dependency.
