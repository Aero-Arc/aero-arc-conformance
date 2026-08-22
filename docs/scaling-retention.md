# Scaling, Retention, and Flight Finalization

## Status and scope

This document is a capacity and lifecycle design, not a claim that the current
prototype has implemented retention or archival. It targets thousands of
managed flights while preserving Conformance's correctness fences.

The service boundaries do not change:

- Agent and Relay own telemetry capture, acknowledgement, attribution, and raw
  telemetry storage. Conformance must not synchronously copy every frame to an
  object store or enter Relay's acknowledgement path.
- InfluxDB, and any Relay-managed telemetry archive behind it, are replayable
  telemetry evidence. They are not assignment authority.
- PostgreSQL remains Conformance's durable authority for assignment intervals,
  evaluation fences, incidents, final reports, and archive manifests.
- Registry, including a Redis-backed implementation, is only an expiring live
  projection. It is not flight history and cannot authorize deletion.

## What the prototype does today

The database model already separates the three correctness fences:
assignment generation, worker lease generation, and evaluation revision.
`CommitEvaluation` atomically advances the evaluation revision and writes the
checkpoint, current summary, incident materialization, immutable transition
events, and Registry outbox message behind the current assignment lease.
Registry delivery is independently leased, idempotent, acknowledged at the
exact evaluation cursor, and ordered per assignment. Historical reconciliation
cannot publish a live projection.

Those properties address duplicate workers, process death, stale assignment
authority, Registry outages, and retry ordering. They do not bound storage or
prove throughput. In particular:

- the telemetry claim/poll/evaluate loop is not yet wired;
- every live `CommitEvaluation` currently inserts one checkpoint and one
  Registry outbox row, even when the projected state is unchanged;
- the summary is bounded to one upserted row per assignment generation, and
  incident/event rows are created only for incident lifecycle changes;
- delivered outbox rows and old checkpoints have no pruning worker;
- there is no final flight report, archive manifest, or finalization state
  machine;
- only baseline process metrics exist.

The statement in the continuous design that Conformance publishes changes and
periodic freshness describes the target policy. The current persistence method
does not yet perform that coalescing.

## Current-rate planning model

Poll intervals are not throughput guarantees. The following is a conservative
planning scenario in which each active assignment evaluates and commits once
per second, matching the example Influx poll interval:

| Work per active assignment | Rate | Rows after one hour |
| --- | ---: | ---: |
| evaluation transaction | 1/second | n/a |
| assignment scheduling update | 1/second | bounded row, 3,600 updates |
| current-summary upsert | 1/second | 1 row, 3,600 updates |
| checkpoint insert | 1/second | 3,600 rows |
| Registry outbox insert | 1/second | 3,600 rows |
| incident and event inserts | on transition | workload-dependent |

At 1,000 concurrently active assignments this scenario produces 1,000
evaluation transactions per second and 7.2 million append-only checkpoint plus
outbox rows per hour. Row count alone understates load because both tables carry
JSON payloads and indexes, summary/assignment rows are repeatedly updated, WAL
must be written, and autovacuum must keep up.

The example Registry publisher claims at most 20 rows, then waits one second
after that flush finishes before claiming again. Its optimistic ceiling is
therefore less than or equal to 20 successful messages per second per publisher
replica, before accounting for RPC latency or retries. With 1,000 new messages
per second and one such replica, the nominal backlog grows by at least 980 rows
per second, or 3.528 million rows per hour. The per-assignment head-of-line fence
also permits only the oldest undelivered cursor for an assignment to be claimed
in a flush. Increasing the batch size or replicas may raise delivery throughput,
but publishing unchanged one-second revisions remains unnecessary load.

Capacity estimates must use concurrent active assignments, not total customer
flights, and measured encoded row sizes. For planning:

```text
evaluation_rate = active_assignments / evaluation_interval
checkpoint_rate = routine checkpoints + transition/status/finalization commits
projection_rate = state changes + freshness heartbeats
outbox_backlog_rate = max(0, projection_rate - measured_delivery_rate)
hot_bytes = retained_rows * measured_row_plus_index_bytes
```

Measure p50, p95, and p99 transaction/RPC latency and PostgreSQL WAL bytes; do
not infer sustainable capacity from configured intervals.

## Separate the cadences

Evaluation, durable recovery, live projection, incident evidence, and archival
are different activities. Production scheduling must configure and observe them
independently.

### Evaluation cadence

Read bounded, overlapping telemetry windows and feed every canonical observation
through the event-time evaluator. This cadence controls detection latency and
may remain near one second. Processing ten frames in one poll is still one
ordered evaluator batch; it does not require ten database rows.

The evaluator may hold progress newer than its last durable checkpoint only if
a takeover can restore that checkpoint and deterministically replay the entire
suffix. No observation cursor may be acknowledged as durably evaluated merely
because it exists in worker memory.

### Checkpoint cadence

Commit immediately when an incident opens or resolves, condition changes,
monitoring or recording status changes, assignment authority changes, or a
flight is finalized. Otherwise create a periodic recovery checkpoint at a
separately configured interval.

The maximum checkpoint interval is a recovery objective: after worker loss it
bounds the normal suffix that must be reread and replayed. Retention must always
keep at least one checkpoint older than the overlap/reconciliation boundary;
retaining only the latest checkpoint is unsafe. A checkpoint commit continues
to atomically include any incident transitions and the current summary behind
the current assignment lease.

### Registry projection and heartbeat cadence

Enqueue a live projection immediately for meaningful condition, violation,
monitoring, or recording changes. Coalesce unchanged evaluations and enqueue a
periodic heartbeat only often enough to refresh Registry's TTL. The heartbeat
interval must be derived from the configured Registry TTL, clock/network safety
margin, and recovery objective; it must not be assumed from the Influx poll
interval.

Registry must continue to compare assignment generation and evaluation revision.
If projection revisions become sparser than checkpoint revisions, the contract
must explicitly preserve monotonic cursors rather than renumbering or collapsing
the existing fences. Coalescing must happen before outbox insertion. Replacing
or deleting an undelivered outbox row can erase evidence of an ambiguous send
and is forbidden without a Registry reconciliation contract proving its exact
cursor accepted or obsolete.

### Incident-event cadence

Immutable events remain transition-driven. They are not sampled or coalesced.
Stable opening-frame occurrence identity, transition-local WAL cursors, and
deterministic event IDs remain part of the permanent evidence model.

### Archival cadence

Archive asynchronously in bounded flight or time partitions after evidence is
durable in PostgreSQL. Object-store latency or failure must never block Relay
acknowledgement, live evaluation commits, or Registry publication. Avoid one
object per evaluation; use versioned, compressed bundles with checksums and a
manifest.

Conformance's bundle contains its own evidence and references the raw telemetry
archive owned by Agent/Relay. It does not silently assume ownership of raw
telemetry retention. PostgreSQL remains authoritative for the durable manifest,
final report, assignment authority, and incident/event history.

## Flight finalization and safe deletion

An assignment reaching `effective_until` or lifecycle `completed` starts
finalization; it does not make its working rows immediately disposable.
Finalization must be resumable and idempotent, with explicit states such as
`pending_reconciliation`, `reconciled`, `archived`, and `prunable`.

A final report should be immutable and keyed by flight/assignment plus report
schema version. At minimum it records:

- every assignment generation and exact half-open authority interval;
- policy version and evaluator build/config identity;
- final evaluation revision, event-time watermark, and WAL/frame cursor;
- final condition and monitoring/recording state;
- incident counts, durations, extrema, and immutable event identities;
- reconciliation window and completion time;
- raw-telemetry archive reference supplied by Agent/Relay;
- Conformance evidence-object locations, sizes, hashes, schema versions, and
  archive verification time.

The current `conformance_summaries` row is a latest generation projection, not
a whole-flight report. It cannot answer duration, occurrence-count, authority
history, reconciliation, or archive-completeness questions.

Working rows become eligible for deletion only when all of these gates hold in
one durable finalization record:

1. Assignment authority has ended and no candidate cutover can move the
   boundary.
2. The configured settle and late-arrival window has closed, final historical
   reconciliation has committed, and the final watermark/cursor is recorded.
3. Every relevant API and Registry outbox row has an exact acknowledgement, or
   an explicit destination reconciliation proves the cursor accepted or
   obsolete. Age and retry count are not proof.
4. The evidence bundle is durably written, read back or otherwise independently
   verified, and its content hash, location, schema version, and size are stored
   in a PostgreSQL manifest.
5. The immutable final report is committed and references that manifest.
6. No active replay, legal hold, incident review, or archival lease covers the
   rows to be removed.

Only then may a fenced retention worker prune delivered outbox rows and obsolete
checkpoints in bounded batches. It must select explicit assignment generations,
use PostgreSQL time, record deletion progress, tolerate retries, and preserve a
checkpoint required by any retained overlap/replay boundary.

Assignment authority history, final reports, archive manifests, incidents, and
immutable transition events remain in PostgreSQL under their audit retention
policy. If scale later requires partition detachment or a separate PostgreSQL
archive tier, PostgreSQL still owns that lifecycle and the authoritative index;
an unverified object alone is not authority.

## Proposed controls and invariants

Add configuration only with its implementation and validation. The planned
controls are:

- evaluator poll interval, settle delay, overlap, and bounded query limits;
- routine checkpoint interval and maximum replay suffix;
- projection-on-change policy, Registry heartbeat interval, and Registry TTL
  safety relationship;
- final reconciliation delay and maximum late-arrival window;
- delivered-outbox safety retention and checkpoint/replay retention;
- retention batch size, transaction timeout, and per-run rate limit;
- archive destination, bundle size/partition policy, verification mode, and
  retry backoff;
- incident/final-report retention and legal-hold policy.

Validation must reject unsafe relationships, including a heartbeat that cannot
refresh the known Registry TTL, checkpoint retention shorter than overlap plus
recovery delay, or deletion retention shorter than final reconciliation and
outbox settlement. There is deliberately no recommended production number
until workload benchmarks and Registry TTL requirements are available.

The following invariants extend the existing correctness model:

- evaluation cadence never defines assignment authority;
- no worker-memory cursor is considered durable;
- state changes and incident transitions force a fenced durable commit;
- historical reconciliation never emits a live Registry projection;
- archive success is not inferred from an upload response alone;
- retention never deletes evidence referenced by an undelivered or ambiguous
  outbox message;
- pruning cannot remove the checkpoint required to replay a retained suffix;
- finalization, archival, and pruning each have independent leases and
  idempotency keys.

## Metrics and capacity gates

At minimum export per-stage counters, gauges, and histograms with bounded
labels—never aircraft, assignment, flight, frame, or incident IDs:

- active/due/leased assignments and claim saturation;
- observations and batches evaluated, batch size, event-time lag, settle lag,
  overlap-replay count, and saturated-query rejection count;
- evaluation transaction latency/errors, lease conflicts, revision conflicts,
  checkpoints created, and replay suffix size/time;
- incident transitions by type and transition;
- outbox depth/age by destination and delivery state, claim count, RPC latency,
  retry count, acknowledgement mismatch, and measured drain rate;
- finalization age/state, late observations reconciled, archive bytes/latency,
  verification failures, manifests awaiting completion, and legal holds;
- retention candidates, deleted rows/bytes, failures, and oldest prunable row;
- PostgreSQL transaction latency, WAL bytes, table/index bytes, dead tuples,
  autovacuum lag, connection-pool saturation, and replica/archive lag.

Production admission requires load and soak tests at 100, 500, 1,000, and 5,000
active assignments. Exercise conforming steady state, simultaneous transitions,
Registry outage and recovery, worker death, Influx delayed visibility, a hot
assignment, assignment cutover, and end-of-flight finalization. Record sustainable
rates and backlog recovery time with realistic JSON payload sizes.

## Test strategy

Unit tests must cover cadence decisions, change detection, heartbeat deadlines,
configuration relationships, deterministic report/bundle identity, retention
eligibility, legal holds, and crash-safe idempotent retries.

PostgreSQL integration tests must prove:

- transition-triggered commit remains atomic with checkpoint, summary, incident,
  event, and any required outbox row;
- unchanged evaluations coalesce without losing replayable progress;
- checkpoint pruning preserves the overlap boundary and fenced takeover;
- an undelivered, leased, retrying, or ambiguously acknowledged outbox row blocks
  deletion;
- finalization waits for late data and commits a deterministic final report;
- archive verification and manifest commit precede eligibility, including crash
  points between upload, verification, manifest, and pruning;
- concurrent finalizers/retention workers cannot double-finalize or over-delete;
- historical reconciliation cannot publish Registry state or roll live state
  backward.

End-to-end tests must use real PostgreSQL, InfluxDB, and an object-store-compatible
test service; container readiness is not evidence. A Registry fake or real test
service must model exact acknowledgements, ambiguous failure, backlog, and
recovery. Soak tests should verify database growth matches the configured model.

## Incremental implementation roadmap

1. **Instrument and benchmark the wired worker.** Add stage metrics, measure
   encoded row/index/WAL cost, and establish the one-second baseline before
   changing semantics.
2. **Separate durable and projection decisions.** Add tested change detection,
   routine checkpoint and heartbeat controls, and explicit projection cursors
   while retaining atomic state-change commits.
3. **Bound hot operational storage.** Partition append-only tables where
   measurements justify it; add fenced, batch-limited pruning for delivered
   outbox rows and replay-safe checkpoints with dry-run metrics.
4. **Add final reports and finalization.** Implement the late-data state machine,
   final reconciliation, immutable report schema, and legal-hold awareness.
5. **Add asynchronous evidence archival.** Define versioned bundle/manifest
   contracts and Agent/Relay raw-telemetry references, verify uploaded content,
   and make archive failures observable without blocking live paths.
6. **Prove fleet capacity.** Run failure and soak matrices through 5,000 active
   assignments, tune PostgreSQL/publisher concurrency from measurements, and set
   admission and autoscaling thresholds from oldest-work age rather than CPU
   alone.

Each step ships with migration upgrade tests, operational rollback guidance,
and retention disabled by default until its safety tests pass.

## Addressed and remaining risks

Already addressed in the prototype design and implementation:

- immutable assignment generations and half-open authority intervals;
- independent assignment, lease, and evaluation fences;
- PostgreSQL-clock claims and non-revivable expired leases;
- atomic summary/checkpoint/incident/event/outbox commits;
- deterministic event identity and immutable evidence insertion;
- bounded telemetry reads and canonical event-time ordering;
- per-assignment ordered, independently leased Registry delivery with exact
  acknowledgement;
- live projection separated from durable incident authority;
- historical reconciliation fenced away from live publication.

Not yet addressed or not yet proven:

- the production evaluator worker and actual fleet throughput;
- merged telemetry WAL identity, durable Relay acknowledgement, delayed Influx
  visibility, and persistent overlap replay semantics;
- evaluation/checkpoint/projection cadence separation and Registry TTL contract;
- backpressure, publisher autoscaling, backlog recovery, and poison-message
  operations;
- safe checkpoint/outbox retention and PostgreSQL partition strategy;
- whole-flight final report, late-data finalization, verified archive manifest,
  and legal holds;
- authoritative cross-service raw-telemetry archive references and retention;
- realistic row-size/WAL/autovacuum measurements and capacity limits;
- same-timestamp pagination and end-to-end 1,000/5,000-aircraft soak coverage.

Until those items land, the current approach is acceptable for bounded prototype
use, but "thousands of flights" is a test target rather than a supported service
limit.
