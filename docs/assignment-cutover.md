# Blue-Green Assignment Cutover

## Purpose

Operational-intent replacement is not an in-place geometry edit. Conformance
keeps an immutable current assignment while it prepares and arms a candidate,
then transfers authority at one explicit event-time boundary.

![A blue-green assignment sequence in which generation seven stays authoritative while generation eight is prepared and armed, followed by an atomic event-time cutover](images/conformance-assignment-cutover.svg)

## Authority model

The API remains authoritative for local mission lifecycle and DSS coordination.
It delivers immutable assignment generations from its transactional outbox.
Conformance stores both generations, but only the current generation has an
authority interval and only current or ending generations are claimable.

The lifecycle is:

1. `PrepareAssignment` persists the candidate as `candidate_received`.
2. Validation and dependency checks finish before `ArmAssignment` marks it
   `candidate_armed`.
3. The current generation remains active throughout preparation and arming.
4. After the API durably commits the new local/DSS authority, it delivers an
   idempotent `CutoverAssignment` with the immutable `effective_at` boundary.
5. One PostgreSQL transaction changes the old generation to `superseded`, sets
   its exclusive `authority_until`, clears its lease, and activates the armed
   candidate with `authority_from = effective_at`.

The cutover transaction also locks the current generation and compares the
proposed boundary with its exact durable evaluation watermark. A boundary at or
before already committed evidence is rejected. This is deliberately
conservative: its Registry outbox may already have been delivered, so silently
moving or deleting that evidence would create two histories. The API must
choose a later safe boundary or enter an explicit reconciliation workflow.

Pending geometry never expands authorization. The evaluator must never union
the current and candidate volumes.

## Event-time routing

Authority intervals use half-open bounds:

```text
generation 7: [authority_from_7, cutover)
generation 8: [cutover, authority_until_8)
```

`ResolveAssignmentAt` selects the immutable generation for an observation's
capture time. A delayed frame received after cutover but captured before it is
therefore evaluated against generation 7. A frame captured exactly at cutover
belongs to generation 8. The replacement interval ends exclusively at its
immutable `effective_until`; observations at or after that boundary have no
assignment authority.

Late evaluation does not revive a generation 7 lease. A worker holding the
current generation 8 lease may call `CommitHistoricalEvaluation`; PostgreSQL
atomically consumes that current lease, verifies the observation belongs to the
stored generation 7 authority interval, and commits generation 7 evidence. The
historical summary and checkpoint remain generation-scoped and no Registry live
projection is emitted, so reconciliation cannot roll generation 8 live state
backward.

An evaluation records both its earliest causal timestamp and its final
watermark. The commit also checks every transition and retained incident-state
timestamp against the same stored half-open authority interval. This prevents a
batch that begins before cutover—but opens and resolves after it—from erasing
the cross-generation input when its final state becomes clear.

The evaluator accepts batches only in the telemetry reader's canonical event
time, agent, WAL identity, WAL sequence, and frame order. Equal-time frames can
change hysteresis outcomes, so their cursor order is validated rather than
treated as interchangeable.

Incident transitions carry the opening frame of their exact occurrence and the
transition's own WAL cursor. Immutable event rows contain only that transition
evidence, not mutable batch-final evaluator state. Replay can therefore move an
incident's current resolution pointer to a newly discovered resolution event
while retaining every prior resolution event for audit.

The unreleased baseline schema stores occurrence identity and transition-local
evidence from its first commit. Once a schema version has shipped, it becomes
immutable and subsequent releases must add explicit, tested upgrade migrations.

PostgreSQL `timestamptz` is retained for readable audit timestamps, but it has
microsecond precision. Authority comparisons use companion signed Unix-
nanosecond columns so two telemetry frames around a sub-microsecond boundary
cannot be rounded onto the wrong generation.

## Failure behavior

- A preparation failure or `CancelCandidate` leaves the current generation and
  its lease untouched.
- A second, higher candidate supersedes only the older candidate.
- Cutover requires the highest candidate to be armed and uses the PostgreSQL
  clock to reject scheduled future authority changes.
- Reusing a source/message ID with identical content is idempotent; changed
  content is a conflict.
- Cutover clears the previous generation's lease. Its paused worker cannot
  renew or commit because both lifecycle and lease fences fail.
- A delayed pre-cutover observation can be reconciled only through a valid
  current-generation lease; an out-of-interval attempt rolls back without
  consuming that lease.
- A retroactive cutover at or before the old generation's committed event-time
  watermark is rejected without recording its inbox command or mutating either
  generation.
- Lower assignment generations are recorded as stale and cannot resurrect.
- The transition history and API outbox are written in the same transaction as
  every lifecycle change.

## Runtime contract

The candidate-specific database names are also a rollback fence: the original
prototype binary only recognizes `received` and `armed`, so it cannot claim a
new candidate if a binary is accidentally rolled back.

The store and external Protobuf contracts implement this lifecycle. The gRPC
surface carries assignment ID, assignment generation, exact intent ID/version,
stable message ID, and cutover `effective_at`; exact-generation reads support
reconciliation after ambiguous delivery. The worker that validates and arms a
candidate remains a later runtime slice and is intentionally not exposed as an
API-owned command.

The current store supports exact suffix replay and a shifted resolution for a
stable incident occurrence. A future arbitrary-suffix reconciliation contract
must also identify the replaced replay range and materialize which immutable
events remain active; otherwise replay cannot safely retract a transition that
disappears entirely or merge two previously distinct occurrences.
