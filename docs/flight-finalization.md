# Monitoring closure after a flight

The API submits `EndAssignment` with a stable event identity, exact flight,
aircraft, intent identity and version, and physical completion timestamp. Assignment generation
zero resolves exactly one active/ending record with that binding. Intent version
is never used as a substitute for assignment generation. Explicit generations
still require the same binding check.

Closure takes the assignment lifecycle lock, records an idempotent inbox entry,
changes the record to `ending`, and invalidates the existing evaluator lease in
one transaction. Replayed event IDs cannot change their content or resolve a new
authority. The half-open authority end includes physical completion and preserves
any already committed summary watermark, without extending an existing boundary.

The existing ending-worker rules can evaluate a bounded tail. This does not prove
that every delayed telemetry frame has arrived. Archival completeness and later
revisions require a separate ingestion/watermark policy; monitoring closure alone
must not be advertised as a complete evidence archive.

Late completion delivery starts a bounded final-drain grace period at its first
durable receipt. Claim, renewal, and rescheduling use that receipt timestamp;
the event-time authority boundary remains unchanged. Duplicate deliveries do not
extend the grace period, and observations outside the original interval remain
inadmissible.

Ending assignments are claimed before active backlog, ordered by their final-drain
deadline, so older overdue active work cannot consume the closure grace window.
