ALTER TABLE conformance_assignments
  ADD COLUMN authority_from timestamptz,
  ADD COLUMN authority_until timestamptz,
  ADD COLUMN authority_from_unix_ns bigint,
  ADD COLUMN authority_until_unix_ns bigint,
  ADD COLUMN prepared_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN armed_at timestamptz,
  ADD COLUMN cutover_at timestamptz;

-- Exact event-time watermarks are required to reject a retroactive cutover
-- that would strand already-committed evidence beyond the new boundary.
-- Version 1 stored only microsecond timestamps, so its migrated watermark is
-- conservatively rounded to the end of that unknown nanosecond interval.
ALTER TABLE conformance_summaries
  ADD COLUMN observed_at_unix_ns bigint;

UPDATE conformance_summaries
SET observed_at_unix_ns = floor(extract(epoch FROM observed_at) * 1000000)::bigint * 1000 + 999;

ALTER TABLE conformance_summaries
  ALTER COLUMN observed_at_unix_ns SET NOT NULL;

-- Version 2 transition events keep their own immutable deviation evidence;
-- the event no longer derives identity from mutable batch-final evaluator
-- state. Existing version 1 rows default to the evidence their schema carried.
ALTER TABLE conformance_events
  ADD COLUMN deviation_m double precision NOT NULL DEFAULT 0,
  ADD COLUMN evidence_version smallint NOT NULL DEFAULT 1;

ALTER TABLE conformance_incidents
  ADD COLUMN opening_frame_id text,
  ADD COLUMN resolution_event_id text;

UPDATE conformance_incidents i
SET opening_frame_id = e.frame_id
FROM conformance_events e
WHERE e.incident_id = i.incident_id AND e.transition = 'opened';

ALTER TABLE conformance_incidents
  ALTER COLUMN opening_frame_id SET NOT NULL,
  ADD CONSTRAINT conformance_incident_resolution_event
    FOREIGN KEY (resolution_event_id) REFERENCES conformance_events(event_id);

CREATE UNIQUE INDEX conformance_incident_occurrence
  ON conformance_incidents (assignment_id, assignment_generation, incident_key, opening_frame_id);

UPDATE conformance_incidents i
SET resolution_event_id = e.event_id
FROM conformance_events e
WHERE e.incident_id = i.incident_id
  AND e.transition = 'resolved'
  AND e.observed_at = i.resolved_at;

-- A v1 checkpoint serialized incident phase but not occurrence identity.
-- Enrich every open/recovering containment state from the immutable incident
-- opening event so a restarted evaluator can resolve the same occurrence.
UPDATE conformance_checkpoints c
SET evaluator_state = jsonb_set(
  c.evaluator_state,
  ARRAY['violations','lateral_deviation','opening_frame_id'],
  to_jsonb((SELECT i.opening_frame_id FROM conformance_incidents i
    WHERE i.assignment_id=c.assignment_id AND i.assignment_generation=c.assignment_generation
      AND i.incident_key='lateral_deviation' AND i.opened_at<=c.state_through_at
      AND (i.resolved_at IS NULL OR c.state_through_at<=i.resolved_at)
    ORDER BY i.opened_at DESC LIMIT 1)), true)
WHERE c.evaluator_state#>>'{violations,lateral_deviation,phase}' IN ('open','recovering');

UPDATE conformance_checkpoints c
SET evaluator_state = jsonb_set(
  c.evaluator_state,
  ARRAY['violations','altitude_deviation','opening_frame_id'],
  to_jsonb((SELECT i.opening_frame_id FROM conformance_incidents i
    WHERE i.assignment_id=c.assignment_id AND i.assignment_generation=c.assignment_generation
      AND i.incident_key='altitude_deviation' AND i.opened_at<=c.state_through_at
      AND (i.resolved_at IS NULL OR c.state_through_at<=i.resolved_at)
    ORDER BY i.opened_at DESC LIMIT 1)), true)
WHERE c.evaluator_state#>>'{violations,altitude_deviation,phase}' IN ('open','recovering');

UPDATE conformance_checkpoints c
SET evaluator_state = jsonb_set(
  c.evaluator_state,
  ARRAY['violations','temporal_deviation','opening_frame_id'],
  to_jsonb((SELECT i.opening_frame_id FROM conformance_incidents i
    WHERE i.assignment_id=c.assignment_id AND i.assignment_generation=c.assignment_generation
      AND i.incident_key='temporal_deviation' AND i.opened_at<=c.state_through_at
      AND (i.resolved_at IS NULL OR c.state_through_at<=i.resolved_at)
    ORDER BY i.opened_at DESC LIMIT 1)), true)
WHERE c.evaluator_state#>>'{violations,temporal_deviation,phase}' IN ('open','recovering');

-- Version 1 used received/armed as claimable prototype states. Retire any such
-- rows and use new candidate-only names so a rolled-back binary cannot claim a
-- blue-green candidate by mistake.
UPDATE conformance_assignments
SET lifecycle_state = 'cancelled', lease_owner = NULL, lease_until = NULL, updated_at = now()
WHERE lifecycle_state IN ('received','armed');

-- Version 1 active rows did not record an authority interval. Their immutable
-- assignment specification is the only durable event-time boundary available,
-- so preserve that boundary exactly while upgrading. PostgreSQL timestamps are
-- microsecond-resolution; parse the RFC3339 fractional seconds separately for
-- the signed nanosecond comparison column.
UPDATE conformance_assignments
SET authority_from = (specification->>'effective_from')::timestamptz,
    authority_from_unix_ns =
      extract(epoch FROM date_trunc('second', (specification->>'effective_from')::timestamptz))::bigint * 1000000000
      + coalesce(rpad((regexp_match(specification->>'effective_from', E'\\.([0-9]+)'))[1], 9, '0')::bigint, 0),
    authority_until = (specification->>'effective_until')::timestamptz,
    authority_until_unix_ns =
      extract(epoch FROM date_trunc('second', (specification->>'effective_until')::timestamptz))::bigint * 1000000000
      + coalesce(rpad((regexp_match(specification->>'effective_until', E'\\.([0-9]+)'))[1], 9, '0')::bigint, 0)
WHERE lifecycle_state IN ('active','ending');

ALTER TABLE conformance_assignments
  DROP CONSTRAINT conformance_assignments_lifecycle_state_check,
  ADD CONSTRAINT conformance_assignments_lifecycle_state_check
  CHECK (lifecycle_state IN ('candidate_received','candidate_armed','active','ending','completed','cancelled','superseded'));

ALTER TABLE conformance_assignments
  ADD CONSTRAINT conformance_authority_interval_valid
  CHECK (
    (authority_from IS NULL) = (authority_from_unix_ns IS NULL)
    AND (authority_until IS NULL) = (authority_until_unix_ns IS NULL)
    AND (authority_until_unix_ns IS NULL OR authority_until_unix_ns > authority_from_unix_ns)
  ),
  ADD CONSTRAINT conformance_lifecycle_authority_valid
  CHECK (
    (lifecycle_state IN ('candidate_received','candidate_armed','cancelled') AND authority_from_unix_ns IS NULL AND authority_until_unix_ns IS NULL)
    OR (lifecycle_state IN ('active','ending') AND authority_from_unix_ns IS NOT NULL AND authority_until_unix_ns IS NOT NULL)
    OR (lifecycle_state = 'superseded' AND ((authority_from_unix_ns IS NULL AND authority_until_unix_ns IS NULL) OR (authority_from_unix_ns IS NOT NULL AND authority_until_unix_ns IS NOT NULL)))
    OR lifecycle_state = 'completed'
  );

DROP INDEX conformance_one_live_assignment;
DROP INDEX conformance_assignments_due;

CREATE UNIQUE INDEX conformance_one_current_assignment
  ON conformance_assignments (assignment_id)
  WHERE lifecycle_state IN ('active','ending');

CREATE UNIQUE INDEX conformance_one_candidate_assignment
  ON conformance_assignments (assignment_id)
  WHERE lifecycle_state IN ('candidate_received','candidate_armed');

CREATE INDEX conformance_assignments_due
  ON conformance_assignments (next_evaluation_at, lease_until)
  WHERE lifecycle_state IN ('active','ending');

CREATE INDEX conformance_assignment_authority_history
  ON conformance_assignments (assignment_id, authority_from_unix_ns, authority_until_unix_ns)
  WHERE authority_from_unix_ns IS NOT NULL;

CREATE TABLE conformance_assignment_transitions (
  transition_id text PRIMARY KEY,
  source text NOT NULL,
  message_id text NOT NULL,
  assignment_id text NOT NULL,
  assignment_generation bigint NOT NULL,
  from_state text,
  to_state text NOT NULL,
  effective_at timestamptz,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (assignment_id, assignment_generation)
    REFERENCES conformance_assignments(assignment_id, assignment_generation)
);
