ALTER TABLE conformance_assignments
  ADD COLUMN authority_from timestamptz,
  ADD COLUMN authority_until timestamptz,
  ADD COLUMN authority_from_unix_ns bigint,
  ADD COLUMN authority_until_unix_ns bigint,
  ADD COLUMN prepared_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN armed_at timestamptz,
  ADD COLUMN cutover_at timestamptz;

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
      + coalesce(rpad((regexp_match(specification->>'effective_from', E'\\.([0-9]+)'))[1], 9, '0')::bigint, 0)
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
    OR (lifecycle_state IN ('active','ending') AND authority_from_unix_ns IS NOT NULL AND authority_until_unix_ns IS NULL)
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
