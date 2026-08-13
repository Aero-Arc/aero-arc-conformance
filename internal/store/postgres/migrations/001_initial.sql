CREATE TABLE IF NOT EXISTS schema_migrations (
  version bigint PRIMARY KEY,
	checksum text NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS conformance_inbox (
  source text NOT NULL,
  message_id text NOT NULL,
  payload_sha256 text NOT NULL,
  message_type text NOT NULL,
  assignment_id text NOT NULL,
  assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
  received_at timestamptz NOT NULL DEFAULT now(),
  processed_at timestamptz,
  outcome jsonb,
  PRIMARY KEY (source, message_id)
);

CREATE TABLE IF NOT EXISTS conformance_assignments (
  assignment_id text NOT NULL,
  assignment_generation bigint NOT NULL CHECK (assignment_generation > 0),
  aircraft_id text NOT NULL,
  agent_id text NOT NULL,
  flight_id text NOT NULL,
  intent_id text NOT NULL,
  intent_version integer NOT NULL CHECK (intent_version > 0),
  policy_version text NOT NULL,
  lifecycle_state text NOT NULL CHECK (lifecycle_state IN ('received','armed','active','ending','completed','cancelled','superseded')),
  specification jsonb NOT NULL,
  next_evaluation_at timestamptz NOT NULL DEFAULT now(),
  lease_owner text,
  lease_generation bigint NOT NULL DEFAULT 0,
  lease_until timestamptz,
  evaluation_revision bigint NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (assignment_id, assignment_generation)
);

CREATE UNIQUE INDEX IF NOT EXISTS conformance_one_live_assignment
  ON conformance_assignments (assignment_id)
  WHERE lifecycle_state IN ('received','armed','active','ending');

CREATE INDEX IF NOT EXISTS conformance_assignments_due
  ON conformance_assignments (next_evaluation_at, lease_until)
  WHERE lifecycle_state IN ('received','armed','active','ending');

CREATE TABLE IF NOT EXISTS conformance_checkpoints (
  assignment_id text NOT NULL,
  assignment_generation bigint NOT NULL,
  evaluation_revision bigint NOT NULL,
  state_through_at timestamptz NOT NULL,
  wal_id text NOT NULL,
  wal_sequence bigint NOT NULL CHECK (wal_sequence >= 0),
  frame_id text NOT NULL,
  evaluator_state jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (assignment_id, assignment_generation, evaluation_revision),
  FOREIGN KEY (assignment_id, assignment_generation) REFERENCES conformance_assignments(assignment_id, assignment_generation)
);

CREATE TABLE IF NOT EXISTS conformance_incidents (
  incident_id text PRIMARY KEY,
  assignment_id text NOT NULL,
  assignment_generation bigint NOT NULL,
  incident_key text NOT NULL,
  violation_type text NOT NULL,
  state text NOT NULL CHECK (state IN ('open','resolved')),
  severity text NOT NULL,
  opened_at timestamptz NOT NULL,
  last_observed_at timestamptz NOT NULL,
  resolved_at timestamptz,
  revision bigint NOT NULL DEFAULT 1,
  details jsonb NOT NULL,
  FOREIGN KEY (assignment_id, assignment_generation) REFERENCES conformance_assignments(assignment_id, assignment_generation)
);

CREATE UNIQUE INDEX IF NOT EXISTS conformance_one_open_incident
  ON conformance_incidents (assignment_id, assignment_generation, incident_key)
  WHERE state = 'open';

CREATE TABLE IF NOT EXISTS conformance_events (
  event_id text PRIMARY KEY,
  assignment_id text NOT NULL,
  assignment_generation bigint NOT NULL,
  incident_id text,
  transition text NOT NULL,
  violation_type text NOT NULL,
  observed_at timestamptz NOT NULL,
  frame_id text NOT NULL,
  wal_id text NOT NULL,
  wal_sequence bigint NOT NULL CHECK (wal_sequence >= 0),
  evaluation_revision bigint NOT NULL,
  payload jsonb NOT NULL,
	payload_sha256 text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
	FOREIGN KEY (assignment_id, assignment_generation) REFERENCES conformance_assignments(assignment_id, assignment_generation),
	FOREIGN KEY (incident_id) REFERENCES conformance_incidents(incident_id)
);

CREATE TABLE IF NOT EXISTS conformance_summaries (
  assignment_id text NOT NULL,
  assignment_generation bigint NOT NULL,
  evaluation_revision bigint NOT NULL,
  condition text NOT NULL,
  monitoring_status text NOT NULL,
  recording_status text NOT NULL,
  observed_at timestamptz NOT NULL,
  frame_id text NOT NULL,
  payload jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (assignment_id, assignment_generation),
  FOREIGN KEY (assignment_id, assignment_generation) REFERENCES conformance_assignments(assignment_id, assignment_generation)
);

CREATE TABLE IF NOT EXISTS conformance_outbox (
  outbox_id text PRIMARY KEY,
  destination text NOT NULL,
  idempotency_key text NOT NULL UNIQUE,
  assignment_id text NOT NULL,
  assignment_generation bigint NOT NULL,
  evaluation_revision bigint NOT NULL,
  payload jsonb NOT NULL,
  attempt_count integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  lease_owner text,
  lease_generation bigint NOT NULL DEFAULT 0,
  lease_until timestamptz,
  delivered_at timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS conformance_outbox_due
  ON conformance_outbox (next_attempt_at, lease_until)
  WHERE delivered_at IS NULL;
