-- Assignment-scoped keyset reads span all generations unless explicitly filtered.
CREATE INDEX IF NOT EXISTS conformance_events_history_read
  ON conformance_events (assignment_id, observed_at DESC, event_id DESC);
