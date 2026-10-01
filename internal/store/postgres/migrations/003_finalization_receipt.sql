ALTER TABLE conformance_assignments
  ADD COLUMN IF NOT EXISTS finalization_requested_at timestamptz;
