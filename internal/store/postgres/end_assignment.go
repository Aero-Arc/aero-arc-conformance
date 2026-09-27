// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package postgres

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/aero-arc/aero-arc-conformance/internal/domain"
)

// EndAssignment closes an exact monitoring generation under the lifecycle lock.
// It atomically records immutable inbox delivery, an ending audit transition, and
// a worker fence. Replaying identical source/message content returns the current
// record without moving the original receipt time or fencing workers again.
//
// Parameters:
//   - ctx: bounds database operations and transaction lifetime.
//   - source, messageID: identify immutable delivery; changed content conflicts.
//   - assignmentID, generation: select authority. Zero resolves exactly one active
//     or ending generation matching every binding field; replay uses the original
//     inbox generation even if that generation is no longer active.
//   - flightID, aircraftID, intentID, agentID, intentVersion: must all match that assignment.
//   - completedAt: aircraft event time within the active interval and not future.
//
// Returns: the ending record after commit, retaining already committed evidence
// beyond completion without extending a prior authority end. A new closure clears
// the lease and increments its generation, invalidating previously claimed workers.
// ErrMessageConflict reports changed replay content; ErrInvalidTransition reports
// ambiguous generation resolution, binding mismatch, terminal lifecycle, or time
// outside active authority. ErrNotFound reports missing explicit authority. Invalid
// arguments, encoding, and storage failures are returned without a partial commit.
// Ending permits a bounded final telemetry drain; it does not prove archive coverage.
func (s *Store) EndAssignment(ctx context.Context, source, messageID, assignmentID string, generation uint64, flightID, aircraftID, intentID, agentID string, intentVersion uint32, completedAt time.Time) (domain.AssignmentRecord, error) {
	if source == "" || messageID == "" || assignmentID == "" || flightID == "" || aircraftID == "" || intentID == "" || agentID == "" || intentVersion == 0 || generation > math.MaxInt64 || !supportedUnixNanoseconds(completedAt) {
		return domain.AssignmentRecord{}, fmt.Errorf("invalid end assignment command")
	}
	completedAt = completedAt.UTC()
	payload, hash, err := encodeCommand("assignment_ending", lifecycleCommand{AssignmentID: assignmentID, Generation: generation, EffectiveAt: &completedAt, FlightID: flightID, AircraftID: aircraftID, IntentID: intentID, AgentID: agentID, IntentVersion: intentVersion})
	if err != nil {
		return domain.AssignmentRecord{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.AssignmentRecord{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, assignmentID); err != nil {
		return domain.AssignmentRecord{}, err
	}
	old, found, err := readInboxHash(ctx, tx, source, messageID)
	if err != nil {
		return domain.AssignmentRecord{}, err
	}
	if found && old != hash {
		return domain.AssignmentRecord{}, ErrMessageConflict
	}
	if found && generation == 0 {
		if err = tx.QueryRow(ctx, `SELECT assignment_generation FROM conformance_inbox WHERE source=$1 AND message_id=$2`, source, messageID).Scan(&generation); err != nil {
			return domain.AssignmentRecord{}, err
		}
	}
	if generation == 0 {
		rows, queryErr := tx.Query(ctx, `SELECT assignment_generation FROM conformance_assignments WHERE assignment_id=$1 AND flight_id=$2 AND aircraft_id=$3 AND intent_version=$4 AND intent_id=$5 AND agent_id=$6 AND lifecycle_state IN ('active','ending') ORDER BY assignment_generation LIMIT 2`, assignmentID, flightID, aircraftID, intentVersion, intentID, agentID)
		if queryErr != nil {
			return domain.AssignmentRecord{}, queryErr
		}
		count := 0
		for rows.Next() {
			if err = rows.Scan(&generation); err != nil {
				rows.Close()
				return domain.AssignmentRecord{}, err
			}
			count++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return domain.AssignmentRecord{}, err
		}
		if count != 1 {
			return domain.AssignmentRecord{}, fmt.Errorf("%w: exact flight binding must resolve one monitoring generation", ErrInvalidTransition)
		}
	}
	record, err := readAssignmentRecord(ctx, tx, assignmentID, generation)
	if err != nil {
		return record, err
	}
	if record.Assignment.FlightID != flightID || record.Assignment.AircraftID != aircraftID || record.Assignment.IntentVersion != intentVersion || record.Assignment.IntentID != intentID || record.Assignment.AgentID != agentID {
		return record, fmt.Errorf("%w: completion flight binding mismatch", ErrInvalidTransition)
	}
	if found {
		return record, tx.Commit(ctx)
	}
	if record.Lifecycle != domain.AssignmentActive && record.Lifecycle != domain.AssignmentEnding {
		return record, ErrInvalidTransition
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return record, err
	}
	if completedAt.After(now) || record.AuthorityFrom == nil || completedAt.Before(*record.AuthorityFrom) {
		return record, fmt.Errorf("%w: completion outside active flight time", ErrInvalidTransition)
	}
	var watermark int64
	if err = tx.QueryRow(ctx, `SELECT COALESCE((SELECT observed_at_unix_ns FROM conformance_summaries WHERE assignment_id=$1 AND assignment_generation=$2),0)`, assignmentID, generation).Scan(&watermark); err != nil {
		return record, err
	}
	boundaryNS := max(completedAt.UnixNano()+1, watermark+1)
	if record.AuthorityUntil != nil {
		boundaryNS = min(boundaryNS, record.AuthorityUntil.UnixNano())
	}
	boundary := time.Unix(0, boundaryNS).UTC()
	if _, err = tx.Exec(ctx, `UPDATE conformance_assignments SET lifecycle_state='ending',finalization_requested_at=COALESCE(finalization_requested_at,clock_timestamp()),authority_until=$3,authority_until_unix_ns=$4,lease_owner=NULL,lease_until=NULL,lease_generation=lease_generation+1,next_evaluation_at=now(),updated_at=now() WHERE assignment_id=$1 AND assignment_generation=$2`, assignmentID, generation, boundary, boundaryNS); err != nil {
		return record, err
	}
	if err = recordLifecycleCommand(ctx, tx, source, messageID, "assignment_ending", assignmentID, generation, string(record.Lifecycle), "ending", &boundary, payload, hash); err != nil {
		return record, err
	}
	record, err = readAssignmentRecord(ctx, tx, assignmentID, generation)
	if err != nil {
		return record, err
	}
	return record, tx.Commit(ctx)
}
