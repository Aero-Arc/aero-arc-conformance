// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package postgres implements durable assignments and fenced evaluation commits.
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/aero-arc/aero-arc-conformance/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrLeaseLost       = errors.New("conformance assignment lease lost")
	ErrMessageConflict = errors.New("inbox message ID reused with different payload")
	ErrStaleEvaluation = errors.New("evaluation is older than the current live watermark")
)

type Store struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, dsn string) (*Store, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres DSN: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	s := &Store{pool: pool}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close()                         { s.pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

type ApplyDisposition string

const (
	ApplyApplied    ApplyDisposition = "applied"
	ApplyIdempotent ApplyDisposition = "idempotent"
	ApplyStale      ApplyDisposition = "stale"
)

type ApplyResult struct {
	Disposition ApplyDisposition
	Assignment  domain.Assignment
}

func (s *Store) ApplyAssignment(ctx context.Context, source, messageID, messageType string, assignment domain.Assignment) (ApplyResult, error) {
	if strings.TrimSpace(source) == "" || strings.TrimSpace(messageID) == "" || strings.TrimSpace(messageType) == "" || strings.TrimSpace(assignment.ID) == "" || assignment.Generation == 0 || assignment.Generation > math.MaxInt64 || strings.TrimSpace(assignment.AircraftID) == "" || strings.TrimSpace(assignment.AgentID) == "" || strings.TrimSpace(assignment.FlightID) == "" || strings.TrimSpace(assignment.IntentID) == "" || assignment.IntentVersion == 0 || strings.TrimSpace(assignment.PolicyVersion) == "" || !assignment.EffectiveUntil.After(assignment.EffectiveFrom) {
		return ApplyResult{}, fmt.Errorf("assignment envelope is invalid")
	}
	payload, err := json.Marshal(assignment)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("encode assignment: %w", err)
	}
	envelope, err := json.Marshal(struct {
		MessageType string          `json:"message_type"`
		Assignment  json.RawMessage `json:"assignment"`
	}{MessageType: messageType, Assignment: payload})
	if err != nil {
		return ApplyResult{}, fmt.Errorf("encode assignment envelope: %w", err)
	}
	hashBytes := sha256.Sum256(envelope)
	hash := hex.EncodeToString(hashBytes[:])
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return ApplyResult{}, fmt.Errorf("begin apply assignment: %w", err)
	}
	defer tx.Rollback(ctx)
	// Serialize all generations for one logical assignment, including the case
	// where no row exists yet. Terminal rows must continue fencing stale events.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, assignment.ID); err != nil {
		return ApplyResult{}, fmt.Errorf("lock assignment: %w", err)
	}

	var existingHash string
	err = tx.QueryRow(ctx, `SELECT payload_sha256 FROM conformance_inbox WHERE source=$1 AND message_id=$2 FOR UPDATE`, source, messageID).Scan(&existingHash)
	if err == nil {
		if existingHash != hash {
			return ApplyResult{}, ErrMessageConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return ApplyResult{}, err
		}
		return ApplyResult{Disposition: ApplyIdempotent, Assignment: assignment}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ApplyResult{}, fmt.Errorf("read inbox: %w", err)
	}

	var current uint64
	var sameSpecification bool
	err = tx.QueryRow(ctx, `SELECT assignment_generation,specification=$2::jsonb FROM conformance_assignments WHERE assignment_id=$1 ORDER BY assignment_generation DESC LIMIT 1 FOR UPDATE`, assignment.ID, payload).Scan(&current, &sameSpecification)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return ApplyResult{}, fmt.Errorf("read live assignment: %w", err)
	}
	if err == nil && assignment.Generation < current {
		if _, err = tx.Exec(ctx, `INSERT INTO conformance_inbox(source,message_id,payload_sha256,message_type,assignment_id,assignment_generation,processed_at,outcome) VALUES($1,$2,$3,$4,$5,$6,now(),'{"disposition":"stale"}')`, source, messageID, hash, messageType, assignment.ID, assignment.Generation); err != nil {
			return ApplyResult{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return ApplyResult{}, err
		}
		return ApplyResult{Disposition: ApplyStale, Assignment: assignment}, nil
	}
	if err == nil && assignment.Generation == current {
		if !sameSpecification {
			return ApplyResult{}, fmt.Errorf("assignment generation %d changed specification: %w", current, ErrMessageConflict)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO conformance_inbox(source,message_id,payload_sha256,message_type,assignment_id,assignment_generation,processed_at,outcome) VALUES($1,$2,$3,$4,$5,$6,now(),'{"disposition":"idempotent"}')`, source, messageID, hash, messageType, assignment.ID, assignment.Generation); err != nil {
			return ApplyResult{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return ApplyResult{}, err
		}
		return ApplyResult{Disposition: ApplyIdempotent, Assignment: assignment}, nil
	}
	if err == nil {
		if _, err = tx.Exec(ctx, `UPDATE conformance_assignments SET lifecycle_state='superseded', lease_owner=NULL, lease_until=NULL, updated_at=now() WHERE assignment_id=$1 AND lifecycle_state IN ('received','armed','active','ending')`, assignment.ID); err != nil {
			return ApplyResult{}, err
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO conformance_assignments(assignment_id,assignment_generation,aircraft_id,agent_id,flight_id,intent_id,intent_version,policy_version,lifecycle_state,specification) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'received',$9)`, assignment.ID, assignment.Generation, assignment.AircraftID, assignment.AgentID, assignment.FlightID, assignment.IntentID, assignment.IntentVersion, assignment.PolicyVersion, payload); err != nil {
		return ApplyResult{}, fmt.Errorf("insert assignment: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO conformance_inbox(source,message_id,payload_sha256,message_type,assignment_id,assignment_generation,processed_at,outcome) VALUES($1,$2,$3,$4,$5,$6,now(),'{"disposition":"applied"}')`, source, messageID, hash, messageType, assignment.ID, assignment.Generation); err != nil {
		return ApplyResult{}, fmt.Errorf("record inbox: %w", err)
	}
	receiptID := stableID("assignment-received", source, messageID)
	if _, err = tx.Exec(ctx, `INSERT INTO conformance_outbox(outbox_id,destination,idempotency_key,assignment_id,assignment_generation,evaluation_revision,payload) VALUES($1,'api',$1,$2,$3,0,$4)`, receiptID, assignment.ID, assignment.Generation, payload); err != nil {
		return ApplyResult{}, fmt.Errorf("enqueue receipt: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return ApplyResult{}, fmt.Errorf("commit assignment: %w", err)
	}
	return ApplyResult{Disposition: ApplyApplied, Assignment: assignment}, nil
}

type Claim struct {
	Assignment         domain.Assignment
	WorkerID           string
	LeaseGeneration    uint64
	LeaseUntil         time.Time
	EvaluationRevision uint64
}

func (s *Store) ClaimDueAssignments(ctx context.Context, workerID string, lease time.Duration, limit int) ([]Claim, error) {
	if workerID == "" || lease <= 0 || limit < 1 {
		return nil, fmt.Errorf("claim arguments are invalid")
	}
	rows, err := s.pool.Query(ctx, `WITH due AS (
SELECT assignment_id,assignment_generation FROM conformance_assignments
WHERE lifecycle_state IN ('received','armed','active','ending') AND next_evaluation_at <= now() AND (lease_until IS NULL OR lease_until < now())
ORDER BY next_evaluation_at FOR UPDATE SKIP LOCKED LIMIT $1
), claimed AS (
UPDATE conformance_assignments a SET lease_owner=$2, lease_generation=a.lease_generation+1, lease_until=now()+$3::interval, updated_at=now()
FROM due WHERE a.assignment_id=due.assignment_id AND a.assignment_generation=due.assignment_generation
RETURNING a.specification,a.lease_generation,a.lease_until,a.evaluation_revision)
SELECT specification,lease_generation,lease_until,evaluation_revision FROM claimed`, limit, workerID, lease.String())
	if err != nil {
		return nil, fmt.Errorf("claim assignments: %w", err)
	}
	defer rows.Close()
	claims := make([]Claim, 0, limit)
	for rows.Next() {
		var raw []byte
		var c Claim
		c.WorkerID = workerID
		if err = rows.Scan(&raw, &c.LeaseGeneration, &c.LeaseUntil, &c.EvaluationRevision); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &c.Assignment); err != nil {
			return nil, fmt.Errorf("decode assignment: %w", err)
		}
		claims = append(claims, c)
	}
	return claims, rows.Err()
}

func (s *Store) RenewAssignmentLease(ctx context.Context, claim Claim, extension time.Duration) error {
	if extension <= 0 {
		return fmt.Errorf("lease extension must be positive")
	}
	tag, err := s.pool.Exec(ctx, `UPDATE conformance_assignments SET lease_until=now()+$1::interval,updated_at=now() WHERE assignment_id=$2 AND assignment_generation=$3 AND lease_owner=$4 AND lease_generation=$5 AND lease_until>now() AND lifecycle_state IN ('received','armed','active','ending')`, extension.String(), claim.Assignment.ID, claim.Assignment.Generation, claim.WorkerID, claim.LeaseGeneration)
	if err != nil {
		return fmt.Errorf("renew assignment lease: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

type EvaluationCommit struct {
	Evaluation       domain.Evaluation
	NextEvaluationAt time.Time
}

type ReplayCheckpoint struct {
	EvaluationRevision uint64
	StateThroughAt     time.Time
	WALID              string
	WALSequence        uint64
	FrameID            string
	State              domain.EvaluatorState
}

// GetReplayCheckpoint returns the newest retained evaluator snapshot at or
// before a replay boundary. A takeover restores this state, then deterministically
// replays the suffix after the returned cursor.
func (s *Store) GetReplayCheckpoint(ctx context.Context, assignmentID string, generation uint64, atOrBefore time.Time) (ReplayCheckpoint, bool, error) {
	var checkpoint ReplayCheckpoint
	var state []byte
	err := s.pool.QueryRow(ctx, `SELECT evaluation_revision,state_through_at,wal_id,wal_sequence,frame_id,evaluator_state FROM conformance_checkpoints WHERE assignment_id=$1 AND assignment_generation=$2 AND state_through_at<=$3 ORDER BY state_through_at DESC,wal_id DESC,wal_sequence DESC,frame_id DESC LIMIT 1`, assignmentID, generation, atOrBefore).Scan(&checkpoint.EvaluationRevision, &checkpoint.StateThroughAt, &checkpoint.WALID, &checkpoint.WALSequence, &checkpoint.FrameID, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReplayCheckpoint{}, false, nil
	}
	if err != nil {
		return ReplayCheckpoint{}, false, fmt.Errorf("read replay checkpoint: %w", err)
	}
	if err = json.Unmarshal(state, &checkpoint.State); err != nil {
		return ReplayCheckpoint{}, false, fmt.Errorf("decode replay checkpoint: %w", err)
	}
	return checkpoint, true, nil
}

func (s *Store) CommitEvaluation(ctx context.Context, claim Claim, commit EvaluationCommit) error {
	if commit.NextEvaluationAt.IsZero() || commit.Evaluation.ObservedAt.IsZero() || commit.Evaluation.FrameID == "" || commit.Evaluation.WALID == "" || commit.Evaluation.WALSequence > math.MaxInt64 {
		return fmt.Errorf("evaluation commit is incomplete")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var revision uint64
	err = tx.QueryRow(ctx, `UPDATE conformance_assignments SET evaluation_revision=evaluation_revision+1,next_evaluation_at=$1,lease_owner=NULL,lease_until=NULL,updated_at=now() WHERE assignment_id=$2 AND assignment_generation=$3 AND lease_owner=$4 AND lease_generation=$5 AND evaluation_revision=$6 AND lease_until>now() AND lifecycle_state IN ('received','armed','active','ending') RETURNING evaluation_revision`, commit.NextEvaluationAt, claim.Assignment.ID, claim.Assignment.Generation, claim.WorkerID, claim.LeaseGeneration, claim.EvaluationRevision).Scan(&revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("fence evaluation commit: %w", err)
	}
	var currentObservedAt time.Time
	err = tx.QueryRow(ctx, `SELECT observed_at FROM conformance_summaries WHERE assignment_id=$1 AND assignment_generation=$2 FOR UPDATE`, claim.Assignment.ID, claim.Assignment.Generation).Scan(&currentObservedAt)
	if err == nil && !commit.Evaluation.ObservedAt.After(currentObservedAt) {
		return ErrStaleEvaluation
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("read live watermark: %w", err)
	}
	commit.Evaluation.Recording = domain.RecordingConfirmed
	payload, err := json.Marshal(commit.Evaluation)
	if err != nil {
		return err
	}
	state, err := json.Marshal(commit.Evaluation.State)
	if err != nil {
		return err
	}
	for _, transition := range commit.Evaluation.Transitions {
		incidentKey := string(transition.Violation)
		incidentID := ""
		eventID := stableID("event", claim.Assignment.ID, fmt.Sprint(claim.Assignment.Generation), incidentKey, string(transition.Transition), transition.FrameID)
		if transition.Transition == domain.TransitionOpened {
			incidentID = stableID("incident", claim.Assignment.ID, fmt.Sprint(claim.Assignment.Generation), incidentKey, transition.FrameID)
			err = tx.QueryRow(ctx, `INSERT INTO conformance_incidents(incident_id,assignment_id,assignment_generation,incident_key,violation_type,state,severity,opened_at,last_observed_at,details) VALUES($1,$2,$3,$4,$4,'open','warning',$5,$5,$6) ON CONFLICT (assignment_id,assignment_generation,incident_key) WHERE state='open' DO UPDATE SET last_observed_at=EXCLUDED.last_observed_at,revision=conformance_incidents.revision+1,details=EXCLUDED.details RETURNING incident_id`, incidentID, claim.Assignment.ID, claim.Assignment.Generation, incidentKey, transition.ObservedAt, payload).Scan(&incidentID)
		} else if transition.Transition == domain.TransitionResolved {
			err = tx.QueryRow(ctx, `UPDATE conformance_incidents SET state='resolved',resolved_at=$1,last_observed_at=$1,revision=revision+1,details=$2 WHERE assignment_id=$3 AND assignment_generation=$4 AND incident_key=$5 AND state='open' RETURNING incident_id`, transition.ObservedAt, payload, claim.Assignment.ID, claim.Assignment.Generation, incidentKey).Scan(&incidentID)
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("resolve %s incident: open incident not found", incidentKey)
			}
		}
		if err != nil {
			return fmt.Errorf("mutate incident: %w", err)
		}
		eventHashBytes := sha256.Sum256(payload)
		eventHash := hex.EncodeToString(eventHashBytes[:])
		var returnedID string
		err = tx.QueryRow(ctx, `INSERT INTO conformance_events(event_id,assignment_id,assignment_generation,incident_id,transition,violation_type,observed_at,frame_id,wal_id,wal_sequence,evaluation_revision,payload,payload_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT(event_id) DO UPDATE SET event_id=EXCLUDED.event_id WHERE conformance_events.payload_sha256=EXCLUDED.payload_sha256 AND conformance_events.observed_at=EXCLUDED.observed_at AND conformance_events.frame_id=EXCLUDED.frame_id AND conformance_events.wal_id=EXCLUDED.wal_id AND conformance_events.wal_sequence=EXCLUDED.wal_sequence RETURNING event_id`, eventID, claim.Assignment.ID, claim.Assignment.Generation, incidentID, transition.Transition, transition.Violation, transition.ObservedAt, transition.FrameID, commit.Evaluation.WALID, commit.Evaluation.WALSequence, revision, payload, eventHash).Scan(&returnedID)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("event %s conflicts with immutable evidence: %w", eventID, ErrMessageConflict)
		}
		if err != nil {
			return fmt.Errorf("insert event: %w", err)
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO conformance_checkpoints(assignment_id,assignment_generation,evaluation_revision,state_through_at,wal_id,wal_sequence,frame_id,evaluator_state) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, claim.Assignment.ID, claim.Assignment.Generation, revision, commit.Evaluation.ObservedAt, commit.Evaluation.WALID, commit.Evaluation.WALSequence, commit.Evaluation.FrameID, state); err != nil {
		return fmt.Errorf("insert checkpoint: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO conformance_summaries(assignment_id,assignment_generation,evaluation_revision,condition,monitoring_status,recording_status,observed_at,frame_id,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(assignment_id,assignment_generation) DO UPDATE SET evaluation_revision=EXCLUDED.evaluation_revision,condition=EXCLUDED.condition,monitoring_status=EXCLUDED.monitoring_status,recording_status=EXCLUDED.recording_status,observed_at=EXCLUDED.observed_at,frame_id=EXCLUDED.frame_id,payload=EXCLUDED.payload,updated_at=now()`, claim.Assignment.ID, claim.Assignment.Generation, revision, commit.Evaluation.Condition, commit.Evaluation.Monitoring, domain.RecordingConfirmed, commit.Evaluation.ObservedAt, commit.Evaluation.FrameID, payload); err != nil {
		return fmt.Errorf("upsert summary: %w", err)
	}
	outboxID := fmt.Sprintf("registry:%s:%d:%d", claim.Assignment.ID, claim.Assignment.Generation, revision)
	if _, err = tx.Exec(ctx, `INSERT INTO conformance_outbox(outbox_id,destination,idempotency_key,assignment_id,assignment_generation,evaluation_revision,payload) VALUES($1,'registry',$1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, outboxID, claim.Assignment.ID, claim.Assignment.Generation, revision, payload); err != nil {
		return fmt.Errorf("enqueue live projection: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit evaluation: %w", err)
	}
	return nil
}

func stableID(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = fmt.Fprintf(h, "%d:%s", len(part), part)
	}
	return hex.EncodeToString(h.Sum(nil))
}
