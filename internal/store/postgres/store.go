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
	ErrLeaseLost         = errors.New("conformance assignment lease lost")
	ErrMessageConflict   = errors.New("inbox message ID reused with different payload")
	ErrStaleEvaluation   = errors.New("evaluation is older than the current live watermark")
	ErrInvalidTransition = errors.New("invalid assignment lifecycle transition")
	ErrStaleAssignment   = errors.New("assignment generation is stale")
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

// PrepareAssignment stores a candidate generation without changing the
// currently authoritative assignment. A prepared generation must be armed and
// explicitly cut over before workers may evaluate it.
func (s *Store) PrepareAssignment(ctx context.Context, source, messageID, messageType string, assignment domain.Assignment) (ApplyResult, error) {
	if strings.TrimSpace(source) == "" || strings.TrimSpace(messageID) == "" || strings.TrimSpace(messageType) == "" || strings.TrimSpace(assignment.ID) == "" || assignment.Generation == 0 || assignment.Generation > math.MaxInt64 || strings.TrimSpace(assignment.AircraftID) == "" || strings.TrimSpace(assignment.AgentID) == "" || strings.TrimSpace(assignment.FlightID) == "" || strings.TrimSpace(assignment.IntentID) == "" || assignment.IntentVersion == 0 || strings.TrimSpace(assignment.PolicyVersion) == "" || !assignment.EffectiveUntil.After(assignment.EffectiveFrom) || !supportedUnixNanoseconds(assignment.EffectiveFrom) || !supportedUnixNanoseconds(assignment.EffectiveUntil) {
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
	// The per-assignment advisory lock serializes generation changes. Read
	// committed is intentional: a transaction waiting on that lock must observe
	// the command committed by its predecessor for idempotent concurrent retries.
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ApplyResult{}, fmt.Errorf("begin apply assignment: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
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
	var currentAircraftID, currentFlightID, currentIntentID string
	err = tx.QueryRow(ctx, `SELECT assignment_generation,specification=$2::jsonb,aircraft_id,flight_id,intent_id FROM conformance_assignments WHERE assignment_id=$1 ORDER BY assignment_generation DESC LIMIT 1 FOR UPDATE`, assignment.ID, payload).Scan(&current, &sameSpecification, &currentAircraftID, &currentFlightID, &currentIntentID)
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
	// The logical assignment identity is immutable even after every previous
	// generation becomes terminal. Otherwise a delayed higher generation could
	// reuse an old assignment ID for unrelated authority.
	if err == nil && (assignment.AircraftID != currentAircraftID || assignment.FlightID != currentFlightID || assignment.IntentID != currentIntentID) {
		return ApplyResult{}, fmt.Errorf("assignment generation changed stable aircraft, flight, or intent identity: %w", ErrMessageConflict)
	}
	var replacedGeneration uint64
	var replacedState string
	replacedErr := tx.QueryRow(ctx, `SELECT assignment_generation,lifecycle_state FROM conformance_assignments WHERE assignment_id=$1 AND lifecycle_state IN ('candidate_received','candidate_armed') FOR UPDATE`, assignment.ID).Scan(&replacedGeneration, &replacedState)
	if replacedErr != nil && !errors.Is(replacedErr, pgx.ErrNoRows) {
		return ApplyResult{}, fmt.Errorf("read candidate assignment: %w", replacedErr)
	}
	if err == nil {
		// A newer candidate replaces only another candidate. The active generation
		// remains authoritative until an explicit cutover transaction.
		if _, err = tx.Exec(ctx, `UPDATE conformance_assignments SET lifecycle_state='superseded', lease_owner=NULL, lease_until=NULL, updated_at=now() WHERE assignment_id=$1 AND lifecycle_state IN ('candidate_received','candidate_armed')`, assignment.ID); err != nil {
			return ApplyResult{}, err
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO conformance_assignments(assignment_id,assignment_generation,aircraft_id,agent_id,flight_id,intent_id,intent_version,policy_version,lifecycle_state,specification) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'candidate_received',$9)`, assignment.ID, assignment.Generation, assignment.AircraftID, assignment.AgentID, assignment.FlightID, assignment.IntentID, assignment.IntentVersion, assignment.PolicyVersion, payload); err != nil {
		return ApplyResult{}, fmt.Errorf("insert assignment: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO conformance_inbox(source,message_id,payload_sha256,message_type,assignment_id,assignment_generation,processed_at,outcome) VALUES($1,$2,$3,$4,$5,$6,now(),'{"disposition":"applied"}')`, source, messageID, hash, messageType, assignment.ID, assignment.Generation); err != nil {
		return ApplyResult{}, fmt.Errorf("record inbox: %w", err)
	}
	eventPayload, err := json.Marshal(struct {
		EventType  string                     `json:"event_type"`
		Lifecycle  domain.AssignmentLifecycle `json:"lifecycle"`
		Assignment domain.Assignment          `json:"assignment"`
	}{EventType: "assignment_received", Lifecycle: domain.AssignmentReceived, Assignment: assignment})
	if err != nil {
		return ApplyResult{}, fmt.Errorf("encode preparation result: %w", err)
	}
	transitionID := stableID("assignment-transition", source, messageID, fmt.Sprint(assignment.Generation), string(domain.AssignmentReceived))
	if _, err = tx.Exec(ctx, `INSERT INTO conformance_assignment_transitions(transition_id,source,message_id,assignment_id,assignment_generation,to_state,payload) VALUES($1,$2,$3,$4,$5,'candidate_received',$6)`, transitionID, source, messageID, assignment.ID, assignment.Generation, eventPayload); err != nil {
		return ApplyResult{}, fmt.Errorf("record preparation transition: %w", err)
	}
	if replacedErr == nil {
		replacedTransitionID := stableID("assignment-transition", source, messageID, fmt.Sprint(replacedGeneration), string(domain.AssignmentSuperseded))
		if _, err = tx.Exec(ctx, `INSERT INTO conformance_assignment_transitions(transition_id,source,message_id,assignment_id,assignment_generation,from_state,to_state,payload) VALUES($1,$2,$3,$4,$5,$6,'superseded',$7)`, replacedTransitionID, source, messageID, assignment.ID, replacedGeneration, replacedState, eventPayload); err != nil {
			return ApplyResult{}, fmt.Errorf("record replaced candidate transition: %w", err)
		}
	}
	receiptID := stableID("assignment-received", source, messageID)
	if _, err = tx.Exec(ctx, `INSERT INTO conformance_outbox(outbox_id,destination,idempotency_key,assignment_id,assignment_generation,evaluation_revision,payload) VALUES($1,'api',$1,$2,$3,0,$4)`, receiptID, assignment.ID, assignment.Generation, eventPayload); err != nil {
		return ApplyResult{}, fmt.Errorf("enqueue receipt: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return ApplyResult{}, fmt.Errorf("commit assignment: %w", err)
	}
	return ApplyResult{Disposition: ApplyApplied, Assignment: assignment}, nil
}

// ApplyAssignment is retained for the initial internal callers. Its semantics
// are preparation-only; it never supersedes the current authority.
func (s *Store) ApplyAssignment(ctx context.Context, source, messageID, messageType string, assignment domain.Assignment) (ApplyResult, error) {
	return s.PrepareAssignment(ctx, source, messageID, messageType, assignment)
}

type LifecycleResult struct {
	Disposition ApplyDisposition
	Record      domain.AssignmentRecord
}

type lifecycleCommand struct {
	AssignmentID string     `json:"assignment_id"`
	Generation   uint64     `json:"assignment_generation"`
	EffectiveAt  *time.Time `json:"effective_at,omitempty"`
}

// ArmAssignment marks a prepared generation ready for cutover. It does not
// grant authority and does not make the generation claimable by evaluators.
func (s *Store) ArmAssignment(ctx context.Context, source, messageID, assignmentID string, generation uint64) (LifecycleResult, error) {
	return s.transitionCandidate(ctx, source, messageID, "assignment_armed", assignmentID, generation, domain.AssignmentArmed)
}

// CancelCandidate discards a received or armed replacement without disturbing
// the active generation.
func (s *Store) CancelCandidate(ctx context.Context, source, messageID, assignmentID string, generation uint64) (LifecycleResult, error) {
	return s.transitionCandidate(ctx, source, messageID, "assignment_cancelled", assignmentID, generation, domain.AssignmentCancelled)
}

func (s *Store) transitionCandidate(ctx context.Context, source, messageID, messageType, assignmentID string, generation uint64, target domain.AssignmentLifecycle) (LifecycleResult, error) {
	if strings.TrimSpace(source) == "" || strings.TrimSpace(messageID) == "" || strings.TrimSpace(assignmentID) == "" || generation == 0 || generation > math.MaxInt64 {
		return LifecycleResult{}, fmt.Errorf("assignment lifecycle command is invalid")
	}
	command := lifecycleCommand{AssignmentID: assignmentID, Generation: generation}
	payload, hash, err := encodeCommand(messageType, command)
	if err != nil {
		return LifecycleResult{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return LifecycleResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, assignmentID); err != nil {
		return LifecycleResult{}, err
	}
	if existing, found, err := readInboxHash(ctx, tx, source, messageID); err != nil {
		return LifecycleResult{}, err
	} else if found {
		if existing != hash {
			return LifecycleResult{}, ErrMessageConflict
		}
		record, err := readAssignmentRecord(ctx, tx, assignmentID, generation)
		if err != nil {
			return LifecycleResult{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return LifecycleResult{}, err
		}
		return LifecycleResult{Disposition: ApplyIdempotent, Record: record}, nil
	}

	record, err := readAssignmentRecord(ctx, tx, assignmentID, generation)
	if err != nil {
		return LifecycleResult{}, err
	}
	var latest uint64
	if err = tx.QueryRow(ctx, `SELECT max(assignment_generation) FROM conformance_assignments WHERE assignment_id=$1`, assignmentID).Scan(&latest); err != nil {
		return LifecycleResult{}, err
	}
	if generation != latest {
		return LifecycleResult{}, ErrStaleAssignment
	}
	from := record.Lifecycle
	valid := (target == domain.AssignmentArmed && from == domain.AssignmentReceived) || (target == domain.AssignmentCancelled && (from == domain.AssignmentReceived || from == domain.AssignmentArmed))
	if !valid {
		return LifecycleResult{}, fmt.Errorf("%w: %s to %s", ErrInvalidTransition, from, target)
	}
	if target == domain.AssignmentArmed {
		_, err = tx.Exec(ctx, `UPDATE conformance_assignments SET lifecycle_state='candidate_armed',armed_at=now(),updated_at=now() WHERE assignment_id=$1 AND assignment_generation=$2 AND lifecycle_state='candidate_received'`, assignmentID, generation)
	} else {
		_, err = tx.Exec(ctx, `UPDATE conformance_assignments SET lifecycle_state='cancelled',lease_owner=NULL,lease_until=NULL,updated_at=now() WHERE assignment_id=$1 AND assignment_generation=$2 AND lifecycle_state IN ('candidate_received','candidate_armed')`, assignmentID, generation)
	}
	if err != nil {
		return LifecycleResult{}, err
	}
	if err = recordLifecycleCommand(ctx, tx, source, messageID, messageType, assignmentID, generation, string(from), string(target), nil, payload, hash); err != nil {
		return LifecycleResult{}, err
	}
	record, err = readAssignmentRecord(ctx, tx, assignmentID, generation)
	if err != nil {
		return LifecycleResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return LifecycleResult{}, err
	}
	return LifecycleResult{Disposition: ApplyApplied, Record: record}, nil
}

// CutoverAssignment atomically closes the current generation's authority
// interval and activates an armed candidate at effectiveAt. effectiveAt is an
// event-time boundary; late observations before it continue to resolve to the
// superseded generation.
func (s *Store) CutoverAssignment(ctx context.Context, source, messageID, assignmentID string, generation uint64, effectiveAt time.Time) (LifecycleResult, error) {
	if strings.TrimSpace(source) == "" || strings.TrimSpace(messageID) == "" || strings.TrimSpace(assignmentID) == "" || generation == 0 || generation > math.MaxInt64 || effectiveAt.IsZero() {
		return LifecycleResult{}, fmt.Errorf("assignment cutover command is invalid")
	}
	effectiveAt = effectiveAt.UTC()
	effectiveUnixNS := effectiveAt.UnixNano()
	if !supportedUnixNanoseconds(effectiveAt) {
		return LifecycleResult{}, fmt.Errorf("assignment cutover is outside the supported nanosecond timestamp range")
	}
	command := lifecycleCommand{AssignmentID: assignmentID, Generation: generation, EffectiveAt: &effectiveAt}
	payload, hash, err := encodeCommand("assignment_cutover", command)
	if err != nil {
		return LifecycleResult{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return LifecycleResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, assignmentID); err != nil {
		return LifecycleResult{}, err
	}
	if existing, found, err := readInboxHash(ctx, tx, source, messageID); err != nil {
		return LifecycleResult{}, err
	} else if found {
		if existing != hash {
			return LifecycleResult{}, ErrMessageConflict
		}
		record, err := readAssignmentRecord(ctx, tx, assignmentID, generation)
		if err != nil {
			return LifecycleResult{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return LifecycleResult{}, err
		}
		return LifecycleResult{Disposition: ApplyIdempotent, Record: record}, nil
	}
	var databaseNow time.Time
	if err = tx.QueryRow(ctx, `SELECT now()`).Scan(&databaseNow); err != nil {
		return LifecycleResult{}, err
	}
	if effectiveAt.After(databaseNow) {
		return LifecycleResult{}, fmt.Errorf("%w: future cutovers must be delivered when authoritative", ErrInvalidTransition)
	}
	candidate, err := readAssignmentRecord(ctx, tx, assignmentID, generation)
	if err != nil {
		return LifecycleResult{}, err
	}
	if candidate.Lifecycle != domain.AssignmentArmed {
		return LifecycleResult{}, fmt.Errorf("%w: %s is not armed", ErrInvalidTransition, candidate.Lifecycle)
	}
	if effectiveAt.Before(candidate.Assignment.EffectiveFrom) || !effectiveAt.Before(candidate.Assignment.EffectiveUntil) {
		return LifecycleResult{}, fmt.Errorf("%w: cutover is outside candidate effective window", ErrInvalidTransition)
	}
	var currentGeneration uint64
	var currentAuthorityFromUnixNS int64
	var currentAuthorityUntilUnixNS int64
	var currentLifecycle string
	var currentEvaluationWatermarkUnixNS *int64
	err = tx.QueryRow(ctx, `SELECT assignment_generation,authority_from_unix_ns,authority_until_unix_ns,lifecycle_state FROM conformance_assignments WHERE assignment_id=$1 AND lifecycle_state IN ('active','ending') FOR UPDATE`, assignmentID).Scan(&currentGeneration, &currentAuthorityFromUnixNS, &currentAuthorityUntilUnixNS, &currentLifecycle)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return LifecycleResult{}, err
	}
	if err == nil {
		// Read the watermark in a separate READ COMMITTED statement after the
		// assignment lock. If an evaluation commit won the lock first, this fresh
		// snapshot must observe the summary committed in that same transaction.
		err = tx.QueryRow(ctx, `SELECT observed_at_unix_ns FROM conformance_summaries WHERE assignment_id=$1 AND assignment_generation=$2`, assignmentID, currentGeneration).Scan(&currentEvaluationWatermarkUnixNS)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return LifecycleResult{}, err
		}
		if currentGeneration >= generation || effectiveUnixNS <= currentAuthorityFromUnixNS || effectiveUnixNS > currentAuthorityUntilUnixNS {
			return LifecycleResult{}, ErrStaleAssignment
		}
		// The old generation's summary is its monotonic durable event-time
		// watermark. Rejecting equality is intentional: authority_until is
		// exclusive, and committed evidence at the boundary cannot be reassigned
		// after its Registry outbox may already have been delivered.
		if currentEvaluationWatermarkUnixNS != nil && *currentEvaluationWatermarkUnixNS >= effectiveUnixNS {
			return LifecycleResult{}, fmt.Errorf("%w: cutover precedes committed evaluation watermark", ErrInvalidTransition)
		}
		if _, err = tx.Exec(ctx, `UPDATE conformance_assignments SET lifecycle_state='superseded',authority_until=$1,authority_until_unix_ns=$2,lease_owner=NULL,lease_until=NULL,updated_at=now() WHERE assignment_id=$3 AND assignment_generation=$4 AND lifecycle_state IN ('active','ending')`, effectiveAt, effectiveUnixNS, assignmentID, currentGeneration); err != nil {
			return LifecycleResult{}, err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE conformance_assignments SET lifecycle_state='active',authority_from=$1,authority_from_unix_ns=$2,authority_until=$3,authority_until_unix_ns=$4,cutover_at=$1,next_evaluation_at=now(),lease_owner=NULL,lease_until=NULL,updated_at=now() WHERE assignment_id=$5 AND assignment_generation=$6 AND lifecycle_state='candidate_armed'`, effectiveAt, effectiveUnixNS, candidate.Assignment.EffectiveUntil, candidate.Assignment.EffectiveUntil.UnixNano(), assignmentID, generation); err != nil {
		return LifecycleResult{}, err
	}
	if err = recordLifecycleCommand(ctx, tx, source, messageID, "assignment_cutover", assignmentID, generation, string(domain.AssignmentArmed), string(domain.AssignmentActive), &effectiveAt, payload, hash); err != nil {
		return LifecycleResult{}, err
	}
	if currentGeneration != 0 {
		oldTransitionID := stableID("assignment-transition", source, messageID, fmt.Sprint(currentGeneration), string(domain.AssignmentSuperseded))
		if _, err = tx.Exec(ctx, `INSERT INTO conformance_assignment_transitions(transition_id,source,message_id,assignment_id,assignment_generation,from_state,to_state,effective_at,payload) VALUES($1,$2,$3,$4,$5,$6,'superseded',$7,$8)`, oldTransitionID, source, messageID, assignmentID, currentGeneration, currentLifecycle, effectiveAt, payload); err != nil {
			return LifecycleResult{}, fmt.Errorf("record superseded current transition: %w", err)
		}
	}
	candidate, err = readAssignmentRecord(ctx, tx, assignmentID, generation)
	if err != nil {
		return LifecycleResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return LifecycleResult{}, err
	}
	return LifecycleResult{Disposition: ApplyApplied, Record: candidate}, nil
}

// ResolveAssignmentAt returns the immutable generation authoritative for an
// observation timestamp. Historical superseded generations remain resolvable.
func (s *Store) ResolveAssignmentAt(ctx context.Context, assignmentID string, observedAt time.Time) (domain.AssignmentRecord, bool, error) {
	if !supportedUnixNanoseconds(observedAt) {
		return domain.AssignmentRecord{}, false, fmt.Errorf("observation is outside the supported nanosecond timestamp range")
	}
	observedUnixNS := observedAt.UnixNano()
	row := s.pool.QueryRow(ctx, `SELECT specification,lifecycle_state,authority_from_unix_ns,authority_until_unix_ns,prepared_at,armed_at,cutover_at FROM conformance_assignments WHERE assignment_id=$1 AND authority_from_unix_ns<=$2 AND (authority_until_unix_ns IS NULL OR $2<authority_until_unix_ns) ORDER BY authority_from_unix_ns DESC LIMIT 1`, assignmentID, observedUnixNS)
	record, err := scanAssignmentRecord(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AssignmentRecord{}, false, nil
	}
	if err != nil {
		return domain.AssignmentRecord{}, false, fmt.Errorf("resolve assignment at observation: %w", err)
	}
	return record, true, nil
}

func supportedUnixNanoseconds(value time.Time) bool {
	return value.Year() >= 1678 && value.Year() <= 2261
}

type rowScanner interface {
	Scan(...any) error
}

func scanAssignmentRecord(row rowScanner) (domain.AssignmentRecord, error) {
	var record domain.AssignmentRecord
	var raw []byte
	var authorityFromUnixNS, authorityUntilUnixNS *int64
	if err := row.Scan(&raw, &record.Lifecycle, &authorityFromUnixNS, &authorityUntilUnixNS, &record.PreparedAt, &record.ArmedAt, &record.CutoverAt); err != nil {
		return domain.AssignmentRecord{}, err
	}
	if authorityFromUnixNS != nil {
		value := time.Unix(0, *authorityFromUnixNS).UTC()
		record.AuthorityFrom = &value
	}
	if authorityUntilUnixNS != nil {
		value := time.Unix(0, *authorityUntilUnixNS).UTC()
		record.AuthorityUntil = &value
	}
	if err := json.Unmarshal(raw, &record.Assignment); err != nil {
		return domain.AssignmentRecord{}, fmt.Errorf("decode assignment record: %w", err)
	}
	return record, nil
}

func readAssignmentRecord(ctx context.Context, tx pgx.Tx, assignmentID string, generation uint64) (domain.AssignmentRecord, error) {
	record, err := scanAssignmentRecord(tx.QueryRow(ctx, `SELECT specification,lifecycle_state,authority_from_unix_ns,authority_until_unix_ns,prepared_at,armed_at,cutover_at FROM conformance_assignments WHERE assignment_id=$1 AND assignment_generation=$2 FOR UPDATE`, assignmentID, generation))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AssignmentRecord{}, fmt.Errorf("assignment %s generation %d: %w", assignmentID, generation, ErrInvalidTransition)
	}
	return record, err
}

func readInboxHash(ctx context.Context, tx pgx.Tx, source, messageID string) (string, bool, error) {
	var hash string
	err := tx.QueryRow(ctx, `SELECT payload_sha256 FROM conformance_inbox WHERE source=$1 AND message_id=$2 FOR UPDATE`, source, messageID).Scan(&hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read lifecycle inbox: %w", err)
	}
	return hash, true, nil
}

func encodeCommand(messageType string, command lifecycleCommand) ([]byte, string, error) {
	payload, err := json.Marshal(command)
	if err != nil {
		return nil, "", fmt.Errorf("encode lifecycle command: %w", err)
	}
	envelope, err := json.Marshal(struct {
		MessageType string          `json:"message_type"`
		Command     json.RawMessage `json:"command"`
	}{MessageType: messageType, Command: payload})
	if err != nil {
		return nil, "", fmt.Errorf("encode lifecycle envelope: %w", err)
	}
	hashBytes := sha256.Sum256(envelope)
	return envelope, hex.EncodeToString(hashBytes[:]), nil
}

func recordLifecycleCommand(ctx context.Context, tx pgx.Tx, source, messageID, messageType, assignmentID string, generation uint64, from, to string, effectiveAt *time.Time, payload []byte, hash string) error {
	outcome, err := json.Marshal(map[string]string{"disposition": "applied", "lifecycle": to})
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO conformance_inbox(source,message_id,payload_sha256,message_type,assignment_id,assignment_generation,processed_at,outcome) VALUES($1,$2,$3,$4,$5,$6,now(),$7)`, source, messageID, hash, messageType, assignmentID, generation, outcome); err != nil {
		return fmt.Errorf("record lifecycle inbox: %w", err)
	}
	transitionID := stableID("assignment-transition", source, messageID, fmt.Sprint(generation), to)
	if _, err = tx.Exec(ctx, `INSERT INTO conformance_assignment_transitions(transition_id,source,message_id,assignment_id,assignment_generation,from_state,to_state,effective_at,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, transitionID, source, messageID, assignmentID, generation, from, to, effectiveAt, payload); err != nil {
		return fmt.Errorf("record lifecycle transition: %w", err)
	}
	outboxID := stableID("assignment-lifecycle", source, messageID)
	if _, err = tx.Exec(ctx, `INSERT INTO conformance_outbox(outbox_id,destination,idempotency_key,assignment_id,assignment_generation,evaluation_revision,payload) VALUES($1,'api',$1,$2,$3,0,$4)`, outboxID, assignmentID, generation, payload); err != nil {
		return fmt.Errorf("enqueue lifecycle result: %w", err)
	}
	return nil
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
WHERE lifecycle_state IN ('active','ending') AND authority_until>now() AND next_evaluation_at <= now() AND (lease_until IS NULL OR lease_until < now())
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
	tag, err := s.pool.Exec(ctx, `UPDATE conformance_assignments SET lease_until=now()+$1::interval,updated_at=now() WHERE assignment_id=$2 AND assignment_generation=$3 AND lease_owner=$4 AND lease_generation=$5 AND lease_until>now() AND authority_until>now() AND lifecycle_state IN ('active','ending')`, extension.String(), claim.Assignment.ID, claim.Assignment.Generation, claim.WorkerID, claim.LeaseGeneration)
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
	if err := validateEvaluationCommit(commit); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var revision uint64
	var authorityFromUnixNS, authorityUntilUnixNS int64
	err = tx.QueryRow(ctx, `UPDATE conformance_assignments SET evaluation_revision=evaluation_revision+1,next_evaluation_at=$1,lease_owner=NULL,lease_until=NULL,updated_at=now() WHERE assignment_id=$2 AND assignment_generation=$3 AND lease_owner=$4 AND lease_generation=$5 AND evaluation_revision=$6 AND lease_until>now() AND lifecycle_state IN ('active','ending') AND authority_from_unix_ns<=$7 AND $7<authority_until_unix_ns RETURNING evaluation_revision,authority_from_unix_ns,authority_until_unix_ns`, commit.NextEvaluationAt, claim.Assignment.ID, claim.Assignment.Generation, claim.WorkerID, claim.LeaseGeneration, claim.EvaluationRevision, commit.Evaluation.ObservedAt.UnixNano()).Scan(&revision, &authorityFromUnixNS, &authorityUntilUnixNS)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("fence evaluation commit: %w", err)
	}
	if err = validateEvaluationWithinAuthority(commit.Evaluation, authorityFromUnixNS, authorityUntilUnixNS); err != nil {
		return err
	}
	if err = writeEvaluation(ctx, tx, claim.Assignment, revision, commit.Evaluation, false, true); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit evaluation: %w", err)
	}
	return nil
}

// CommitHistoricalEvaluation persists an event-time reconciliation for a
// superseded generation while fencing the transaction with the lease of the
// currently active generation. It never revives the historical worker and
// never publishes the historical summary as the current Registry projection.
func (s *Store) CommitHistoricalEvaluation(ctx context.Context, current Claim, historicalGeneration uint64, commit EvaluationCommit) error {
	if err := validateEvaluationCommit(commit); err != nil {
		return err
	}
	if historicalGeneration == 0 || historicalGeneration > math.MaxInt64 || historicalGeneration >= current.Assignment.Generation || !supportedUnixNanoseconds(commit.Evaluation.ObservedAt) {
		return fmt.Errorf("historical evaluation target is invalid")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Consuming the current lease makes this a one-shot fenced reconciliation.
	// The current live evaluation revision is intentionally unchanged because
	// historical evidence must not advance or replace the live projection.
	tag, err := tx.Exec(ctx, `UPDATE conformance_assignments SET next_evaluation_at=$1,lease_owner=NULL,lease_until=NULL,updated_at=now() WHERE assignment_id=$2 AND assignment_generation=$3 AND lease_owner=$4 AND lease_generation=$5 AND evaluation_revision=$6 AND lease_until>now() AND authority_until>now() AND lifecycle_state IN ('active','ending')`, commit.NextEvaluationAt, current.Assignment.ID, current.Assignment.Generation, current.WorkerID, current.LeaseGeneration, current.EvaluationRevision)
	if err != nil {
		return fmt.Errorf("fence historical evaluation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	var raw []byte
	var revision uint64
	var authorityFromUnixNS, authorityUntilUnixNS int64
	observedUnixNS := commit.Evaluation.ObservedAt.UnixNano()
	err = tx.QueryRow(ctx, `UPDATE conformance_assignments SET evaluation_revision=evaluation_revision+1,updated_at=now() WHERE assignment_id=$1 AND assignment_generation=$2 AND lifecycle_state='superseded' AND authority_from_unix_ns<=$3 AND $3<authority_until_unix_ns RETURNING specification,evaluation_revision,authority_from_unix_ns,authority_until_unix_ns`, current.Assignment.ID, historicalGeneration, observedUnixNS).Scan(&raw, &revision, &authorityFromUnixNS, &authorityUntilUnixNS)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("historical generation does not authorize the observation: %w", ErrInvalidTransition)
	}
	if err != nil {
		return fmt.Errorf("fence historical generation: %w", err)
	}
	if err = validateEvaluationWithinAuthority(commit.Evaluation, authorityFromUnixNS, authorityUntilUnixNS); err != nil {
		return err
	}
	var historical domain.Assignment
	if err = json.Unmarshal(raw, &historical); err != nil {
		return fmt.Errorf("decode historical assignment: %w", err)
	}
	if err = writeEvaluation(ctx, tx, historical, revision, commit.Evaluation, true, false); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit historical evaluation: %w", err)
	}
	return nil
}

func validateEvaluationCommit(commit EvaluationCommit) error {
	if commit.NextEvaluationAt.IsZero() || commit.Evaluation.ObservedAt.IsZero() || !supportedUnixNanoseconds(commit.Evaluation.ObservedAt) || commit.Evaluation.FrameID == "" || commit.Evaluation.WALID == "" || commit.Evaluation.WALSequence > math.MaxInt64 {
		return fmt.Errorf("evaluation commit is incomplete")
	}
	for _, transition := range commit.Evaluation.Transitions {
		if transition.ObservedAt.IsZero() || !supportedUnixNanoseconds(transition.ObservedAt) || transition.ObservedAt.After(commit.Evaluation.ObservedAt) || transition.FrameID == "" || transition.OpeningFrameID == "" || transition.WALID == "" || transition.WALSequence > math.MaxInt64 {
			return fmt.Errorf("evaluation transition is incomplete or beyond its watermark")
		}
	}
	return nil
}

// validateEvaluationWithinAuthority prevents an overlap/replay batch from
// attributing causal state to the wrong assignment generation. The final
// watermark, every transition, and every retained state timestamp must all be
// inside the same stored half-open authority interval.
func validateEvaluationWithinAuthority(evaluation domain.Evaluation, authorityFromUnixNS, authorityUntilUnixNS int64) error {
	if authorityUntilUnixNS <= authorityFromUnixNS {
		return fmt.Errorf("assignment authority interval is invalid: %w", ErrInvalidTransition)
	}
	validateTimestamp := func(label string, value time.Time) error {
		if value.IsZero() {
			return nil
		}
		if !supportedUnixNanoseconds(value) || value.After(evaluation.ObservedAt) {
			return fmt.Errorf("%s is invalid or beyond the evaluation watermark: %w", label, ErrInvalidTransition)
		}
		unixNS := value.UnixNano()
		if unixNS < authorityFromUnixNS || unixNS >= authorityUntilUnixNS {
			return fmt.Errorf("%s is outside assignment authority: %w", label, ErrInvalidTransition)
		}
		return nil
	}
	if err := validateTimestamp("evaluation watermark", evaluation.ObservedAt); err != nil {
		return err
	}
	for index, transition := range evaluation.Transitions {
		if err := validateTimestamp(fmt.Sprintf("transition %d timestamp", index), transition.ObservedAt); err != nil {
			return err
		}
	}
	for violation, state := range evaluation.State.Violations {
		for label, value := range map[string]time.Time{
			"first suspected": state.FirstSuspectedAt,
			"opened":          state.OpenedAt,
			"last observed":   state.LastObservedAt,
		} {
			if err := validateTimestamp(fmt.Sprintf("%s %s state timestamp", violation, label), value); err != nil {
				return err
			}
		}
	}
	return nil
}

type transitionEvidence struct {
	SchemaVersion  uint32               `json:"schema_version"`
	IncidentID     string               `json:"incident_id"`
	Violation      domain.ViolationType `json:"violation"`
	Transition     domain.Transition    `json:"transition"`
	ObservedAt     time.Time            `json:"observed_at"`
	FrameID        string               `json:"frame_id"`
	OpeningFrameID string               `json:"opening_frame_id"`
	WALID          string               `json:"wal_id"`
	WALSequence    uint64               `json:"wal_sequence"`
	DeviationM     float64              `json:"deviation_m"`
}

func writeEvaluation(ctx context.Context, tx pgx.Tx, assignment domain.Assignment, revision uint64, evaluation domain.Evaluation, allowEqualWatermark, publishLive bool) error {
	var currentObservedUnixNS int64
	err := tx.QueryRow(ctx, `SELECT observed_at_unix_ns FROM conformance_summaries WHERE assignment_id=$1 AND assignment_generation=$2 FOR UPDATE`, assignment.ID, assignment.Generation).Scan(&currentObservedUnixNS)
	evaluationUnixNS := evaluation.ObservedAt.UnixNano()
	if err == nil && (evaluationUnixNS < currentObservedUnixNS || (!allowEqualWatermark && evaluationUnixNS == currentObservedUnixNS)) {
		return ErrStaleEvaluation
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("read live watermark: %w", err)
	}
	evaluation.Recording = domain.RecordingConfirmed
	payload, err := json.Marshal(evaluation)
	if err != nil {
		return err
	}
	state, err := json.Marshal(evaluation.State)
	if err != nil {
		return err
	}
	for _, transition := range evaluation.Transitions {
		incidentKey := string(transition.Violation)
		incidentID := stableID("incident", assignment.ID, fmt.Sprint(assignment.Generation), incidentKey, transition.OpeningFrameID)
		// The frame ID makes transition evidence stable across deterministic replay;
		// the incident ID predicate below also fences recurrence identity.
		eventID := stableID("event", assignment.ID, fmt.Sprint(assignment.Generation), incidentKey, string(transition.Transition), transition.FrameID)
		if transition.Transition == domain.TransitionOpened {
			err = tx.QueryRow(ctx, `INSERT INTO conformance_incidents(incident_id,assignment_id,assignment_generation,incident_key,violation_type,state,severity,opened_at,last_observed_at,details,opening_frame_id) VALUES($1,$2,$3,$4,$4,'open','warning',$5,$5,$6,$7)
ON CONFLICT (incident_id) DO UPDATE SET incident_id=EXCLUDED.incident_id
WHERE conformance_incidents.assignment_id=EXCLUDED.assignment_id
  AND conformance_incidents.assignment_generation=EXCLUDED.assignment_generation
  AND conformance_incidents.incident_key=EXCLUDED.incident_key
  AND conformance_incidents.violation_type=EXCLUDED.violation_type
  AND conformance_incidents.opened_at=EXCLUDED.opened_at
  AND conformance_incidents.opening_frame_id=EXCLUDED.opening_frame_id
RETURNING incident_id`, incidentID, assignment.ID, assignment.Generation, incidentKey, transition.ObservedAt, payload, transition.OpeningFrameID).Scan(&incidentID)
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("incident %s conflicts with immutable opening evidence: %w", incidentID, ErrMessageConflict)
			}
		} else if transition.Transition == domain.TransitionResolved {
			err = tx.QueryRow(ctx, `SELECT incident_id FROM conformance_incidents WHERE assignment_id=$1 AND assignment_generation=$2 AND incident_key=$3 AND opening_frame_id=$4 AND opened_at<=$5 FOR UPDATE`, assignment.ID, assignment.Generation, incidentKey, transition.OpeningFrameID, transition.ObservedAt).Scan(&incidentID)
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("resolve %s incident occurrence: matching opening not found", incidentKey)
			}
		}
		if err != nil {
			return fmt.Errorf("mutate incident: %w", err)
		}
		evidencePayload, marshalErr := json.Marshal(transitionEvidence{SchemaVersion: 2, IncidentID: incidentID, Violation: transition.Violation, Transition: transition.Transition, ObservedAt: transition.ObservedAt, FrameID: transition.FrameID, OpeningFrameID: transition.OpeningFrameID, WALID: transition.WALID, WALSequence: transition.WALSequence, DeviationM: transition.DeviationM})
		if marshalErr != nil {
			return fmt.Errorf("encode transition evidence: %w", marshalErr)
		}
		eventHashBytes := sha256.Sum256(evidencePayload)
		eventHash := hex.EncodeToString(eventHashBytes[:])
		var returnedID string
		err = tx.QueryRow(ctx, `INSERT INTO conformance_events(event_id,assignment_id,assignment_generation,incident_id,transition,violation_type,observed_at,frame_id,wal_id,wal_sequence,evaluation_revision,payload,payload_sha256,deviation_m,evidence_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,2)
ON CONFLICT(event_id) DO UPDATE SET event_id=EXCLUDED.event_id
WHERE conformance_events.assignment_id=EXCLUDED.assignment_id
  AND conformance_events.assignment_generation=EXCLUDED.assignment_generation
  AND conformance_events.incident_id=EXCLUDED.incident_id
  AND conformance_events.transition=EXCLUDED.transition
  AND conformance_events.violation_type=EXCLUDED.violation_type
  AND conformance_events.observed_at=EXCLUDED.observed_at
  AND conformance_events.frame_id=EXCLUDED.frame_id
  AND conformance_events.wal_id=EXCLUDED.wal_id
  AND conformance_events.wal_sequence=EXCLUDED.wal_sequence
  AND conformance_events.evidence_version=2
  AND conformance_events.deviation_m=EXCLUDED.deviation_m
  AND conformance_events.payload_sha256=EXCLUDED.payload_sha256
RETURNING event_id`, eventID, assignment.ID, assignment.Generation, incidentID, transition.Transition, transition.Violation, transition.ObservedAt, transition.FrameID, transition.WALID, transition.WALSequence, revision, evidencePayload, eventHash, transition.DeviationM).Scan(&returnedID)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("event %s conflicts with immutable evidence: %w", eventID, ErrMessageConflict)
		}
		if err != nil {
			return fmt.Errorf("insert event: %w", err)
		}
		if transition.Transition == domain.TransitionResolved {
			tag, updateErr := tx.Exec(ctx, `UPDATE conformance_incidents SET state='resolved',resolved_at=$1,resolution_event_id=$2,last_observed_at=$1,revision=revision+CASE WHEN resolution_event_id IS DISTINCT FROM $2 THEN 1 ELSE 0 END,details=$3 WHERE incident_id=$4 AND assignment_id=$5 AND assignment_generation=$6 AND incident_key=$7`, transition.ObservedAt, returnedID, payload, incidentID, assignment.ID, assignment.Generation, incidentKey)
			if updateErr != nil {
				return fmt.Errorf("materialize incident resolution: %w", updateErr)
			}
			if tag.RowsAffected() != 1 {
				return fmt.Errorf("materialize incident resolution: matching occurrence not found")
			}
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO conformance_checkpoints(assignment_id,assignment_generation,evaluation_revision,state_through_at,wal_id,wal_sequence,frame_id,evaluator_state) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, assignment.ID, assignment.Generation, revision, evaluation.ObservedAt, evaluation.WALID, evaluation.WALSequence, evaluation.FrameID, state); err != nil {
		return fmt.Errorf("insert checkpoint: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO conformance_summaries(assignment_id,assignment_generation,evaluation_revision,condition,monitoring_status,recording_status,observed_at,observed_at_unix_ns,frame_id,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(assignment_id,assignment_generation) DO UPDATE SET evaluation_revision=EXCLUDED.evaluation_revision,condition=EXCLUDED.condition,monitoring_status=EXCLUDED.monitoring_status,recording_status=EXCLUDED.recording_status,observed_at=EXCLUDED.observed_at,observed_at_unix_ns=EXCLUDED.observed_at_unix_ns,frame_id=EXCLUDED.frame_id,payload=EXCLUDED.payload,updated_at=now()`, assignment.ID, assignment.Generation, revision, evaluation.Condition, evaluation.Monitoring, domain.RecordingConfirmed, evaluation.ObservedAt, evaluation.ObservedAt.UnixNano(), evaluation.FrameID, payload); err != nil {
		return fmt.Errorf("upsert summary: %w", err)
	}
	if publishLive {
		outboxID := fmt.Sprintf("registry:%s:%d:%d", assignment.ID, assignment.Generation, revision)
		if _, err = tx.Exec(ctx, `INSERT INTO conformance_outbox(outbox_id,destination,idempotency_key,assignment_id,assignment_generation,evaluation_revision,payload) VALUES($1,'registry',$1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, outboxID, assignment.ID, assignment.Generation, revision, payload); err != nil {
			return fmt.Errorf("enqueue live projection: %w", err)
		}
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
