//go:build integration

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package postgres_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/aero-arc/aero-arc-conformance/internal/domain"
	postgresstore "github.com/aero-arc/aero-arc-conformance/internal/store/postgres"
	"github.com/aero-arc/aero-arc-conformance/internal/testsupport"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
)

func TestBlueGreenAssignmentLifecycleAgainstPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	dsn := os.Getenv("AERO_CONFORMANCE_TEST_POSTGRES_URL")
	if dsn == "" {
		testcontainers.SkipIfProviderIsNotHealthy(t)
		container, err := testsupport.StartPostgres(ctx)
		if err != nil {
			t.Fatal(err)
		}
		dsn = container.URL
		t.Cleanup(func() {
			if err := container.Dependency.Shutdown(t.Failed(), os.Stderr); err != nil {
				t.Error(err)
			}
		})
	}
	now := time.Now().UTC()
	upgraded := seedVersionOneActive(t, ctx, dsn, now)
	first, err := postgresstore.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := postgresstore.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	resolvedUpgrade, found, err := first.ResolveAssignmentAt(ctx, upgraded.ID, upgraded.EffectiveFrom)
	if err != nil || !found || resolvedUpgrade.Assignment.Generation != upgraded.Generation || resolvedUpgrade.AuthorityFrom == nil || !resolvedUpgrade.AuthorityFrom.Equal(upgraded.EffectiveFrom) || resolvedUpgrade.AuthorityUntil == nil || !resolvedUpgrade.AuthorityUntil.Equal(upgraded.EffectiveUntil) {
		t.Fatalf("v1 active assignment was not upgraded exactly: %#v found=%v err=%v", resolvedUpgrade, found, err)
	}
	if _, found, err = first.ResolveAssignmentAt(ctx, upgraded.ID, upgraded.EffectiveUntil); err != nil || found {
		t.Fatalf("v1 authority remained open at its exclusive end: found=%v err=%v", found, err)
	}
	assertVersionOneWatermarkMigration(t, ctx, dsn, upgraded.ID)
	assertVersionOneOccurrenceMigration(t, ctx, dsn, first, upgraded)

	terminal := assignment(now, 1, 1)
	terminal.ID = "assignment-terminal"
	if _, err = first.PrepareAssignment(ctx, "api", "terminal-prepare", "assignment_prepared", terminal); err != nil {
		t.Fatal(err)
	}
	if _, err = first.CancelCandidate(ctx, "api", "terminal-cancel", terminal.ID, terminal.Generation); err != nil {
		t.Fatal(err)
	}
	reused := terminal
	reused.Generation = 2
	reused.AircraftID = "different-aircraft"
	if _, err = first.PrepareAssignment(ctx, "api", "terminal-reuse", "assignment_prepared", reused); !errors.Is(err, postgresstore.ErrMessageConflict) {
		t.Fatalf("terminal assignment identity was reusable: %v", err)
	}

	base := assignment(now, 1, 1)
	activate(t, ctx, first, base, "v1", now.Add(-time.Minute))
	claims, err := first.ClaimDueAssignments(ctx, "old-worker", time.Minute, 10)
	if err != nil || len(claims) != 1 {
		t.Fatalf("old claim=%#v err=%v", claims, err)
	}
	initialEvaluation := domain.Evaluation{
		Condition:  domain.ConditionConforming,
		Monitoring: domain.MonitoringCurrent,
		Recording:  domain.RecordingPending,
		State:      domain.EvaluatorState{Violations: map[domain.ViolationType]domain.IncidentState{}},
		Transitions: []domain.IncidentTransition{
			{Violation: domain.ViolationLateral, Transition: domain.TransitionOpened, ObservedAt: now.Add(-2 * time.Second), FrameID: "incident-open", OpeningFrameID: "incident-open", WALID: "initial-wal", WALSequence: 1, DeviationM: 4},
			{Violation: domain.ViolationLateral, Transition: domain.TransitionResolved, ObservedAt: now.Add(-time.Second), FrameID: "incident-resolved", OpeningFrameID: "incident-open", WALID: "initial-wal", WALSequence: 2},
		},
		ObservedAt:  now.Add(-time.Second),
		FrameID:     "initial-frame",
		WALID:       "initial-wal",
		WALSequence: 1,
	}
	if err = first.CommitEvaluation(ctx, claims[0], postgresstore.EvaluationCommit{Evaluation: initialEvaluation, NextEvaluationAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	claims, err = first.ClaimDueAssignments(ctx, "old-worker-reclaimed", time.Minute, 10)
	if err != nil || len(claims) != 1 {
		t.Fatalf("reclaimed old assignment=%#v err=%v", claims, err)
	}
	oldClaim := claims[0]

	replacedCandidate := assignment(now, 2, 2)
	if _, err = first.PrepareAssignment(ctx, "api", "v2-prepare", "assignment_prepared", replacedCandidate); err != nil {
		t.Fatal(err)
	}
	if _, err = first.ArmAssignment(ctx, "api", "v2-arm", replacedCandidate.ID, replacedCandidate.Generation); err != nil {
		t.Fatal(err)
	}
	candidate := assignment(now, 3, 3)
	if _, err = first.PrepareAssignment(ctx, "api", "v3-prepare", "assignment_prepared", candidate); err != nil {
		t.Fatal(err)
	}
	if _, err = first.ArmAssignment(ctx, "api", "v2-arm-after-replacement", replacedCandidate.ID, replacedCandidate.Generation); !errors.Is(err, postgresstore.ErrStaleAssignment) {
		t.Fatalf("replaced candidate was not fenced: %v", err)
	}
	if _, err = first.ArmAssignment(ctx, "api", "v3-arm", candidate.ID, candidate.Generation); err != nil {
		t.Fatal(err)
	}

	cutover := time.Now().UTC().Add(-time.Millisecond)
	const callers = 8
	results := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(store *postgresstore.Store) {
			defer wg.Done()
			_, err := store.CutoverAssignment(ctx, "api", "v3-cutover", candidate.ID, candidate.Generation, cutover)
			results <- err
		}([]*postgresstore.Store{first, second}[i%2])
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent idempotent cutover: %v", err)
		}
	}
	before, found, err := first.ResolveAssignmentAt(ctx, base.ID, cutover.Add(-time.Nanosecond))
	if err != nil || !found || before.Assignment.Generation != 1 {
		t.Fatalf("before=%#v found=%v err=%v", before, found, err)
	}
	after, found, err := second.ResolveAssignmentAt(ctx, base.ID, cutover)
	if err != nil || !found || after.Assignment.Generation != 3 || after.AuthorityUntil == nil || !after.AuthorityUntil.Equal(candidate.EffectiveUntil) {
		t.Fatalf("after=%#v found=%v err=%v", after, found, err)
	}
	if _, found, err = second.ResolveAssignmentAt(ctx, base.ID, candidate.EffectiveUntil); err != nil || found {
		t.Fatalf("replacement authority remained open at its exclusive end: found=%v err=%v", found, err)
	}
	if err = first.RenewAssignmentLease(ctx, oldClaim, time.Minute); !errors.Is(err, postgresstore.ErrLeaseLost) {
		t.Fatalf("superseded lease renewed: %v", err)
	}
	claims, err = second.ClaimDueAssignments(ctx, "new-worker", time.Minute, 10)
	if err != nil || len(claims) != 1 || claims[0].Assignment.Generation != 3 {
		t.Fatalf("replacement claim=%#v err=%v", claims, err)
	}
	currentClaim := claims[0]
	// Replay the exact suffix that already opened and resolved an incident. The
	// historical commit must reuse the immutable incident occurrence and events
	// instead of failing on the resolved incident's primary key.
	historical := initialEvaluation
	historicalCommit := postgresstore.EvaluationCommit{Evaluation: historical, NextEvaluationAt: time.Now().UTC()}
	if err = second.CommitHistoricalEvaluation(ctx, currentClaim, base.Generation, historicalCommit); err != nil {
		t.Fatalf("historical reconciliation commit: %v", err)
	}
	if err = second.CommitHistoricalEvaluation(ctx, currentClaim, base.Generation, historicalCommit); !errors.Is(err, postgresstore.ErrLeaseLost) {
		t.Fatalf("consumed current fence was reusable: %v", err)
	}
	assertHistoricalPersistence(t, ctx, dsn, base.ID, base.Generation, candidate.Generation, historical)

	claims, err = second.ClaimDueAssignments(ctx, "shifted-history-worker", time.Minute, 10)
	if err != nil || len(claims) != 1 || claims[0].Assignment.Generation != candidate.Generation {
		t.Fatalf("shifted-history claim=%#v err=%v", claims, err)
	}
	shifted := initialEvaluation
	shifted.ObservedAt = cutover.Add(-time.Microsecond)
	shifted.FrameID = "shifted-batch-final"
	shifted.WALSequence = 3
	shifted.State = domain.EvaluatorState{Violations: map[domain.ViolationType]domain.IncidentState{domain.ViolationLateral: {Phase: domain.IncidentClear, WorstDeviationM: 99}}}
	shifted.Transitions = append([]domain.IncidentTransition(nil), initialEvaluation.Transitions...)
	shifted.Transitions[1] = domain.IncidentTransition{Violation: domain.ViolationLateral, Transition: domain.TransitionResolved, ObservedAt: shifted.ObservedAt, FrameID: "incident-resolved-shifted", OpeningFrameID: "incident-open", WALID: "initial-wal", WALSequence: 3}
	shiftedCommit := postgresstore.EvaluationCommit{Evaluation: shifted, NextEvaluationAt: time.Now().UTC()}
	if err = second.CommitHistoricalEvaluation(ctx, claims[0], base.Generation, shiftedCommit); err != nil {
		t.Fatalf("shift historical resolution: %v", err)
	}
	assertShiftedResolution(t, ctx, dsn, base.ID, base.Generation, shifted.ObservedAt, 3, 3)

	claims, err = second.ClaimDueAssignments(ctx, "exact-shift-replay-worker", time.Minute, 10)
	if err != nil || len(claims) != 1 || claims[0].Assignment.Generation != candidate.Generation {
		t.Fatalf("exact-shift replay claim=%#v err=%v", claims, err)
	}
	if err = second.CommitHistoricalEvaluation(ctx, claims[0], base.Generation, shiftedCommit); err != nil {
		t.Fatalf("exact shifted-resolution replay: %v", err)
	}
	assertShiftedResolution(t, ctx, dsn, base.ID, base.Generation, shifted.ObservedAt, 3, 3)
	claims, err = second.ClaimDueAssignments(ctx, "shift-back-worker", time.Minute, 10)
	if err != nil || len(claims) != 1 || claims[0].Assignment.Generation != candidate.Generation {
		t.Fatalf("shift-back claim=%#v err=%v", claims, err)
	}
	shiftBack := shifted
	shiftBack.Transitions = append([]domain.IncidentTransition(nil), shifted.Transitions...)
	shiftBack.Transitions[1] = initialEvaluation.Transitions[1]
	shiftBackCommit := postgresstore.EvaluationCommit{Evaluation: shiftBack, NextEvaluationAt: time.Now().UTC()}
	if err = second.CommitHistoricalEvaluation(ctx, claims[0], base.Generation, shiftBackCommit); err != nil {
		t.Fatalf("shift resolution back to existing immutable event: %v", err)
	}
	assertShiftedResolution(t, ctx, dsn, base.ID, base.Generation, initialEvaluation.Transitions[1].ObservedAt, 3, 4)

	claims, err = second.ClaimDueAssignments(ctx, "invalid-history-worker", time.Minute, 10)
	if err != nil || len(claims) != 1 || claims[0].Assignment.Generation != candidate.Generation {
		t.Fatalf("invalid-history claim=%#v err=%v", claims, err)
	}
	invalidHistorical := historicalCommit
	invalidHistorical.Evaluation.ObservedAt = cutover
	invalidHistorical.Evaluation.FrameID = "wrong-interval-frame"
	if err = second.CommitHistoricalEvaluation(ctx, claims[0], base.Generation, invalidHistorical); !errors.Is(err, postgresstore.ErrInvalidTransition) {
		t.Fatalf("out-of-interval historical evaluation: %v", err)
	}
	if err = second.RenewAssignmentLease(ctx, claims[0], time.Minute); err != nil {
		t.Fatalf("invalid history attempt consumed the current fence: %v", err)
	}
	afterAuthority := historicalCommit
	afterAuthority.Evaluation.ObservedAt = candidate.EffectiveUntil
	afterAuthority.Evaluation.FrameID = "after-authority-frame"
	if err = second.CommitEvaluation(ctx, claims[0], afterAuthority); !errors.Is(err, postgresstore.ErrLeaseLost) {
		t.Fatalf("live commit crossed exclusive authority end: %v", err)
	}
	if err = second.RenewAssignmentLease(ctx, claims[0], time.Minute); err != nil {
		t.Fatalf("rejected post-authority commit consumed the lease: %v", err)
	}

	assertRetroactiveCutoverFence(t, ctx, first, dsn, now)
	assertConcurrentCommitCutoverFence(t, ctx, first, second, dsn, now)
}

func assertConcurrentCommitCutoverFence(t *testing.T, ctx context.Context, first, second *postgresstore.Store, dsn string, now time.Time) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for round := 0; round < 8; round++ {
		if _, err = conn.Exec(ctx, `UPDATE conformance_assignments SET next_evaluation_at=now()+interval '1 day',lease_owner=NULL,lease_until=NULL`); err != nil {
			t.Fatal(err)
		}
		current := assignment(now, 1, 1)
		current.ID = fmt.Sprintf("assignment-cutover-race-%d", round)
		activate(t, ctx, first, current, fmt.Sprintf("race-%d-v1", round), now.Add(-time.Minute))
		claims, claimErr := first.ClaimDueAssignments(ctx, fmt.Sprintf("race-worker-%d", round), time.Minute, 10)
		if claimErr != nil || len(claims) != 1 || claims[0].Assignment.ID != current.ID {
			t.Fatalf("race claim round=%d claims=%#v err=%v", round, claims, claimErr)
		}
		replacement := assignment(now, 2, 2)
		replacement.ID = current.ID
		if _, err = first.PrepareAssignment(ctx, "api", fmt.Sprintf("race-%d-prepare", round), "assignment_prepared", replacement); err != nil {
			t.Fatal(err)
		}
		if _, err = first.ArmAssignment(ctx, "api", fmt.Sprintf("race-%d-arm", round), replacement.ID, replacement.Generation); err != nil {
			t.Fatal(err)
		}
		boundary := now.Add(-10 * time.Second)
		evaluation := domain.Evaluation{Condition: domain.ConditionConforming, Monitoring: domain.MonitoringCurrent, Recording: domain.RecordingPending, State: domain.EvaluatorState{Violations: map[domain.ViolationType]domain.IncidentState{}}, ObservedAt: boundary, FrameID: fmt.Sprintf("race-frame-%d", round), WALID: "race-wal", WALSequence: uint64(round + 1)}
		start := make(chan struct{})
		commitResult := make(chan error, 1)
		cutoverResult := make(chan error, 1)
		go func() {
			<-start
			commitResult <- first.CommitEvaluation(ctx, claims[0], postgresstore.EvaluationCommit{Evaluation: evaluation, NextEvaluationAt: time.Now().UTC()})
		}()
		go func() {
			<-start
			_, cutoverErr := second.CutoverAssignment(ctx, "api", fmt.Sprintf("race-%d-cutover", round), replacement.ID, replacement.Generation, boundary)
			cutoverResult <- cutoverErr
		}()
		close(start)
		commitErr, cutoverErr := <-commitResult, <-cutoverResult
		commitWon := commitErr == nil && errors.Is(cutoverErr, postgresstore.ErrInvalidTransition)
		cutoverWon := errors.Is(commitErr, postgresstore.ErrLeaseLost) && cutoverErr == nil
		if !commitWon && !cutoverWon {
			t.Fatalf("race round=%d commit=%v cutover=%v", round, commitErr, cutoverErr)
		}
		var stranded int
		if err = conn.QueryRow(ctx, `SELECT count(*) FROM conformance_assignments a JOIN conformance_summaries s USING(assignment_id,assignment_generation) WHERE a.assignment_id=$1 AND a.lifecycle_state='superseded' AND s.observed_at_unix_ns>=a.authority_until_unix_ns`, current.ID).Scan(&stranded); err != nil {
			t.Fatal(err)
		}
		if stranded != 0 {
			t.Fatalf("race round=%d stranded %d post-boundary summaries", round, stranded)
		}
	}
}

func assertShiftedResolution(t *testing.T, ctx context.Context, dsn, assignmentID string, generation uint64, wantResolvedAt time.Time, wantEvents, wantIncidentRevision int) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var resolvedAt time.Time
	var events, incidentRevision int
	var resolutionEventID string
	if err = conn.QueryRow(ctx, `SELECT
	(SELECT resolved_at FROM conformance_incidents WHERE assignment_id=$1 AND assignment_generation=$2),
	(SELECT resolution_event_id FROM conformance_incidents WHERE assignment_id=$1 AND assignment_generation=$2),
	(SELECT revision FROM conformance_incidents WHERE assignment_id=$1 AND assignment_generation=$2),
	(SELECT count(*) FROM conformance_events WHERE assignment_id=$1 AND assignment_generation=$2)`, assignmentID, generation).Scan(&resolvedAt, &resolutionEventID, &incidentRevision, &events); err != nil {
		t.Fatal(err)
	}
	if resolvedAt.Sub(wantResolvedAt).Abs() >= time.Microsecond || resolutionEventID == "" || events != wantEvents || incidentRevision != wantIncidentRevision {
		t.Fatalf("shifted resolution at=%s event=%q events=%d revision=%d", resolvedAt, resolutionEventID, events, incidentRevision)
	}
}

func assertRetroactiveCutoverFence(t *testing.T, ctx context.Context, store *postgresstore.Store, dsn string, now time.Time) {
	t.Helper()
	current := assignment(now, 1, 1)
	current.ID = "assignment-retroactive-cutover"
	activate(t, ctx, store, current, "retro-v1", now.Add(-time.Minute))
	claims, err := store.ClaimDueAssignments(ctx, "retro-worker", time.Minute, 10)
	if err != nil || len(claims) != 1 || claims[0].Assignment.ID != current.ID {
		t.Fatalf("retro claim=%#v err=%v", claims, err)
	}
	watermark := now.Add(-20 * time.Second)
	evaluation := domain.Evaluation{Condition: domain.ConditionConforming, Monitoring: domain.MonitoringCurrent, Recording: domain.RecordingPending, State: domain.EvaluatorState{Violations: map[domain.ViolationType]domain.IncidentState{}}, ObservedAt: watermark, FrameID: "retro-frame", WALID: "retro-wal", WALSequence: 1}
	if err = store.CommitEvaluation(ctx, claims[0], postgresstore.EvaluationCommit{Evaluation: evaluation, NextEvaluationAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	replacement := assignment(now, 2, 2)
	replacement.ID = current.ID
	if _, err = store.PrepareAssignment(ctx, "api", "retro-prepare", "assignment_prepared", replacement); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ArmAssignment(ctx, "api", "retro-arm", replacement.ID, replacement.Generation); err != nil {
		t.Fatal(err)
	}
	if _, err = store.CutoverAssignment(ctx, "api", "retro-rejected", replacement.ID, replacement.Generation, watermark); !errors.Is(err, postgresstore.ErrInvalidTransition) {
		t.Fatalf("retroactive cutover crossed committed watermark: %v", err)
	}
	if _, err = store.CutoverAssignment(ctx, "api", "retro-rejected", replacement.ID, replacement.Generation, watermark); !errors.Is(err, postgresstore.ErrInvalidTransition) {
		t.Fatalf("identical rejected cutover was not safely retryable: %v", err)
	}
	assertRejectedCutoverState(t, ctx, dsn, current.ID)
	safeBoundary := watermark.Add(time.Second)
	if _, err = store.CutoverAssignment(ctx, "api", "retro-safe", replacement.ID, replacement.Generation, safeBoundary); err != nil {
		t.Fatalf("safe later cutover: %v", err)
	}
	before, found, err := store.ResolveAssignmentAt(ctx, current.ID, safeBoundary.Add(-time.Nanosecond))
	if err != nil || !found || before.Assignment.Generation != current.Generation {
		t.Fatalf("safe cutover old boundary: %#v found=%v err=%v", before, found, err)
	}
	after, found, err := store.ResolveAssignmentAt(ctx, current.ID, safeBoundary)
	if err != nil || !found || after.Assignment.Generation != replacement.Generation {
		t.Fatalf("safe cutover new boundary: %#v found=%v err=%v", after, found, err)
	}
}

func assertRejectedCutoverState(t *testing.T, ctx context.Context, dsn, assignmentID string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var active, armed, rejectedInbox, checkpoints, summaries, registryOutbox int
	if err = conn.QueryRow(ctx, `SELECT
	(SELECT count(*) FROM conformance_assignments WHERE assignment_id=$1 AND assignment_generation=1 AND lifecycle_state='active'),
	(SELECT count(*) FROM conformance_assignments WHERE assignment_id=$1 AND assignment_generation=2 AND lifecycle_state='candidate_armed'),
	(SELECT count(*) FROM conformance_inbox WHERE source='api' AND message_id='retro-rejected'),
	(SELECT count(*) FROM conformance_checkpoints WHERE assignment_id=$1 AND assignment_generation=1),
	(SELECT count(*) FROM conformance_summaries WHERE assignment_id=$1 AND assignment_generation=1),
	(SELECT count(*) FROM conformance_outbox WHERE assignment_id=$1 AND assignment_generation=1 AND destination='registry')`, assignmentID).Scan(&active, &armed, &rejectedInbox, &checkpoints, &summaries, &registryOutbox); err != nil {
		t.Fatal(err)
	}
	if active != 1 || armed != 1 || rejectedInbox != 0 || checkpoints != 1 || summaries != 1 || registryOutbox != 1 {
		t.Fatalf("rejected cutover mutated state active=%d armed=%d inbox=%d checkpoints=%d summaries=%d registry_outbox=%d", active, armed, rejectedInbox, checkpoints, summaries, registryOutbox)
	}
}

func assertHistoricalPersistence(t *testing.T, ctx context.Context, dsn, assignmentID string, historicalGeneration, currentGeneration uint64, evaluation domain.Evaluation) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var historicalRevision, currentRevision, checkpoints, summaries, registryOutbox, incidents, events int
	var incidentState string
	var observedAt time.Time
	if err = conn.QueryRow(ctx, `SELECT
  (SELECT evaluation_revision FROM conformance_assignments WHERE assignment_id=$1 AND assignment_generation=$2),
  (SELECT evaluation_revision FROM conformance_assignments WHERE assignment_id=$1 AND assignment_generation=$3),
  (SELECT count(*) FROM conformance_checkpoints WHERE assignment_id=$1 AND assignment_generation=$2),
  (SELECT count(*) FROM conformance_summaries WHERE assignment_id=$1 AND assignment_generation=$2),
  (SELECT count(*) FROM conformance_outbox WHERE assignment_id=$1 AND assignment_generation=$2 AND destination='registry'),
	(SELECT observed_at FROM conformance_summaries WHERE assignment_id=$1 AND assignment_generation=$2),
	(SELECT count(*) FROM conformance_incidents WHERE assignment_id=$1 AND assignment_generation=$2),
	(SELECT count(*) FROM conformance_events WHERE assignment_id=$1 AND assignment_generation=$2),
	(SELECT state FROM conformance_incidents WHERE assignment_id=$1 AND assignment_generation=$2)`, assignmentID, historicalGeneration, currentGeneration).Scan(&historicalRevision, &currentRevision, &checkpoints, &summaries, &registryOutbox, &observedAt, &incidents, &events, &incidentState); err != nil {
		t.Fatal(err)
	}
	if historicalRevision != 2 || currentRevision != 0 || checkpoints != 2 || summaries != 1 || registryOutbox != 1 || incidents != 1 || events != 2 || incidentState != "resolved" || observedAt.Sub(evaluation.ObservedAt).Abs() >= time.Microsecond {
		t.Fatalf("historical persistence revision=%d current_revision=%d checkpoints=%d summaries=%d registry_outbox=%d incidents=%d events=%d incident_state=%s observed_at=%s", historicalRevision, currentRevision, checkpoints, summaries, registryOutbox, incidents, events, incidentState, observedAt)
	}
}

func seedVersionOneActive(t *testing.T, ctx context.Context, dsn string, now time.Time) domain.Assignment {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate integration test source")
	}
	migration, err := os.ReadFile(filepath.Join(filepath.Dir(sourceFile), "migrations", "001_initial.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	checksumBytes := sha256.Sum256(migration)
	if _, err = conn.Exec(ctx, `INSERT INTO schema_migrations(version,checksum) VALUES(1,$1)`, hex.EncodeToString(checksumBytes[:])); err != nil {
		t.Fatal(err)
	}
	value := assignment(now, 1, 1)
	value.ID = "assignment-upgrade"
	value.EffectiveFrom = now.Add(-time.Hour).Truncate(time.Second).Add(123456789 * time.Nanosecond)
	value.EffectiveUntil = value.EffectiveFrom.Add(2 * time.Hour)
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO conformance_assignments(assignment_id,assignment_generation,aircraft_id,agent_id,flight_id,intent_id,intent_version,policy_version,lifecycle_state,specification,next_evaluation_at,evaluation_revision) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'active',$9,now(),1)`, value.ID, value.Generation, value.AircraftID, value.AgentID, value.FlightID, value.IntentID, value.IntentVersion, value.PolicyVersion, raw); err != nil {
		t.Fatal(err)
	}
	openedAt := value.EffectiveFrom.Add(time.Second)
	checkpointAt := openedAt.Add(time.Second + 123*time.Nanosecond)
	legacyPayload := []byte(`{"legacy_batch_state":"recovering"}`)
	legacyHash := sha256.Sum256(legacyPayload)
	legacyViolations := []domain.ViolationType{domain.ViolationLateral, domain.ViolationVertical, domain.ViolationTemporal}
	legacyStates := make(map[domain.ViolationType]domain.IncidentState, len(legacyViolations))
	for index, violation := range legacyViolations {
		openingFrame := legacyOpeningFrame(violation)
		incidentID := testStableID("incident", value.ID, fmt.Sprint(value.Generation), string(violation), openingFrame)
		eventID := testStableID("event", value.ID, fmt.Sprint(value.Generation), string(violation), string(domain.TransitionOpened), openingFrame)
		if _, err = conn.Exec(ctx, `INSERT INTO conformance_incidents(incident_id,assignment_id,assignment_generation,incident_key,violation_type,state,severity,opened_at,last_observed_at,details) VALUES($1,$2,$3,$4,$4,'open','warning',$5,$6,$7)`, incidentID, value.ID, value.Generation, violation, openedAt, checkpointAt, legacyPayload); err != nil {
			t.Fatal(err)
		}
		if _, err = conn.Exec(ctx, `INSERT INTO conformance_events(event_id,assignment_id,assignment_generation,incident_id,transition,violation_type,observed_at,frame_id,wal_id,wal_sequence,evaluation_revision,payload,payload_sha256) VALUES($1,$2,$3,$4,'opened',$5,$6,$7,'v1-wal',$8,1,$9,$10)`, eventID, value.ID, value.Generation, incidentID, violation, openedAt, openingFrame, index+1, legacyPayload, hex.EncodeToString(legacyHash[:])); err != nil {
			t.Fatal(err)
		}
		legacyStates[violation] = domain.IncidentState{Phase: domain.IncidentRecovering, OpenedAt: openedAt, LastObservedAt: checkpointAt}
	}
	legacyState, err := json.Marshal(domain.EvaluatorState{Violations: legacyStates})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO conformance_checkpoints(assignment_id,assignment_generation,evaluation_revision,state_through_at,wal_id,wal_sequence,frame_id,evaluator_state) VALUES($1,$2,1,$3,'v1-wal',2,'v1-checkpoint',$4)`, value.ID, value.Generation, checkpointAt, legacyState); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO conformance_summaries(assignment_id,assignment_generation,evaluation_revision,condition,monitoring_status,recording_status,observed_at,frame_id,payload) VALUES($1,$2,1,'recovering','current','confirmed',$3,'v1-checkpoint','{}')`, value.ID, value.Generation, checkpointAt); err != nil {
		t.Fatal(err)
	}
	return value
}

func assertVersionOneWatermarkMigration(t *testing.T, ctx context.Context, dsn, assignmentID string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var observedAt time.Time
	var observedUnixNS int64
	if err = conn.QueryRow(ctx, `SELECT observed_at,observed_at_unix_ns FROM conformance_summaries WHERE assignment_id=$1 AND assignment_generation=1`, assignmentID).Scan(&observedAt, &observedUnixNS); err != nil {
		t.Fatal(err)
	}
	if want := observedAt.UnixMicro()*1000 + 999; observedUnixNS != want {
		t.Fatalf("v1 watermark migration=%d want conservative microsecond end %d", observedUnixNS, want)
	}
}

func assertVersionOneOccurrenceMigration(t *testing.T, ctx context.Context, dsn string, store *postgresstore.Store, assignment domain.Assignment) {
	t.Helper()
	checkpoint, found, err := store.GetReplayCheckpoint(ctx, assignment.ID, assignment.Generation, assignment.EffectiveUntil)
	if err != nil || !found {
		t.Fatalf("migrated checkpoint found=%v err=%v", found, err)
	}
	for _, violation := range []domain.ViolationType{domain.ViolationLateral, domain.ViolationVertical, domain.ViolationTemporal} {
		state := checkpoint.State.Violations[violation]
		if state.OpeningFrameID != legacyOpeningFrame(violation) || state.Phase != domain.IncidentRecovering {
			t.Fatalf("migrated %s occurrence state=%#v", violation, state)
		}
	}
	openingFrame := legacyOpeningFrame(domain.ViolationLateral)
	claims, err := store.ClaimDueAssignments(ctx, "v1-recovery-worker", time.Minute, 10)
	if err != nil || len(claims) != 1 || claims[0].Assignment.ID != assignment.ID {
		t.Fatalf("migrated recovery claim=%#v err=%v", claims, err)
	}
	openedAt := assignment.EffectiveFrom.Add(time.Second)
	resolvedAt := checkpoint.StateThroughAt.Add(time.Second)
	evaluation := domain.Evaluation{
		Condition:  domain.ConditionConforming,
		Monitoring: domain.MonitoringCurrent,
		Recording:  domain.RecordingPending,
		State:      domain.EvaluatorState{Violations: map[domain.ViolationType]domain.IncidentState{}},
		Transitions: []domain.IncidentTransition{
			{Violation: domain.ViolationLateral, Transition: domain.TransitionOpened, ObservedAt: openedAt, FrameID: openingFrame, OpeningFrameID: openingFrame, WALID: "v1-wal", WALSequence: 1},
			{Violation: domain.ViolationLateral, Transition: domain.TransitionResolved, ObservedAt: resolvedAt, FrameID: "v2-resolution-frame", OpeningFrameID: openingFrame, WALID: "v1-wal", WALSequence: 3},
		},
		ObservedAt: resolvedAt, FrameID: "v2-recovered-batch", WALID: "v1-wal", WALSequence: 3,
	}
	if err = store.CommitEvaluation(ctx, claims[0], postgresstore.EvaluationCommit{Evaluation: evaluation, NextEvaluationAt: time.Now().UTC().Add(time.Hour)}); err != nil {
		t.Fatalf("resolve migrated occurrence: %v", err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var incidents, events, legacyEvents, versionTwoEvents int
	var incidentState, migratedOpeningFrame string
	if err = conn.QueryRow(ctx, `SELECT
	(SELECT count(*) FROM conformance_incidents WHERE assignment_id=$1 AND assignment_generation=1),
	(SELECT count(*) FROM conformance_events WHERE assignment_id=$1 AND assignment_generation=1),
	(SELECT count(*) FROM conformance_events WHERE assignment_id=$1 AND assignment_generation=1 AND evidence_version=1),
	(SELECT count(*) FROM conformance_events WHERE assignment_id=$1 AND assignment_generation=1 AND evidence_version=2),
	(SELECT state FROM conformance_incidents WHERE assignment_id=$1 AND assignment_generation=1 AND incident_key='lateral_deviation'),
	(SELECT opening_frame_id FROM conformance_incidents WHERE assignment_id=$1 AND assignment_generation=1 AND incident_key='lateral_deviation')`, assignment.ID).Scan(&incidents, &events, &legacyEvents, &versionTwoEvents, &incidentState, &migratedOpeningFrame); err != nil {
		t.Fatal(err)
	}
	if incidents != 3 || events != 4 || legacyEvents != 3 || versionTwoEvents != 1 || incidentState != "resolved" || migratedOpeningFrame != openingFrame {
		t.Fatalf("migrated replay incidents=%d events=%d legacy=%d v2=%d state=%s opening=%s", incidents, events, legacyEvents, versionTwoEvents, incidentState, migratedOpeningFrame)
	}
}

func legacyOpeningFrame(violation domain.ViolationType) string {
	return "v1-" + string(violation) + "-opening-frame"
}

func testStableID(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = fmt.Fprintf(h, "%d:%s", len(part), part)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func activate(t *testing.T, ctx context.Context, store *postgresstore.Store, value domain.Assignment, prefix string, effectiveAt time.Time) {
	t.Helper()
	if _, err := store.PrepareAssignment(ctx, "api", prefix+"-prepare", "assignment_prepared", value); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ArmAssignment(ctx, "api", prefix+"-arm", value.ID, value.Generation); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CutoverAssignment(ctx, "api", prefix+"-cutover", value.ID, value.Generation, effectiveAt); err != nil {
		t.Fatal(err)
	}
}

func assignment(now time.Time, generation uint64, intentVersion uint32) domain.Assignment {
	return domain.Assignment{ID: "assignment-focused", Generation: generation, AircraftID: "aircraft", AgentID: "agent", FlightID: "flight", IntentID: "intent", IntentVersion: intentVersion, PolicyVersion: "standard-v1", EffectiveFrom: now.Add(-time.Hour), EffectiveUntil: now.Add(time.Hour), Volumes: []domain.Volume{{ID: "volume", Polygon: []domain.Point{{Latitude: 35, Longitude: -97.01}, {Latitude: 35.01, Longitude: -97.01}, {Latitude: 35.01, Longitude: -97}, {Latitude: 35, Longitude: -97}}, AltitudeLowerM: 80, AltitudeUpperM: 120, AltitudeReference: domain.AltitudeMSL, StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour)}}}
}
