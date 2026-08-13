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
			{Violation: domain.ViolationLateral, Transition: domain.TransitionOpened, ObservedAt: now.Add(-2 * time.Second), FrameID: "incident-open"},
			{Violation: domain.ViolationLateral, Transition: domain.TransitionResolved, ObservedAt: now.Add(-time.Second), FrameID: "incident-resolved"},
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
	value.EffectiveFrom = time.Date(2026, 8, 13, 12, 0, 0, 123456789, time.UTC)
	value.EffectiveUntil = value.EffectiveFrom.Add(time.Hour)
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO conformance_assignments(assignment_id,assignment_generation,aircraft_id,agent_id,flight_id,intent_id,intent_version,policy_version,lifecycle_state,specification,next_evaluation_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'active',$9,now()+interval '1 day')`, value.ID, value.Generation, value.AircraftID, value.AgentID, value.FlightID, value.IntentID, value.IntentVersion, value.PolicyVersion, raw); err != nil {
		t.Fatal(err)
	}
	return value
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
