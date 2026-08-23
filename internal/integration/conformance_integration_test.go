//go:build integration

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package integration

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	influxdb3 "github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/aero-arc/aero-arc-conformance/internal/domain"
	"github.com/aero-arc/aero-arc-conformance/internal/evaluator"
	postgresstore "github.com/aero-arc/aero-arc-conformance/internal/store/postgres"
	telemetryinflux "github.com/aero-arc/aero-arc-conformance/internal/telemetry/influx"
	"github.com/aero-arc/aero-arc-conformance/internal/testsupport"
	evaluationworker "github.com/aero-arc/aero-arc-conformance/internal/worker"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
)

func TestRealDependenciesEvaluatePersistAndReclaim(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pg, err := testsupport.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := pg.Dependency.Shutdown(t.Failed(), os.Stderr); err != nil {
			t.Error(err)
		}
	})
	inf, err := testsupport.StartInflux(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := inf.Dependency.Shutdown(t.Failed(), os.Stderr); err != nil {
			t.Error(err)
		}
	})
	store, err := postgresstore.Open(ctx, pg.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC()
	assignment := domain.Assignment{ID: "assignment-integration", Generation: 1, AircraftID: "aircraft-1", AgentID: "agent-1", FlightID: "flight-1", IntentID: "intent-1", IntentVersion: 1, PolicyVersion: "standard-v1", EffectiveFrom: now.Add(-time.Minute), EffectiveUntil: now.Add(time.Hour), Volumes: []domain.Volume{{ID: "volume-1", Polygon: []domain.Point{{Latitude: 35, Longitude: -97.01}, {Latitude: 35.01, Longitude: -97.01}, {Latitude: 35.01, Longitude: -97}, {Latitude: 35, Longitude: -97}}, AltitudeLowerM: 80, AltitudeUpperM: 120, AltitudeReference: domain.AltitudeMSL, StartsAt: now.Add(-time.Minute), EndsAt: now.Add(time.Hour)}}}
	activateAssignment(t, ctx, store, assignment, "message-1", now)
	claims, err := store.ClaimDueAssignments(ctx, "worker-a", 30*time.Second, 1)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claims=%#v err=%v", claims, err)
	}
	oldClaim := claims[0]
	client, err := influxdb3.New(influxdb3.ClientConfig{Host: inf.Host, Token: inf.Token, Database: inf.Database})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	points := []*influxdb3.Point{positionPoint(now, "frame-1", 1, 35.005, -97.005, 100), positionPoint(now.Add(time.Second), "frame-2", 2, 35.02, -97.02, 130), positionPoint(now.Add(2*time.Second), "frame-3", 3, 35.02, -97.02, 130)}
	if err := client.WritePoints(ctx, points); err != nil {
		t.Fatal(err)
	}
	reader, err := telemetryinflux.New(inf.Host, inf.Token, inf.Database, 100, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var observations []domain.Observation
	for len(observations) < 3 {
		result, queryErr := reader.ReadPositions(ctx, []string{"aircraft-1"}, now.Add(-time.Second), now.Add(time.Minute))
		if queryErr == nil {
			observations = result.Observations
		}
		if len(observations) < 3 {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	e, err := evaluator.New(evaluator.Policy{Version: "standard-v1", HorizontalToleranceM: 3, VerticalToleranceM: 2, OpenAfterSamples: 2, RecoverAfterSamples: 2, TelemetryFreshness: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	evaluation, err := e.EvaluateBatch(assignment, observations, domain.EvaluatorState{})
	if err != nil {
		t.Fatal(err)
	}
	if evaluation.Condition != domain.ConditionNonConforming {
		t.Fatalf("condition=%s", evaluation.Condition)
	}
	if err := store.CommitEvaluation(ctx, oldClaim, postgresstore.EvaluationCommit{Evaluation: evaluation, NextEvaluationAt: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	assertDurableEvaluation(t, ctx, pg.URL, "assignment-integration", 2, 2, 1)
	if err := store.CommitEvaluation(ctx, oldClaim, postgresstore.EvaluationCommit{Evaluation: evaluation, NextEvaluationAt: now.Add(time.Minute)}); err != postgresstore.ErrLeaseLost {
		t.Fatalf("ambiguous retry was not fenced: %v", err)
	}
	time.Sleep(600 * time.Millisecond)
	claims, err = store.ClaimDueAssignments(ctx, "worker-b", time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 0 {
		t.Fatalf("assignment was reclaimed before next_evaluation_at: %#v", claims)
	}
	// Build a durable checkpoint, then let a worker die after claiming the next
	// suffix. The takeover restores that checkpoint, replays the remaining
	// observations, commits once, and fences the stale worker.
	assignment.ID = "assignment-reclaim"
	activateAssignment(t, ctx, store, assignment, "message-2", now)
	claims, err = store.ClaimDueAssignments(ctx, "worker-a", time.Second, 1)
	if err != nil || len(claims) != 1 {
		t.Fatalf("first reclaim claim=%#v err=%v", claims, err)
	}
	firstEvaluation, err := e.EvaluateBatch(assignment, observations[:1], domain.EvaluatorState{})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.CommitEvaluation(ctx, claims[0], postgresstore.EvaluationCommit{Evaluation: firstEvaluation, NextEvaluationAt: now}); err != nil {
		t.Fatal(err)
	}
	assertDurableEvaluation(t, ctx, pg.URL, "assignment-reclaim", 0, 0, 1)
	claims, err = store.ClaimDueAssignments(ctx, "worker-a", 100*time.Millisecond, 1)
	if err != nil || len(claims) != 1 {
		t.Fatalf("crashing worker claim=%#v err=%v", claims, err)
	}
	stale := claims[0]
	if err = store.RenewAssignmentLease(ctx, stale, 0); err == nil {
		t.Fatal("non-positive lease renewal was accepted")
	}
	time.Sleep(150 * time.Millisecond)
	claims, err = store.ClaimDueAssignments(ctx, "worker-b", time.Second, 1)
	if err != nil || len(claims) != 1 {
		t.Fatalf("takeover=%#v err=%v", claims, err)
	}
	if claims[0].LeaseGeneration <= stale.LeaseGeneration {
		t.Fatal("lease generation did not advance")
	}
	checkpoint, found, err := store.GetReplayCheckpoint(ctx, assignment.ID, assignment.Generation, now.Add(time.Minute))
	if err != nil || !found || checkpoint.EvaluationRevision != 1 || checkpoint.FrameID != "frame-1" {
		t.Fatalf("checkpoint=%#v found=%v err=%v", checkpoint, found, err)
	}
	replayed, err := e.EvaluateBatch(assignment, observations[1:], checkpoint.State)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.CommitEvaluation(ctx, claims[0], postgresstore.EvaluationCommit{Evaluation: replayed, NextEvaluationAt: now.Add(time.Minute)}); err != nil {
		t.Fatalf("takeover worker could not commit: %v", err)
	}
	assertDurableEvaluation(t, ctx, pg.URL, "assignment-reclaim", 2, 2, 2)
	if err = store.CommitEvaluation(ctx, stale, postgresstore.EvaluationCommit{Evaluation: evaluation, NextEvaluationAt: now}); err != postgresstore.ErrLeaseLost {
		t.Fatalf("stale commit err=%v", err)
	}

	// Exercise the production orchestration across both real dependencies: the
	// worker claims PostgreSQL authority, waits for a complete Influx suffix,
	// evaluates it, and atomically creates the Registry publication handoff.
	workerNow := time.Now().UTC().Add(-3 * time.Second)
	workerAssignment := assignment
	workerAssignment.ID = "assignment-runtime-worker"
	workerAssignment.EffectiveFrom = workerNow
	workerAssignment.EffectiveUntil = workerNow.Add(time.Hour)
	workerAssignment.Volumes[0].StartsAt = workerNow
	workerAssignment.Volumes[0].EndsAt = workerNow.Add(time.Hour)
	activateAssignment(t, ctx, store, workerAssignment, "runtime-worker", workerNow)
	workerPoint := positionPoint(workerNow.Add(time.Second), "runtime-frame-1", 100, 35.005, -97.005, 100)
	if err = client.WritePoints(ctx, []*influxdb3.Point{workerPoint}); err != nil {
		t.Fatal(err)
	}
	for {
		visible, readErr := reader.ReadPositions(ctx, []string{workerAssignment.AircraftID}, workerNow, workerNow.Add(2*time.Second))
		if readErr == nil && len(visible.Observations) > 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	runtimeWorker, err := evaluationworker.New(store, reader, e, evaluationworker.Config{WorkerID: "runtime-worker", PollInterval: time.Second, LeaseDuration: 30 * time.Second, RenewInterval: 10 * time.Second, SettleDelay: 0, OverlapWindow: 30 * time.Second, ClaimBatchSize: 20}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err = runtimeWorker.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	assertDurableEvaluation(t, ctx, pg.URL, workerAssignment.ID, 0, 0, 1)

	tailStart := time.Now().UTC()
	tailAssignment := workerAssignment
	tailAssignment.ID = "assignment-final-settle-tail"
	tailAssignment.EffectiveFrom = tailStart
	tailAssignment.EffectiveUntil = tailStart.Add(300 * time.Millisecond)
	tailAssignment.Volumes[0].StartsAt = tailAssignment.EffectiveFrom
	tailAssignment.Volumes[0].EndsAt = tailAssignment.EffectiveUntil
	activateAssignment(t, ctx, store, tailAssignment, "final-tail", tailStart)
	time.Sleep(400 * time.Millisecond)
	withoutGrace, err := store.ClaimDueAssignments(ctx, "tail-without-grace", 5*time.Second, 20)
	if err != nil {
		t.Fatal(err)
	}
	for _, claim := range withoutGrace {
		if claim.Assignment.ID == tailAssignment.ID {
			t.Fatal("ended assignment was claimed without finalization grace")
		}
	}
	withGrace, err := store.ClaimDueAssignmentsWithFinalizationGrace(ctx, "tail-with-grace", 5*time.Second, 2*time.Second, 20)
	if err != nil {
		t.Fatal(err)
	}
	var tailClaim postgresstore.Claim
	for _, claim := range withGrace {
		if claim.Assignment.ID == tailAssignment.ID {
			tailClaim = claim
		}
	}
	if tailClaim.Assignment.ID == "" || tailClaim.FinalizationGrace != 2*time.Second {
		t.Fatalf("final tail was not claimable during settle grace: %#v", withGrace)
	}
	if err = store.DeferAssignmentEvaluation(ctx, tailClaim, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("release final-tail claim: %v", err)
	}

	precisionStart := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Microsecond).Add(123 * time.Nanosecond)
	precisionEnd := precisionStart.Add(time.Hour).Add(211 * time.Nanosecond)
	precisionAssignment := workerAssignment
	precisionAssignment.ID = "assignment-nanosecond-cursors"
	precisionAssignment.EffectiveFrom = precisionStart
	precisionAssignment.EffectiveUntil = precisionEnd
	precisionAssignment.Volumes = append([]domain.Volume(nil), workerAssignment.Volumes...)
	precisionAssignment.Volumes[0].StartsAt = precisionStart
	precisionAssignment.Volumes[0].EndsAt = precisionEnd
	activateAssignment(t, ctx, store, precisionAssignment, "nanosecond-cursors", precisionStart)
	precisionClaims, err := store.ClaimDueAssignments(ctx, "nanosecond-worker", 5*time.Second, 20)
	if err != nil {
		t.Fatal(err)
	}
	var precisionClaim postgresstore.Claim
	for _, claim := range precisionClaims {
		if claim.Assignment.ID == precisionAssignment.ID {
			precisionClaim = claim
		}
	}
	if precisionClaim.Assignment.ID == "" {
		t.Fatalf("nanosecond assignment was not claimed: %#v", precisionClaims)
	}
	if !precisionClaim.AuthorityFrom.Equal(precisionStart) || !precisionClaim.AuthorityUntil.Equal(precisionEnd) {
		t.Fatalf("claim lost exact authority bounds: got [%s,%s), want [%s,%s)", precisionClaim.AuthorityFrom, precisionClaim.AuthorityUntil, precisionStart, precisionEnd)
	}
	precisionObservedAt := precisionStart.Add(time.Second).Add(317 * time.Nanosecond)
	if err = store.CommitEvaluation(ctx, precisionClaim, postgresstore.EvaluationCommit{Evaluation: sampleEvaluation(precisionObservedAt), NextEvaluationAt: time.Now().Add(-time.Second)}); err != nil {
		t.Fatalf("commit nanosecond checkpoint: %v", err)
	}
	if _, found, err = store.GetReplayCheckpoint(ctx, precisionAssignment.ID, precisionAssignment.Generation, precisionObservedAt.Add(-time.Nanosecond)); err != nil || found {
		t.Fatalf("checkpoint crossed exact nanosecond boundary: found=%v err=%v", found, err)
	}
	precisionCheckpoint, found, err := store.GetReplayCheckpoint(ctx, precisionAssignment.ID, precisionAssignment.Generation, precisionObservedAt)
	if err != nil || !found || !precisionCheckpoint.StateThroughAt.Equal(precisionObservedAt) {
		t.Fatalf("checkpoint lost exact nanosecond cursor: checkpoint=%#v found=%v err=%v", precisionCheckpoint, found, err)
	}
	precisionClaims, err = store.ClaimDueAssignments(ctx, "nanosecond-worker", 5*time.Second, 20)
	if err != nil {
		t.Fatal(err)
	}
	precisionClaim = postgresstore.Claim{}
	for _, claim := range precisionClaims {
		if claim.Assignment.ID == precisionAssignment.ID {
			precisionClaim = claim
		}
	}
	if precisionClaim.Assignment.ID == "" {
		t.Fatalf("nanosecond assignment was not reclaimed: %#v", precisionClaims)
	}
	sameTimeLaterCursor := sampleEvaluation(precisionObservedAt)
	sameTimeLaterCursor.WALSequence = 2
	sameTimeLaterCursor.FrameID = "blue-frame-2"
	if err = store.CommitEvaluation(ctx, precisionClaim, postgresstore.EvaluationCommit{Evaluation: sameTimeLaterCursor, NextEvaluationAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("commit later same-time cursor: %v", err)
	}
	precisionCheckpoint, found, err = store.GetReplayCheckpoint(ctx, precisionAssignment.ID, precisionAssignment.Generation, precisionObservedAt)
	if err != nil || !found || precisionCheckpoint.WALSequence != 2 || precisionCheckpoint.FrameID != sameTimeLaterCursor.FrameID {
		t.Fatalf("later same-time cursor did not advance: checkpoint=%#v found=%v err=%v", precisionCheckpoint, found, err)
	}

	t.Run("blue-green assignment cutover", func(t *testing.T) {
		testBlueGreenCutover(t, ctx, store, pg.URL, now)
	})
}

func activateAssignment(t *testing.T, ctx context.Context, store *postgresstore.Store, assignment domain.Assignment, messagePrefix string, effectiveAt time.Time) {
	t.Helper()
	if result, err := store.PrepareAssignment(ctx, "api", messagePrefix+"-prepare", "assignment_prepared", assignment); err != nil || result.Disposition != postgresstore.ApplyApplied {
		t.Fatalf("prepare=%#v err=%v", result, err)
	}
	if result, err := store.ArmAssignment(ctx, "api", messagePrefix+"-arm", assignment.ID, assignment.Generation); err != nil || result.Record.Lifecycle != domain.AssignmentArmed {
		t.Fatalf("arm=%#v err=%v", result, err)
	}
	if result, err := store.CutoverAssignment(ctx, "api", messagePrefix+"-cutover", assignment.ID, assignment.Generation, effectiveAt); err != nil || result.Record.Lifecycle != domain.AssignmentActive {
		t.Fatalf("cutover=%#v err=%v", result, err)
	}
}

func testBlueGreenCutover(t *testing.T, ctx context.Context, store *postgresstore.Store, postgresURL string, now time.Time) {
	base := domain.Assignment{ID: "assignment-blue-green", Generation: 1, AircraftID: "aircraft-blue", AgentID: "agent-blue", FlightID: "flight-blue", IntentID: "intent-blue", IntentVersion: 1, PolicyVersion: "standard-v1", EffectiveFrom: now.Add(-time.Hour), EffectiveUntil: now.Add(time.Hour), Volumes: []domain.Volume{{ID: "green-1", Polygon: []domain.Point{{Latitude: 35, Longitude: -97.01}, {Latitude: 35.01, Longitude: -97.01}, {Latitude: 35.01, Longitude: -97}, {Latitude: 35, Longitude: -97}}, AltitudeLowerM: 80, AltitudeUpperM: 120, AltitudeReference: domain.AltitudeMSL, StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour)}}}
	firstAuthority := now.Add(-10 * time.Second)
	activateAssignment(t, ctx, store, base, "blue-v1", firstAuthority)
	claims, err := store.ClaimDueAssignments(ctx, "blue-old-worker", 30*time.Second, 20)
	if err != nil {
		t.Fatal(err)
	}
	var oldClaim postgresstore.Claim
	for _, claim := range claims {
		if claim.Assignment.ID == base.ID {
			oldClaim = claim
		}
	}
	if oldClaim.Assignment.ID == "" {
		t.Fatalf("active generation was not claimable: %#v", claims)
	}

	candidate := base
	candidate.Generation = 2
	candidate.IntentVersion = 2
	candidate.Volumes[0].ID = "green-2"
	if result, err := store.PrepareAssignment(ctx, "api", "blue-v2-prepare", "assignment_prepared", candidate); err != nil || result.Disposition != postgresstore.ApplyApplied {
		t.Fatalf("prepare candidate=%#v err=%v", result, err)
	}
	before, found, err := store.ResolveAssignmentAt(ctx, base.ID, now.Add(-time.Second))
	if err != nil || !found || before.Assignment.Generation != 1 {
		t.Fatalf("prepared candidate changed authority: %#v found=%v err=%v", before, found, err)
	}
	if result, err := store.ArmAssignment(ctx, "api", "blue-v2-arm", candidate.ID, candidate.Generation); err != nil || result.Record.Lifecycle != domain.AssignmentArmed || result.Record.AuthorityFrom != nil {
		t.Fatalf("armed candidate gained authority: %#v err=%v", result, err)
	}
	claims, err = store.ClaimDueAssignments(ctx, "blue-candidate-probe", time.Second, 20)
	if err != nil {
		t.Fatal(err)
	}
	for _, claim := range claims {
		if claim.Assignment.ID == candidate.ID && claim.Assignment.Generation == candidate.Generation {
			t.Fatal("armed candidate was claimable before cutover")
		}
	}
	if result, err := store.CancelCandidate(ctx, "api", "blue-v2-cancel", candidate.ID, candidate.Generation); err != nil || result.Record.Lifecycle != domain.AssignmentCancelled {
		t.Fatalf("cancel candidate=%#v err=%v", result, err)
	}
	stillCurrent, found, err := store.ResolveAssignmentAt(ctx, base.ID, now)
	if err != nil || !found || stillCurrent.Assignment.Generation != 1 {
		t.Fatalf("cancel disturbed current authority: %#v found=%v err=%v", stillCurrent, found, err)
	}

	replacement := candidate
	replacement.Generation = 3
	replacement.IntentVersion = 3
	replacement.Volumes[0].ID = "green-3"
	if _, err = store.PrepareAssignment(ctx, "api", "blue-v3-prepare", "assignment_prepared", replacement); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ArmAssignment(ctx, "api", "blue-v3-arm", replacement.ID, replacement.Generation); err != nil {
		t.Fatal(err)
	}
	cutover := time.Now().UTC().Add(-time.Millisecond)
	result, err := store.CutoverAssignment(ctx, "api", "blue-v3-cutover", replacement.ID, replacement.Generation, cutover)
	if err != nil || result.Record.Lifecycle != domain.AssignmentActive || result.Record.AuthorityFrom == nil || !result.Record.AuthorityFrom.Equal(cutover) {
		t.Fatalf("replacement cutover=%#v err=%v", result, err)
	}
	oldAtBoundary, found, err := store.ResolveAssignmentAt(ctx, base.ID, cutover.Add(-time.Nanosecond))
	if err != nil || !found || oldAtBoundary.Assignment.Generation != 1 {
		t.Fatalf("pre-cutover observation resolved to %#v found=%v err=%v", oldAtBoundary, found, err)
	}
	newAtBoundary, found, err := store.ResolveAssignmentAt(ctx, base.ID, cutover)
	if err != nil || !found || newAtBoundary.Assignment.Generation != 3 {
		t.Fatalf("cutover observation resolved to %#v found=%v err=%v", newAtBoundary, found, err)
	}
	if err = store.CommitEvaluation(ctx, oldClaim, postgresstore.EvaluationCommit{Evaluation: sampleEvaluation(cutover), NextEvaluationAt: cutover.Add(time.Second)}); err != postgresstore.ErrLeaseLost {
		t.Fatalf("superseded worker commit=%v", err)
	}
	claims, err = store.ClaimDueAssignments(ctx, "blue-new-worker", time.Second, 20)
	if err != nil {
		t.Fatal(err)
	}
	foundReplacementClaim := false
	for _, claim := range claims {
		if claim.Assignment.ID == replacement.ID && claim.Assignment.Generation == replacement.Generation {
			foundReplacementClaim = true
		}
	}
	if !foundReplacementClaim {
		t.Fatal("replacement was not claimable after cutover")
	}
	idempotent, err := store.CutoverAssignment(ctx, "api", "blue-v3-cutover", replacement.ID, replacement.Generation, cutover)
	if err != nil || idempotent.Disposition != postgresstore.ApplyIdempotent {
		t.Fatalf("cutover retry=%#v err=%v", idempotent, err)
	}
	if _, err = store.CutoverAssignment(ctx, "api", "blue-v3-cutover", replacement.ID, replacement.Generation, cutover.Add(time.Second)); err != postgresstore.ErrMessageConflict {
		t.Fatalf("conflicting cutover retry=%v", err)
	}
	stale := base
	if result, err := store.PrepareAssignment(ctx, "api", "blue-stale", "assignment_prepared", stale); err != nil || result.Disposition != postgresstore.ApplyStale {
		t.Fatalf("stale prepare=%#v err=%v", result, err)
	}

	future := replacement
	future.Generation = 4
	future.IntentVersion = 4
	if _, err = store.PrepareAssignment(ctx, "api", "blue-v4-prepare", "assignment_prepared", future); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ArmAssignment(ctx, "api", "blue-v4-arm", future.ID, future.Generation); err != nil {
		t.Fatal(err)
	}
	if _, err = store.CutoverAssignment(ctx, "api", "blue-v4-future", future.ID, future.Generation, time.Now().UTC().Add(time.Minute)); !errors.Is(err, postgresstore.ErrInvalidTransition) {
		t.Fatalf("future cutover error=%v", err)
	}
	if _, err = store.CancelCandidate(ctx, "api", "blue-v4-cancel", future.ID, future.Generation); err != nil {
		t.Fatal(err)
	}
	current, found, err := store.ResolveAssignmentAt(ctx, base.ID, time.Now().UTC())
	if err != nil || !found || current.Assignment.Generation != 3 {
		t.Fatalf("failed future cutover disturbed authority: %#v found=%v err=%v", current, found, err)
	}
	assertBlueGreenPersistence(t, ctx, postgresURL, base.ID, cutover, replacement.EffectiveUntil)
}

func assertBlueGreenPersistence(t *testing.T, ctx context.Context, url, assignmentID string, cutover, replacementEnd time.Time) {
	t.Helper()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var currentCount, candidateCount, oldSupersededTransitions int
	var oldUntilUnixNS, newFromUnixNS, newUntilUnixNS int64
	err = conn.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM conformance_assignments WHERE assignment_id=$1 AND lifecycle_state IN ('active','ending')),
	  (SELECT count(*) FROM conformance_assignments WHERE assignment_id=$1 AND lifecycle_state IN ('candidate_received','candidate_armed')),
	  (SELECT authority_until_unix_ns FROM conformance_assignments WHERE assignment_id=$1 AND assignment_generation=1),
	  (SELECT authority_from_unix_ns FROM conformance_assignments WHERE assignment_id=$1 AND assignment_generation=3),
	  (SELECT authority_until_unix_ns FROM conformance_assignments WHERE assignment_id=$1 AND assignment_generation=3),
	  (SELECT count(*) FROM conformance_assignment_transitions WHERE assignment_id=$1 AND assignment_generation=1 AND from_state='active' AND to_state='superseded' AND effective_at=$2)`, assignmentID, cutover).Scan(&currentCount, &candidateCount, &oldUntilUnixNS, &newFromUnixNS, &newUntilUnixNS, &oldSupersededTransitions)
	if err != nil {
		t.Fatal(err)
	}
	if currentCount != 1 || candidateCount != 0 || oldUntilUnixNS != cutover.UnixNano() || newFromUnixNS != cutover.UnixNano() || newUntilUnixNS != replacementEnd.UnixNano() || oldSupersededTransitions != 1 {
		t.Fatalf("blue-green persistence current=%d candidate=%d old_until_ns=%d new_from_ns=%d new_until_ns=%d old_transitions=%d", currentCount, candidateCount, oldUntilUnixNS, newFromUnixNS, newUntilUnixNS, oldSupersededTransitions)
	}
}

func sampleEvaluation(at time.Time) domain.Evaluation {
	return domain.Evaluation{Condition: domain.ConditionConforming, Monitoring: domain.MonitoringCurrent, Recording: domain.RecordingPending, State: domain.EvaluatorState{Violations: map[domain.ViolationType]domain.IncidentState{}}, CausalFrom: at, ObservedAt: at, FrameID: "blue-frame", WALID: "blue-wal", WALSequence: 1}
}

func assertDurableEvaluation(t *testing.T, ctx context.Context, url, assignmentID string, wantIncidents, wantEvents, wantRevisions int) {
	t.Helper()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var incidents, events, checkpoints, summaries, registryOutbox int
	var recordingColumn, recordingPayload string
	err = conn.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM conformance_incidents WHERE assignment_id=$1),
  (SELECT count(*) FROM conformance_events WHERE assignment_id=$1),
  (SELECT count(*) FROM conformance_checkpoints WHERE assignment_id=$1),
  (SELECT count(*) FROM conformance_summaries WHERE assignment_id=$1),
  (SELECT count(*) FROM conformance_outbox WHERE assignment_id=$1 AND destination='registry'),
  (SELECT recording_status FROM conformance_summaries WHERE assignment_id=$1),
  (SELECT payload->>'recording' FROM conformance_summaries WHERE assignment_id=$1)`, assignmentID).Scan(&incidents, &events, &checkpoints, &summaries, &registryOutbox, &recordingColumn, &recordingPayload)
	if err != nil {
		t.Fatal(err)
	}
	if incidents != wantIncidents || events != wantEvents || checkpoints != wantRevisions || summaries != 1 || registryOutbox != wantRevisions || recordingColumn != "confirmed" || recordingPayload != "confirmed" {
		t.Fatalf("durable state incidents=%d events=%d checkpoints=%d summaries=%d outbox=%d recording=%s/%s", incidents, events, checkpoints, summaries, registryOutbox, recordingColumn, recordingPayload)
	}
}

func positionPoint(at time.Time, frame string, seq uint64, lat, lon, alt float64) *influxdb3.Point {
	return influxdb3.NewPoint("aircraft_telemetry", map[string]string{"agent_id": "agent-1", "frame_id": frame, "message_name": "global_position_int", "schema_version": "1"}, map[string]interface{}{"aircraft_id": "aircraft-1", "flight_id": "flight-1", "intent_id": "intent-1", "intent_version": uint64(1), "wal_id": "wal-1", "wal_sequence": seq, "latitude_deg": lat, "longitude_deg": lon, "altitude_msl_m": alt, "relay_id": "relay-1", "session_id": "session-1", "timestamp_source": "agent_capture", "message_id": uint64(33), "dialect": "common"}, at)
}
