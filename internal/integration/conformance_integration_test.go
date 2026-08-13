//go:build integration

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	influxdb3 "github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/aero-arc/aero-arc-conformance/internal/domain"
	"github.com/aero-arc/aero-arc-conformance/internal/evaluator"
	postgresstore "github.com/aero-arc/aero-arc-conformance/internal/store/postgres"
	telemetryinflux "github.com/aero-arc/aero-arc-conformance/internal/telemetry/influx"
	"github.com/aero-arc/aero-arc-conformance/internal/testsupport"
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
	if result, err := store.ApplyAssignment(ctx, "api", "message-1", "prepare", assignment); err != nil || result.Disposition != postgresstore.ApplyApplied {
		t.Fatalf("apply=%#v err=%v", result, err)
	}
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
	if _, err = store.ApplyAssignment(ctx, "api", "message-2", "prepare", assignment); err != nil {
		t.Fatal(err)
	}
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
