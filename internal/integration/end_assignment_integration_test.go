//go:build integration

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package integration

import (
	"context"
	"errors"
	"fmt"
	"github.com/aero-arc/aero-arc-conformance/internal/domain"
	postgres "github.com/aero-arc/aero-arc-conformance/internal/store/postgres"
	"github.com/aero-arc/aero-arc-conformance/internal/testsupport"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"os"
	"testing"
	"time"
)

func TestFlightCompletionClosesExactBindingAndFencesLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	dsn := os.Getenv("AERO_CONFORMANCE_TEST_POSTGRES_URL")
	if dsn == "" {
		testcontainers.SkipIfProviderIsNotHealthy(t)
		pg, err := testsupport.StartPostgres(ctx)
		if err != nil {
			t.Fatal(err)
		}
		dsn = pg.URL
		t.Cleanup(func() {
			if err := pg.Dependency.Shutdown(t.Failed(), os.Stderr); err != nil {
				t.Error(err)
			}
		})
	}
	s, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	now := time.Now().UTC().Add(-time.Minute)
	id := "completion-" + time.Now().Format("150405.000000000")
	a := domain.Assignment{ID: id, Generation: 7, AircraftID: id, AgentID: "agent", FlightID: id, IntentID: id, IntentVersion: 2, PolicyVersion: "standard-v1", EffectiveFrom: now.Add(-time.Hour), EffectiveUntil: now.Add(time.Hour), Volumes: []domain.Volume{{ID: "volume", Polygon: []domain.Point{{Latitude: 35, Longitude: -97.01}, {Latitude: 35.01, Longitude: -97.01}, {Latitude: 35.01, Longitude: -97}}, AltitudeLowerM: 0, AltitudeUpperM: 120, AltitudeReference: domain.AltitudeMSL, StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour)}}}
	activateAssignment(t, ctx, s, a, id, now.Add(-time.Second))
	if _, err = conn.Exec(ctx, `UPDATE conformance_assignments SET lease_owner='old-worker',lease_until=clock_timestamp()+interval '1 minute',lease_generation=10 WHERE assignment_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err = s.EndAssignment(ctx, "api", id+"-wrong", id, 7, "wrong-flight", a.AircraftID, a.IntentID, a.AgentID, 2, now); !errors.Is(err, postgres.ErrInvalidTransition) {
		t.Fatalf("wrong binding accepted: %v", err)
	}
	for _, gen := range []uint64{0, 7} {
		if _, err = s.EndAssignment(ctx, "api", id+"-wrong-intent", id, gen, a.FlightID, a.AircraftID, "different-intent", a.AgentID, 2, now); !errors.Is(err, postgres.ErrInvalidTransition) {
			t.Fatalf("wrong intent accepted for generation %d: %v", gen, err)
		}
	}
	for _, gen := range []uint64{0, 7} {
		if _, err = s.EndAssignment(ctx, "api", id+"-wrong-agent", id, gen, a.FlightID, a.AircraftID, a.IntentID, "other-agent", 2, now); !errors.Is(err, postgres.ErrInvalidTransition) {
			t.Fatalf("wrong Agent accepted for generation %d: %v", gen, err)
		}
	}
	if _, err = s.EndAssignment(ctx, "api", id+"-absent", id, 999, a.FlightID, a.AircraftID, a.IntentID, a.AgentID, 2, now); !errors.Is(err, postgres.ErrAssignmentNotFound) {
		t.Fatalf("absent generation: %v", err)
	}
	// Use a past upper bound so the upper-bound check is independent of future-time rejection.
	if _, err = conn.Exec(ctx, `UPDATE conformance_assignments SET authority_until=$2,authority_until_unix_ns=$3 WHERE assignment_id=$1`, id, now.Add(time.Second), now.Add(time.Second).UnixNano()); err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{now.Add(time.Second), now.Add(2 * time.Second)} {
		if _, err = s.EndAssignment(ctx, "api", id+"-outside", id, 7, a.FlightID, a.AircraftID, a.IntentID, a.AgentID, 2, at); !errors.Is(err, postgres.ErrInvalidTransition) {
			t.Fatalf("completion at/after end: %v", err)
		}
	}
	if _, err = conn.Exec(ctx, `UPDATE conformance_assignments SET authority_until=$2,authority_until_unix_ns=$3 WHERE assignment_id=$1`, id, a.EffectiveUntil, a.EffectiveUntil.UnixNano()); err != nil {
		t.Fatal(err)
	}
	watermark := now.Add(2 * time.Second)
	if _, err = conn.Exec(ctx, `INSERT INTO conformance_summaries(assignment_id,assignment_generation,evaluation_revision,condition,monitoring_status,recording_status,observed_at,observed_at_unix_ns,frame_id,payload) VALUES($1,7,1,'conforming','current','recorded',$2,$3,'frame','{}')`, id, watermark, watermark.UnixNano()); err != nil {
		t.Fatal(err)
	}
	candidate := a
	candidate.Generation = 8
	if _, err = s.PrepareAssignment(ctx, "api", id+"-candidate", "assignment_prepared", candidate); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ArmAssignment(ctx, "api", id+"-candidate-arm", id, 8); err != nil {
		t.Fatal(err)
	}
	ended, err := s.EndAssignment(ctx, "api", id+"-end", id, 0, a.FlightID, a.AircraftID, a.IntentID, a.AgentID, 2, now)
	if err != nil || ended.Lifecycle != domain.AssignmentEnding || ended.Assignment.Generation != 7 || !ended.AuthorityUntil.Equal(watermark.Add(time.Nanosecond)) {
		t.Fatalf("end=%+v err=%v", ended, err)
	}
	if _, err = s.EndAssignment(ctx, "api", id+"-second-end", id, 7, a.FlightID, a.AircraftID, a.IntentID, a.AgentID, 2, now.Add(-time.Millisecond)); !errors.Is(err, postgres.ErrInvalidTransition) {
		t.Fatalf("second closure changed ending authority: %v", err)
	}
	if _, err = s.CutoverAssignment(ctx, "api", id+"-delayed-cutover", id, 8, now); !errors.Is(err, postgres.ErrInvalidTransition) {
		t.Fatalf("cutover reopened completion: %v", err)
	}
	var generation int64
	var owner *string
	if err = conn.QueryRow(ctx, `SELECT lease_generation,lease_owner FROM conformance_assignments WHERE assignment_id=$1 AND assignment_generation=7`, id).Scan(&generation, &owner); err != nil || generation != 11 || owner != nil {
		t.Fatalf("lease not fenced: %d %v %v", generation, owner, err)
	}
	replay, err := s.EndAssignment(ctx, "api", id+"-end", id, 0, a.FlightID, a.AircraftID, a.IntentID, a.AgentID, 2, now)
	if err != nil || !replay.AuthorityUntil.Equal(*ended.AuthorityUntil) {
		t.Fatalf("replay=%+v %v", replay, err)
	}
	if _, err = s.EndAssignment(ctx, "api", id+"-end", id, 0, a.FlightID, a.AircraftID, a.IntentID, a.AgentID, 2, now.Add(time.Second)); !errors.Is(err, postgres.ErrMessageConflict) {
		t.Fatalf("changed event accepted: %v", err)
	}
	for i := 0; i < 3; i++ {
		active := a
		active.ID = fmt.Sprintf("%s-backlog-%d", id, i)
		active.AircraftID = active.ID
		active.FlightID = active.ID
		activateAssignment(t, ctx, s, active, active.ID, now.Add(-time.Second))
		if _, err = conn.Exec(ctx, `UPDATE conformance_assignments SET next_evaluation_at=clock_timestamp()-interval '1 hour' WHERE assignment_id=$1`, active.ID); err != nil {
			t.Fatal(err)
		}
	}
	claims, err := s.ClaimDueAssignmentsWithFinalizationGrace(ctx, "late-tail", time.Second, 10*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	var tail *postgres.Claim
	for i := range claims {
		if claims[i].Assignment.ID == id {
			tail = &claims[i]
		}
	}
	if tail == nil {
		t.Fatal("late completion stranded the final telemetry drain")
	}
	if !tail.AuthorityUntil.Equal(*ended.AuthorityUntil) {
		t.Fatal("claim extended event-time authority")
	}
	if err = s.RenewAssignmentLease(ctx, *tail, time.Second); err != nil {
		t.Fatalf("late final claim cannot renew: %v", err)
	}

	historical := a
	historical.ID = id + "-historical"
	historical.AircraftID = historical.ID
	historical.FlightID = historical.ID
	past := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
	historical.EffectiveFrom = past.Add(-time.Hour)
	historical.EffectiveUntil = past.Add(time.Hour)
	historical.Volumes = append([]domain.Volume(nil), a.Volumes...)
	historical.Volumes[0].StartsAt = historical.EffectiveFrom
	historical.Volumes[0].EndsAt = historical.EffectiveUntil
	activateAssignment(t, ctx, s, historical, historical.ID, past.Add(-time.Second))
	closed, err := s.EndAssignment(ctx, "api", id+"-historic-end", historical.ID, 7, historical.FlightID, historical.AircraftID, historical.IntentID, historical.AgentID, 2, past)
	if err != nil || closed.AuthorityUntil == nil || !closed.AuthorityUntil.Equal(past.Add(time.Nanosecond)) {
		t.Fatalf("pre-epoch closure: %+v %v", closed, err)
	}

}
