//go:build integration

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package integration

import (
	"context"
	"errors"
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
	if _, err = s.EndAssignment(ctx, "api", id+"-wrong", id, 7, "wrong-flight", a.AircraftID, 2, now); !errors.Is(err, postgres.ErrInvalidTransition) {
		t.Fatalf("wrong binding accepted: %v", err)
	}
	watermark := now.Add(2 * time.Second)
	if _, err = conn.Exec(ctx, `INSERT INTO conformance_summaries(assignment_id,assignment_generation,evaluation_revision,condition,monitoring_status,recording_status,observed_at,observed_at_unix_ns,frame_id,payload) VALUES($1,7,1,'conforming','current','recorded',$2,$3,'frame','{}')`, id, watermark, watermark.UnixNano()); err != nil {
		t.Fatal(err)
	}
	ended, err := s.EndAssignment(ctx, "api", id+"-end", id, 0, a.FlightID, a.AircraftID, 2, now)
	if err != nil || ended.Lifecycle != domain.AssignmentEnding || ended.Assignment.Generation != 7 || !ended.AuthorityUntil.Equal(watermark.Add(time.Nanosecond)) {
		t.Fatalf("end=%+v err=%v", ended, err)
	}
	var generation int64
	var owner *string
	if err = conn.QueryRow(ctx, `SELECT lease_generation,lease_owner FROM conformance_assignments WHERE assignment_id=$1`, id).Scan(&generation, &owner); err != nil || generation != 11 || owner != nil {
		t.Fatalf("lease not fenced: %d %v %v", generation, owner, err)
	}
	replay, err := s.EndAssignment(ctx, "api", id+"-end", id, 0, a.FlightID, a.AircraftID, 2, now)
	if err != nil || !replay.AuthorityUntil.Equal(*ended.AuthorityUntil) {
		t.Fatalf("replay=%+v %v", replay, err)
	}
	if _, err = s.EndAssignment(ctx, "api", id+"-end", id, 0, a.FlightID, a.AircraftID, 2, now.Add(time.Second)); !errors.Is(err, postgres.ErrMessageConflict) {
		t.Fatalf("changed event accepted: %v", err)
	}
}
