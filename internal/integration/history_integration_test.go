//go:build integration

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package integration

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	postgresstore "github.com/aero-arc/aero-arc-conformance/internal/store/postgres"
	"github.com/aero-arc/aero-arc-conformance/internal/testsupport"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"os"
	"testing"
	"time"
)

func TestDurableHistoryPaginationAndIsolation(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	conn, err := pgx.Connect(ctx, pg.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	// Start from the existing baseline to prove the new index migration preserves history.
	baseline, err := os.ReadFile("../store/postgres/migrations/001_initial.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, string(baseline)); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO schema_migrations(version,checksum) VALUES(1,$1)`, fmt.Sprintf("%x", sha256.Sum256(baseline))); err != nil {
		t.Fatal(err)
	}
	// Read fixtures intentionally use completed authority so no worker can claim them.
	for _, assignment := range []string{"a", "other"} {
		for _, generation := range []int{1, 2} {
			_, err = conn.Exec(ctx, `INSERT INTO conformance_assignments(assignment_id,assignment_generation,aircraft_id,agent_id,flight_id,intent_id,intent_version,policy_version,lifecycle_state,specification) VALUES($1,$2,'aircraft','agent','flight',$1,$3,'v1','completed','{}')`, assignment, generation, generation)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	at := time.Date(2026, 9, 1, 0, 0, 0, 123456000, time.UTC)
	if _, err := conn.Exec(ctx, `UPDATE conformance_assignments SET specification='{"volumes":[{"starts_at":"2026-08-31T23:00:00Z","ends_at":"2026-09-01T00:00:00Z"}]}' WHERE assignment_id='a' AND assignment_generation=2`); err != nil {
		t.Fatal(err)
	}
	for _, e := range []struct {
		id, a, kind, transition string
		g                       int
		at                      time.Time
		d                       float64
	}{
		{"a", "a", "lateral_deviation", "opened", 1, at, 12.5},
		{"b", "a", "lateral_deviation", "resolved", 1, at, 0},
		{"c", "a", "temporal_deviation", "opened", 2, at.Add(time.Second), 0},
		{"z", "other", "lateral_deviation", "opened", 1, at.Add(time.Second), 900},
	} {
		_, err = conn.Exec(ctx, `INSERT INTO conformance_events(event_id,assignment_id,assignment_generation,transition,violation_type,observed_at,frame_id,wal_id,wal_sequence,evaluation_revision,payload,payload_sha256,deviation_m) VALUES($1,$2,$3,$4,$5,$6,'frame','wal',1,1,'{}','test-hash',$7)`, e.id, e.a, e.g, e.transition, e.kind, e.at, e.d)
		if err != nil {
			t.Fatal(err)
		}
	}
	query := postgresstore.HistoryQuery{AssignmentID: "a", PageSize: 1}
	store, err := postgresstore.Open(ctx, pg.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var ids []string
	for {
		page, err := store.ListConformanceEvents(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Events {
			ids = append(ids, e.ID)
			if e.ID == "c" && e.DeviationM != nil {
				t.Fatal("temporal event fabricated meter measurement")
			}
			if e.ID == "c" && (e.PlannedEndAt == nil || !e.PlannedEndAt.Equal(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))) {
				t.Fatal("historical planned end lost")
			}
			if e.ID == "b" && e.PlannedEndAt != nil {
				t.Fatal("plan bounds leaked across generations")
			}
			if e.ID == "b" && (e.DeviationM == nil || *e.DeviationM != 0) {
				t.Fatal("measured zero lost")
			}
		}
		if page.NextPageToken == "" {
			break
		}
		query.PageToken = page.NextPageToken
	}
	if len(ids) != 3 || ids[0] != "c" || ids[1] != "b" || ids[2] != "a" {
		t.Fatalf("unstable pagination: %v", ids)
	}
	until := at.Add(time.Second)
	page, err := store.ListConformanceEvents(ctx, postgresstore.HistoryQuery{AssignmentID: "a", Generation: 1, From: &at, Until: &until})
	if err != nil || len(page.Events) != 2 {
		t.Fatalf("bounded generation read: %+v %v", page, err)
	}
	page, err = store.ListConformanceEvents(ctx, postgresstore.HistoryQuery{AssignmentID: "missing"})
	if err != nil || len(page.Events) != 0 {
		t.Fatalf("empty read: %+v %v", page, err)
	}
	query.AssignmentID = "other"
	if _, err = store.ListConformanceEvents(ctx, query); !errors.Is(err, postgresstore.ErrInvalidHistoryQuery) {
		t.Fatalf("cross-scope token: %v", err)
	}
	// Reopening applies no destructive migration and preserves existing evidence.
	reopened, err := postgresstore.Open(ctx, pg.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	page, err = reopened.ListConformanceEvents(ctx, postgresstore.HistoryQuery{AssignmentID: "a"})
	if err != nil || len(page.Events) != 3 {
		t.Fatalf("reopen lost history: %+v %v", page, err)
	}
	var indexCount int
	if err = conn.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE indexname='conformance_events_history_read'`).Scan(&indexCount); err != nil || indexCount != 1 {
		t.Fatalf("history migration: count=%d err=%v", indexCount, err)
	}
}
