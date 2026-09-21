// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/aero-arc/aero-arc-conformance/internal/domain"
)

// ErrInvalidHistoryQuery identifies invalid filters or continuation tokens.
var ErrInvalidHistoryQuery = errors.New("invalid conformance history query")

// HistoryQuery selects bounded, newest-first immutable incident evidence.
type HistoryQuery struct {
	AssignmentID string
	Generation   uint64
	From, Until  *time.Time
	PageSize     int
	PageToken    string
}

// HistoryEvent identifies the assignment generation and immutable transition.
type HistoryEvent struct {
	ID, AssignmentID, IntentID, AircraftID, FlightID, IncidentID string
	Generation                                                   uint64
	IntentVersion                                                uint32
	Transition, ViolationType, FrameID                           string
	ObservedAt                                                   time.Time
	DeviationM                                                   *float64
	EvaluationRevision                                           uint64
	PlannedStartAt, PlannedEndAt                                 *time.Time
}

// HistoryPage contains a bounded page and an opaque continuation token.
type HistoryPage struct {
	Events        []HistoryEvent
	NextPageToken string
}

type historyCursor struct {
	Version      int    `json:"v"`
	AssignmentID string `json:"a"`
	Generation   uint64 `json:"g"`
	From, Until  *time.Time
	At           time.Time `json:"t"`
	ID           string    `json:"i"`
}

func historyBounds(q HistoryQuery) (int, *historyCursor, error) {
	invalid := func() (int, *historyCursor, error) { return 0, nil, ErrInvalidHistoryQuery }
	if strings.TrimSpace(q.AssignmentID) == "" || len(q.AssignmentID) > 512 || q.Generation > math.MaxInt64 || q.PageSize < 0 || q.PageSize > 200 || len(q.PageToken) > 4096 {
		return invalid()
	}
	if q.From != nil && q.Until != nil && !q.From.Before(*q.Until) {
		return invalid()
	}
	size := q.PageSize
	if size == 0 {
		size = 50
	}
	if q.PageToken == "" {
		return size, nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(q.PageToken)
	if err != nil {
		return invalid()
	}
	var c historyCursor
	if json.Unmarshal(raw, &c) != nil || c.Version != 1 || c.AssignmentID != q.AssignmentID || c.Generation != q.Generation || !sameHistoryTime(c.From, q.From) || !sameHistoryTime(c.Until, q.Until) || c.At.IsZero() || c.ID == "" || len(c.ID) > 1024 {
		return invalid()
	}
	return size, &c, nil
}

func sameHistoryTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

// ListConformanceEvents reads durable transition evidence without modifying
// evaluation, incident, checkpoint, or live projection state.
//
// Parameters:
//   - ctx: bounds the database read and cancellation.
//   - query: scopes one assignment, optional generation and half-open time
//     bounds; page size defaults to 50 and is capped at 200.
//
// Returns:
//   - page: newest-first rows, using (observed_at, event_id) keyset pagination.
//     Concurrent late inserts require a first-page refresh; this is not a snapshot.
//   - error: reports invalid filters/tokens or database failures, never a false empty page.
func (s *Store) ListConformanceEvents(ctx context.Context, query HistoryQuery) (HistoryPage, error) {
	size, cursor, err := historyBounds(query)
	if err != nil {
		return HistoryPage{}, err
	}
	var afterAt *time.Time
	var afterID string
	if cursor != nil {
		afterAt, afterID = &cursor.At, cursor.ID
	}
	rows, err := s.pool.Query(ctx, `SELECT e.event_id,e.assignment_id,e.assignment_generation,
 a.intent_id,a.intent_version,a.aircraft_id,a.flight_id,COALESCE(e.incident_id,''),
 e.transition,e.violation_type,e.observed_at,e.deviation_m,e.frame_id,e.evaluation_revision,a.specification
 FROM conformance_events e JOIN conformance_assignments a USING(assignment_id,assignment_generation)
 WHERE e.assignment_id=$1 AND ($2::bigint=0 OR e.assignment_generation=$2)
 AND ($3::timestamptz IS NULL OR e.observed_at >= $3)
 AND ($4::timestamptz IS NULL OR e.observed_at < $4)
 AND ($5::timestamptz IS NULL OR (e.observed_at,e.event_id) < ($5,$6::text))
 ORDER BY e.observed_at DESC,e.event_id DESC LIMIT $7`, query.AssignmentID, int64(query.Generation), query.From, query.Until, afterAt, afterID, size+1)
	if err != nil {
		return HistoryPage{}, fmt.Errorf("read conformance history: %w", err)
	}
	defer rows.Close()
	page := HistoryPage{Events: make([]HistoryEvent, 0, size)}
	for rows.Next() {
		var e HistoryEvent
		var deviation float64
		var specification []byte
		if err := rows.Scan(&e.ID, &e.AssignmentID, &e.Generation, &e.IntentID, &e.IntentVersion, &e.AircraftID, &e.FlightID, &e.IncidentID, &e.Transition, &e.ViolationType, &e.ObservedAt, &deviation, &e.FrameID, &e.EvaluationRevision, &specification); err != nil {
			return HistoryPage{}, fmt.Errorf("decode conformance history: %w", err)
		}
		if e.ViolationType == "lateral_deviation" || e.ViolationType == "altitude_deviation" {
			e.DeviationM = &deviation
		}
		var assignment domain.Assignment
		if err := json.Unmarshal(specification, &assignment); err != nil {
			return HistoryPage{}, fmt.Errorf("decode history assignment: %w", err)
		}
		e.PlannedStartAt, e.PlannedEndAt = historyPlanBounds(assignment.Volumes)
		page.Events = append(page.Events, e)
	}
	if err := rows.Err(); err != nil {
		return HistoryPage{}, fmt.Errorf("read conformance history: %w", err)
	}
	if len(page.Events) > size {
		page.Events = page.Events[:size]
		last := page.Events[size-1]
		raw, err := json.Marshal(historyCursor{Version: 1, AssignmentID: query.AssignmentID, Generation: query.Generation, From: query.From, Until: query.Until, At: last.ObservedAt, ID: last.ID})
		if err != nil {
			return HistoryPage{}, err
		}
		page.NextPageToken = base64.RawURLEncoding.EncodeToString(raw)
	}
	return page, nil
}

func historyPlanBounds(volumes []domain.Volume) (*time.Time, *time.Time) {
	if len(volumes) == 0 {
		return nil, nil
	}
	start, end := volumes[0].StartsAt, volumes[0].EndsAt
	for _, v := range volumes {
		if v.StartsAt.IsZero() || v.EndsAt.IsZero() || !v.StartsAt.Before(v.EndsAt) {
			return nil, nil
		}
		if v.StartsAt.Before(start) {
			start = v.StartsAt
		}
		if v.EndsAt.After(end) {
			end = v.EndsAt
		}
	}
	return &start, &end
}
