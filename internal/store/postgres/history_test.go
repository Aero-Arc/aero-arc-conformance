// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package postgres

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/aero-arc/aero-arc-conformance/internal/domain"
	"math"
	"testing"
	"time"
)

func TestHistoryPlanBounds(t *testing.T) {
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	start, end := historyPlanBounds([]domain.Volume{{StartsAt: at.Add(time.Hour), EndsAt: at.Add(2 * time.Hour)}, {StartsAt: at, EndsAt: at.Add(time.Minute)}})
	if start == nil || end == nil || !start.Equal(at) || !end.Equal(at.Add(2*time.Hour)) {
		t.Fatalf("bounds=%v %v", start, end)
	}
	for _, volumes := range [][]domain.Volume{nil, {{StartsAt: at}}, {{StartsAt: at, EndsAt: at}}, {{StartsAt: at, EndsAt: at.Add(time.Hour)}, {}}} {
		start, end := historyPlanBounds(volumes)
		if start != nil || end != nil {
			t.Fatal("fabricated bounds for incomplete plan")
		}
	}
}

func TestHistoryBounds(t *testing.T) {
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	raw, _ := json.Marshal(historyCursor{Version: 1, AssignmentID: "a", Generation: 2, At: now, ID: "event"})
	token := base64.RawURLEncoding.EncodeToString(raw)
	for _, q := range []HistoryQuery{{}, {AssignmentID: "a", PageSize: -1}, {AssignmentID: "a", PageSize: 201}, {AssignmentID: "a", Generation: math.MaxUint64}, {AssignmentID: "a", From: &now, Until: &now}, {AssignmentID: "a", PageToken: "broken"}, {AssignmentID: "other", Generation: 2, PageToken: token}, {AssignmentID: "a", Generation: 3, PageToken: token}, {AssignmentID: "a", Generation: 2, From: &now, PageToken: token}} {
		if _, _, err := historyBounds(q); !errors.Is(err, ErrInvalidHistoryQuery) {
			t.Fatalf("accepted %#v: %v", q, err)
		}
	}
	size, c, err := historyBounds(HistoryQuery{AssignmentID: "a", Generation: 2, PageToken: token})
	if err != nil || size != 50 || c.ID != "event" {
		t.Fatalf("size=%d cursor=%+v err=%v", size, c, err)
	}
}
