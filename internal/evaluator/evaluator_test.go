// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package evaluator

import (
	"testing"
	"time"

	"github.com/aero-arc/aero-arc-conformance/internal/domain"
)

func TestEvaluatorOpensAndResolvesIndependentIncidents(t *testing.T) {
	now := time.Date(2026, 8, 12, 18, 0, 0, 0, time.UTC)
	e := mustEvaluator(t, Policy{Version: "standard-v1", HorizontalToleranceM: 3, VerticalToleranceM: 2, OpenAfterSamples: 2, RecoverAfterSamples: 2, TelemetryFreshness: time.Minute})
	a := testAssignment(now)
	state := domain.EvaluatorState{}

	inside := observation(now, 1, 35.005, -97.005, 100)
	result := mustEvaluate(t, e, now, a, inside, state)
	if result.Condition != domain.ConditionConforming {
		t.Fatalf("inside condition = %s", result.Condition)
	}
	state = result.State

	outside := observation(now.Add(time.Second), 2, 35.02, -97.02, 130)
	result = mustEvaluate(t, e, now.Add(time.Second), a, outside, state)
	if result.Condition != domain.ConditionSuspected {
		t.Fatalf("first outside condition = %s", result.Condition)
	}
	state = result.State

	outside = observation(now.Add(2*time.Second), 3, 35.02, -97.02, 130)
	result = mustEvaluate(t, e, now.Add(2*time.Second), a, outside, state)
	if result.Condition != domain.ConditionNonConforming {
		t.Fatalf("second outside condition = %s", result.Condition)
	}
	if len(result.Transitions) != 2 {
		t.Fatalf("opened transitions = %d, want lateral+vertical", len(result.Transitions))
	}
	state = result.State

	result = mustEvaluate(t, e, now.Add(3*time.Second), a, observation(now.Add(3*time.Second), 4, 35.005, -97.005, 100), state)
	if result.Condition != domain.ConditionRecovering {
		t.Fatalf("first recovery = %s", result.Condition)
	}
	result = mustEvaluate(t, e, now.Add(4*time.Second), a, observation(now.Add(4*time.Second), 5, 35.005, -97.005, 100), result.State)
	if result.Condition != domain.ConditionConforming || len(result.Transitions) != 2 {
		t.Fatalf("resolved result = %#v", result)
	}
}

func TestEvaluatorDoesNotGuessAltitudeReference(t *testing.T) {
	now := time.Date(2026, 8, 12, 18, 0, 0, 0, time.UTC)
	e := mustEvaluator(t, Policy{Version: "standard-v1", OpenAfterSamples: 1, RecoverAfterSamples: 1, TelemetryFreshness: time.Minute})
	a := testAssignment(now)
	o := observation(now, 1, 35.005, -97.005, 100)
	o.AltitudeReference = domain.AltitudeAGL
	result := mustEvaluate(t, e, now, a, o, domain.EvaluatorState{})
	if result.Condition != domain.ConditionUnknown {
		t.Fatalf("condition = %s, want unknown", result.Condition)
	}
	if _, exists := result.State.Violations[domain.ViolationVertical]; exists && result.State.Violations[domain.ViolationVertical].Phase != domain.IncidentClear {
		t.Fatal("unknown vertical reference created a violation")
	}
}

func TestEvaluatorDoesNotCombineDifferentVolumes(t *testing.T) {
	now := time.Date(2026, 8, 12, 18, 0, 0, 0, time.UTC)
	e := mustEvaluator(t, Policy{Version: "standard-v1", OpenAfterSamples: 1, RecoverAfterSamples: 1, TelemetryFreshness: time.Minute})
	a := testAssignment(now)
	// The point is laterally inside volume A, but its altitude is authorized
	// only by spatially disjoint volume B. No single 4D volume contains it.
	a.Volumes[0].AltitudeLowerM, a.Volumes[0].AltitudeUpperM = 80, 100
	a.Volumes = append(a.Volumes, domain.Volume{ID: "volume-2", Polygon: []domain.Point{{Latitude: 36, Longitude: -98.01}, {Latitude: 36.01, Longitude: -98.01}, {Latitude: 36.01, Longitude: -98}, {Latitude: 36, Longitude: -98}}, AltitudeLowerM: 120, AltitudeUpperM: 140, AltitudeReference: domain.AltitudeMSL, StartsAt: now.Add(-time.Minute), EndsAt: now.Add(time.Hour)})
	result := mustEvaluate(t, e, now, a, observation(now, 1, 35.005, -97.005, 130), domain.EvaluatorState{})
	if result.Condition != domain.ConditionNonConforming || result.State.Violations[domain.ViolationVertical].Phase != domain.IncidentOpen {
		t.Fatalf("different volumes produced %#v", result)
	}
}

func TestUnknownAltitudeDoesNotResolveOpenVerticalIncident(t *testing.T) {
	now := time.Date(2026, 8, 12, 18, 0, 0, 0, time.UTC)
	e := mustEvaluator(t, Policy{Version: "standard-v1", OpenAfterSamples: 1, RecoverAfterSamples: 2, TelemetryFreshness: time.Minute})
	a := testAssignment(now)
	opened := mustEvaluate(t, e, now, a, observation(now, 1, 35.005, -97.005, 130), domain.EvaluatorState{})
	unknown := observation(now.Add(time.Second), 2, 35.005, -97.005, 0)
	unknown.AltitudeKnown = false
	for i := 0; i < 3; i++ {
		opened = mustEvaluate(t, e, now.Add(time.Duration(i+1)*time.Second), a, unknown, opened.State)
	}
	if opened.State.Violations[domain.ViolationVertical].Phase != domain.IncidentOpen {
		t.Fatalf("unknown altitude changed incident: %#v", opened.State)
	}
}

func TestMonitoringFreshnessIsTimerDriven(t *testing.T) {
	now := time.Now().UTC()
	e := mustEvaluator(t, Policy{Version: "standard-v1", OpenAfterSamples: 1, RecoverAfterSamples: 1, TelemetryFreshness: time.Minute})
	if got := e.AssessMonitoring(now, time.Time{}, true); got != domain.MonitoringStale {
		t.Fatalf("no telemetry = %s", got)
	}
	if got := e.AssessMonitoring(now, now.Add(-2*time.Minute), true); got != domain.MonitoringStale {
		t.Fatalf("silent telemetry = %s", got)
	}
	if got := e.AssessMonitoring(now, now.Add(-time.Hour), false); got != domain.MonitoringUnavailable {
		t.Fatalf("dependency outage = %s", got)
	}
}

func TestVolumeWindowsAreHalfOpenAndGapsAreTemporal(t *testing.T) {
	now := time.Date(2026, 8, 12, 18, 0, 0, 0, time.UTC)
	e := mustEvaluator(t, Policy{Version: "standard-v1", OpenAfterSamples: 1, RecoverAfterSamples: 1, TelemetryFreshness: time.Minute})
	a := testAssignment(now)
	a.Volumes[0].EndsAt = now
	a.Volumes = append(a.Volumes, domain.Volume{ID: "later", Polygon: a.Volumes[0].Polygon, AltitudeLowerM: 80, AltitudeUpperM: 120, AltitudeReference: domain.AltitudeMSL, StartsAt: now.Add(time.Second), EndsAt: now.Add(time.Hour)})
	result := mustEvaluate(t, e, now, a, observation(now, 1, 35.005, -97.005, 100), domain.EvaluatorState{})
	if result.State.Violations[domain.ViolationTemporal].Phase != domain.IncidentOpen {
		t.Fatalf("volume gap was not temporal: %#v", result)
	}
}

func TestEvaluateDoesNotMutatePreviousState(t *testing.T) {
	now := time.Now().UTC()
	e := mustEvaluator(t, Policy{Version: "standard-v1", OpenAfterSamples: 2, RecoverAfterSamples: 2, TelemetryFreshness: time.Minute})
	a := testAssignment(now)
	previous := domain.EvaluatorState{Violations: map[domain.ViolationType]domain.IncidentState{domain.ViolationLateral: {Phase: domain.IncidentClear, ConsecutiveInside: 7}}}
	_, _ = e.Evaluate(now, a, observation(now, 1, 35.02, -97.02, 100), previous)
	if previous.Violations[domain.ViolationLateral].ConsecutiveInside != 7 {
		t.Fatal("Evaluate mutated its input state")
	}
}

func TestEvaluatorRejectsMismatchedIdentity(t *testing.T) {
	now := time.Now().UTC()
	e := mustEvaluator(t, Policy{Version: "standard-v1", OpenAfterSamples: 1, RecoverAfterSamples: 1, TelemetryFreshness: time.Minute})
	a := testAssignment(now)
	o := observation(now, 1, 35, -97, 100)
	o.AgentID = "other"
	if _, err := e.Evaluate(now, a, o, domain.EvaluatorState{}); err == nil {
		t.Fatal("mismatched agent was accepted")
	}
}

func mustEvaluator(t *testing.T, p Policy) *Evaluator {
	t.Helper()
	e, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func mustEvaluate(t *testing.T, e *Evaluator, now time.Time, a domain.Assignment, o domain.Observation, s domain.EvaluatorState) domain.Evaluation {
	t.Helper()
	result, err := e.Evaluate(now, a, o, s)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func testAssignment(now time.Time) domain.Assignment {
	return domain.Assignment{ID: "assignment-1", Generation: 1, AircraftID: "aircraft-1", AgentID: "agent-1", FlightID: "flight-1", IntentID: "intent-1", IntentVersion: 3, PolicyVersion: "standard-v1", EffectiveFrom: now.Add(-time.Minute), EffectiveUntil: now.Add(time.Hour), Volumes: []domain.Volume{{ID: "volume-1", Polygon: []domain.Point{{Latitude: 35, Longitude: -97.01}, {Latitude: 35.01, Longitude: -97.01}, {Latitude: 35.01, Longitude: -97}, {Latitude: 35, Longitude: -97}}, AltitudeLowerM: 80, AltitudeUpperM: 120, AltitudeReference: domain.AltitudeMSL, StartsAt: now.Add(-time.Minute), EndsAt: now.Add(time.Hour)}}}
}

func observation(at time.Time, seq uint64, lat, lon, altitude float64) domain.Observation {
	return domain.Observation{FrameID: "frame-" + at.Format(time.RFC3339Nano), AgentID: "agent-1", WALID: "wal-1", WALSequence: seq, AircraftID: "aircraft-1", FlightID: "flight-1", IntentID: "intent-1", IntentVersion: 3, Latitude: lat, Longitude: lon, AltitudeM: altitude, AltitudeKnown: true, AltitudeReference: domain.AltitudeMSL, ObservedAt: at}
}
