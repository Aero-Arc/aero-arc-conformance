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

func TestEvaluateBatchTransitionsKeepOccurrenceAndOwnWALCursor(t *testing.T) {
	now := time.Date(2026, 8, 12, 18, 0, 0, 0, time.UTC)
	e := mustEvaluator(t, Policy{Version: "standard-v1", OpenAfterSamples: 1, RecoverAfterSamples: 1, TelemetryFreshness: time.Minute})
	a := testAssignment(now)
	opened := observation(now, 41, 35.02, -97.02, 100)
	resolved := observation(now.Add(time.Second), 42, 35.005, -97.005, 100)
	result, err := e.EvaluateBatch(a, []domain.Observation{opened, resolved}, domain.EvaluatorState{})
	if err != nil {
		t.Fatal(err)
	}
	var opening, resolution *domain.IncidentTransition
	for i := range result.Transitions {
		transition := &result.Transitions[i]
		if transition.Violation != domain.ViolationLateral {
			continue
		}
		switch transition.Transition {
		case domain.TransitionOpened:
			opening = transition
		case domain.TransitionResolved:
			resolution = transition
		}
	}
	if opening == nil || resolution == nil || opening.OpeningFrameID != opened.FrameID || resolution.OpeningFrameID != opened.FrameID || opening.WALID != opened.WALID || opening.WALSequence != opened.WALSequence || resolution.WALID != resolved.WALID || resolution.WALSequence != resolved.WALSequence {
		t.Fatalf("transition correlation opening=%#v resolution=%#v", opening, resolution)
	}
	if !result.CausalFrom.Equal(opened.ObservedAt) || !result.ObservedAt.Equal(resolved.ObservedAt) {
		t.Fatalf("batch causal interval = [%s,%s], want [%s,%s]", result.CausalFrom, result.ObservedAt, opened.ObservedAt, resolved.ObservedAt)
	}
	if _, err = e.EvaluateBatch(a, []domain.Observation{resolved, opened}, domain.EvaluatorState{}); err == nil {
		t.Fatal("out-of-order event-time batch was accepted")
	}
	priorStart := now.Add(-time.Second)
	previous := domain.EvaluatorState{Violations: map[domain.ViolationType]domain.IncidentState{
		domain.ViolationLateral: {Phase: domain.IncidentSuspected, FirstSuspectedAt: priorStart, LastObservedAt: priorStart},
	}}
	cleared, err := e.EvaluateBatch(a, []domain.Observation{resolved}, previous)
	if err != nil {
		t.Fatal(err)
	}
	if !cleared.CausalFrom.Equal(priorStart) {
		t.Fatalf("cleared batch lost retained causal start: got %s want %s", cleared.CausalFrom, priorStart)
	}
	direct := mustEvaluate(t, e, resolved.ObservedAt, a, resolved, previous)
	if !direct.CausalFrom.Equal(priorStart) {
		t.Fatalf("single evaluation lost retained causal start: got %s want %s", direct.CausalFrom, priorStart)
	}
	equalTimeEarlier := observation(now, 50, 35.02, -97.02, 100)
	equalTimeLater := observation(now, 51, 35.005, -97.005, 100)
	if _, err = e.EvaluateBatch(a, []domain.Observation{equalTimeLater, equalTimeEarlier}, domain.EvaluatorState{}); err == nil {
		t.Fatal("equal-time decreasing WAL sequence was accepted")
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
	previous := domain.EvaluatorState{Violations: map[domain.ViolationType]domain.IncidentState{
		domain.ViolationLateral:  {Phase: domain.IncidentClear, LastObservedAt: now.Add(-time.Second)},
		domain.ViolationVertical: {Phase: domain.IncidentClear, LastObservedAt: now.Add(-time.Second)},
	}}
	result := mustEvaluate(t, e, now, a, observation(now, 1, 35.005, -97.005, 100), previous)
	if result.State.Violations[domain.ViolationTemporal].Phase != domain.IncidentOpen {
		t.Fatalf("volume gap was not temporal: %#v", result)
	}
	if _, exists := result.State.Violations[domain.ViolationLateral]; exists {
		t.Fatalf("gap retained stale lateral clear: %#v", result.State)
	}
	if _, exists := result.State.Violations[domain.ViolationVertical]; exists {
		t.Fatalf("gap retained stale vertical clear: %#v", result.State)
	}
}

func TestPlannedWindowOverrunEvaluatesUniqueTerminalVolume(t *testing.T) {
	plannedEnd := time.Date(2026, 8, 12, 18, 0, 0, 0, time.UTC)
	e := mustEvaluator(t, Policy{Version: "standard-v1", OpenAfterSamples: 1, RecoverAfterSamples: 1, TelemetryFreshness: time.Minute})
	a := testAssignment(plannedEnd)
	a.EffectiveFrom = plannedEnd.Add(-time.Hour)
	a.EffectiveUntil = plannedEnd.Add(24 * time.Hour)
	a.Volumes[0].StartsAt = plannedEnd.Add(-time.Hour)
	a.Volumes[0].EndsAt = plannedEnd

	t.Run("inside remains spatially current while temporal is open", func(t *testing.T) {
		overrun := observation(plannedEnd.Add(30*time.Second), 1, 35.005, -97.005, 100)
		result := mustEvaluate(t, e, overrun.ObservedAt, a, overrun, domain.EvaluatorState{})
		if result.Condition != domain.ConditionNonConforming || result.State.Violations[domain.ViolationTemporal].Phase != domain.IncidentOpen {
			t.Fatalf("planned-window overrun result = %#v", result)
		}
		for _, violation := range []domain.ViolationType{domain.ViolationLateral, domain.ViolationVertical} {
			state := result.State.Violations[violation]
			if state.Phase != domain.IncidentClear || !state.LastObservedAt.Equal(overrun.ObservedAt) {
				t.Fatalf("%s state = %#v, want current clear", violation, state)
			}
		}
		if result.Monitoring != domain.MonitoringCurrent {
			t.Fatalf("monitoring = %s, want current", result.Monitoring)
		}
	})

	t.Run("outside opens simultaneous spatial and temporal findings", func(t *testing.T) {
		overrun := observation(plannedEnd.Add(30*time.Second), 2, 35.02, -97.02, 130)
		result := mustEvaluate(t, e, overrun.ObservedAt, a, overrun, domain.EvaluatorState{})
		for _, violation := range []domain.ViolationType{domain.ViolationLateral, domain.ViolationVertical, domain.ViolationTemporal} {
			if state := result.State.Violations[violation]; state.Phase != domain.IncidentOpen || !state.LastObservedAt.Equal(overrun.ObservedAt) {
				t.Fatalf("%s state = %#v, want current open", violation, state)
			}
		}
		if result.State.Violations[domain.ViolationLateral].WorstDeviationM <= 1 || result.State.Violations[domain.ViolationVertical].WorstDeviationM != 10 {
			t.Fatalf("spatial deviations = lateral %.2f vertical %.2f", result.State.Violations[domain.ViolationLateral].WorstDeviationM, result.State.Violations[domain.ViolationVertical].WorstDeviationM)
		}
		if len(result.Transitions) != 3 {
			t.Fatalf("transitions = %#v, want lateral+vertical+temporal opens", result.Transitions)
		}
	})
}

func TestOverrunSpatialIncidentsRecoverIndependentlyOfTemporal(t *testing.T) {
	plannedEnd := time.Date(2026, 8, 12, 18, 0, 0, 0, time.UTC)
	e := mustEvaluator(t, Policy{Version: "standard-v1", OpenAfterSamples: 2, RecoverAfterSamples: 2, TelemetryFreshness: time.Minute})
	a := testAssignment(plannedEnd)
	a.EffectiveFrom = plannedEnd.Add(-time.Hour)
	a.EffectiveUntil = plannedEnd.Add(time.Hour)
	a.Volumes[0].StartsAt = plannedEnd.Add(-time.Hour)
	a.Volumes[0].EndsAt = plannedEnd

	state := domain.EvaluatorState{}
	for seq := uint64(1); seq <= 2; seq++ {
		at := plannedEnd.Add(time.Duration(seq) * time.Second)
		result := mustEvaluate(t, e, at, a, observation(at, seq, 35.02, -97.02, 130), state)
		state = result.State
	}
	if state.Violations[domain.ViolationLateral].Phase != domain.IncidentOpen || state.Violations[domain.ViolationVertical].Phase != domain.IncidentOpen || state.Violations[domain.ViolationTemporal].Phase != domain.IncidentOpen {
		t.Fatalf("outside overrun did not open independent findings: %#v", state)
	}
	for seq := uint64(3); seq <= 4; seq++ {
		at := plannedEnd.Add(time.Duration(seq) * time.Second)
		result := mustEvaluate(t, e, at, a, observation(at, seq, 35.005, -97.005, 100), state)
		state = result.State
		if result.Condition != domain.ConditionNonConforming {
			t.Fatalf("temporal finding stopped dominating during spatial recovery: %#v", result)
		}
	}
	if state.Violations[domain.ViolationLateral].Phase != domain.IncidentClear || state.Violations[domain.ViolationVertical].Phase != domain.IncidentClear || state.Violations[domain.ViolationTemporal].Phase != domain.IncidentOpen {
		t.Fatalf("reentry did not resolve only spatial findings: %#v", state)
	}
}

func TestOverrunUsesLatestSequentialVolume(t *testing.T) {
	plannedEnd := time.Date(2026, 8, 12, 18, 0, 0, 0, time.UTC)
	e := mustEvaluator(t, Policy{Version: "standard-v1", OpenAfterSamples: 1, RecoverAfterSamples: 1, TelemetryFreshness: time.Minute})
	a := testAssignment(plannedEnd)
	a.EffectiveFrom = plannedEnd.Add(-time.Hour)
	a.EffectiveUntil = plannedEnd.Add(time.Hour)
	a.Volumes[0].StartsAt = plannedEnd.Add(-time.Hour)
	a.Volumes[0].EndsAt = plannedEnd.Add(-30 * time.Minute)
	a.Volumes = append(a.Volumes, domain.Volume{ID: "terminal", Polygon: shiftedPolygon(), AltitudeLowerM: 120, AltitudeUpperM: 140, AltitudeReference: domain.AltitudeMSL, StartsAt: plannedEnd.Add(-20 * time.Minute), EndsAt: plannedEnd})

	overrun := observation(plannedEnd.Add(time.Second), 1, 36.005, -98.005, 130)
	result := mustEvaluate(t, e, overrun.ObservedAt, a, overrun, domain.EvaluatorState{})
	if result.State.Violations[domain.ViolationLateral].Phase != domain.IncidentClear || result.State.Violations[domain.ViolationVertical].Phase != domain.IncidentClear || result.State.Violations[domain.ViolationTemporal].Phase != domain.IncidentOpen {
		t.Fatalf("overrun did not use latest sequential volume: %#v", result)
	}
}

func TestOverrunOmitsSpatialPhasesWhenTerminalGeometryIsAmbiguous(t *testing.T) {
	plannedEnd := time.Date(2026, 8, 12, 18, 0, 0, 0, time.UTC)
	e := mustEvaluator(t, Policy{Version: "standard-v1", OpenAfterSamples: 1, RecoverAfterSamples: 1, TelemetryFreshness: time.Minute})
	a := testAssignment(plannedEnd)
	a.EffectiveFrom = plannedEnd.Add(-time.Hour)
	a.EffectiveUntil = plannedEnd.Add(time.Hour)
	a.Volumes[0].StartsAt = plannedEnd.Add(-time.Hour)
	a.Volumes[0].EndsAt = plannedEnd
	a.Volumes = append(a.Volumes, domain.Volume{ID: "alternate-terminal", Polygon: shiftedPolygon(), AltitudeLowerM: 120, AltitudeUpperM: 140, AltitudeReference: domain.AltitudeMSL, StartsAt: plannedEnd.Add(-time.Hour), EndsAt: plannedEnd})
	previous := domain.EvaluatorState{Violations: map[domain.ViolationType]domain.IncidentState{
		domain.ViolationLateral:  {Phase: domain.IncidentClear, LastObservedAt: plannedEnd.Add(-time.Second)},
		domain.ViolationVertical: {Phase: domain.IncidentClear, LastObservedAt: plannedEnd.Add(-time.Second)},
	}}

	overrun := observation(plannedEnd.Add(time.Second), 1, 35.005, -97.005, 100)
	result := mustEvaluate(t, e, overrun.ObservedAt, a, overrun, previous)
	if result.State.Violations[domain.ViolationTemporal].Phase != domain.IncidentOpen {
		t.Fatalf("ambiguous overrun was not temporal: %#v", result)
	}
	for _, violation := range []domain.ViolationType{domain.ViolationLateral, domain.ViolationVertical} {
		if _, exists := result.State.Violations[violation]; exists {
			t.Fatalf("ambiguous overrun retained stale %s phase: %#v", violation, result.State)
		}
	}

	openedAt := plannedEnd.Add(-time.Minute)
	unresolved := domain.EvaluatorState{Violations: map[domain.ViolationType]domain.IncidentState{
		domain.ViolationLateral: {Phase: domain.IncidentOpen, OpeningFrameID: "lateral-open", OpenedAt: openedAt, LastObservedAt: plannedEnd.Add(-time.Second), ConsecutiveOutside: 3, WorstDeviationM: 20},
	}}
	result = mustEvaluate(t, e, overrun.ObservedAt, a, overrun, unresolved)
	lateral := result.State.Violations[domain.ViolationLateral]
	if lateral.Phase != domain.IncidentOpen || lateral.LastObservedAt.Equal(overrun.ObservedAt) || lateral.ConsecutiveOutside != 3 || lateral.WorstDeviationM != 20 {
		t.Fatalf("ambiguous geometry mutated unresolved incident: %#v", lateral)
	}
	for _, transition := range result.Transitions {
		if transition.Violation == domain.ViolationLateral {
			t.Fatalf("ambiguous geometry manufactured lateral transition: %#v", transition)
		}
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

func shiftedPolygon() []domain.Point {
	return []domain.Point{{Latitude: 36, Longitude: -98.01}, {Latitude: 36.01, Longitude: -98.01}, {Latitude: 36.01, Longitude: -98}, {Latitude: 36, Longitude: -98}}
}
