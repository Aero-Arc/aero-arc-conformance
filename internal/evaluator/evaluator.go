// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package evaluator contains deterministic, infrastructure-independent flight
// conformance evaluation.
package evaluator

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/aero-arc/aero-arc-conformance/internal/domain"
)

var ErrInvalidInput = errors.New("invalid conformance input")

type Policy struct {
	Version              string
	HorizontalToleranceM float64
	VerticalToleranceM   float64
	OpenAfterSamples     int
	RecoverAfterSamples  int
	TelemetryFreshness   time.Duration
}

// Validate reports whether the policy is complete and safe to evaluate.
func (p Policy) Validate() error {
	if p.Version == "" || !finiteNonnegative(p.HorizontalToleranceM) || !finiteNonnegative(p.VerticalToleranceM) || p.OpenAfterSamples < 1 || p.RecoverAfterSamples < 1 || p.TelemetryFreshness <= 0 {
		return fmt.Errorf("%w: incomplete evaluator policy", ErrInvalidInput)
	}
	return nil
}

type Evaluator struct{ policy Policy }

// New constructs an evaluator after validating policy.
func New(policy Policy) (*Evaluator, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return &Evaluator{policy: policy}, nil
}

// Evaluate applies one observation to previous evaluator state and returns the
// resulting condition, incident state, and immutable transitions.
func (e *Evaluator) Evaluate(now time.Time, assignment domain.Assignment, observation domain.Observation, previous domain.EvaluatorState) (domain.Evaluation, error) {
	if err := validateAssignment(assignment, e.policy); err != nil {
		return domain.Evaluation{}, err
	}
	if observation.FrameID == "" || observation.AgentID == "" || observation.WALID == "" || observation.AircraftID != assignment.AircraftID || observation.AgentID != assignment.AgentID || observation.FlightID != assignment.FlightID || observation.IntentID != assignment.IntentID || observation.IntentVersion != assignment.IntentVersion || observation.ObservedAt.IsZero() || !validCoordinate(observation.Latitude, observation.Longitude) || (observation.AltitudeKnown && !finite(observation.AltitudeM)) {
		return domain.Evaluation{}, fmt.Errorf("%w: observation identity does not match assignment", ErrInvalidInput)
	}
	causalFrom := earliestCausalTimestamp(previous, observation.ObservedAt)
	cloned := domain.EvaluatorState{Violations: make(map[domain.ViolationType]domain.IncidentState, len(previous.Violations))}
	for violation, state := range previous.Violations {
		cloned.Violations[violation] = state
	}

	type evidence struct {
		known     bool
		breached  bool
		deviation float64
	}
	evidenceByType := map[domain.ViolationType]evidence{}
	if observation.ObservedAt.Before(assignment.EffectiveFrom) || !observation.ObservedAt.Before(assignment.EffectiveUntil) {
		evidenceByType[domain.ViolationTemporal] = evidence{known: true, breached: true, deviation: math.Abs(observation.ObservedAt.Sub(clampTime(observation.ObservedAt, assignment.EffectiveFrom, assignment.EffectiveUntil)).Seconds())}
	}

	activeVolumes := make([]domain.Volume, 0, len(assignment.Volumes))
	for _, volume := range assignment.Volumes {
		if observation.ObservedAt.Before(volume.StartsAt) || !observation.ObservedAt.Before(volume.EndsAt) {
			continue
		}
		activeVolumes = append(activeVolumes, volume)
	}
	if len(activeVolumes) == 0 {
		evidenceByType[domain.ViolationTemporal] = evidence{known: true, breached: true}
	} else if _, temporal := evidenceByType[domain.ViolationTemporal]; !temporal {
		evidenceByType[domain.ViolationTemporal] = evidence{known: true}
	}

	if len(activeVolumes) > 0 {
		bestScore := math.Inf(1)
		bestLateral, bestVertical := math.Inf(1), math.Inf(1)
		nearestLateral := math.Inf(1)
		anyLateralPass := false
		compatibleVolume := false
		jointPass := false
		for _, volume := range activeVolumes {
			lateralDistance := distanceToRingMeters(observation, volume.Polygon)
			if lateralDistance < nearestLateral {
				nearestLateral = lateralDistance
			}
			insideLateral := pointInPolygon(observation.Longitude, observation.Latitude, volume.Polygon) || lateralDistance <= e.policy.HorizontalToleranceM
			anyLateralPass = anyLateralPass || insideLateral
			if !observation.AltitudeKnown || observation.AltitudeReference != volume.AltitudeReference {
				continue
			}
			compatibleVolume = true
			verticalDeviation := altitudeDeviationFromVolume(observation, volume)
			score := math.Max(0, lateralDistance-e.policy.HorizontalToleranceM) + math.Max(0, verticalDeviation-e.policy.VerticalToleranceM)
			if score < bestScore {
				bestScore, bestLateral, bestVertical = score, lateralDistance, verticalDeviation
			}
			if insideLateral && verticalDeviation <= e.policy.VerticalToleranceM {
				jointPass = true
				break
			}
		}
		if jointPass {
			evidenceByType[domain.ViolationLateral] = evidence{known: true}
			evidenceByType[domain.ViolationVertical] = evidence{known: true}
		} else if compatibleVolume {
			evidenceByType[domain.ViolationLateral] = evidence{known: true, breached: bestLateral > e.policy.HorizontalToleranceM, deviation: bestLateral}
			evidenceByType[domain.ViolationVertical] = evidence{known: true, breached: bestVertical > e.policy.VerticalToleranceM, deviation: bestVertical}
		} else {
			evidenceByType[domain.ViolationLateral] = evidence{known: true, breached: !anyLateralPass, deviation: nearestLateral}
		}
	}

	transitions := make([]domain.IncidentTransition, 0, 4)
	for _, violation := range []domain.ViolationType{domain.ViolationLateral, domain.ViolationVertical, domain.ViolationTemporal} {
		signal := evidenceByType[violation]
		if !signal.known {
			continue
		}
		state := cloned.Violations[violation]
		next, transition := e.advance(violation, state, signal.breached, signal.deviation, observation)
		cloned.Violations[violation] = next
		if transition != nil {
			transitions = append(transitions, *transition)
		}
	}

	condition := domain.ConditionConforming
	for violation, state := range cloned.Violations {
		if violation == domain.ViolationTelemetryLoss {
			continue
		}
		switch state.Phase {
		case domain.IncidentOpen:
			condition = domain.ConditionNonConforming
		case domain.IncidentSuspected:
			if condition != domain.ConditionNonConforming {
				condition = domain.ConditionSuspected
			}
		case domain.IncidentRecovering:
			if condition == domain.ConditionConforming {
				condition = domain.ConditionRecovering
			}
		}
	}
	if _, verticalKnown := evidenceByType[domain.ViolationVertical]; !verticalKnown && condition == domain.ConditionConforming {
		condition = domain.ConditionUnknown
	}
	_ = now // freshness is assessed from the poll watermark, never event age during replay.
	return domain.Evaluation{Condition: condition, Monitoring: domain.MonitoringCurrent, Recording: domain.RecordingPending, State: cloned, Transitions: transitions, CausalFrom: causalFrom, ObservedAt: observation.ObservedAt, FrameID: observation.FrameID, WALID: observation.WALID, WALSequence: observation.WALSequence}, nil
}

// AssessMonitoring evaluates dependency availability and telemetry silence on a
// wall-clock timer. It is deliberately separate from event-time containment so
// historical replay cannot manufacture a telemetry-loss incident.
func (e *Evaluator) AssessMonitoring(now, latestArrival time.Time, dependencyAvailable bool) domain.MonitoringStatus {
	if !dependencyAvailable {
		return domain.MonitoringUnavailable
	}
	if latestArrival.IsZero() || now.Sub(latestArrival) > e.policy.TelemetryFreshness {
		return domain.MonitoringStale
	}
	return domain.MonitoringCurrent
}

// EvaluateBatch applies an event-time ordered suffix and preserves every
// transition produced inside the batch. Callers must never keep only the final
// sample's transitions: an incident may open and resolve within one poll.
func (e *Evaluator) EvaluateBatch(assignment domain.Assignment, observations []domain.Observation, previous domain.EvaluatorState) (domain.Evaluation, error) {
	if len(observations) == 0 {
		return domain.Evaluation{}, fmt.Errorf("%w: empty observation batch", ErrInvalidInput)
	}
	causalFrom := earliestCausalTimestamp(previous, observations[0].ObservedAt)
	allTransitions := make([]domain.IncidentTransition, 0)
	var result domain.Evaluation
	var err error
	for index, observation := range observations {
		if index > 0 && observationBefore(observation, observations[index-1]) {
			return domain.Evaluation{}, fmt.Errorf("%w: observation batch is not canonically ordered", ErrInvalidInput)
		}
		result, err = e.Evaluate(observation.ObservedAt, assignment, observation, previous)
		if err != nil {
			return domain.Evaluation{}, err
		}
		previous = result.State
		allTransitions = append(allTransitions, result.Transitions...)
	}
	result.Transitions = allTransitions
	result.CausalFrom = causalFrom
	return result, nil
}

func earliestCausalTimestamp(state domain.EvaluatorState, fallback time.Time) time.Time {
	earliest := fallback
	for _, incident := range state.Violations {
		if incident.Phase == domain.IncidentClear || incident.Phase == "" {
			continue
		}
		for _, candidate := range []time.Time{incident.FirstSuspectedAt, incident.OpenedAt} {
			if !candidate.IsZero() && candidate.Before(earliest) {
				earliest = candidate
			}
		}
	}
	return earliest
}

// observationBefore mirrors the Influx reader's canonical ordering. Evaluation
// is hysteresis-sensitive, so callers may not permute equal-time frames.
func observationBefore(a, b domain.Observation) bool {
	if !a.ObservedAt.Equal(b.ObservedAt) {
		return a.ObservedAt.Before(b.ObservedAt)
	}
	if a.AgentID != b.AgentID {
		return a.AgentID < b.AgentID
	}
	if a.WALID != b.WALID {
		return a.WALID < b.WALID
	}
	if a.WALSequence != b.WALSequence {
		return a.WALSequence < b.WALSequence
	}
	return a.FrameID < b.FrameID
}

func (e *Evaluator) advance(v domain.ViolationType, state domain.IncidentState, breached bool, deviation float64, o domain.Observation) (domain.IncidentState, *domain.IncidentTransition) {
	state.LastObservedAt = o.ObservedAt
	if breached {
		state.ConsecutiveInside = 0
		state.ConsecutiveOutside++
		if deviation > state.WorstDeviationM {
			state.WorstDeviationM = deviation
		}
		switch state.Phase {
		case "", domain.IncidentClear:
			state.Phase = domain.IncidentSuspected
			state.FirstSuspectedAt = o.ObservedAt
		case domain.IncidentRecovering:
			state.Phase = domain.IncidentOpen
		}
		if state.Phase == domain.IncidentSuspected && state.ConsecutiveOutside >= e.policy.OpenAfterSamples {
			state.Phase = domain.IncidentOpen
			state.OpenedAt = state.FirstSuspectedAt
			state.OpeningFrameID = o.FrameID
			return state, &domain.IncidentTransition{Violation: v, Transition: domain.TransitionOpened, ObservedAt: o.ObservedAt, FrameID: o.FrameID, OpeningFrameID: o.FrameID, WALID: o.WALID, WALSequence: o.WALSequence, DeviationM: deviation}
		}
		return state, nil
	}

	state.ConsecutiveOutside = 0
	state.ConsecutiveInside++
	if state.Phase == domain.IncidentSuspected {
		state = domain.IncidentState{Phase: domain.IncidentClear, LastObservedAt: o.ObservedAt}
		return state, nil
	}
	if state.Phase == domain.IncidentOpen {
		state.Phase = domain.IncidentRecovering
	}
	if state.Phase == domain.IncidentRecovering && state.ConsecutiveInside >= e.policy.RecoverAfterSamples {
		resolved := &domain.IncidentTransition{Violation: v, Transition: domain.TransitionResolved, ObservedAt: o.ObservedAt, FrameID: o.FrameID, OpeningFrameID: state.OpeningFrameID, WALID: o.WALID, WALSequence: o.WALSequence}
		return domain.IncidentState{Phase: domain.IncidentClear, LastObservedAt: o.ObservedAt}, resolved
	}
	if state.Phase == "" {
		state.Phase = domain.IncidentClear
	}
	return state, nil
}

func clampTime(value, lower, upper time.Time) time.Time {
	if value.Before(lower) {
		return lower
	}
	if value.After(upper) {
		return upper
	}
	return value
}

func pointInPolygon(x, y float64, ring []domain.Point) bool {
	if len(ring) < 3 {
		return false
	}
	inside := false
	for i, j := 0, len(ring)-1; i < len(ring); j, i = i, i+1 {
		xi, yi, xj, yj := ring[i].Longitude, ring[i].Latitude, ring[j].Longitude, ring[j].Latitude
		if ((yi > y) != (yj > y)) && x < (xj-xi)*(y-yi)/(yj-yi)+xi {
			inside = !inside
		}
	}
	return inside
}

func distanceToRingMeters(o domain.Observation, ring []domain.Point) float64 {
	if len(ring) < 2 {
		return math.Inf(1)
	}
	best := math.Inf(1)
	latScale := 111132.0
	lonScale := 111320.0 * math.Cos(o.Latitude*math.Pi/180)
	for i := range ring {
		j := (i + 1) % len(ring)
		ax := (ring[i].Longitude - o.Longitude) * lonScale
		ay := (ring[i].Latitude - o.Latitude) * latScale
		bx := (ring[j].Longitude - o.Longitude) * lonScale
		by := (ring[j].Latitude - o.Latitude) * latScale
		dx, dy := bx-ax, by-ay
		t := 0.0
		if denom := dx*dx + dy*dy; denom > 0 {
			t = -(ax*dx + ay*dy) / denom
			if t < 0 {
				t = 0
			} else if t > 1 {
				t = 1
			}
		}
		distance := math.Hypot(ax+t*dx, ay+t*dy)
		if distance < best {
			best = distance
		}
	}
	return best
}

func altitudeDeviationFromVolume(o domain.Observation, volume domain.Volume) float64 {
	if o.AltitudeM < volume.AltitudeLowerM {
		return volume.AltitudeLowerM - o.AltitudeM
	}
	if o.AltitudeM > volume.AltitudeUpperM {
		return o.AltitudeM - volume.AltitudeUpperM
	}
	return 0
}

func finite(v float64) bool            { return !math.IsNaN(v) && !math.IsInf(v, 0) }
func finiteNonnegative(v float64) bool { return finite(v) && v >= 0 }
func validCoordinate(lat, lon float64) bool {
	return finite(lat) && finite(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180
}

func validateAssignment(a domain.Assignment, policy Policy) error {
	if a.ID == "" || a.Generation == 0 || a.AircraftID == "" || a.AgentID == "" || a.FlightID == "" || a.IntentID == "" || a.IntentVersion == 0 || a.PolicyVersion != policy.Version || a.EffectiveFrom.IsZero() || !a.EffectiveUntil.After(a.EffectiveFrom) || len(a.Volumes) == 0 {
		return fmt.Errorf("%w: invalid assignment", ErrInvalidInput)
	}
	for _, v := range a.Volumes {
		if v.ID == "" || len(v.Polygon) < 3 || !v.EndsAt.After(v.StartsAt) || v.AltitudeLowerM > v.AltitudeUpperM || !finite(v.AltitudeLowerM) || !finite(v.AltitudeUpperM) {
			return fmt.Errorf("%w: invalid volume %q", ErrInvalidInput, v.ID)
		}
		for _, point := range v.Polygon {
			if !validCoordinate(point.Latitude, point.Longitude) {
				return fmt.Errorf("%w: invalid volume coordinate", ErrInvalidInput)
			}
		}
	}
	return nil
}
