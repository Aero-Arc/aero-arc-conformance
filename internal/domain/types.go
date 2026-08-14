// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package domain defines infrastructure-independent conformance contracts.
package domain

import "time"

type Condition string

const (
	ConditionUnknown       Condition = "unknown"
	ConditionConforming    Condition = "conforming"
	ConditionSuspected     Condition = "suspected"
	ConditionNonConforming Condition = "non_conforming"
	ConditionRecovering    Condition = "recovering"
)

type MonitoringStatus string

const (
	MonitoringReceived    MonitoringStatus = "received"
	MonitoringArmed       MonitoringStatus = "armed"
	MonitoringCurrent     MonitoringStatus = "current"
	MonitoringStale       MonitoringStatus = "stale"
	MonitoringUnavailable MonitoringStatus = "unavailable"
)

type RecordingStatus string

const (
	RecordingPending   RecordingStatus = "pending"
	RecordingConfirmed RecordingStatus = "confirmed"
	RecordingDegraded  RecordingStatus = "degraded"
)

type ViolationType string

const (
	ViolationLateral       ViolationType = "lateral_deviation"
	ViolationVertical      ViolationType = "altitude_deviation"
	ViolationTemporal      ViolationType = "temporal_deviation"
	ViolationTelemetryLoss ViolationType = "telemetry_loss"
)

type AltitudeReference string

const (
	AltitudeMSL AltitudeReference = "msl"
	AltitudeAGL AltitudeReference = "agl"
)

type Point struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// Volume is an immutable monitoring volume. Polygon is a closed or open outer
// ring; the evaluator closes it logically.
type Volume struct {
	ID                string            `json:"id"`
	Polygon           []Point           `json:"polygon"`
	AltitudeLowerM    float64           `json:"altitude_lower_m"`
	AltitudeUpperM    float64           `json:"altitude_upper_m"`
	AltitudeReference AltitudeReference `json:"altitude_reference"`
	StartsAt          time.Time         `json:"starts_at"`
	EndsAt            time.Time         `json:"ends_at"`
}

type Assignment struct {
	ID             string    `json:"assignment_id"`
	Generation     uint64    `json:"assignment_generation"`
	OperatorID     string    `json:"operator_id,omitempty"`
	AircraftID     string    `json:"aircraft_id"`
	AgentID        string    `json:"agent_id"`
	FlightID       string    `json:"flight_id"`
	IntentID       string    `json:"intent_id"`
	IntentVersion  uint32    `json:"intent_version"`
	PolicyVersion  string    `json:"policy_version"`
	EffectiveFrom  time.Time `json:"effective_from"`
	EffectiveUntil time.Time `json:"effective_until"`
	Volumes        []Volume  `json:"volumes"`
}

// AssignmentLifecycle describes whether an immutable assignment generation is
// merely being prepared, ready for an authority cutover, currently
// authoritative, or historical. Preparing and arming never authorize flight.
type AssignmentLifecycle string

const (
	AssignmentReceived   AssignmentLifecycle = "candidate_received"
	AssignmentArmed      AssignmentLifecycle = "candidate_armed"
	AssignmentActive     AssignmentLifecycle = "active"
	AssignmentEnding     AssignmentLifecycle = "ending"
	AssignmentCompleted  AssignmentLifecycle = "completed"
	AssignmentCancelled  AssignmentLifecycle = "cancelled"
	AssignmentSuperseded AssignmentLifecycle = "superseded"
)

// AssignmentRecord pairs an immutable assignment with its authority interval.
// AuthorityUntil is exclusive: an observation exactly at a cutover belongs to
// the replacement generation.
type AssignmentRecord struct {
	Assignment     Assignment          `json:"assignment"`
	Lifecycle      AssignmentLifecycle `json:"lifecycle"`
	AuthorityFrom  *time.Time          `json:"authority_from,omitempty"`
	AuthorityUntil *time.Time          `json:"authority_until,omitempty"`
	PreparedAt     time.Time           `json:"prepared_at"`
	ArmedAt        *time.Time          `json:"armed_at,omitempty"`
	CutoverAt      *time.Time          `json:"cutover_at,omitempty"`
}

// Authorizes reports whether observedAt belongs to this record's half-open
// authority interval.
//
// Parameters:
//   - observedAt: is the telemetry event time being attributed.
//
// Returns:
//   - authorized: is true only for authority_from <= observedAt < authority_until.
func (r AssignmentRecord) Authorizes(observedAt time.Time) bool {
	return r.AuthorityFrom != nil && !observedAt.Before(*r.AuthorityFrom) && (r.AuthorityUntil == nil || observedAt.Before(*r.AuthorityUntil))
}

// Observation is one normalized GLOBAL_POSITION_INT record read from InfluxDB.
type Observation struct {
	FrameID           string            `json:"frame_id"`
	AgentID           string            `json:"agent_id"`
	WALID             string            `json:"wal_id"`
	WALSequence       uint64            `json:"wal_sequence"`
	AircraftID        string            `json:"aircraft_id"`
	FlightID          string            `json:"flight_id,omitempty"`
	IntentID          string            `json:"intent_id,omitempty"`
	IntentVersion     uint32            `json:"intent_version,omitempty"`
	Latitude          float64           `json:"latitude"`
	Longitude         float64           `json:"longitude"`
	AltitudeM         float64           `json:"altitude_m"`
	AltitudeKnown     bool              `json:"altitude_known"`
	AltitudeReference AltitudeReference `json:"altitude_reference"`
	ObservedAt        time.Time         `json:"observed_at"`
}

type IncidentPhase string

const (
	IncidentClear      IncidentPhase = "clear"
	IncidentSuspected  IncidentPhase = "suspected"
	IncidentOpen       IncidentPhase = "open"
	IncidentRecovering IncidentPhase = "recovering"
)

type IncidentState struct {
	Phase IncidentPhase `json:"phase"`
	// OpeningFrameID is the stable identity of this incident occurrence. It
	// survives recovery hysteresis so replay can amend the same episode without
	// guessing from timestamps or another occurrence of the same violation.
	OpeningFrameID     string    `json:"opening_frame_id,omitempty"`
	ConsecutiveOutside int       `json:"consecutive_outside"`
	ConsecutiveInside  int       `json:"consecutive_inside"`
	FirstSuspectedAt   time.Time `json:"first_suspected_at,omitempty"`
	OpenedAt           time.Time `json:"opened_at,omitempty"`
	LastObservedAt     time.Time `json:"last_observed_at,omitempty"`
	WorstDeviationM    float64   `json:"worst_deviation_m,omitempty"`
}

type EvaluatorState struct {
	Violations map[ViolationType]IncidentState `json:"violations"`
}

type Transition string

const (
	TransitionOpened   Transition = "opened"
	TransitionUpdated  Transition = "updated"
	TransitionResolved Transition = "resolved"
)

type IncidentTransition struct {
	Violation      ViolationType `json:"violation"`
	Transition     Transition    `json:"transition"`
	ObservedAt     time.Time     `json:"observed_at"`
	FrameID        string        `json:"frame_id"`
	OpeningFrameID string        `json:"opening_frame_id"`
	WALID          string        `json:"wal_id"`
	WALSequence    uint64        `json:"wal_sequence"`
	DeviationM     float64       `json:"deviation_m,omitempty"`
}

type Evaluation struct {
	Condition   Condition            `json:"condition"`
	Monitoring  MonitoringStatus     `json:"monitoring"`
	Recording   RecordingStatus      `json:"recording"`
	State       EvaluatorState       `json:"state"`
	Transitions []IncidentTransition `json:"transitions"`
	// CausalFrom is the earliest observation or retained non-clear incident
	// timestamp that contributed to this result. Durable commits fence it to the
	// same assignment authority interval as the final watermark.
	CausalFrom  time.Time `json:"causal_from"`
	ObservedAt  time.Time `json:"observed_at"`
	FrameID     string    `json:"frame_id"`
	WALID       string    `json:"wal_id"`
	WALSequence uint64    `json:"wal_sequence"`
}
