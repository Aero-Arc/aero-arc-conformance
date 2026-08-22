// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package grpc

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/aero-arc/aero-arc-conformance/internal/domain"
	postgresstore "github.com/aero-arc/aero-arc-conformance/internal/store/postgres"
	conformancev1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/conformance/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type assignmentStoreStub struct {
	prepared           domain.Assignment
	record             domain.AssignmentRecord
	prepareDisposition postgresstore.ApplyDisposition
	getCalls           int
	getErr             error
	err                error
}

func (s *assignmentStoreStub) PrepareAssignment(_ context.Context, _, _, _ string, assignment domain.Assignment) (postgresstore.ApplyResult, error) {
	s.prepared = assignment
	disposition := s.prepareDisposition
	if disposition == "" {
		disposition = postgresstore.ApplyApplied
	}
	return postgresstore.ApplyResult{Disposition: disposition, Assignment: assignment}, s.err
}
func (s *assignmentStoreStub) CancelCandidate(context.Context, string, string, string, uint64) (postgresstore.LifecycleResult, error) {
	return postgresstore.LifecycleResult{Disposition: postgresstore.ApplyApplied, Record: s.record}, s.err
}
func (s *assignmentStoreStub) CutoverAssignment(context.Context, string, string, string, uint64, time.Time) (postgresstore.LifecycleResult, error) {
	return postgresstore.LifecycleResult{Disposition: postgresstore.ApplyApplied, Record: s.record}, s.err
}
func (s *assignmentStoreStub) GetAssignment(context.Context, string, uint64) (domain.AssignmentRecord, error) {
	s.getCalls++
	if s.getErr != nil {
		return domain.AssignmentRecord{}, s.getErr
	}
	if s.err != nil {
		return domain.AssignmentRecord{}, s.err
	}
	if s.record.Assignment.ID == "" {
		return domain.AssignmentRecord{Assignment: s.prepared, Lifecycle: domain.AssignmentReceived}, nil
	}
	return s.record, nil
}

func TestPrepareAssignmentReturnsStaleWithoutStoredRecord(t *testing.T) {
	store := &assignmentStoreStub{prepareDisposition: postgresstore.ApplyStale, getErr: postgresstore.ErrAssignmentNotFound}
	handler, err := NewAssignmentHandler(store, "standard-v1")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	response, err := handler.PrepareAssignment(context.Background(), &conformancev1.PrepareAssignmentRequest{
		Source: "api", MessageId: "stale-message",
		Assignment: validAssignmentProto(now, 6),
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetDisposition() != conformancev1.AssignmentCommandDisposition_ASSIGNMENT_COMMAND_DISPOSITION_STALE || response.GetAssignment() != nil || store.getCalls != 0 {
		t.Fatalf("response=%+v GetAssignment calls=%d", response, store.getCalls)
	}
}

func TestPrepareAssignmentMapsImmutableContract(t *testing.T) {
	store := &assignmentStoreStub{}
	handler, err := NewAssignmentHandler(store, "standard-v1")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	response, err := handler.PrepareAssignment(context.Background(), &conformancev1.PrepareAssignmentRequest{
		Source: "api", MessageId: "message-1",
		Assignment: &conformancev1.Assignment{
			AssignmentId: "assignment-1", AssignmentGeneration: 7, AircraftId: "aircraft-1", AgentId: "agent-1",
			FlightId: "flight-1", IntentId: "intent-1", IntentVersion: 3, PolicyVersion: "standard-v1",
			EffectiveFrom: timestamppb.New(now), EffectiveUntil: timestamppb.New(now.Add(time.Hour)),
			Volumes: []*conformancev1.ConformanceVolume{{VolumeId: "volume-1", AltitudeReference: conformancev1.AltitudeReference_ALTITUDE_REFERENCE_MSL, StartsAt: timestamppb.New(now), EndsAt: timestamppb.New(now.Add(time.Hour)), Polygon: []*conformancev1.GeographicPoint{{Latitude: 1, Longitude: 2}, {Latitude: 1, Longitude: 3}, {Latitude: 2, Longitude: 3}}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if store.prepared.Generation != 7 || len(store.prepared.Volumes) != 1 || response.GetAssignment().GetLifecycle() != conformancev1.AssignmentLifecycle_ASSIGNMENT_LIFECYCLE_CANDIDATE_RECEIVED {
		t.Fatalf("prepared=%+v response=%+v", store.prepared, response)
	}
}

func TestPrepareAssignmentRejectsEvaluatorInvalidAssignments(t *testing.T) {
	now := time.Now().UTC()
	tests := map[string]func(*conformancev1.Assignment){
		"missing volumes": func(assignment *conformancev1.Assignment) { assignment.Volumes = nil },
		"missing volume":  func(assignment *conformancev1.Assignment) { assignment.Volumes[0] = nil },
		"blank volume ID": func(assignment *conformancev1.Assignment) { assignment.Volumes[0].VolumeId = " " },
		"short polygon": func(assignment *conformancev1.Assignment) {
			assignment.Volumes[0].Polygon = assignment.Volumes[0].Polygon[:2]
		},
		"reversed volume time": func(assignment *conformancev1.Assignment) {
			assignment.Volumes[0].EndsAt = assignment.Volumes[0].StartsAt
		},
		"reversed altitude bounds": func(assignment *conformancev1.Assignment) { assignment.Volumes[0].AltitudeLowerM = 121 },
		"invalid latitude":         func(assignment *conformancev1.Assignment) { assignment.Volumes[0].Polygon[0].Latitude = 91 },
		"invalid longitude":        func(assignment *conformancev1.Assignment) { assignment.Volumes[0].Polygon[0].Longitude = -181 },
		"unsupported policy":       func(assignment *conformancev1.Assignment) { assignment.PolicyVersion = "future-v2" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			store := &assignmentStoreStub{}
			handler, err := NewAssignmentHandler(store, "standard-v1")
			if err != nil {
				t.Fatal(err)
			}
			assignment := proto.Clone(validAssignmentProto(now, 1)).(*conformancev1.Assignment)
			mutate(assignment)
			_, err = handler.PrepareAssignment(context.Background(), &conformancev1.PrepareAssignmentRequest{Source: "api", MessageId: "invalid-assignment", Assignment: assignment})
			if status.Code(err) != codes.InvalidArgument || store.prepared.ID != "" {
				t.Fatalf("error=%v prepared=%+v", err, store.prepared)
			}
		})
	}
}

func validAssignmentProto(now time.Time, generation uint64) *conformancev1.Assignment {
	return &conformancev1.Assignment{
		AssignmentId: "assignment-1", AssignmentGeneration: generation, AircraftId: "aircraft-1", AgentId: "agent-1",
		FlightId: "flight-1", IntentId: "intent-1", IntentVersion: 2, PolicyVersion: "standard-v1",
		EffectiveFrom: timestamppb.New(now), EffectiveUntil: timestamppb.New(now.Add(time.Hour)),
		Volumes: []*conformancev1.ConformanceVolume{{
			VolumeId: "volume-1", AltitudeLowerM: 80, AltitudeUpperM: 120,
			AltitudeReference: conformancev1.AltitudeReference_ALTITUDE_REFERENCE_MSL,
			StartsAt:          timestamppb.New(now), EndsAt: timestamppb.New(now.Add(time.Hour)),
			Polygon: []*conformancev1.GeographicPoint{{Latitude: 1, Longitude: 2}, {Latitude: 1, Longitude: 3}, {Latitude: 2, Longitude: 3}},
		}},
	}
}

func TestAssignmentHandlersRejectUnsupportedStorageRanges(t *testing.T) {
	handler, err := NewAssignmentHandler(&assignmentStoreStub{}, "standard-v1")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	overflowGeneration := uint64(math.MaxInt64) + 1
	overflowTime := time.Date(2262, time.January, 1, 0, 0, 0, 0, time.UTC)
	validAssignment := func(generation uint64, effectiveFrom, effectiveUntil time.Time) *conformancev1.Assignment {
		assignment := validAssignmentProto(effectiveFrom, generation)
		assignment.EffectiveUntil = timestamppb.New(effectiveUntil)
		return assignment
	}
	assignmentWithAltitude := func(lower, upper float64) *conformancev1.Assignment {
		assignment := validAssignment(1, now, now.Add(time.Hour))
		assignment.Volumes[0].AltitudeLowerM = lower
		assignment.Volumes[0].AltitudeUpperM = upper
		return assignment
	}
	tests := map[string]func() error{
		"prepare generation": func() error {
			_, err := handler.PrepareAssignment(context.Background(), &conformancev1.PrepareAssignmentRequest{Source: "api", MessageId: "prepare-generation", Assignment: validAssignment(overflowGeneration, now, now.Add(time.Hour))})
			return err
		},
		"prepare timestamp": func() error {
			_, err := handler.PrepareAssignment(context.Background(), &conformancev1.PrepareAssignmentRequest{Source: "api", MessageId: "prepare-time", Assignment: validAssignment(1, overflowTime, overflowTime.Add(time.Hour))})
			return err
		},
		"prepare NaN altitude": func() error {
			_, err := handler.PrepareAssignment(context.Background(), &conformancev1.PrepareAssignmentRequest{Source: "api", MessageId: "prepare-nan-altitude", Assignment: assignmentWithAltitude(math.NaN(), 120)})
			return err
		},
		"prepare infinite altitude": func() error {
			_, err := handler.PrepareAssignment(context.Background(), &conformancev1.PrepareAssignmentRequest{Source: "api", MessageId: "prepare-infinite-altitude", Assignment: assignmentWithAltitude(80, math.Inf(1))})
			return err
		},
		"cancel generation": func() error {
			_, err := handler.CancelAssignmentCandidate(context.Background(), &conformancev1.CancelAssignmentCandidateRequest{Source: "api", MessageId: "cancel-generation", AssignmentId: "assignment-1", AssignmentGeneration: overflowGeneration})
			return err
		},
		"cutover generation": func() error {
			_, err := handler.CutoverAssignment(context.Background(), &conformancev1.CutoverAssignmentRequest{Source: "api", MessageId: "cutover-generation", AssignmentId: "assignment-1", AssignmentGeneration: overflowGeneration, EffectiveAt: timestamppb.New(now)})
			return err
		},
		"cutover timestamp": func() error {
			_, err := handler.CutoverAssignment(context.Background(), &conformancev1.CutoverAssignmentRequest{Source: "api", MessageId: "cutover-time", AssignmentId: "assignment-1", AssignmentGeneration: 1, EffectiveAt: timestamppb.New(overflowTime)})
			return err
		},
		"get generation": func() error {
			_, err := handler.GetAssignment(context.Background(), &conformancev1.GetAssignmentRequest{AssignmentId: "assignment-1", AssignmentGeneration: overflowGeneration})
			return err
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			if err := run(); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error = %v, want InvalidArgument", err)
			}
		})
	}
}

func TestAssignmentHandlersValidateAndMapFences(t *testing.T) {
	handler, err := NewAssignmentHandler(&assignmentStoreStub{err: postgresstore.ErrStaleAssignment}, "standard-v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handler.CutoverAssignment(context.Background(), &conformancev1.CutoverAssignmentRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing cutover timestamp error = %v", err)
	}
	if _, err := handler.GetAssignment(context.Background(), &conformancev1.GetAssignmentRequest{AssignmentId: "assignment-1", AssignmentGeneration: 1}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale assignment error = %v", err)
	}
	missing, err := NewAssignmentHandler(&assignmentStoreStub{err: postgresstore.ErrAssignmentNotFound}, "standard-v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := missing.GetAssignment(context.Background(), &conformancev1.GetAssignmentRequest{AssignmentId: "assignment-1", AssignmentGeneration: 1}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing assignment error = %v", err)
	}
}
