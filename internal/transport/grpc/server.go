// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package grpc exposes Conformance assignment lifecycle commands over gRPC.
package grpc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"time"

	"github.com/aero-arc/aero-arc-conformance/internal/domain"
	postgresstore "github.com/aero-arc/aero-arc-conformance/internal/store/postgres"
	conformancev1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/conformance/v1"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// AssignmentStore is the durable lifecycle surface required by the gRPC adapter.
type AssignmentStore interface {
	PrepareAssignment(context.Context, string, string, string, domain.Assignment) (postgresstore.ApplyResult, error)
	CancelCandidate(context.Context, string, string, string, uint64) (postgresstore.LifecycleResult, error)
	CutoverAssignment(context.Context, string, string, string, uint64, time.Time) (postgresstore.LifecycleResult, error)
	GetAssignment(context.Context, string, uint64) (domain.AssignmentRecord, error)
}

// Server adapts generated Conformance RPCs to the durable assignment store.
type Server struct {
	conformancev1.UnimplementedConformanceServiceServer
	store      AssignmentStore
	grpcServer *gogrpc.Server
}

var _ conformancev1.ConformanceServiceServer = (*Server)(nil)

// New constructs an assignment lifecycle gRPC server.
//
// Parameters:
//   - store: persists idempotent lifecycle commands and authority cutovers.
//   - options: configure the underlying gRPC server.
//
// Returns:
//   - server: is registered with reflection and ready to serve.
//   - error: reports a missing durable store.
func New(store AssignmentStore, options ...gogrpc.ServerOption) (*Server, error) {
	if store == nil {
		return nil, fmt.Errorf("assignment store is required")
	}
	server := &Server{store: store, grpcServer: gogrpc.NewServer(options...)}
	conformancev1.RegisterConformanceServiceServer(server.grpcServer, server)
	reflection.Register(server.grpcServer)
	return server, nil
}

// Serve accepts assignment lifecycle RPCs until the server stops.
//
// Parameters:
//   - listener: owns the bound Conformance gRPC address.
//
// Returns:
//   - error: reports terminal listener or transport failure.
func (s *Server) Serve(listener net.Listener) error { return s.grpcServer.Serve(listener) }

// GracefulStop stops accepting RPCs and waits for active lifecycle commands.
// It is intended for the normal process shutdown path and returns only after
// all in-flight handlers have completed.
func (s *Server) GracefulStop() { s.grpcServer.GracefulStop() }

// Stop immediately terminates the gRPC server during a bounded shutdown
// fallback. Active handlers may observe transport cancellation.
func (s *Server) Stop() { s.grpcServer.Stop() }

// PrepareAssignment durably stores an immutable candidate without granting
// evaluation authority.
//
// Parameters:
//   - ctx: controls the durable command and reconciliation read.
//   - request: contains the idempotency identity and immutable assignment.
//
// Returns:
//   - response: contains the applied or idempotently replayed candidate.
//   - error: reports validation, command conflicts, or store failure as gRPC status.
func (s *Server) PrepareAssignment(ctx context.Context, request *conformancev1.PrepareAssignmentRequest) (*conformancev1.PrepareAssignmentResponse, error) {
	if strings.TrimSpace(request.GetSource()) == "" || strings.TrimSpace(request.GetMessageId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "source and message_id are required")
	}
	assignment, err := assignmentFromProto(request.GetAssignment())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := s.store.PrepareAssignment(ctx, request.GetSource(), request.GetMessageId(), "assignment_prepared", assignment)
	if err != nil {
		return nil, assignmentStatusError(err)
	}
	record, err := s.store.GetAssignment(ctx, result.Assignment.ID, result.Assignment.Generation)
	if err != nil {
		return nil, assignmentStatusError(err)
	}
	return &conformancev1.PrepareAssignmentResponse{Disposition: dispositionToProto(result.Disposition), Assignment: assignmentRecordToProto(record)}, nil
}

// CancelAssignmentCandidate cancels only the selected received or armed
// candidate; it cannot cancel the active assignment.
//
// Parameters:
//   - ctx: controls the durable lifecycle command.
//   - request: identifies the candidate generation and idempotency identity.
//
// Returns:
//   - response: contains the resulting immutable assignment record.
//   - error: reports validation, fencing, or store failure as gRPC status.
func (s *Server) CancelAssignmentCandidate(ctx context.Context, request *conformancev1.CancelAssignmentCandidateRequest) (*conformancev1.CancelAssignmentCandidateResponse, error) {
	if strings.TrimSpace(request.GetSource()) == "" || strings.TrimSpace(request.GetMessageId()) == "" || strings.TrimSpace(request.GetAssignmentId()) == "" || request.GetAssignmentGeneration() == 0 {
		return nil, status.Error(codes.InvalidArgument, "source, message_id, assignment_id, and assignment_generation are required")
	}
	result, err := s.store.CancelCandidate(ctx, request.GetSource(), request.GetMessageId(), request.GetAssignmentId(), request.GetAssignmentGeneration())
	if err != nil {
		return nil, assignmentStatusError(err)
	}
	return &conformancev1.CancelAssignmentCandidateResponse{Disposition: dispositionToProto(result.Disposition), Assignment: assignmentRecordToProto(result.Record)}, nil
}

// CutoverAssignment atomically transfers event-time authority to an armed
// candidate at the requested effective timestamp.
//
// Parameters:
//   - ctx: controls the durable cutover transaction.
//   - request: identifies the armed generation, idempotency identity, and boundary.
//
// Returns:
//   - response: contains the newly active immutable assignment record.
//   - error: reports validation, lifecycle fencing, or store failure as gRPC status.
func (s *Server) CutoverAssignment(ctx context.Context, request *conformancev1.CutoverAssignmentRequest) (*conformancev1.CutoverAssignmentResponse, error) {
	if strings.TrimSpace(request.GetSource()) == "" || strings.TrimSpace(request.GetMessageId()) == "" || strings.TrimSpace(request.GetAssignmentId()) == "" || request.GetAssignmentGeneration() == 0 {
		return nil, status.Error(codes.InvalidArgument, "source, message_id, assignment_id, and assignment_generation are required")
	}
	if request.GetEffectiveAt() == nil || request.GetEffectiveAt().CheckValid() != nil {
		return nil, status.Error(codes.InvalidArgument, "effective_at is required and must be valid")
	}
	result, err := s.store.CutoverAssignment(ctx, request.GetSource(), request.GetMessageId(), request.GetAssignmentId(), request.GetAssignmentGeneration(), request.GetEffectiveAt().AsTime())
	if err != nil {
		return nil, assignmentStatusError(err)
	}
	return &conformancev1.CutoverAssignmentResponse{Disposition: dispositionToProto(result.Disposition), Assignment: assignmentRecordToProto(result.Record)}, nil
}

// GetAssignment reconciles one exact immutable assignment generation after
// ambiguous command delivery.
//
// Parameters:
//   - ctx: controls the durable read.
//   - request: identifies the exact assignment generation.
//
// Returns:
//   - response: contains the immutable assignment and current lifecycle metadata.
//   - error: reports validation, absence, or store failure as gRPC status.
func (s *Server) GetAssignment(ctx context.Context, request *conformancev1.GetAssignmentRequest) (*conformancev1.GetAssignmentResponse, error) {
	if strings.TrimSpace(request.GetAssignmentId()) == "" || request.GetAssignmentGeneration() == 0 {
		return nil, status.Error(codes.InvalidArgument, "assignment_id and assignment_generation are required")
	}
	record, err := s.store.GetAssignment(ctx, request.GetAssignmentId(), request.GetAssignmentGeneration())
	if err != nil {
		return nil, assignmentStatusError(err)
	}
	return &conformancev1.GetAssignmentResponse{Assignment: assignmentRecordToProto(record)}, nil
}

func assignmentFromProto(value *conformancev1.Assignment) (domain.Assignment, error) {
	if value == nil || value.GetEffectiveFrom() == nil || value.GetEffectiveUntil() == nil {
		return domain.Assignment{}, fmt.Errorf("assignment and effective window are required")
	}
	if err := value.GetEffectiveFrom().CheckValid(); err != nil {
		return domain.Assignment{}, fmt.Errorf("effective_from: %w", err)
	}
	if err := value.GetEffectiveUntil().CheckValid(); err != nil {
		return domain.Assignment{}, fmt.Errorf("effective_until: %w", err)
	}
	assignment := domain.Assignment{
		ID: value.GetAssignmentId(), Generation: value.GetAssignmentGeneration(), OperatorID: value.GetOperatorId(),
		AircraftID: value.GetAircraftId(), AgentID: value.GetAgentId(), FlightID: value.GetFlightId(),
		IntentID: value.GetIntentId(), IntentVersion: value.GetIntentVersion(), PolicyVersion: value.GetPolicyVersion(),
		EffectiveFrom: value.GetEffectiveFrom().AsTime(), EffectiveUntil: value.GetEffectiveUntil().AsTime(),
		Volumes: make([]domain.Volume, 0, len(value.GetVolumes())),
	}
	for _, input := range value.GetVolumes() {
		if input == nil || input.GetStartsAt() == nil || input.GetEndsAt() == nil || input.GetStartsAt().CheckValid() != nil || input.GetEndsAt().CheckValid() != nil {
			return domain.Assignment{}, fmt.Errorf("every volume requires valid start and end timestamps")
		}
		reference, ok := altitudeReferenceFromProto[input.GetAltitudeReference()]
		if !ok {
			return domain.Assignment{}, fmt.Errorf("volume altitude reference is invalid")
		}
		volume := domain.Volume{ID: input.GetVolumeId(), AltitudeLowerM: input.GetAltitudeLowerM(), AltitudeUpperM: input.GetAltitudeUpperM(), AltitudeReference: reference, StartsAt: input.GetStartsAt().AsTime(), EndsAt: input.GetEndsAt().AsTime(), Polygon: make([]domain.Point, 0, len(input.GetPolygon()))}
		for _, point := range input.GetPolygon() {
			if point == nil || math.IsNaN(point.GetLatitude()) || math.IsInf(point.GetLatitude(), 0) || math.IsNaN(point.GetLongitude()) || math.IsInf(point.GetLongitude(), 0) {
				return domain.Assignment{}, fmt.Errorf("volume polygon contains an invalid point")
			}
			volume.Polygon = append(volume.Polygon, domain.Point{Latitude: point.GetLatitude(), Longitude: point.GetLongitude()})
		}
		assignment.Volumes = append(assignment.Volumes, volume)
	}
	if strings.TrimSpace(assignment.ID) == "" || assignment.Generation == 0 || strings.TrimSpace(assignment.AircraftID) == "" || strings.TrimSpace(assignment.AgentID) == "" || strings.TrimSpace(assignment.FlightID) == "" || strings.TrimSpace(assignment.IntentID) == "" || assignment.IntentVersion == 0 || strings.TrimSpace(assignment.PolicyVersion) == "" || !assignment.EffectiveUntil.After(assignment.EffectiveFrom) {
		return domain.Assignment{}, fmt.Errorf("assignment identity and effective window are invalid")
	}
	return assignment, nil
}

func assignmentRecordToProto(record domain.AssignmentRecord) *conformancev1.AssignmentRecord {
	result := &conformancev1.AssignmentRecord{Assignment: assignmentToProto(record.Assignment), Lifecycle: lifecycleToProto[record.Lifecycle], PreparedAt: optionalTime(record.PreparedAt), ArmedAt: optionalPointerTime(record.ArmedAt), CutoverAt: optionalPointerTime(record.CutoverAt), AuthorityFrom: optionalPointerTime(record.AuthorityFrom), AuthorityUntil: optionalPointerTime(record.AuthorityUntil)}
	return result
}

func assignmentToProto(assignment domain.Assignment) *conformancev1.Assignment {
	result := &conformancev1.Assignment{AssignmentId: assignment.ID, AssignmentGeneration: assignment.Generation, OperatorId: assignment.OperatorID, AircraftId: assignment.AircraftID, AgentId: assignment.AgentID, FlightId: assignment.FlightID, IntentId: assignment.IntentID, IntentVersion: assignment.IntentVersion, PolicyVersion: assignment.PolicyVersion, EffectiveFrom: timestamppb.New(assignment.EffectiveFrom), EffectiveUntil: timestamppb.New(assignment.EffectiveUntil), Volumes: make([]*conformancev1.ConformanceVolume, len(assignment.Volumes))}
	for index, volume := range assignment.Volumes {
		converted := &conformancev1.ConformanceVolume{VolumeId: volume.ID, AltitudeLowerM: volume.AltitudeLowerM, AltitudeUpperM: volume.AltitudeUpperM, AltitudeReference: altitudeReferenceToProto[volume.AltitudeReference], StartsAt: timestamppb.New(volume.StartsAt), EndsAt: timestamppb.New(volume.EndsAt), Polygon: make([]*conformancev1.GeographicPoint, len(volume.Polygon))}
		for pointIndex, point := range volume.Polygon {
			converted.Polygon[pointIndex] = &conformancev1.GeographicPoint{Latitude: point.Latitude, Longitude: point.Longitude}
		}
		result.Volumes[index] = converted
	}
	return result
}

func optionalPointerTime(value *time.Time) *timestamppb.Timestamp {
	if value == nil || value.IsZero() {
		return nil
	}
	return timestamppb.New(*value)
}

func optionalTime(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	return timestamppb.New(value)
}

func dispositionToProto(value postgresstore.ApplyDisposition) conformancev1.AssignmentCommandDisposition {
	return map[postgresstore.ApplyDisposition]conformancev1.AssignmentCommandDisposition{postgresstore.ApplyApplied: conformancev1.AssignmentCommandDisposition_ASSIGNMENT_COMMAND_DISPOSITION_APPLIED, postgresstore.ApplyIdempotent: conformancev1.AssignmentCommandDisposition_ASSIGNMENT_COMMAND_DISPOSITION_IDEMPOTENT, postgresstore.ApplyStale: conformancev1.AssignmentCommandDisposition_ASSIGNMENT_COMMAND_DISPOSITION_STALE}[value]
}

func assignmentStatusError(err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, postgresstore.ErrAssignmentNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, postgresstore.ErrMessageConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, postgresstore.ErrStaleAssignment), errors.Is(err, postgresstore.ErrInvalidTransition):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

var altitudeReferenceFromProto = map[conformancev1.AltitudeReference]domain.AltitudeReference{conformancev1.AltitudeReference_ALTITUDE_REFERENCE_MSL: domain.AltitudeMSL, conformancev1.AltitudeReference_ALTITUDE_REFERENCE_AGL: domain.AltitudeAGL}
var altitudeReferenceToProto = map[domain.AltitudeReference]conformancev1.AltitudeReference{domain.AltitudeMSL: conformancev1.AltitudeReference_ALTITUDE_REFERENCE_MSL, domain.AltitudeAGL: conformancev1.AltitudeReference_ALTITUDE_REFERENCE_AGL}
var lifecycleToProto = map[domain.AssignmentLifecycle]conformancev1.AssignmentLifecycle{domain.AssignmentReceived: conformancev1.AssignmentLifecycle_ASSIGNMENT_LIFECYCLE_CANDIDATE_RECEIVED, domain.AssignmentArmed: conformancev1.AssignmentLifecycle_ASSIGNMENT_LIFECYCLE_CANDIDATE_ARMED, domain.AssignmentActive: conformancev1.AssignmentLifecycle_ASSIGNMENT_LIFECYCLE_ACTIVE, domain.AssignmentEnding: conformancev1.AssignmentLifecycle_ASSIGNMENT_LIFECYCLE_ENDING, domain.AssignmentCompleted: conformancev1.AssignmentLifecycle_ASSIGNMENT_LIFECYCLE_COMPLETED, domain.AssignmentCancelled: conformancev1.AssignmentLifecycle_ASSIGNMENT_LIFECYCLE_CANCELLED, domain.AssignmentSuperseded: conformancev1.AssignmentLifecycle_ASSIGNMENT_LIFECYCLE_SUPERSEDED}
