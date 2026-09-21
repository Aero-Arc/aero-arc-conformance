// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package grpc

import (
	"context"
	"errors"
	"time"

	postgresstore "github.com/aero-arc/aero-arc-conformance/internal/store/postgres"
	conformancev1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/conformance/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type historyStore interface {
	ListConformanceEvents(context.Context, postgresstore.HistoryQuery) (postgresstore.HistoryPage, error)
}

// ListConformanceEvents reads durable history through the service-owned store.
//
// Parameters:
//   - ctx: propagates the caller's cancellation and read deadline.
//   - request: supplies one assignment plus generation/time filters and pagination.
//
// Returns:
//   - response: immutable newest-first transition evidence, not current state.
//   - error: InvalidArgument for invalid filters, Unavailable for storage failure,
//     or the caller's cancellation/deadline status. Empty history is successful.
func (s *AssignmentHandler) ListConformanceEvents(ctx context.Context, request *conformancev1.ListConformanceEventsRequest) (*conformancev1.ListConformanceEventsResponse, error) {
	q := postgresstore.HistoryQuery{AssignmentID: request.GetAssignmentId(), Generation: request.GetAssignmentGeneration(), PageSize: int(request.GetPageSize()), PageToken: request.GetPageToken()}
	for _, bound := range []struct {
		src *timestamppb.Timestamp
		dst **time.Time
	}{{request.GetFrom(), &q.From}, {request.GetUntil(), &q.Until}} {
		if bound.src != nil {
			if bound.src.CheckValid() != nil {
				return nil, status.Error(codes.InvalidArgument, "invalid event-time bound")
			}
			v := bound.src.AsTime()
			*bound.dst = &v
		}
	}
	store, ok := s.store.(historyStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "conformance history is not configured")
	}
	page, err := store.ListConformanceEvents(ctx, q)
	if err != nil {
		if errors.Is(err, postgresstore.ErrInvalidHistoryQuery) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, status.Error(codes.Unavailable, "conformance history read failed")
	}
	result := &conformancev1.ListConformanceEventsResponse{NextPageToken: page.NextPageToken}
	for _, e := range page.Events {
		kind := conformancev1.ViolationType_VIOLATION_TYPE_UNSPECIFIED
		switch e.ViolationType {
		case "lateral_deviation":
			kind = conformancev1.ViolationType_VIOLATION_TYPE_LATERAL_DEVIATION
		case "altitude_deviation":
			kind = conformancev1.ViolationType_VIOLATION_TYPE_ALTITUDE_DEVIATION
		case "temporal_deviation":
			kind = conformancev1.ViolationType_VIOLATION_TYPE_TEMPORAL_DEVIATION
		case "telemetry_loss":
			kind = conformancev1.ViolationType_VIOLATION_TYPE_TELEMETRY_LOSS
		}
		result.Events = append(result.Events, &conformancev1.ConformanceHistoryEvent{EventId: e.ID, AssignmentId: e.AssignmentID, AssignmentGeneration: e.Generation, IntentId: e.IntentID, IntentVersion: e.IntentVersion, AircraftId: e.AircraftID, FlightId: e.FlightID, IncidentId: e.IncidentID, Transition: e.Transition, ViolationType: kind, ObservedAt: timestamppb.New(e.ObservedAt), DeviationM: e.DeviationM, FrameId: e.FrameID, EvaluationRevision: e.EvaluationRevision})
		last := result.Events[len(result.Events)-1]
		if e.PlannedStartAt != nil {
			last.PlannedStartAt = timestamppb.New(*e.PlannedStartAt)
		}
		if e.PlannedEndAt != nil {
			last.PlannedEndAt = timestamppb.New(*e.PlannedEndAt)
		}
	}
	return result, nil
}
