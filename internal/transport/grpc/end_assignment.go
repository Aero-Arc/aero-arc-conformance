// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package grpc

import (
	"context"
	"github.com/aero-arc/aero-arc-conformance/internal/domain"
	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/conformance/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

// EndAssignment records monitoring closure for an exact assignment generation.
// Parameters: ctx bounds persistence; req supplies stable identity and aircraft
// completion time. Returns: the durable authority boundary or a gRPC error.
func (s *AssignmentHandler) EndAssignment(ctx context.Context, req *pb.EndAssignmentRequest) (*pb.EndAssignmentResponse, error) {
	if req.GetFlightCompletedAt() == nil || req.GetFlightCompletedAt().CheckValid() != nil {
		return nil, status.Error(codes.InvalidArgument, "valid completion time required")
	}
	store, ok := s.store.(interface {
		EndAssignment(context.Context, string, string, string, uint64, string, string, uint32, time.Time) (domain.AssignmentRecord, error)
	})
	if !ok {
		return nil, status.Error(codes.Unimplemented, "assignment completion unavailable")
	}
	record, err := store.EndAssignment(ctx, req.GetSource(), req.GetMessageId(), req.GetAssignmentId(), req.GetAssignmentGeneration(), req.GetFlightId(), req.GetAircraftId(), req.GetIntentVersion(), req.GetFlightCompletedAt().AsTime())
	if err != nil {
		return nil, assignmentStatusError(err)
	}
	return &pb.EndAssignmentResponse{Record: assignmentRecordToProto(record)}, nil
}
