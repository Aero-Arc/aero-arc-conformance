// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package grpc

import (
	"context"
	"github.com/aero-arc/aero-arc-conformance/internal/domain"
	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/conformance/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math"
	"strings"
	"time"
)

// EndAssignment records monitoring closure for an exact assignment generation.
// Generation zero resolves one active/ending generation matching the exact
// flight, aircraft, Agent, intent identity and version, under the assignment lifecycle lock.
// A replay of source/message identity uses the originally resolved generation;
// changed content conflicts. Closure fences workers and preserves committed
// evidence: the half-open authority boundary includes completion and the existing
// watermark, without extending a previously established authority end.
//
// Parameters:
//   - ctx bounds database persistence and cancellation.
//   - req supplies immutable source/message identity, exact flight binding,
//     optional explicit generation, and aircraft event time of completion.
//
// Returns: the durable ending record; InvalidArgument for malformed fields,
// AlreadyExists for changed replay content, NotFound for missing authority,
// FailedPrecondition for binding/lifecycle conflicts, or a dependency error.
func (s *AssignmentHandler) EndAssignment(ctx context.Context, req *pb.EndAssignmentRequest) (*pb.EndAssignmentResponse, error) {
	if strings.TrimSpace(req.GetSource()) == "" || strings.TrimSpace(req.GetMessageId()) == "" || strings.TrimSpace(req.GetAssignmentId()) == "" || strings.TrimSpace(req.GetFlightId()) == "" || strings.TrimSpace(req.GetAircraftId()) == "" || strings.TrimSpace(req.GetIntentId()) == "" || strings.TrimSpace(req.GetAgentId()) == "" || req.GetIntentVersion() == 0 || req.GetIntentVersion() > math.MaxInt32 || req.GetAssignmentGeneration() > math.MaxInt64 {
		return nil, status.Error(codes.InvalidArgument, "valid completion identity and binding required")
	}
	if req.GetFlightCompletedAt() == nil || req.GetFlightCompletedAt().CheckValid() != nil || !supportedUnixNanoseconds(req.GetFlightCompletedAt().AsTime()) {
		return nil, status.Error(codes.InvalidArgument, "valid completion time required")
	}
	store, ok := s.store.(interface {
		EndAssignment(context.Context, string, string, string, uint64, string, string, string, string, uint32, time.Time) (domain.AssignmentRecord, error)
	})
	if !ok {
		return nil, status.Error(codes.Unimplemented, "assignment completion unavailable")
	}
	record, err := store.EndAssignment(ctx, req.GetSource(), req.GetMessageId(), req.GetAssignmentId(), req.GetAssignmentGeneration(), req.GetFlightId(), req.GetAircraftId(), req.GetIntentId(), req.GetAgentId(), req.GetIntentVersion(), req.GetFlightCompletedAt().AsTime())
	if err != nil {
		return nil, assignmentStatusError(err)
	}
	return &pb.EndAssignmentResponse{Record: assignmentRecordToProto(record)}, nil
}
