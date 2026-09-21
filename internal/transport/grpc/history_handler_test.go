// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package grpc

import (
	"context"
	"errors"
	postgresstore "github.com/aero-arc/aero-arc-conformance/internal/store/postgres"
	conformancev1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/conformance/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
)

type historyStub struct {
	assignmentStoreStub
	query postgresstore.HistoryQuery
	err   error
}

func (s *historyStub) ListConformanceEvents(_ context.Context, q postgresstore.HistoryQuery) (postgresstore.HistoryPage, error) {
	s.query = q
	zero := 0.0
	return postgresstore.HistoryPage{Events: []postgresstore.HistoryEvent{{ID: "resolved", AssignmentID: q.AssignmentID, Generation: 2, Transition: "resolved", ViolationType: "lateral_deviation", ObservedAt: time.Now(), DeviationM: &zero}}, NextPageToken: "next"}, s.err
}
func TestHistoryHandlerMappingAndFailure(t *testing.T) {
	store := &historyStub{}
	handler, _ := NewAssignmentHandler(store, "v1")
	result, err := handler.ListConformanceEvents(context.Background(), &conformancev1.ListConformanceEventsRequest{AssignmentId: "a", AssignmentGeneration: 2, PageSize: 10})
	if err != nil || len(result.Events) != 1 || result.Events[0].DeviationM == nil || result.Events[0].GetDeviationM() != 0 || result.NextPageToken != "next" || store.query.Generation != 2 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{{postgresstore.ErrInvalidHistoryQuery, codes.InvalidArgument}, {errors.New("database disconnected"), codes.Unavailable}} {
		store.err = tc.err
		_, err = handler.ListConformanceEvents(context.Background(), &conformancev1.ListConformanceEventsRequest{AssignmentId: "a"})
		if status.Code(err) != tc.code {
			t.Fatalf("error=%v", err)
		}
	}
	_, err = handler.ListConformanceEvents(context.Background(), &conformancev1.ListConformanceEventsRequest{From: &timestamppb.Timestamp{Seconds: mathMaxTimestamp}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid timestamp: %v", err)
	}
}

const mathMaxTimestamp = 253402300800
