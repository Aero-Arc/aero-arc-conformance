// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package registry

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/aero-arc/aero-arc-conformance/internal/domain"
	postgresstore "github.com/aero-arc/aero-arc-conformance/internal/store/postgres"
	conformancev1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/conformance/v1"
	registryv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/registry/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type publisherStoreStub struct {
	claims    []postgresstore.OutboxClaim
	delivered []string
	retried   []string
}

func (s *publisherStoreStub) ClaimOutbox(context.Context, string, string, time.Duration, int) ([]postgresstore.OutboxClaim, error) {
	return s.claims, nil
}
func (s *publisherStoreStub) MarkOutboxDelivered(_ context.Context, claim postgresstore.OutboxClaim) error {
	s.delivered = append(s.delivered, claim.ID)
	return nil
}
func (s *publisherStoreStub) RetryOutbox(_ context.Context, claim postgresstore.OutboxClaim, _ time.Duration, _ error) error {
	s.retried = append(s.retried, claim.ID)
	return nil
}

type publisherClientStub struct {
	publish func(*registryv1.PublishConformanceSummaryRequest) (*registryv1.PublishConformanceSummaryResponse, error)
}

func (c *publisherClientStub) PublishConformanceSummary(_ context.Context, request *registryv1.PublishConformanceSummaryRequest, _ ...grpc.CallOption) (*registryv1.PublishConformanceSummaryResponse, error) {
	return c.publish(request)
}

func TestFlushAcknowledgesExactRegistryProjection(t *testing.T) {
	claim := publisherTestClaim()
	store := &publisherStoreStub{claims: []postgresstore.OutboxClaim{claim}}
	client := &publisherClientStub{publish: func(request *registryv1.PublishConformanceSummaryRequest) (*registryv1.PublishConformanceSummaryResponse, error) {
		if request.GetSummary().GetCondition().String() != "CONFORMANCE_CONDITION_NON_CONFORMING" || len(request.GetSummary().GetViolations()) != 1 {
			t.Fatalf("published summary = %+v", request.GetSummary())
		}
		return &registryv1.PublishConformanceSummaryResponse{Projection: &registryv1.ConformanceProjection{Summary: request.GetSummary()}}, nil
	}}
	publisher := newPublisherForTest(t, store, client)
	processed, err := publisher.Flush(context.Background())
	if err != nil || processed != 1 || len(store.delivered) != 1 || len(store.retried) != 0 {
		t.Fatalf("Flush() processed=%d delivered=%v retried=%v error=%v", processed, store.delivered, store.retried, err)
	}
}

func TestFlushRetriesAmbiguousFailures(t *testing.T) {
	claim := publisherTestClaim()
	t.Run("failed precondition is not proof of a higher cursor", func(t *testing.T) {
		store := &publisherStoreStub{claims: []postgresstore.OutboxClaim{claim}}
		client := &publisherClientStub{publish: func(*registryv1.PublishConformanceSummaryRequest) (*registryv1.PublishConformanceSummaryResponse, error) {
			return nil, status.Error(codes.FailedPrecondition, "conflicting projection")
		}}
		_, err := newPublisherForTest(t, store, client).Flush(context.Background())
		if err == nil || len(store.delivered) != 0 || len(store.retried) != 1 {
			t.Fatalf("ambiguous precondition delivered=%v retried=%v error=%v", store.delivered, store.retried, err)
		}
	})
	t.Run("unavailable registry retries", func(t *testing.T) {
		store := &publisherStoreStub{claims: []postgresstore.OutboxClaim{claim}}
		client := &publisherClientStub{publish: func(*registryv1.PublishConformanceSummaryRequest) (*registryv1.PublishConformanceSummaryResponse, error) {
			return nil, status.Error(codes.Unavailable, "offline")
		}}
		_, err := newPublisherForTest(t, store, client).Flush(context.Background())
		if err == nil || len(store.delivered) != 0 || len(store.retried) != 1 {
			t.Fatalf("failed delivery delivered=%v retried=%v error=%v", store.delivered, store.retried, err)
		}
	})
	t.Run("mismatched acknowledgement retries", func(t *testing.T) {
		store := &publisherStoreStub{claims: []postgresstore.OutboxClaim{claim}}
		client := &publisherClientStub{publish: func(request *registryv1.PublishConformanceSummaryRequest) (*registryv1.PublishConformanceSummaryResponse, error) {
			accepted := proto.Clone(request.GetSummary()).(*conformancev1.ConformanceSummary)
			accepted.EvaluationRevision++
			return &registryv1.PublishConformanceSummaryResponse{Projection: &registryv1.ConformanceProjection{Summary: accepted}}, nil
		}}
		_, err := newPublisherForTest(t, store, client).Flush(context.Background())
		if err == nil || len(store.retried) != 1 {
			t.Fatalf("mismatched acknowledgement retried=%v error=%v", store.retried, err)
		}
	})
}

func newPublisherForTest(t *testing.T, store Store, client Client) *Publisher {
	t.Helper()
	publisher, err := New(store, client, Config{WorkerID: "worker-1", PollInterval: time.Second, RequestTimeout: time.Second, LeaseDuration: 2 * time.Second, RetryDelay: time.Second, BatchSize: 10}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	return publisher
}

func publisherTestClaim() postgresstore.OutboxClaim {
	observedAt := time.Unix(1_786_468_800, 0).UTC()
	return postgresstore.OutboxClaim{
		ID: "registry:assignment-1:7:3", Destination: destination, IdempotencyKey: "registry:assignment-1:7:3",
		Assignment:         domain.Assignment{ID: "assignment-1", Generation: 7, OperatorID: "operator-1", AircraftID: "aircraft-1", FlightID: "flight-1", IntentID: "intent-1", IntentVersion: 2},
		EvaluationRevision: 3,
		Evaluation:         domain.Evaluation{Condition: domain.ConditionNonConforming, Monitoring: domain.MonitoringCurrent, Recording: domain.RecordingConfirmed, ObservedAt: observedAt, FrameID: "frame-3", State: domain.EvaluatorState{Violations: map[domain.ViolationType]domain.IncidentState{domain.ViolationLateral: {Phase: domain.IncidentOpen, OpeningFrameID: "frame-1", OpenedAt: observedAt.Add(-time.Second), LastObservedAt: observedAt, WorstDeviationM: 10}}}},
	}
}
