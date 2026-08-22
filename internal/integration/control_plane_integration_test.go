//go:build integration

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package integration_test

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aero-arc/aero-arc-conformance/internal/domain"
	registryprojection "github.com/aero-arc/aero-arc-conformance/internal/projection/registry"
	postgresstore "github.com/aero-arc/aero-arc-conformance/internal/store/postgres"
	"github.com/aero-arc/aero-arc-conformance/internal/testsupport"
	assignmentgrpc "github.com/aero-arc/aero-arc-conformance/internal/transport/grpc"
	conformancev1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/conformance/v1"
	registryv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/registry/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAssignmentIngressAndRegistryOutboxAgainstPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	dsn := os.Getenv("AERO_CONFORMANCE_TEST_POSTGRES_URL")
	if dsn == "" {
		testcontainers.SkipIfProviderIsNotHealthy(t)
		postgres, err := testsupport.StartPostgres(ctx)
		if err != nil {
			t.Fatal(err)
		}
		dsn = postgres.URL
		t.Cleanup(func() {
			if err := postgres.Dependency.Shutdown(t.Failed(), os.Stderr); err != nil {
				t.Error(err)
			}
		})
	}
	store, err := postgresstore.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	assignmentListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	assignmentHandler, err := assignmentgrpc.NewAssignmentHandler(store, "standard-v1")
	if err != nil {
		t.Fatal(err)
	}
	assignmentServer := grpc.NewServer()
	conformancev1.RegisterConformanceServiceServer(assignmentServer, assignmentHandler)
	assignmentDone := make(chan error, 1)
	go func() { assignmentDone <- assignmentServer.Serve(assignmentListener) }()
	t.Cleanup(func() {
		assignmentServer.GracefulStop()
		if serveErr := <-assignmentDone; serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			t.Errorf("assignment server: %v", serveErr)
		}
	})
	assignmentConnection, err := grpc.NewClient(assignmentListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = assignmentConnection.Close() })
	assignmentClient := conformancev1.NewConformanceServiceClient(assignmentConnection)

	now := time.Now().UTC()
	assignment := &conformancev1.Assignment{
		AssignmentId: "assignment-control-plane", AssignmentGeneration: 2,
		OperatorId: "operator-1", AircraftId: "aircraft-1", AgentId: "agent-1",
		FlightId: "flight-1", IntentId: "intent-1", IntentVersion: 1, PolicyVersion: "standard-v1",
		EffectiveFrom: timestamppb.New(now.Add(-time.Hour)), EffectiveUntil: timestamppb.New(now.Add(time.Hour)),
		Volumes: []*conformancev1.ConformanceVolume{{
			VolumeId: "volume-1", AltitudeLowerM: 80, AltitudeUpperM: 120,
			AltitudeReference: conformancev1.AltitudeReference_ALTITUDE_REFERENCE_MSL,
			StartsAt:          timestamppb.New(now.Add(-time.Hour)), EndsAt: timestamppb.New(now.Add(time.Hour)),
			Polygon: []*conformancev1.GeographicPoint{{Latitude: 35, Longitude: -97.01}, {Latitude: 35.01, Longitude: -97.01}, {Latitude: 35.01, Longitude: -97}},
		}},
	}
	prepared, err := assignmentClient.PrepareAssignment(ctx, &conformancev1.PrepareAssignmentRequest{Source: "api", MessageId: "prepare-1", Assignment: assignment})
	if err != nil || prepared.GetAssignment().GetLifecycle() != conformancev1.AssignmentLifecycle_ASSIGNMENT_LIFECYCLE_CANDIDATE_RECEIVED {
		t.Fatalf("PrepareAssignment() = %+v, error = %v", prepared, err)
	}
	if _, err = store.ArmAssignment(ctx, "conformance-worker", "arm-1", assignment.GetAssignmentId(), assignment.GetAssignmentGeneration()); err != nil {
		t.Fatal(err)
	}
	cutoverAt := now.Add(-time.Second)
	cutover, err := assignmentClient.CutoverAssignment(ctx, &conformancev1.CutoverAssignmentRequest{Source: "api", MessageId: "cutover-1", AssignmentId: assignment.GetAssignmentId(), AssignmentGeneration: assignment.GetAssignmentGeneration(), EffectiveAt: timestamppb.New(cutoverAt)})
	if err != nil || cutover.GetAssignment().GetLifecycle() != conformancev1.AssignmentLifecycle_ASSIGNMENT_LIFECYCLE_ACTIVE {
		t.Fatalf("CutoverAssignment() = %+v, error = %v", cutover, err)
	}
	read, err := assignmentClient.GetAssignment(ctx, &conformancev1.GetAssignmentRequest{AssignmentId: assignment.GetAssignmentId(), AssignmentGeneration: assignment.GetAssignmentGeneration()})
	if err != nil || !read.GetAssignment().GetAuthorityFrom().AsTime().Equal(cutoverAt) {
		t.Fatalf("GetAssignment() = %+v, error = %v", read, err)
	}
	staleAssignment := proto.Clone(assignment).(*conformancev1.Assignment)
	staleAssignment.AssignmentGeneration = 1
	for attempt := 1; attempt <= 2; attempt++ {
		stale, staleErr := assignmentClient.PrepareAssignment(ctx, &conformancev1.PrepareAssignmentRequest{Source: "api", MessageId: "stale-prepare-1", Assignment: staleAssignment})
		if staleErr != nil || stale.GetDisposition() != conformancev1.AssignmentCommandDisposition_ASSIGNMENT_COMMAND_DISPOSITION_STALE || stale.GetAssignment() != nil {
			t.Fatalf("stale PrepareAssignment() attempt %d = %+v, error = %v", attempt, stale, staleErr)
		}
	}

	claims, err := store.ClaimDueAssignments(ctx, "evaluation-worker", time.Minute, 1)
	if err != nil || len(claims) != 1 {
		t.Fatalf("ClaimDueAssignments() = %+v, error = %v", claims, err)
	}
	observedAt := now
	evaluation := domain.Evaluation{
		Condition: domain.ConditionConforming, Monitoring: domain.MonitoringCurrent, Recording: domain.RecordingPending,
		State:      domain.EvaluatorState{Violations: map[domain.ViolationType]domain.IncidentState{}},
		CausalFrom: observedAt, ObservedAt: observedAt, FrameID: "frame-1", WALID: "wal-1", WALSequence: 1,
	}
	if err = store.CommitEvaluation(ctx, claims[0], postgresstore.EvaluationCommit{Evaluation: evaluation, NextEvaluationAt: now.Add(-time.Second)}); err != nil {
		t.Fatal(err)
	}
	claims, err = store.ClaimDueAssignments(ctx, "evaluation-worker-2", time.Minute, 1)
	if err != nil || len(claims) != 1 {
		t.Fatalf("second ClaimDueAssignments() = %+v, error = %v", claims, err)
	}
	secondEvaluation := evaluation
	secondEvaluation.CausalFrom = observedAt.Add(time.Nanosecond)
	secondEvaluation.ObservedAt = observedAt.Add(time.Nanosecond)
	secondEvaluation.FrameID = "frame-2"
	secondEvaluation.WALSequence = 2
	if err = store.CommitEvaluation(ctx, claims[0], postgresstore.EvaluationCommit{Evaluation: secondEvaluation, NextEvaluationAt: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}

	firstPublisherClaims, err := store.ClaimOutbox(ctx, "registry", "ordering-probe-1", time.Minute, 10)
	if err != nil || len(firstPublisherClaims) != 1 || firstPublisherClaims[0].EvaluationRevision != 1 {
		t.Fatalf("first Registry claim = %+v, error = %v", firstPublisherClaims, err)
	}
	secondPublisherClaims, err := store.ClaimOutbox(ctx, "registry", "ordering-probe-2", time.Minute, 10)
	if err != nil || len(secondPublisherClaims) != 0 {
		t.Fatalf("concurrent Registry claim bypassed the assignment head: %+v, error = %v", secondPublisherClaims, err)
	}
	if err = store.RetryOutbox(ctx, firstPublisherClaims[0], time.Millisecond, errors.New("release ordering probe")); err != nil {
		t.Fatal(err)
	}

	registryListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	registryService := &capturingRegistry{}
	registryServer := grpc.NewServer()
	registryv1.RegisterAeroRegistryServer(registryServer, registryService)
	registryDone := make(chan error, 1)
	go func() { registryDone <- registryServer.Serve(registryListener) }()
	t.Cleanup(func() {
		registryServer.GracefulStop()
		if serveErr := <-registryDone; serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			t.Errorf("Registry server: %v", serveErr)
		}
	})
	registryConnection, err := grpc.NewClient(registryListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registryConnection.Close() })
	publisher, err := registryprojection.New(store, registryv1.NewAeroRegistryClient(registryConnection), registryprojection.Config{WorkerID: "publisher-1", PollInterval: time.Second, RequestTimeout: 5 * time.Second, LeaseDuration: 10 * time.Second, RetryDelay: time.Second, BatchSize: 10}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	var processed int
	for deadline := time.Now().Add(time.Second); ; {
		processed, err = publisher.Flush(ctx)
		if err != nil || processed != 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil || processed != 1 {
		t.Fatalf("publisher.Flush() processed=%d error=%v", processed, err)
	}
	published := registryService.summary()
	if published.GetAssignmentId() != assignment.GetAssignmentId() || published.GetAssignmentGeneration() != assignment.GetAssignmentGeneration() || published.GetEvaluationRevision() != 1 || published.GetRecordingStatus() != conformancev1.RecordingStatus_RECORDING_STATUS_CONFIRMED {
		t.Fatalf("published summary = %+v", published)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var delivered bool
	var attemptCount int
	if err = pool.QueryRow(ctx, `SELECT delivered_at IS NOT NULL,attempt_count FROM conformance_outbox WHERE destination='registry' AND assignment_id=$1 AND evaluation_revision=1`, assignment.GetAssignmentId()).Scan(&delivered, &attemptCount); err != nil {
		t.Fatal(err)
	}
	if !delivered || attemptCount != 2 {
		t.Fatalf("outbox delivered=%v attempt_count=%d", delivered, attemptCount)
	}
	if processed, err = publisher.Flush(ctx); err != nil || processed != 1 {
		t.Fatalf("second Flush() processed=%d error=%v", processed, err)
	}
	published = registryService.summary()
	if published.GetEvaluationRevision() != 2 || published.GetFrameId() != "frame-2" {
		t.Fatalf("second published summary = %+v", published)
	}
	if processed, err = publisher.Flush(ctx); err != nil || processed != 0 {
		t.Fatalf("third Flush() processed=%d error=%v", processed, err)
	}
}

type capturingRegistry struct {
	registryv1.UnimplementedAeroRegistryServer
	mu       sync.Mutex
	received *conformancev1.ConformanceSummary
}

func (r *capturingRegistry) PublishConformanceSummary(_ context.Context, request *registryv1.PublishConformanceSummaryRequest) (*registryv1.PublishConformanceSummaryResponse, error) {
	r.mu.Lock()
	r.received = proto.Clone(request.GetSummary()).(*conformancev1.ConformanceSummary)
	r.mu.Unlock()
	now := timestamppb.Now()
	return &registryv1.PublishConformanceSummaryResponse{Disposition: registryv1.ConformancePublishDisposition_CONFORMANCE_PUBLISH_DISPOSITION_APPLIED, Projection: &registryv1.ConformanceProjection{Summary: request.GetSummary(), StoredAt: now, ExpiresAt: timestamppb.New(now.AsTime().Add(time.Minute))}}, nil
}

func (r *capturingRegistry) summary() *conformancev1.ConformanceSummary {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.received == nil {
		return nil
	}
	return proto.Clone(r.received).(*conformancev1.ConformanceSummary)
}
