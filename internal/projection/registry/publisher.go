// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package registry publishes durable Conformance outbox messages to Registry.
package registry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/aero-arc/aero-arc-conformance/internal/domain"
	postgresstore "github.com/aero-arc/aero-arc-conformance/internal/store/postgres"
	conformancev1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/conformance/v1"
	registryv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/registry/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const destination = "registry"

// Store is the independently leased durable delivery surface used by Publisher.
type Store interface {
	ClaimOutbox(context.Context, string, string, time.Duration, int) ([]postgresstore.OutboxClaim, error)
	MarkOutboxDelivered(context.Context, postgresstore.OutboxClaim) error
	RetryOutbox(context.Context, postgresstore.OutboxClaim, time.Duration, error) error
}

// Client is the generated Registry publication method used by Publisher.
type Client interface {
	PublishConformanceSummary(context.Context, *registryv1.PublishConformanceSummaryRequest, ...grpc.CallOption) (*registryv1.PublishConformanceSummaryResponse, error)
}

// Config controls durable outbox claiming, request deadlines, and retries.
type Config struct {
	WorkerID       string
	PollInterval   time.Duration
	RequestTimeout time.Duration
	LeaseDuration  time.Duration
	RetryDelay     time.Duration
	BatchSize      int
}

// Publisher delivers committed current-state projections without coupling the
// evaluation transaction to Registry availability.
type Publisher struct {
	store  Store
	client Client
	config Config
	log    *slog.Logger
}

// New constructs a Registry outbox publisher with independent delivery leases.
//
// Parameters:
//   - store: claims and completes durable Registry outbox messages.
//   - client: publishes one fenced current-state projection.
//   - config: defines publisher identity, cadence, deadlines, and batch bounds.
//   - log: records transient delivery failures without terminating the process.
//
// Returns:
//   - publisher: is ready to run; it owns neither store nor client shutdown.
//   - error: reports missing dependencies or unsafe timing/batch configuration.
func New(store Store, client Client, config Config, log *slog.Logger) (*Publisher, error) {
	if store == nil || client == nil || log == nil || config.WorkerID == "" || config.PollInterval <= 0 || config.RequestTimeout <= 0 || config.LeaseDuration <= config.RequestTimeout || config.RetryDelay <= 0 || config.BatchSize < 1 {
		return nil, fmt.Errorf("Registry publisher configuration is invalid")
	}
	return &Publisher{store: store, client: client, config: config, log: log}, nil
}

// Run repeatedly drains bounded Registry outbox batches until cancellation.
// Transient claim and delivery errors are logged and retried; cancellation is
// the only normal terminal condition.
//
// Parameters:
//   - ctx: stops new claims and in-flight Registry requests.
//
// Returns:
//   - error: is nil after cancellation.
func (p *Publisher) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			if _, err := p.Flush(ctx); err != nil && !errors.Is(err, context.Canceled) {
				p.log.Error("publish Registry outbox", "error", err)
			}
			timer.Reset(p.config.PollInterval)
		}
	}
}

// Flush claims and attempts one bounded batch, continuing past individual
// poison or unavailable messages so one assignment cannot block its neighbors.
//
// Parameters:
//   - ctx: controls the claim and every delivery attempt.
//
// Returns:
//   - processed: is the number of messages claimed, regardless of outcome.
//   - error: joins claim, publication, acknowledgement, and retry-scheduling failures.
func (p *Publisher) Flush(ctx context.Context) (int, error) {
	claims, err := p.store.ClaimOutbox(ctx, destination, p.config.WorkerID, p.config.LeaseDuration, p.config.BatchSize)
	if err != nil {
		return 0, err
	}
	errCh := make(chan error, len(claims))
	var workers sync.WaitGroup
	for _, claim := range claims {
		workers.Add(1)
		go func(claim postgresstore.OutboxClaim) {
			defer workers.Done()
			if err := p.publish(ctx, claim); err != nil {
				errCh <- err
			}
		}(claim)
	}
	workers.Wait()
	close(errCh)
	errs := make([]error, 0, len(errCh))
	for err := range errCh {
		errs = append(errs, err)
	}
	return len(claims), errors.Join(errs...)
}

func (p *Publisher) publish(ctx context.Context, claim postgresstore.OutboxClaim) error {
	request := &registryv1.PublishConformanceSummaryRequest{Summary: summaryToProto(claim)}
	requestCtx, cancel := context.WithTimeout(ctx, p.config.RequestTimeout)
	response, err := p.client.PublishConformanceSummary(requestCtx, request)
	cancel()
	if status.Code(err) == codes.FailedPrecondition {
		// A higher Registry cursor already exists. This durable outbox item is
		// obsolete, and exact generation/revision fencing makes completion safe.
		return p.store.MarkOutboxDelivered(ctx, claim)
	}
	if err != nil {
		return p.scheduleRetry(ctx, claim, err)
	}
	if err = validateAcknowledgement(request.GetSummary(), response); err != nil {
		return p.scheduleRetry(ctx, claim, err)
	}
	if err = p.store.MarkOutboxDelivered(ctx, claim); err != nil {
		return fmt.Errorf("complete Registry outbox %s: %w", claim.ID, err)
	}
	return nil
}

func (p *Publisher) scheduleRetry(ctx context.Context, claim postgresstore.OutboxClaim, deliveryErr error) error {
	if retryErr := p.store.RetryOutbox(ctx, claim, p.config.RetryDelay, deliveryErr); retryErr != nil {
		return errors.Join(fmt.Errorf("publish Registry outbox %s: %w", claim.ID, deliveryErr), fmt.Errorf("schedule retry: %w", retryErr))
	}
	return fmt.Errorf("publish Registry outbox %s: %w", claim.ID, deliveryErr)
}

func validateAcknowledgement(sent *conformancev1.ConformanceSummary, response *registryv1.PublishConformanceSummaryResponse) error {
	if response == nil || response.GetProjection() == nil || response.GetProjection().GetSummary() == nil {
		return fmt.Errorf("Registry acknowledgement omitted the accepted projection")
	}
	accepted := response.GetProjection().GetSummary()
	if accepted.GetAssignmentId() != sent.GetAssignmentId() || accepted.GetAssignmentGeneration() != sent.GetAssignmentGeneration() || accepted.GetEvaluationRevision() != sent.GetEvaluationRevision() || accepted.GetEvaluationId() != sent.GetEvaluationId() {
		return fmt.Errorf("Registry acknowledgement did not match the exact evaluation cursor")
	}
	return nil
}

func summaryToProto(claim postgresstore.OutboxClaim) *conformancev1.ConformanceSummary {
	violations := make([]domain.ViolationType, 0, len(claim.Evaluation.State.Violations))
	for violation := range claim.Evaluation.State.Violations {
		violations = append(violations, violation)
	}
	sort.Slice(violations, func(i, j int) bool { return violations[i] < violations[j] })
	result := &conformancev1.ConformanceSummary{
		AssignmentId: claim.Assignment.ID, AssignmentGeneration: claim.Assignment.Generation,
		EvaluationRevision: claim.EvaluationRevision, EvaluationId: claim.IdempotencyKey,
		OperatorId: claim.Assignment.OperatorID, AircraftId: claim.Assignment.AircraftID,
		FlightId: claim.Assignment.FlightID, IntentId: claim.Assignment.IntentID,
		IntentVersion: claim.Assignment.IntentVersion, Condition: conditionToProto[claim.Evaluation.Condition],
		MonitoringStatus: monitoringToProto[claim.Evaluation.Monitoring], RecordingStatus: recordingToProto[claim.Evaluation.Recording],
		ObservedAt: timestamppb.New(claim.Evaluation.ObservedAt), FrameId: claim.Evaluation.FrameID,
		Violations: make([]*conformancev1.ViolationSummary, 0, len(violations)),
	}
	for _, violation := range violations {
		state := claim.Evaluation.State.Violations[violation]
		result.Violations = append(result.Violations, &conformancev1.ViolationSummary{
			ViolationType: violationToProto[violation], Phase: phaseToProto[state.Phase], OpeningFrameId: state.OpeningFrameID,
			OpenedAt: optionalTime(state.OpenedAt), LastObservedAt: optionalTime(state.LastObservedAt), WorstDeviationM: state.WorstDeviationM,
		})
	}
	return result
}

func optionalTime(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	return timestamppb.New(value)
}

var conditionToProto = map[domain.Condition]conformancev1.ConformanceCondition{domain.ConditionUnknown: conformancev1.ConformanceCondition_CONFORMANCE_CONDITION_UNKNOWN, domain.ConditionConforming: conformancev1.ConformanceCondition_CONFORMANCE_CONDITION_CONFORMING, domain.ConditionSuspected: conformancev1.ConformanceCondition_CONFORMANCE_CONDITION_SUSPECTED, domain.ConditionNonConforming: conformancev1.ConformanceCondition_CONFORMANCE_CONDITION_NON_CONFORMING, domain.ConditionRecovering: conformancev1.ConformanceCondition_CONFORMANCE_CONDITION_RECOVERING}
var monitoringToProto = map[domain.MonitoringStatus]conformancev1.MonitoringStatus{domain.MonitoringReceived: conformancev1.MonitoringStatus_MONITORING_STATUS_RECEIVED, domain.MonitoringArmed: conformancev1.MonitoringStatus_MONITORING_STATUS_ARMED, domain.MonitoringCurrent: conformancev1.MonitoringStatus_MONITORING_STATUS_CURRENT, domain.MonitoringStale: conformancev1.MonitoringStatus_MONITORING_STATUS_STALE, domain.MonitoringUnavailable: conformancev1.MonitoringStatus_MONITORING_STATUS_UNAVAILABLE}
var recordingToProto = map[domain.RecordingStatus]conformancev1.RecordingStatus{domain.RecordingPending: conformancev1.RecordingStatus_RECORDING_STATUS_PENDING, domain.RecordingConfirmed: conformancev1.RecordingStatus_RECORDING_STATUS_CONFIRMED, domain.RecordingDegraded: conformancev1.RecordingStatus_RECORDING_STATUS_DEGRADED}
var violationToProto = map[domain.ViolationType]conformancev1.ViolationType{domain.ViolationLateral: conformancev1.ViolationType_VIOLATION_TYPE_LATERAL_DEVIATION, domain.ViolationVertical: conformancev1.ViolationType_VIOLATION_TYPE_ALTITUDE_DEVIATION, domain.ViolationTemporal: conformancev1.ViolationType_VIOLATION_TYPE_TEMPORAL_DEVIATION, domain.ViolationTelemetryLoss: conformancev1.ViolationType_VIOLATION_TYPE_TELEMETRY_LOSS}
var phaseToProto = map[domain.IncidentPhase]conformancev1.IncidentPhase{domain.IncidentClear: conformancev1.IncidentPhase_INCIDENT_PHASE_CLEAR, domain.IncidentSuspected: conformancev1.IncidentPhase_INCIDENT_PHASE_SUSPECTED, domain.IncidentOpen: conformancev1.IncidentPhase_INCIDENT_PHASE_OPEN, domain.IncidentRecovering: conformancev1.IncidentPhase_INCIDENT_PHASE_RECOVERING}
