// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package worker coordinates bounded telemetry reads, deterministic evaluation,
// and fenced durable commits for live assignment generations.
package worker

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
	telemetryinflux "github.com/aero-arc/aero-arc-conformance/internal/telemetry/influx"
)

var errCheckpointMismatch = errors.New("durable checkpoint does not match claimed evaluation revision")

type assignmentStore interface {
	ClaimDueAssignmentsWithFinalizationGrace(context.Context, string, time.Duration, time.Duration, int) ([]postgresstore.Claim, error)
	RenewAssignmentLease(context.Context, postgresstore.Claim, time.Duration) error
	DeferAssignmentEvaluation(context.Context, postgresstore.Claim, time.Time) error
	GetReplayCheckpoint(context.Context, string, uint64, time.Time) (postgresstore.ReplayCheckpoint, bool, error)
	CommitEvaluation(context.Context, postgresstore.Claim, postgresstore.EvaluationCommit) error
}

type telemetryReader interface {
	ReadPositions(context.Context, []string, time.Time, time.Time) (telemetryinflux.ReadResult, error)
}

type batchEvaluator interface {
	EvaluateBatch(domain.Assignment, []domain.Observation, domain.EvaluatorState) (domain.Evaluation, error)
}

// Config defines the evaluator worker's independent claim, renewal, query,
// settle, overlap, and scheduling cadences.
type Config struct {
	WorkerID       string
	PollInterval   time.Duration
	LeaseDuration  time.Duration
	RenewInterval  time.Duration
	SettleDelay    time.Duration
	OverlapWindow  time.Duration
	ClaimBatchSize int
}

// Validate checks that worker timing and ownership settings are safe.
//
// Returns:
//   - error: reports missing identity, non-positive timing or batch settings,
//     negative settle delay, or a renewal interval that cannot precede expiry.
func (c Config) Validate() error {
	if c.WorkerID == "" || c.PollInterval <= 0 || c.LeaseDuration <= 0 || c.RenewInterval <= 0 || c.RenewInterval >= c.LeaseDuration || c.SettleDelay < 0 || c.OverlapWindow <= 0 || c.ClaimBatchSize < 1 {
		return fmt.Errorf("evaluation worker configuration is invalid")
	}
	return nil
}

// Worker owns the live claim/read/evaluate/commit loop. Historical generations
// are never claimed or published by this worker.
type Worker struct {
	store     assignmentStore
	reader    telemetryReader
	evaluator batchEvaluator
	cfg       Config
	log       *slog.Logger
	now       func() time.Time
}

// New constructs a live evaluator worker without claiming assignment authority.
//
// Parameters:
//   - store: provides PostgreSQL-clock claims, renewals, checkpoints, and atomic commits.
//   - reader: provides bounded, canonically ordered Influx telemetry windows.
//   - engine: applies deterministic evaluator policy to an ordered suffix.
//   - cfg: separates polling, settling, overlap, lease, and renewal timing.
//   - log: receives dependency, rejection, and lease-loss diagnostics.
//
// Returns:
//   - worker: is ready to run and only processes active/ending claims returned by store.
//   - error: reports nil dependencies or invalid timing and ownership configuration.
func New(store assignmentStore, reader telemetryReader, engine batchEvaluator, cfg Config, log *slog.Logger) (*Worker, error) {
	if store == nil || reader == nil || engine == nil || log == nil {
		return nil, fmt.Errorf("evaluation worker dependencies are required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Worker{store: store, reader: reader, evaluator: engine, cfg: cfg, log: log, now: time.Now}, nil
}

// Run claims due live assignments immediately and on every poll tick until
// cancellation. Ordinary dependency and lease failures are logged and retried;
// the forward-contract WAL identity error is terminal and returned loudly.
//
// Parameters:
//   - ctx: bounds claims, reads, renewals, commits, and the worker lifetime.
//
// Returns:
//   - error: reports cancellation only as nil, and returns the terminal missing
//     WAL identity contract error so the service cannot appear operational while
//     silently ordering telemetry by an unsafe sequence-only fallback.
func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if err := w.Poll(ctx); err != nil {
			if errors.Is(err, telemetryinflux.ErrWALIdentityUnavailable) {
				return err
			}
			if ctx.Err() != nil {
				return nil
			}
			w.log.Error("evaluation poll failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Poll performs one bounded claim batch. Each successful live evaluation is
// committed atomically with its checkpoint, summary, incidents, transitions,
// and Registry outbox handoff; empty or incomplete evidence only reschedules.
//
// Parameters:
//   - ctx: bounds the claim batch, telemetry queries, renewals, and commits.
//
// Returns:
//   - error: joins per-assignment dependency, evaluation, lease, or persistence
//     failures. Missing WAL identity is returned immediately and no affected
//     checkpoint or evaluation revision is advanced.
func (w *Worker) Poll(ctx context.Context) error {
	finalizationGrace := w.cfg.SettleDelay + w.cfg.PollInterval
	claims, err := w.store.ClaimDueAssignmentsWithFinalizationGrace(ctx, w.cfg.WorkerID, w.cfg.LeaseDuration, finalizationGrace, w.cfg.ClaimBatchSize)
	if err != nil {
		return err
	}
	batchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type claimResult struct {
		claim postgresstore.Claim
		err   error
	}
	results := make(chan claimResult, len(claims))
	var group sync.WaitGroup
	for _, claim := range claims {
		claim := claim
		group.Add(1)
		go func() {
			defer group.Done()
			claimErr := w.processClaim(batchCtx, claim)
			if errors.Is(claimErr, telemetryinflux.ErrWALIdentityUnavailable) {
				cancel()
			}
			results <- claimResult{claim: claim, err: claimErr}
		}()
	}
	group.Wait()
	close(results)
	var pollErr error
	for result := range results {
		if result.err == nil || (errors.Is(result.err, context.Canceled) && errors.Is(batchCtx.Err(), context.Canceled)) {
			continue
		}
		if errors.Is(result.err, telemetryinflux.ErrWALIdentityUnavailable) {
			return result.err
		}
		w.log.Error("assignment evaluation failed", "assignment_id", result.claim.Assignment.ID, "assignment_generation", result.claim.Assignment.Generation, "lease_generation", result.claim.LeaseGeneration, "error", result.err)
		pollErr = errors.Join(pollErr, fmt.Errorf("evaluate assignment %s generation %d: %w", result.claim.Assignment.ID, result.claim.Assignment.Generation, result.err))
	}
	return pollErr
}

func (w *Worker) processClaim(parent context.Context, claim postgresstore.Claim) error {
	ctx, cancel := context.WithCancel(parent)
	renewErr := make(chan error, 1)
	var renewDone sync.WaitGroup
	renewDone.Add(1)
	go func() {
		defer renewDone.Done()
		ticker := time.NewTicker(w.cfg.RenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := w.store.RenewAssignmentLease(ctx, claim, w.cfg.LeaseDuration); err != nil {
					select {
					case renewErr <- err:
					default:
					}
					cancel()
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		renewDone.Wait()
	}()

	now := w.now().UTC()
	end := now.Add(-w.cfg.SettleDelay)
	if claim.AuthorityUntil.Before(end) {
		end = claim.AuthorityUntil
	}
	if !end.After(claim.AuthorityFrom) {
		return w.deferClaim(parent, claim, now.Add(w.cfg.PollInterval))
	}
	checkpoint, found, err := w.store.GetReplayCheckpoint(ctx, claim.Assignment.ID, claim.Assignment.Generation, end)
	if err != nil {
		return w.deferAfterError(parent, claim, now, err)
	}
	if (claim.EvaluationRevision == 0 && found) || (claim.EvaluationRevision > 0 && (!found || checkpoint.EvaluationRevision != claim.EvaluationRevision)) {
		return w.deferAfterError(parent, claim, now, errCheckpointMismatch)
	}
	start := claim.AuthorityFrom
	previous := domain.EvaluatorState{}
	if found {
		start = checkpoint.StateThroughAt.Add(-w.cfg.OverlapWindow)
		if start.Before(claim.AuthorityFrom) {
			start = claim.AuthorityFrom
		}
		previous = checkpoint.State
	}
	result, err := w.readComplete(ctx, claim.Assignment.AircraftID, start, end)
	if err != nil {
		return w.deferAfterError(parent, claim, now, err)
	}
	for _, rejection := range result.Rejections {
		w.log.Warn("telemetry row rejected", "assignment_id", claim.Assignment.ID, "frame_id", rejection.FrameID, "reason", rejection.Reason)
	}
	observations := evaluableSuffix(result.Observations, claim, checkpoint, found, w.log)
	if len(observations) == 0 {
		return w.deferClaim(parent, claim, now.Add(w.cfg.PollInterval))
	}
	evaluationAssignment := claim.Assignment
	evaluationAssignment.EffectiveFrom = claim.AuthorityFrom
	evaluationAssignment.EffectiveUntil = claim.AuthorityUntil
	evaluation, err := w.evaluator.EvaluateBatch(evaluationAssignment, observations, previous)
	if err != nil {
		return w.deferAfterError(parent, claim, now, err)
	}
	if err = firstRenewalError(renewErr); err != nil {
		return err
	}
	if err = w.store.CommitEvaluation(ctx, claim, postgresstore.EvaluationCommit{Evaluation: evaluation, NextEvaluationAt: now.Add(w.cfg.PollInterval)}); err != nil {
		return err
	}
	return nil
}

func (w *Worker) deferAfterError(ctx context.Context, claim postgresstore.Claim, now time.Time, cause error) error {
	if errors.Is(cause, telemetryinflux.ErrWALIdentityUnavailable) || errors.Is(cause, postgresstore.ErrLeaseLost) || errors.Is(cause, context.Canceled) {
		return cause
	}
	if err := w.deferClaim(ctx, claim, now.Add(w.cfg.PollInterval)); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (w *Worker) deferClaim(ctx context.Context, claim postgresstore.Claim, next time.Time) error {
	return w.store.DeferAssignmentEvaluation(ctx, claim, next)
}

func (w *Worker) readComplete(ctx context.Context, aircraftID string, start, end time.Time) (telemetryinflux.ReadResult, error) {
	result, err := w.reader.ReadPositions(ctx, []string{aircraftID}, start, end)
	if !errors.Is(err, telemetryinflux.ErrWindowSaturated) {
		return result, err
	}
	middle := start.Add(end.Sub(start) / 2)
	if !middle.After(start) || !end.After(middle) {
		return telemetryinflux.ReadResult{}, err
	}
	left, leftErr := w.readComplete(ctx, aircraftID, start, middle)
	if leftErr != nil {
		return telemetryinflux.ReadResult{}, leftErr
	}
	right, rightErr := w.readComplete(ctx, aircraftID, middle, end)
	if rightErr != nil {
		return telemetryinflux.ReadResult{}, rightErr
	}
	left.Observations = append(left.Observations, right.Observations...)
	left.Rejections = append(left.Rejections, right.Rejections...)
	seenFrames := make(map[string]struct{}, len(left.Observations))
	deduplicated := left.Observations[:0]
	for _, observation := range left.Observations {
		if _, seen := seenFrames[observation.FrameID]; seen {
			continue
		}
		seenFrames[observation.FrameID] = struct{}{}
		deduplicated = append(deduplicated, observation)
	}
	left.Observations = deduplicated
	return left, nil
}

func evaluableSuffix(observations []domain.Observation, claim postgresstore.Claim, checkpoint postgresstore.ReplayCheckpoint, found bool, log *slog.Logger) []domain.Observation {
	result := make([]domain.Observation, 0, len(observations))
	for _, observation := range observations {
		if observation.AgentID != claim.Assignment.AgentID || observation.FlightID != claim.Assignment.FlightID || observation.IntentID != claim.Assignment.IntentID || observation.IntentVersion != claim.Assignment.IntentVersion {
			log.Warn("telemetry attribution rejected", "assignment_id", claim.Assignment.ID, "frame_id", observation.FrameID)
			continue
		}
		if observation.ObservedAt.Before(claim.AuthorityFrom) || !observation.ObservedAt.Before(claim.AuthorityUntil) {
			continue
		}
		if found && !afterCheckpoint(observation, checkpoint) {
			continue
		}
		result = append(result, observation)
	}
	sort.SliceStable(result, func(i, j int) bool { return observationBefore(result[i], result[j]) })
	return result
}

func afterCheckpoint(observation domain.Observation, checkpoint postgresstore.ReplayCheckpoint) bool {
	if !observation.ObservedAt.Equal(checkpoint.StateThroughAt) {
		return observation.ObservedAt.After(checkpoint.StateThroughAt)
	}
	if observation.WALID != checkpoint.WALID {
		return observation.WALID > checkpoint.WALID
	}
	if observation.WALSequence != checkpoint.WALSequence {
		return observation.WALSequence > checkpoint.WALSequence
	}
	return observation.FrameID > checkpoint.FrameID
}

func observationBefore(a, b domain.Observation) bool {
	if !a.ObservedAt.Equal(b.ObservedAt) {
		return a.ObservedAt.Before(b.ObservedAt)
	}
	if a.AgentID != b.AgentID {
		return a.AgentID < b.AgentID
	}
	if a.WALID != b.WALID {
		return a.WALID < b.WALID
	}
	if a.WALSequence != b.WALSequence {
		return a.WALSequence < b.WALSequence
	}
	return a.FrameID < b.FrameID
}

func firstRenewalError(ch <-chan error) error {
	select {
	case err := <-ch:
		return err
	default:
		return nil
	}
}
