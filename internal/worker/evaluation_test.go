// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/aero-arc/aero-arc-conformance/internal/domain"
	"github.com/aero-arc/aero-arc-conformance/internal/evaluator"
	postgresstore "github.com/aero-arc/aero-arc-conformance/internal/store/postgres"
	telemetryinflux "github.com/aero-arc/aero-arc-conformance/internal/telemetry/influx"
)

func TestWorkerCommitsOnlySuffixAfterDurableCheckpoint(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	claim := testClaim(now)
	checkpointObservation := testObservation(now.Add(-5*time.Second), "frame-1", 1)
	engine := testEvaluator(t)
	first, err := engine.EvaluateBatch(claim.Assignment, []domain.Observation{checkpointObservation}, domain.EvaluatorState{})
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{checkpoint: postgresstore.ReplayCheckpoint{EvaluationRevision: 1, StateThroughAt: checkpointObservation.ObservedAt, WALID: checkpointObservation.WALID, WALSequence: 1, FrameID: checkpointObservation.FrameID, State: first.State}, checkpointFound: true}
	claim.EvaluationRevision = 1
	reader := &fakeReader{result: telemetryinflux.ReadResult{Observations: []domain.Observation{
		checkpointObservation,
		testObservation(checkpointObservation.ObservedAt, "frame-2", 2),
		testObservation(now.Add(-3*time.Second), "frame-3", 3),
	}}}
	w := newTestWorker(t, store, reader, engine, now)
	if err = w.processClaim(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if len(store.commits) != 1 {
		t.Fatalf("commits=%d", len(store.commits))
	}
	if got := store.commits[0].Evaluation.FrameID; got != "frame-3" {
		t.Fatalf("final frame=%q", got)
	}
	if reader.starts[0] != checkpointObservation.ObservedAt.Add(-10*time.Second) {
		t.Fatalf("overlap start=%s", reader.starts[0])
	}
	if reader.ends[0] != now.Add(-2*time.Second) {
		t.Fatalf("settled end=%s", reader.ends[0])
	}
	if !store.commits[0].NextEvaluationAt.Equal(now.Add(time.Second)) {
		t.Fatalf("next=%s", store.commits[0].NextEvaluationAt)
	}
}

func TestWorkerEmptyWindowDefersWithoutAdvancingRevision(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	store := &fakeStore{}
	w := newTestWorker(t, store, &fakeReader{}, testEvaluator(t), now)
	if err := w.processClaim(context.Background(), testClaim(now)); err != nil {
		t.Fatal(err)
	}
	if len(store.commits) != 0 || len(store.deferred) != 1 {
		t.Fatalf("commits=%d deferred=%d", len(store.commits), len(store.deferred))
	}
	if !store.deferred[0].Equal(now.Add(time.Second)) {
		t.Fatalf("deferred=%s", store.deferred[0])
	}
}

func TestWorkerSplitsSaturatedWindowBeforeCommit(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	store := &fakeStore{}
	reader := &fakeReader{saturateOnce: true, result: telemetryinflux.ReadResult{Observations: []domain.Observation{testObservation(now.Add(-3*time.Second), "frame-1", 1)}}}
	w := newTestWorker(t, store, reader, testEvaluator(t), now)
	if err := w.processClaim(context.Background(), testClaim(now)); err != nil {
		t.Fatal(err)
	}
	if len(reader.starts) != 3 {
		t.Fatalf("reads=%d, want initial plus two halves", len(reader.starts))
	}
	if len(store.commits) != 1 {
		t.Fatalf("commits=%d", len(store.commits))
	}
}

func TestWorkerMissingWALIdentityIsTerminalAndNotDeferred(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	store := &fakeStore{claims: []postgresstore.Claim{testClaim(now)}}
	reader := &fakeReader{err: telemetryinflux.ErrWALIdentityUnavailable}
	w := newTestWorker(t, store, reader, testEvaluator(t), now)
	err := w.Poll(context.Background())
	if !errors.Is(err, telemetryinflux.ErrWALIdentityUnavailable) {
		t.Fatalf("error=%v", err)
	}
	if len(store.deferred) != 0 || len(store.commits) != 0 {
		t.Fatalf("deferred=%d commits=%d", len(store.deferred), len(store.commits))
	}
}

func TestWorkerRenewsLeaseDuringSlowRead(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	store := &fakeStore{}
	reader := &fakeReader{delay: 30 * time.Millisecond, result: telemetryinflux.ReadResult{Observations: []domain.Observation{testObservation(now.Add(-3*time.Second), "frame-1", 1)}}}
	w := newTestWorker(t, store, reader, testEvaluator(t), now)
	w.cfg.RenewInterval = 5 * time.Millisecond
	w.cfg.LeaseDuration = 50 * time.Millisecond
	if err := w.processClaim(context.Background(), testClaim(now)); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	renewals := store.renewals
	store.mu.Unlock()
	if renewals == 0 {
		t.Fatal("slow read did not renew lease")
	}
}

func TestWorkerCheckpointRevisionMismatchDefersWithoutReading(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	claim := testClaim(now)
	claim.EvaluationRevision = 2
	store := &fakeStore{checkpoint: postgresstore.ReplayCheckpoint{EvaluationRevision: 1}, checkpointFound: true}
	reader := &fakeReader{}
	w := newTestWorker(t, store, reader, testEvaluator(t), now)
	err := w.processClaim(context.Background(), claim)
	if !errors.Is(err, errCheckpointMismatch) {
		t.Fatalf("error=%v", err)
	}
	if len(reader.starts) != 0 || len(store.deferred) != 1 {
		t.Fatalf("reads=%d deferred=%d", len(reader.starts), len(store.deferred))
	}
}

func TestWorkerStartsEveryClaimBeforeBatchLeaseCanExpire(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	first, second := testClaim(now), testClaim(now)
	second.Assignment.ID = "assignment-2"
	store := &fakeStore{claims: []postgresstore.Claim{first, second}}
	reader := &barrierReader{entered: make(chan struct{}, 2), release: make(chan struct{})}
	w := newTestWorker(t, store, reader, testEvaluator(t), now)
	w.cfg.LeaseDuration = 40 * time.Millisecond
	w.cfg.RenewInterval = 5 * time.Millisecond
	done := make(chan error, 1)
	go func() { done <- w.Poll(context.Background()) }()
	for count := 0; count < 2; count++ {
		select {
		case <-reader.entered:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("claimed batch was processed sequentially; a later lease could expire before renewal")
		}
	}
	time.Sleep(15 * time.Millisecond)
	close(reader.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.commits) != 2 || store.renewalsByAssignment[first.Assignment.ID] == 0 || store.renewalsByAssignment[second.Assignment.ID] == 0 {
		t.Fatalf("commits=%d renewals=%v", len(store.commits), store.renewalsByAssignment)
	}
}

func TestWorkerEvaluatesSettledTailAfterAuthorityEnds(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	claim := testClaim(now)
	claim.AuthorityUntil = now.Add(-2 * time.Second)
	claim.Assignment.EffectiveUntil = claim.AuthorityUntil
	claim.Assignment.Volumes[0].EndsAt = claim.AuthorityUntil
	observation := testObservation(claim.AuthorityUntil.Add(-500*time.Millisecond), "final-frame", 9)
	store := &fakeStore{}
	reader := &fakeReader{result: telemetryinflux.ReadResult{Observations: []domain.Observation{observation}}}
	w := newTestWorker(t, store, reader, testEvaluator(t), now)
	if err := w.processClaim(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if len(store.commits) != 1 {
		t.Fatalf("commits=%d", len(store.commits))
	}
	if got := reader.ends[0]; !got.Equal(claim.AuthorityUntil) {
		t.Fatalf("tail end=%s want=%s", got, claim.AuthorityUntil)
	}
}

type fakeStore struct {
	mu                   sync.Mutex
	claims               []postgresstore.Claim
	checkpoint           postgresstore.ReplayCheckpoint
	checkpointFound      bool
	checkpointErr        error
	renewErr             error
	renewals             int
	renewalsByAssignment map[string]int
	deferred             []time.Time
	commits              []postgresstore.EvaluationCommit
}

func (s *fakeStore) ClaimDueAssignmentsWithFinalizationGrace(_ context.Context, _ string, _ time.Duration, grace time.Duration, _ int) ([]postgresstore.Claim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	claims := s.claims
	for index := range claims {
		claims[index].FinalizationGrace = grace
	}
	s.claims = nil
	return claims, nil
}
func (s *fakeStore) RenewAssignmentLease(_ context.Context, claim postgresstore.Claim, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewals++
	if s.renewalsByAssignment == nil {
		s.renewalsByAssignment = map[string]int{}
	}
	s.renewalsByAssignment[claim.Assignment.ID]++
	return s.renewErr
}
func (s *fakeStore) DeferAssignmentEvaluation(_ context.Context, _ postgresstore.Claim, next time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deferred = append(s.deferred, next)
	return nil
}
func (s *fakeStore) GetReplayCheckpoint(context.Context, string, uint64, time.Time) (postgresstore.ReplayCheckpoint, bool, error) {
	return s.checkpoint, s.checkpointFound, s.checkpointErr
}
func (s *fakeStore) CommitEvaluation(_ context.Context, _ postgresstore.Claim, commit postgresstore.EvaluationCommit) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commits = append(s.commits, commit)
	return nil
}

type fakeReader struct {
	mu           sync.Mutex
	result       telemetryinflux.ReadResult
	err          error
	saturateOnce bool
	delay        time.Duration
	starts       []time.Time
	ends         []time.Time
}

type barrierReader struct {
	entered chan struct{}
	release chan struct{}
}

func (r *barrierReader) ReadPositions(ctx context.Context, _ []string, _, end time.Time) (telemetryinflux.ReadResult, error) {
	r.entered <- struct{}{}
	select {
	case <-ctx.Done():
		return telemetryinflux.ReadResult{}, ctx.Err()
	case <-r.release:
	}
	return telemetryinflux.ReadResult{Observations: []domain.Observation{testObservation(end.Add(-time.Second), "frame-1", 1)}}, nil
}

func (r *fakeReader) ReadPositions(ctx context.Context, _ []string, start, end time.Time) (telemetryinflux.ReadResult, error) {
	r.mu.Lock()
	r.starts = append(r.starts, start)
	r.ends = append(r.ends, end)
	saturate := r.saturateOnce
	r.saturateOnce = false
	delay, result, err := r.delay, r.result, r.err
	r.mu.Unlock()
	if delay > 0 {
		select {
		case <-ctx.Done():
			return telemetryinflux.ReadResult{}, ctx.Err()
		case <-time.After(delay):
		}
	}
	if saturate {
		return telemetryinflux.ReadResult{}, telemetryinflux.ErrWindowSaturated
	}
	return result, err
}

func newTestWorker(t *testing.T, store assignmentStore, reader telemetryReader, engine batchEvaluator, now time.Time) *Worker {
	t.Helper()
	w, err := New(store, reader, engine, Config{WorkerID: "worker-1", PollInterval: time.Second, LeaseDuration: time.Minute, RenewInterval: 20 * time.Second, SettleDelay: 2 * time.Second, OverlapWindow: 10 * time.Second, ClaimBatchSize: 10}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	w.now = func() time.Time { return now }
	return w
}

func testEvaluator(t *testing.T) *evaluator.Evaluator {
	t.Helper()
	engine, err := evaluator.New(evaluator.Policy{Version: "standard-v1", HorizontalToleranceM: 5, VerticalToleranceM: 3, OpenAfterSamples: 2, RecoverAfterSamples: 2, TelemetryFreshness: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func testClaim(now time.Time) postgresstore.Claim {
	assignment := domain.Assignment{ID: "assignment-1", Generation: 1, AircraftID: "aircraft-1", AgentID: "agent-1", FlightID: "flight-1", IntentID: "intent-1", IntentVersion: 1, PolicyVersion: "standard-v1", EffectiveFrom: now.Add(-time.Minute), EffectiveUntil: now.Add(time.Hour), Volumes: []domain.Volume{{ID: "volume-1", Polygon: []domain.Point{{Latitude: 35, Longitude: -97.01}, {Latitude: 35.01, Longitude: -97.01}, {Latitude: 35.01, Longitude: -97}, {Latitude: 35, Longitude: -97}}, AltitudeLowerM: 80, AltitudeUpperM: 120, AltitudeReference: domain.AltitudeMSL, StartsAt: now.Add(-time.Minute), EndsAt: now.Add(time.Hour)}}}
	return postgresstore.Claim{Assignment: assignment, AuthorityFrom: assignment.EffectiveFrom, AuthorityUntil: assignment.EffectiveUntil, WorkerID: "worker-1", LeaseGeneration: 1, LeaseUntil: now.Add(time.Minute)}
}

func testObservation(at time.Time, frame string, sequence uint64) domain.Observation {
	return domain.Observation{FrameID: frame, AgentID: "agent-1", WALID: "wal-1", WALSequence: sequence, AircraftID: "aircraft-1", FlightID: "flight-1", IntentID: "intent-1", IntentVersion: 1, Latitude: 35.005, Longitude: -97.005, AltitudeM: 100, AltitudeKnown: true, AltitudeReference: domain.AltitudeMSL, ObservedAt: at}
}
