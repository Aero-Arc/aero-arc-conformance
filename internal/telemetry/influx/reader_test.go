// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package influx

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeRunner struct {
	rows   []map[string]any
	query  string
	params map[string]any
}

func (f *fakeRunner) Query(_ context.Context, q string, p map[string]any) ([]map[string]any, error) {
	f.query = q
	f.params = p
	return f.rows, nil
}
func (f *fakeRunner) Close() error { return nil }

func TestReadPositionsOrdersDeduplicatesAndIsolatesMalformedRows(t *testing.T) {
	now := time.Now().UTC()
	valid := func(frame string, seq uint64, at time.Time) map[string]any {
		return map[string]any{"time": at, "frame_id": frame, "agent_id": "agent-1", "aircraft_id": "aircraft-1", "wal_id": "wal-1", "wal_sequence": seq, "latitude_deg": 35.0, "longitude_deg": -97.0}
	}
	runner := &fakeRunner{rows: []map[string]any{valid("frame-2", 2, now.Add(time.Second)), valid("frame-1", 1, now), valid("frame-1", 1, now), {"time": now}}}
	reader, err := NewWithRunner(runner, 100, 10)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.ReadPositions(context.Background(), []string{"aircraft-1", "aircraft-1"}, now.Add(-time.Second), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Observations) != 2 || result.Observations[0].FrameID != "frame-1" || len(result.Rejections) != 1 {
		t.Fatalf("result=%#v", result)
	}
	if runner.params["aircraft_0"] != "aircraft-1" {
		t.Fatalf("params=%#v", runner.params)
	}
}

func TestReadPositionsFailsLoudlyWithoutWALIdentity(t *testing.T) {
	now := time.Now().UTC()
	runner := &fakeRunner{rows: []map[string]any{{"time": now, "frame_id": "legacy", "agent_id": "agent", "aircraft_id": "aircraft", "wal_sequence": uint64(1), "latitude_deg": 1.0, "longitude_deg": 1.0}}}
	reader, _ := NewWithRunner(runner, 10, 10)
	if _, err := reader.ReadPositions(context.Background(), []string{"aircraft"}, now.Add(-time.Second), now.Add(time.Second)); !errors.Is(err, ErrWALIdentityUnavailable) {
		t.Fatalf("error = %v", err)
	}
}

func TestReadPositionsRejectsSaturatedWindow(t *testing.T) {
	now := time.Now().UTC()
	runner := &fakeRunner{rows: []map[string]any{{}, {}}}
	reader, _ := NewWithRunner(runner, 10, 2)
	if _, err := reader.ReadPositions(context.Background(), []string{"aircraft-1"}, now, now.Add(time.Second)); err == nil {
		t.Fatal("saturated window was silently accepted")
	}
}

func TestDecodeObservationPreservesAbsentAltitude(t *testing.T) {
	now := time.Now().UTC()
	o, err := decodeObservation(map[string]any{"time": now, "frame_id": "frame", "agent_id": "agent", "aircraft_id": "aircraft", "wal_id": "wal", "wal_sequence": uint64(0), "latitude_deg": 0.0, "longitude_deg": 0.0})
	if err != nil {
		t.Fatal(err)
	}
	if o.AltitudeKnown {
		t.Fatal("absent altitude decoded as known")
	}
}
