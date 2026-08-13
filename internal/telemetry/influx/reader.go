// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package influx reads bounded, overlapping position windows from InfluxDB 3.
package influx

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	influxdb3 "github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/aero-arc/aero-arc-conformance/internal/domain"
)

const (
	measurement     = "aircraft_telemetry"
	positionMessage = "global_position_int"
	schemaVersion   = "1"
)

var ErrWALIdentityUnavailable = errors.New("telemetry WAL identity is unavailable; deploy the Agent and Relay wal_id contract before enabling Conformance")

type QueryRunner interface {
	Query(context.Context, string, map[string]any) ([]map[string]any, error)
	Close() error
}

type clientRunner struct{ client *influxdb3.Client }

func (r *clientRunner) Query(ctx context.Context, query string, params map[string]any) ([]map[string]any, error) {
	iterator, err := r.client.QueryWithParameters(ctx, query, params)
	if err != nil {
		return nil, err
	}
	rows := []map[string]any{}
	for iterator.Next() {
		rows = append(rows, iterator.Value())
	}
	return rows, iterator.Err()
}
func (r *clientRunner) Close() error { return r.client.Close() }

type Reader struct {
	runner    QueryRunner
	chunkSize int
	maxRows   int
}

func New(host, token, database string, chunkSize, maxRows int) (*Reader, error) {
	client, err := influxdb3.New(influxdb3.ClientConfig{Host: host, Token: token, Database: database})
	if err != nil {
		return nil, fmt.Errorf("create influx client: %w", err)
	}
	return NewWithRunner(&clientRunner{client: client}, chunkSize, maxRows)
}
func NewWithRunner(runner QueryRunner, chunkSize, maxRows int) (*Reader, error) {
	if runner == nil || chunkSize < 1 || maxRows < 1 {
		return nil, fmt.Errorf("reader runner, chunk size, and max rows are required")
	}
	return &Reader{runner: runner, chunkSize: chunkSize, maxRows: maxRows}, nil
}
func (r *Reader) Close() error { return r.runner.Close() }

type ReadResult struct {
	Observations []domain.Observation
	Rejections   []RowRejection
}

type RowRejection struct {
	FrameID string
	Reason  string
}

// ReadPositions returns a deterministic logical set for [start,end). It rejects
// a saturated chunk rather than silently truncating; the worker must split the
// time window and retry before advancing a checkpoint.
func (r *Reader) ReadPositions(ctx context.Context, aircraftIDs []string, start, end time.Time) (ReadResult, error) {
	ids := uniqueIDs(aircraftIDs)
	if len(ids) == 0 {
		return ReadResult{}, nil
	}
	if start.IsZero() || !end.After(start) {
		return ReadResult{}, fmt.Errorf("invalid telemetry window")
	}
	all := make([]domain.Observation, 0)
	rejected := make([]RowRejection, 0)
	for offset := 0; offset < len(ids); offset += r.chunkSize {
		last := offset + r.chunkSize
		if last > len(ids) {
			last = len(ids)
		}
		rows, err := r.queryChunk(ctx, ids[offset:last], start, end)
		if err != nil {
			if missingColumn(err, "aircraft_id") {
				continue
			}
			if missingColumn(err, "wal_id") {
				return ReadResult{}, ErrWALIdentityUnavailable
			}
			return ReadResult{}, err
		}
		if len(rows) >= r.maxRows {
			return ReadResult{}, fmt.Errorf("telemetry window overflow: chunk returned limit %d; split the window", r.maxRows)
		}
		for _, row := range rows {
			o, err := decodeObservation(row)
			if err != nil {
				if errors.Is(err, ErrWALIdentityUnavailable) {
					return ReadResult{}, err
				}
				rejected = append(rejected, RowRejection{FrameID: stringValue(row["frame_id"]), Reason: err.Error()})
				continue
			}
			all = append(all, o)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		a, b := all[i], all[j]
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
	})
	seen := make(map[string]struct{}, len(all))
	dedup := all[:0]
	for _, o := range all {
		if _, ok := seen[o.FrameID]; ok {
			continue
		}
		seen[o.FrameID] = struct{}{}
		dedup = append(dedup, o)
	}
	return ReadResult{Observations: dedup, Rejections: rejected}, nil
}

func (r *Reader) queryChunk(ctx context.Context, ids []string, start, end time.Time) ([]map[string]any, error) {
	params := map[string]any{"start": start.UTC().Format(time.RFC3339Nano), "end": end.UTC().Format(time.RFC3339Nano), "message": positionMessage, "schema": schemaVersion}
	bindings := make([]string, len(ids))
	for i, id := range ids {
		name := fmt.Sprintf("aircraft_%d", i)
		bindings[i] = "$" + name
		params[name] = id
	}
	query := fmt.Sprintf(`SELECT * FROM %q WHERE time >= $start AND time < $end AND message_name = $message AND schema_version = $schema AND aircraft_id IN (%s) ORDER BY time ASC, agent_id ASC, wal_id ASC, wal_sequence ASC, frame_id ASC LIMIT %d`, measurement, strings.Join(bindings, ", "), r.maxRows)
	rows, err := r.runner.Query(ctx, query, params)
	if err != nil {
		return nil, fmt.Errorf("query position telemetry: %w", err)
	}
	return rows, nil
}

func uniqueIDs(values []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func decodeObservation(row map[string]any) (domain.Observation, error) {
	observedAt, ok := row["time"].(time.Time)
	if !ok {
		return domain.Observation{}, fmt.Errorf("time has type %T", row["time"])
	}
	requiredString := func(name string) (string, error) {
		v, ok := row[name].(string)
		if !ok || strings.TrimSpace(v) == "" {
			return "", fmt.Errorf("%s is missing", name)
		}
		return v, nil
	}
	frameID, err := requiredString("frame_id")
	if err != nil {
		return domain.Observation{}, err
	}
	agentID, err := requiredString("agent_id")
	if err != nil {
		return domain.Observation{}, err
	}
	aircraftID, err := requiredString("aircraft_id")
	if err != nil {
		return domain.Observation{}, err
	}
	walID, err := requiredString("wal_id")
	if err != nil {
		return domain.Observation{}, fmt.Errorf("%w: %v", ErrWALIdentityUnavailable, err)
	}
	sequence, ok := uintValue(row["wal_sequence"])
	if !ok {
		return domain.Observation{}, fmt.Errorf("wal_sequence invalid")
	}
	lat, ok := floatValue(row["latitude_deg"])
	if !ok || lat < -90 || lat > 90 {
		return domain.Observation{}, fmt.Errorf("latitude invalid")
	}
	lon, ok := floatValue(row["longitude_deg"])
	if !ok || lon < -180 || lon > 180 {
		return domain.Observation{}, fmt.Errorf("longitude invalid")
	}
	o := domain.Observation{FrameID: frameID, AgentID: agentID, WALID: walID, WALSequence: sequence, AircraftID: aircraftID, FlightID: stringValue(row["flight_id"]), IntentID: stringValue(row["intent_id"]), IntentVersion: uint32Value(row["intent_version"]), Latitude: lat, Longitude: lon, ObservedAt: observedAt.UTC()}
	if alt, ok := floatValue(row["altitude_msl_m"]); ok {
		o.AltitudeM = alt
		o.AltitudeKnown = true
		o.AltitudeReference = domain.AltitudeMSL
	}
	return o, nil
}

func missingColumn(err error, column string) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "column") && strings.Contains(message, strings.ToLower(column)) && (strings.Contains(message, "not found") || strings.Contains(message, "unknown") || strings.Contains(message, "does not exist"))
}

func stringValue(v any) string { s, _ := v.(string); return s }
func floatValue(v any) (float64, bool) {
	var f float64
	switch n := v.(type) {
	case float64:
		f = n
	case float32:
		f = float64(n)
	case int64:
		f = float64(n)
	case uint64:
		f = float64(n)
	default:
		return 0, false
	}
	return f, !math.IsNaN(f) && !math.IsInf(f, 0)
}
func uintValue(v any) (uint64, bool) {
	switch n := v.(type) {
	case uint64:
		return n, true
	case int64:
		if n >= 0 {
			return uint64(n), true
		}
	case float64:
		if n >= 0 && n == math.Trunc(n) && n <= math.MaxUint64 {
			return uint64(n), true
		}
	case string:
		u, e := strconv.ParseUint(n, 10, 64)
		return u, e == nil
	}
	return 0, false
}
func uint32Value(v any) uint32 {
	u, ok := uintValue(v)
	if !ok || u > math.MaxUint32 {
		return 0
	}
	return uint32(u)
}
