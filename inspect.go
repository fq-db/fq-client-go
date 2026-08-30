//nolint:dupl // it's ok
package fq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type InspectSection string

const (
	InspectSectionSummary InspectSection = ""
	InspectSectionAll     InspectSection = "ALL"
	InspectSectionWAL     InspectSection = "WAL"
	InspectSectionDump    InspectSection = "DUMP"
	InspectSectionRepl    InspectSection = "REPL"
	InspectSectionEngine  InspectSection = "ENGINE"
	InspectSectionStreams InspectSection = "STREAMS"
)

type InspectReport struct {
	Section     InspectSection   `json:"section"`
	TS          int64            `json:"ts"`
	Instance    *InstanceInfo    `json:"instance,omitempty"`
	Persistence *PersistenceInfo `json:"persistence,omitempty"`
	WAL         *WALInfo         `json:"wal,omitempty"`
	Dump        *DumpInfo        `json:"dump,omitempty"`
	Repl        *ReplInfo        `json:"repl,omitempty"`
	Engine      *EngineInfo      `json:"engine,omitempty"`
	Streams     *StreamsInfo     `json:"streams,omitempty"`
	Warnings    []InspectWarning `json:"warnings,omitempty"`
}

type InstanceInfo struct {
	Version     string  `json:"version"`
	Commit      string  `json:"commit"`
	BuildDate   string  `json:"build_date"`
	GoVersion   string  `json:"go_version"`
	Platform    string  `json:"platform"`
	UptimeSec   float64 `json:"uptime_sec"`
	PID         int     `json:"pid"`
	Role        string  `json:"role"`
	ReplicaID   *string `json:"replica_id"`
	ListenAddr  string  `json:"listen_addr"`
	Connections int     `json:"connections"`
}

type PersistenceInfo struct {
	Mode       string  `json:"mode"`
	SyncCommit *string `json:"sync_commit"`
}

type WALInfo struct {
	Enabled             bool     `json:"enabled"`
	SyncCommit          *string  `json:"sync_commit"`
	DataDirectory       *string  `json:"data_directory"`
	QueueDepth          *int     `json:"queue_depth"`
	QueueCapacity       *int     `json:"queue_capacity"`
	LSNAssigned         uint64   `json:"lsn_assigned"`
	LSNFlushed          *uint64  `json:"lsn_flushed"`
	SegmentPath         *string  `json:"segment_path"`
	SegmentSizeBytes    *int     `json:"segment_size_bytes"`
	LastFlushAt         *int64   `json:"last_flush_at"`
	LastFlushDurationMs *float64 `json:"last_flush_duration_ms"`
	FlushAvgDurationMs  *float64 `json:"flush_avg_duration_ms"`
	FlushTotal          *float64 `json:"flush_total"`
}

type DumpInfo struct {
	Enabled              bool     `json:"enabled"`
	Directory            *string  `json:"directory"`
	IntervalSec          *float64 `json:"interval_sec"`
	LastDumpAt           *int64   `json:"last_dump_at"`
	LastDumpDurationMs   *float64 `json:"last_dump_duration_ms"`
	LastDumpError        *string  `json:"last_dump_error"`
	LastDumpTx           *uint64  `json:"last_dump_tx"`
	CurrentDumpPath      *string  `json:"current_dump_path"`
	CurrentDumpSizeBytes *int64   `json:"current_dump_size_bytes"`
	NextDumpAt           *int64   `json:"next_dump_at"`
}

type ReplicaInfo struct {
	ReplicaID       string  `json:"replica_id"`
	LastSegmentName string  `json:"last_segment_name"`
	LastAppliedLSN  uint64  `json:"last_applied_lsn"`
	LagLSN          uint64  `json:"lag_lsn"`
	LagSec          float64 `json:"lag_sec"`
	Stale           bool    `json:"stale"`
	UpdatedAt       int64   `json:"updated_at"`
}

type SlaveInfo struct {
	MasterAddress     string `json:"master_address"`
	Connected         bool   `json:"connected"`
	LastSegmentName   string `json:"last_segment_name"`
	LastAppliedLSN    uint64 `json:"last_applied_lsn"`
	ConsecutiveErrors int    `json:"consecutive_errors"`
	ReconnectTotal    uint64 `json:"reconnect_total"`
	LastReconnectAt   *int64 `json:"last_reconnect_at"`
	UpdatedAt         int64  `json:"updated_at"`
}

type ReplInfo struct {
	Role          string        `json:"role"`
	KnownReplicas int           `json:"known_replicas,omitempty"`
	Replicas      []ReplicaInfo `json:"replicas,omitempty"`
	Slave         *SlaveInfo    `json:"slave,omitempty"`
	Truncated     bool          `json:"truncated,omitempty"`
}

type PartitionStat struct {
	Index            int `json:"index"`
	Counters         int `json:"counters"`
	SlidingWindows   int `json:"sliding_windows"`
	TokenBuckets     int `json:"token_buckets"`
	Quotas           int `json:"quotas"`
	QuotaAllocations int `json:"quota_allocations"`
}

type EngineInfo struct {
	Partitions       int             `json:"partitions"`
	Counters         int             `json:"counters"`
	SlidingWindows   int             `json:"sliding_windows"`
	TokenBuckets     int             `json:"token_buckets"`
	Quotas           int             `json:"quotas"`
	QuotaAllocations int             `json:"quota_allocations"`
	KeyIndexEnabled  bool            `json:"key_index_enabled"`
	PerPartition     []PartitionStat `json:"per_partition,omitempty"`
}

type SubscriberInfo struct {
	Prefix   *string `json:"prefix"`
	QueueLen int     `json:"queue_len"`
	QueueCap int     `json:"queue_cap"`
	Dropped  uint64  `json:"dropped"`
}

type StreamsInfo struct {
	LimitSubscribers        []SubscriberInfo `json:"limit_subscribers"`
	QuotaSubscribers        []SubscriberInfo `json:"quota_subscribers"`
	LimitEventsDroppedTotal uint64           `json:"limit_events_dropped_total"`
	QuotaEventsDroppedTotal uint64           `json:"quota_events_dropped_total"`
}

type InspectWarning struct {
	Code     string         `json:"code"`
	Severity string         `json:"severity"`
	Message  string         `json:"message"`
	Details  map[string]any `json:"details,omitempty"`
}

func (c *Client) Inspect(ctx context.Context, section InspectSection) (InspectReport, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeInspectCommand(buf, section)

	conn, err := c.pool.GetConnection()
	if err != nil {
		return InspectReport{}, fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	resp, err := sendChunkedWithReconnect(ctx, conn, buf.Bytes())
	if err != nil {
		return InspectReport{}, fmt.Errorf("send: %w", err)
	}

	result, err := parseInspectResponse(resp)
	if err != nil {
		return InspectReport{}, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return result.report, nil
	case ResponseStatusError:
		return InspectReport{}, result.err
	default:
		return InspectReport{}, ErrUnknownRespStatus
	}
}

func writeInspectCommand(buf *bytes.Buffer, section InspectSection) {
	buf.WriteString(CommandInspect)
	if section != "" {
		buf.WriteByte(' ')
		buf.WriteString(string(section))
	}
}

func sendChunkedWithReconnect(ctx context.Context, conn *TCPClient, data []byte) ([]byte, error) {
	resp, err := conn.SendChunked(ctx, data)
	if err == nil {
		return resp, nil
	}
	if !errors.Is(err, ErrConnClosed) {
		return nil, err
	}

	lastErr := err
	for attempt := 1; attempt <= maxReconnectAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if err := conn.Reconnect(); err != nil {
			lastErr = fmt.Errorf("reconnect attempt %d: %w", attempt, err)

			continue
		}

		resp, err = conn.SendChunked(ctx, data)
		if err == nil {
			return resp, nil
		}
		if !errors.Is(err, ErrConnClosed) {
			return nil, err
		}

		lastErr = err
	}

	return nil, fmt.Errorf("reconnect attempts exceeded: %w", lastErr)
}

type inspectResponseStruct struct {
	status ResponseStatus
	report InspectReport
	err    error
}

func parseInspectResponse(resp []byte) (inspectResponseStruct, error) {
	idx := bytes.IndexByte(resp, respDelimiter)
	if idx == -1 {
		return inspectResponseStruct{}, ErrCorruptedResponse
	}

	status := string(resp[:idx])
	data := resp[idx+1:]

	switch status {
	case frameStatusOK:
		var report InspectReport
		if err := json.Unmarshal(data, &report); err != nil {
			return inspectResponseStruct{}, fmt.Errorf("%w: %w", ErrCorruptedResponse, err)
		}

		return inspectResponseStruct{status: ResponseStatusSuccess, report: report}, nil
	case frameStatusError:
		return inspectResponseStruct{status: ResponseStatusError, err: errors.New(string(data))}, nil
	default:
		return inspectResponseStruct{}, ErrUnknownRespStatus
	}
}
