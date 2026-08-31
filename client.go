//nolint:dupl // it's ok
package fq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const (
	CommandIncr       = "INCR"
	CommandGet        = "GET"
	CommandDel        = "DEL"
	CommandMDel       = "MDEL"
	CommandRLimit     = "RLIMIT"
	CommandStream     = "STREAM"
	CommandPStream    = "PSTREAM"
	CommandQuota      = "QUOTA"
	CommandQStream    = "QSTREAM"
	CommandQPStream   = "QPSTREAM"
	CommandScan       = "SCAN"
	CommandPScan      = "PSCAN"
	CommandFlushDB    = "FLUSHDB"
	CommandTruncate   = "TRUNCATE"
	CommandMsgSize    = "MSGSIZE"
	CommandInspect    = "INSPECT"
	CommandAuth       = "AUTH"
	RLimitAlgorithmFW = "FW"
	RLimitAlgorithmSW = "SW"
	RLimitAlgorithmTB = "TB"
)

const ScanCursorInitial = "0"

const maxReconnectAttempts = 16

type CappingKey struct {
	Key     string
	Capping uint32
}

type LimitKey struct {
	Key    string
	Window uint32
}

type RateLimitResult struct {
	Allowed    bool
	Current    uint64
	Remaining  uint64
	ResetAfter uint32
}

type LimitEvent struct {
	Key        string
	Window     uint32
	Current    uint64
	ResetAfter uint32
}

type QuotaAcquireResult struct {
	Acquired     bool
	Allocated    uint64
	Used         uint64
	Remaining    uint64
	ExpiresAfter uint32
}

type QuotaClientInfo struct {
	ClientID  string
	Amount    uint64
	ExpiresAt uint32
}

type QuotaInfo struct {
	Limit     uint64
	Used      uint64
	Remaining uint64
	Clients   []QuotaClientInfo
}

type ScanKey struct {
	Key    string
	Window uint32
}

type ScanResult struct {
	Cursor string
	Keys   []ScanKey
}

type QuotaEvent struct {
	Event     string
	Name      string
	ClientID  string
	Amount    uint64
	Used      uint64
	Remaining uint64
	ExpiresAt uint32
}

type Client struct {
	pool *ConnectionPool
}

func New(address string, idleTimeout time.Duration, poolSize int, opts ...Option) (*Client, error) {
	newConnFn := func() (*TCPClient, error) {
		return NewTCPClient(address, 4096, idleTimeout, opts...)
	}

	pool := NewConnectionPool(poolSize, newConnFn)

	conns := make([]*TCPClient, 0, poolSize)
	for i := 0; i < poolSize; i++ {
		c, err := pool.GetConnection()
		if err != nil {
			return nil, err
		}

		conns = append(conns, c)
	}

	for _, conn := range conns {
		pool.ReleaseConnection(conn)
	}

	return &Client{pool: pool}, nil
}

func (c *Client) Close() {
	c.pool.Close()
}

func (c *Client) Incr(ctx context.Context, key CappingKey) (uint64, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeCommand(buf, CommandIncr, key)

	conn, err := c.pool.GetConnection()
	if err != nil {
		return 0, fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	resp, err := sendWithReconnect(ctx, conn, buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("send: %w", err)
	}

	result, err := parseResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return result.value, nil
	case ResponseStatusError:
		return 0, result.err
	default:
		return 0, ErrUnknownRespStatus
	}
}

func (c *Client) Get(ctx context.Context, key CappingKey) (uint64, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeCommand(buf, CommandGet, key)

	conn, err := c.pool.GetConnection()
	if err != nil {
		return 0, fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	resp, err := sendWithReconnect(ctx, conn, buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("send: %w", err)
	}

	result, err := parseResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return result.value, nil
	case ResponseStatusError:
		return 0, result.err
	default:
		return 0, ErrUnknownRespStatus
	}
}

func (c *Client) Del(ctx context.Context, key CappingKey) (bool, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeCommand(buf, CommandDel, key)

	conn, err := c.pool.GetConnection()
	if err != nil {
		return false, fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	resp, err := sendWithReconnect(ctx, conn, buf.Bytes())
	if err != nil {
		return false, fmt.Errorf("send: %w", err)
	}

	result, err := parseResponse(resp)
	if err != nil {
		return false, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		boolResult := result.value == 1

		return boolResult, nil
	case ResponseStatusError:
		return false, result.err
	default:
		return false, ErrUnknownRespStatus
	}
}

func (c *Client) MDel(ctx context.Context, keys []CappingKey) ([]bool, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeMultiCommand(buf, CommandMDel, keys)

	conn, err := c.pool.GetConnection()
	if err != nil {
		return nil, fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	resp, err := sendWithReconnect(ctx, conn, buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("send: %w", err)
	}

	result, err := parseMultiResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return valuesToBools(result.values), nil
	case ResponseStatusError:
		return nil, result.err
	default:
		return nil, ErrUnknownRespStatus
	}
}

func (c *Client) RLimitFixedWindow(ctx context.Context, key LimitKey, limit uint32) (RateLimitResult, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeRLimitWindowCommand(buf, RLimitAlgorithmFW, key, limit)

	return c.rlimit(ctx, buf.Bytes())
}

func (c *Client) RLimitSlidingWindow(ctx context.Context, key LimitKey, limit uint32) (RateLimitResult, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeRLimitWindowCommand(buf, RLimitAlgorithmSW, key, limit)

	return c.rlimit(ctx, buf.Bytes())
}

func (c *Client) RLimitTokenBucket(
	ctx context.Context,
	key LimitKey,
	capacity uint32,
	refillAmount uint32,
) (RateLimitResult, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeRLimitTokenBucketCommand(buf, key, capacity, refillAmount)

	return c.rlimit(ctx, buf.Bytes())
}

func (c *Client) rlimit(ctx context.Context, command []byte) (RateLimitResult, error) {
	conn, err := c.pool.GetConnection()
	if err != nil {
		return RateLimitResult{}, fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	resp, err := sendWithReconnect(ctx, conn, command)
	if err != nil {
		return RateLimitResult{}, fmt.Errorf("send: %w", err)
	}

	result, err := parseRateLimitResponse(resp)
	if err != nil {
		return RateLimitResult{}, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return result.result, nil
	case ResponseStatusError:
		return RateLimitResult{}, result.err
	default:
		return RateLimitResult{}, ErrUnknownRespStatus
	}
}

func (c *Client) Stream(ctx context.Context, handle func(LimitEvent) error) error {
	return c.stream(ctx, []byte(CommandStream), handle)
}

func (c *Client) PStream(ctx context.Context, prefix string, handle func(LimitEvent) error) error {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	buf.WriteString(CommandPStream)
	buf.WriteByte(' ')
	buf.WriteString(prefix)

	return c.stream(ctx, buf.Bytes(), handle)
}

func (c *Client) QuotaAcquire(
	ctx context.Context,
	name string,
	amount uint32,
	clientID string,
	ttl ...uint32,
) (QuotaAcquireResult, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeQuotaAcquireCommand(buf, name, amount, clientID, ttl...)

	return c.quotaAcquire(ctx, buf.Bytes())
}

func (c *Client) QuotaAcquireLease(
	ctx context.Context,
	name string,
	limit uint32,
	amount uint32,
	clientID string,
	ttl ...uint32,
) (QuotaAcquireResult, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeQuotaAcquireLeaseCommand(buf, name, limit, amount, clientID, ttl...)

	return c.quotaAcquire(ctx, buf.Bytes())
}

func (c *Client) QuotaAcquireN(
	ctx context.Context,
	name string,
	clientID string,
	ttl ...uint32,
) (QuotaAcquireResult, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeQuotaAcquireNCommand(buf, name, clientID, ttl...)

	return c.quotaAcquire(ctx, buf.Bytes())
}

func (c *Client) QuotaSet(ctx context.Context, name string, limit uint32) (bool, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeQuotaSetCommand(buf, name, limit)

	return c.quotaBool(ctx, buf.Bytes())
}

func (c *Client) QuotaSetN(ctx context.Context, name string, limit, clients uint32) (bool, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeQuotaSetNCommand(buf, name, limit, clients)

	return c.quotaBool(ctx, buf.Bytes())
}

func (c *Client) quotaAcquire(ctx context.Context, command []byte) (QuotaAcquireResult, error) {
	conn, err := c.pool.GetConnection()
	if err != nil {
		return QuotaAcquireResult{}, fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	resp, err := sendWithReconnect(ctx, conn, command)
	if err != nil {
		return QuotaAcquireResult{}, fmt.Errorf("send: %w", err)
	}

	result, err := parseQuotaAcquireResponse(resp)
	if err != nil {
		return QuotaAcquireResult{}, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return result.result, nil
	case ResponseStatusError:
		return QuotaAcquireResult{}, result.err
	default:
		return QuotaAcquireResult{}, ErrUnknownRespStatus
	}
}

func (c *Client) QuotaRelease(ctx context.Context, name, clientID string) (bool, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeQuotaReleaseCommand(buf, name, clientID)

	return c.quotaBool(ctx, buf.Bytes())
}

func (c *Client) QuotaDelete(ctx context.Context, name string) (bool, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeQuotaDeleteCommand(buf, name)

	return c.quotaBool(ctx, buf.Bytes())
}

func (c *Client) QuotaInfo(ctx context.Context, name string) (QuotaInfo, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeQuotaInfoCommand(buf, name)

	conn, err := c.pool.GetConnection()
	if err != nil {
		return QuotaInfo{}, fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	resp, err := sendWithReconnect(ctx, conn, buf.Bytes())
	if err != nil {
		return QuotaInfo{}, fmt.Errorf("send: %w", err)
	}

	result, err := parseQuotaInfoResponse(resp)
	if err != nil {
		return QuotaInfo{}, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return result.info, nil
	case ResponseStatusError:
		return QuotaInfo{}, result.err
	default:
		return QuotaInfo{}, ErrUnknownRespStatus
	}
}

func (c *Client) Scan(ctx context.Context, cursor string, count uint32) (ScanResult, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeScanCommand(buf, cursor, count)

	return c.scan(ctx, buf.Bytes())
}

func (c *Client) PScan(ctx context.Context, prefix, cursor string, count uint32) (ScanResult, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writePScanCommand(buf, prefix, cursor, count)

	return c.scan(ctx, buf.Bytes())
}

func (c *Client) scan(ctx context.Context, command []byte) (ScanResult, error) {
	conn, err := c.pool.GetConnection()
	if err != nil {
		return ScanResult{}, fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	resp, err := sendWithReconnect(ctx, conn, command)
	if err != nil {
		return ScanResult{}, fmt.Errorf("send: %w", err)
	}

	result, err := parseScanResponse(resp)
	if err != nil {
		return ScanResult{}, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return result.result, nil
	case ResponseStatusError:
		return ScanResult{}, result.err
	default:
		return ScanResult{}, ErrUnknownRespStatus
	}
}

func (c *Client) FlushDB(ctx context.Context) (bool, error) {
	return c.boolCommand(ctx, []byte(CommandFlushDB))
}

func (c *Client) Truncate(ctx context.Context) (bool, error) {
	return c.boolCommand(ctx, []byte(CommandTruncate))
}

func (c *Client) QStream(ctx context.Context, handle func(QuotaEvent) error) error {
	return c.qstream(ctx, []byte(CommandQStream), handle)
}

func (c *Client) QPStream(ctx context.Context, prefix string, handle func(QuotaEvent) error) error {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	buf.WriteString(CommandQPStream)
	buf.WriteByte(' ')
	buf.WriteString(prefix)

	return c.qstream(ctx, buf.Bytes(), handle)
}

func (c *Client) quotaBool(ctx context.Context, command []byte) (bool, error) {
	return c.boolCommand(ctx, command)
}

func (c *Client) boolCommand(ctx context.Context, command []byte) (bool, error) {
	conn, err := c.pool.GetConnection()
	if err != nil {
		return false, fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	resp, err := sendWithReconnect(ctx, conn, command)
	if err != nil {
		return false, fmt.Errorf("send: %w", err)
	}

	result, err := parseResponse(resp)
	if err != nil {
		return false, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return result.value == 1, nil
	case ResponseStatusError:
		return false, result.err
	default:
		return false, ErrUnknownRespStatus
	}
}

func (c *Client) stream(ctx context.Context, command []byte, handle func(LimitEvent) error) error {
	conn, err := c.pool.GetConnection()
	if err != nil {
		return fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	for {
		err = conn.Stream(ctx, command, func(response []byte) error {
			result, err := parseLimitEventResponse(response)
			if err != nil {
				return fmt.Errorf("parse response: %w", err)
			}

			switch result.status {
			case ResponseStatusSuccess:
				return handle(result.event)
			case ResponseStatusError:
				return result.err
			default:
				return ErrUnknownRespStatus
			}
		})
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if !errors.Is(err, ErrConnClosed) {
			return err
		}

		var reconnectErr error
		for attempt := 1; attempt <= maxReconnectAttempts; attempt++ {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}

			if reconnectErr = conn.Reconnect(); reconnectErr == nil {
				break
			}
		}
		if reconnectErr != nil {
			return fmt.Errorf("stream reconnect attempts exceeded: %w", reconnectErr)
		}
	}
}

func (c *Client) qstream(ctx context.Context, command []byte, handle func(QuotaEvent) error) error {
	conn, err := c.pool.GetConnection()
	if err != nil {
		return fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	for {
		err = conn.Stream(ctx, command, func(response []byte) error {
			result, err := parseQuotaEventResponse(response)
			if err != nil {
				return fmt.Errorf("parse response: %w", err)
			}

			switch result.status {
			case ResponseStatusSuccess:
				return handle(result.event)
			case ResponseStatusError:
				return result.err
			default:
				return ErrUnknownRespStatus
			}
		})
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if !errors.Is(err, ErrConnClosed) {
			return err
		}

		var reconnectErr error
		for attempt := 1; attempt <= maxReconnectAttempts; attempt++ {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}

			if reconnectErr = conn.Reconnect(); reconnectErr == nil {
				break
			}
		}
		if reconnectErr != nil {
			return fmt.Errorf("stream reconnect attempts exceeded: %w", reconnectErr)
		}
	}
}

func writeCommand(buf *bytes.Buffer, command string, key CappingKey) {
	cappingStr := strconv.FormatUint(uint64(key.Capping), 10)

	buf.WriteString(command)
	buf.WriteByte(' ')
	buf.WriteString(key.Key)
	buf.WriteByte(' ')
	buf.WriteString(cappingStr)
}

func writeMultiCommand(buf *bytes.Buffer, command string, keys []CappingKey) {
	buf.WriteString(command)
	buf.WriteByte(' ')

	for i, key := range keys {
		cappingStr := strconv.FormatUint(uint64(key.Capping), 10)

		buf.WriteString(key.Key)
		buf.WriteByte(' ')
		buf.WriteString(cappingStr)

		if i < len(keys)-1 {
			buf.WriteByte(' ')
		}
	}
}

func writeRLimitWindowCommand(buf *bytes.Buffer, algorithm string, key LimitKey, limit uint32) {
	limitStr := strconv.FormatUint(uint64(limit), 10)
	windowStr := strconv.FormatUint(uint64(key.Window), 10)

	buf.WriteString(CommandRLimit)
	buf.WriteByte(' ')
	buf.WriteString(algorithm)
	buf.WriteByte(' ')
	buf.WriteString(key.Key)
	buf.WriteByte(' ')
	buf.WriteString(limitStr)
	buf.WriteByte(' ')
	buf.WriteString(windowStr)
}

func writeRLimitTokenBucketCommand(buf *bytes.Buffer, key LimitKey, capacity, refillAmount uint32) {
	capacityStr := strconv.FormatUint(uint64(capacity), 10)
	refillAmountStr := strconv.FormatUint(uint64(refillAmount), 10)
	refillWindowStr := strconv.FormatUint(uint64(key.Window), 10)

	buf.WriteString(CommandRLimit)
	buf.WriteByte(' ')
	buf.WriteString(RLimitAlgorithmTB)
	buf.WriteByte(' ')
	buf.WriteString(key.Key)
	buf.WriteByte(' ')
	buf.WriteString(capacityStr)
	buf.WriteByte(' ')
	buf.WriteString(refillAmountStr)
	buf.WriteByte(' ')
	buf.WriteString(refillWindowStr)
}

func writeQuotaAcquireCommand(
	buf *bytes.Buffer,
	name string,
	amount uint32,
	clientID string,
	ttl ...uint32,
) {
	amountStr := strconv.FormatUint(uint64(amount), 10)

	buf.WriteString(CommandQuota)
	buf.WriteString(" ACQ ")
	buf.WriteString(name)
	buf.WriteByte(' ')
	buf.WriteString(amountStr)
	buf.WriteByte(' ')
	buf.WriteString(clientID)

	if len(ttl) > 0 {
		ttlStr := strconv.FormatUint(uint64(ttl[0]), 10)
		buf.WriteByte(' ')
		buf.WriteString(ttlStr)
	}
}

func writeQuotaAcquireLeaseCommand(
	buf *bytes.Buffer,
	name string,
	limit uint32,
	amount uint32,
	clientID string,
	ttl ...uint32,
) {
	limitStr := strconv.FormatUint(uint64(limit), 10)
	amountStr := strconv.FormatUint(uint64(amount), 10)

	buf.WriteString(CommandQuota)
	buf.WriteString(" ACQL ")
	buf.WriteString(name)
	buf.WriteByte(' ')
	buf.WriteString(limitStr)
	buf.WriteByte(' ')
	buf.WriteString(amountStr)
	buf.WriteByte(' ')
	buf.WriteString(clientID)

	if len(ttl) > 0 {
		ttlStr := strconv.FormatUint(uint64(ttl[0]), 10)
		buf.WriteByte(' ')
		buf.WriteString(ttlStr)
	}
}

func writeQuotaAcquireNCommand(
	buf *bytes.Buffer,
	name string,
	clientID string,
	ttl ...uint32,
) {
	buf.WriteString(CommandQuota)
	buf.WriteString(" ACQN ")
	buf.WriteString(name)
	buf.WriteByte(' ')
	buf.WriteString(clientID)

	if len(ttl) > 0 {
		ttlStr := strconv.FormatUint(uint64(ttl[0]), 10)
		buf.WriteByte(' ')
		buf.WriteString(ttlStr)
	}
}

func writeQuotaSetCommand(buf *bytes.Buffer, name string, limit uint32) {
	limitStr := strconv.FormatUint(uint64(limit), 10)

	buf.WriteString(CommandQuota)
	buf.WriteString(" SET ")
	buf.WriteString(name)
	buf.WriteByte(' ')
	buf.WriteString(limitStr)
}

func writeQuotaSetNCommand(buf *bytes.Buffer, name string, limit, clients uint32) {
	limitStr := strconv.FormatUint(uint64(limit), 10)
	clientsStr := strconv.FormatUint(uint64(clients), 10)

	buf.WriteString(CommandQuota)
	buf.WriteString(" SETN ")
	buf.WriteString(name)
	buf.WriteByte(' ')
	buf.WriteString(limitStr)
	buf.WriteByte(' ')
	buf.WriteString(clientsStr)
}

func writeQuotaReleaseCommand(buf *bytes.Buffer, name, clientID string) {
	buf.WriteString(CommandQuota)
	buf.WriteString(" REL ")
	buf.WriteString(name)
	buf.WriteByte(' ')
	buf.WriteString(clientID)
}

func writeQuotaDeleteCommand(buf *bytes.Buffer, name string) {
	buf.WriteString(CommandQuota)
	buf.WriteString(" DEL ")
	buf.WriteString(name)
}

func writeQuotaInfoCommand(buf *bytes.Buffer, name string) {
	buf.WriteString(CommandQuota)
	buf.WriteString(" INF ")
	buf.WriteString(name)
}

func writeScanCommand(buf *bytes.Buffer, cursor string, count uint32) {
	if cursor == "" {
		cursor = ScanCursorInitial
	}

	buf.WriteString(CommandScan)
	buf.WriteByte(' ')
	buf.WriteString(cursor)
	buf.WriteByte(' ')
	buf.WriteString(strconv.FormatUint(uint64(count), 10))
}

func writePScanCommand(buf *bytes.Buffer, prefix, cursor string, count uint32) {
	if cursor == "" {
		cursor = ScanCursorInitial
	}

	buf.WriteString(CommandPScan)
	buf.WriteByte(' ')
	buf.WriteString(prefix)
	buf.WriteByte(' ')
	buf.WriteString(cursor)
	buf.WriteByte(' ')
	buf.WriteString(strconv.FormatUint(uint64(count), 10))
}

func valuesToBools(values []uint64) []bool {
	bools := make([]bool, len(values))
	for i, value := range values {
		bools[i] = value == 1
	}

	return bools
}

func sendWithReconnect(ctx context.Context, conn *TCPClient, data []byte) ([]byte, error) {
	resp, err := conn.Send(ctx, data)
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

		resp, err = conn.Send(ctx, data)
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
