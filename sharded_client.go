package fq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

const shardedScanCursorSeparator = ":"

var (
	ErrNoShards             = errors.New("no shards configured")
	ErrNilShardingFunc      = errors.New("nil sharding function")
	ErrShardIndexOutOfRange = errors.New("shard index out of range")
)

type ShardingFunc func(key string, shardCount int) int

type ShardedClient struct {
	shards       []*Client
	shardingFunc ShardingFunc
}

func NewSharded(
	addresses []string,
	idleTimeout time.Duration,
	poolSize int,
	shardingFunc ShardingFunc,
	opts ...Option,
) (*ShardedClient, error) {
	if len(addresses) == 0 {
		return nil, ErrNoShards
	}
	if shardingFunc == nil {
		return nil, ErrNilShardingFunc
	}

	shards := make([]*Client, 0, len(addresses))
	for _, address := range addresses {
		shard, err := New(address, idleTimeout, poolSize, opts...)
		if err != nil {
			for _, initialized := range shards {
				initialized.Close()
			}

			return nil, fmt.Errorf("create shard %q: %w", address, err)
		}

		shards = append(shards, shard)
	}

	return &ShardedClient{
		shards:       shards,
		shardingFunc: shardingFunc,
	}, nil
}

func (c *ShardedClient) Close() {
	for _, shard := range c.shards {
		shard.Close()
	}
}

func (c *ShardedClient) ShardCount() int {
	return len(c.shards)
}

func (c *ShardedClient) Shard(index int) (*Client, error) {
	if index < 0 || index >= len(c.shards) {
		return nil, fmt.Errorf("%w: %d", ErrShardIndexOutOfRange, index)
	}

	return c.shards[index], nil
}

func (c *ShardedClient) Incr(ctx context.Context, key CappingKey) (uint64, error) {
	shard, err := c.shardByKey(key.Key)
	if err != nil {
		return 0, err
	}

	return shard.Incr(ctx, key)
}

func (c *ShardedClient) Get(ctx context.Context, key CappingKey) (uint64, error) {
	shard, err := c.shardByKey(key.Key)
	if err != nil {
		return 0, err
	}

	return shard.Get(ctx, key)
}

func (c *ShardedClient) Watch(ctx context.Context, key CappingKey) (uint64, error) {
	shard, err := c.shardByKey(key.Key)
	if err != nil {
		return 0, err
	}

	return shard.Watch(ctx, key)
}

func (c *ShardedClient) Del(ctx context.Context, key CappingKey) (bool, error) {
	shard, err := c.shardByKey(key.Key)
	if err != nil {
		return false, err
	}

	return shard.Del(ctx, key)
}

func (c *ShardedClient) MDel(ctx context.Context, keys []CappingKey) ([]bool, error) {
	type indexedCappingKey struct {
		key   CappingKey
		index int
	}

	results := make([]bool, len(keys))
	grouped := make(map[int][]indexedCappingKey)

	for i, key := range keys {
		shardIndex, err := c.shardIndexByKey(key.Key)
		if err != nil {
			return nil, err
		}

		grouped[shardIndex] = append(grouped[shardIndex], indexedCappingKey{
			key:   key,
			index: i,
		})
	}

	for shardIndex, indexedKeys := range grouped {
		shardKeys := make([]CappingKey, len(indexedKeys))
		for i, indexedKey := range indexedKeys {
			shardKeys[i] = indexedKey.key
		}

		shardResults, err := c.shards[shardIndex].MDel(ctx, shardKeys)
		if err != nil {
			return nil, err
		}
		if len(shardResults) != len(indexedKeys) {
			return nil, ErrCorruptedResponse
		}

		for i, shardResult := range shardResults {
			results[indexedKeys[i].index] = shardResult
		}
	}

	return results, nil
}

func (c *ShardedClient) RLimitFixedWindow(ctx context.Context, key LimitKey, limit uint32) (RateLimitResult, error) {
	shard, err := c.shardByKey(key.Key)
	if err != nil {
		return RateLimitResult{}, err
	}

	return shard.RLimitFixedWindow(ctx, key, limit)
}

func (c *ShardedClient) RLimitSlidingWindow(ctx context.Context, key LimitKey, limit uint32) (RateLimitResult, error) {
	shard, err := c.shardByKey(key.Key)
	if err != nil {
		return RateLimitResult{}, err
	}

	return shard.RLimitSlidingWindow(ctx, key, limit)
}

func (c *ShardedClient) RLimitTokenBucket(
	ctx context.Context,
	key LimitKey,
	capacity uint32,
	refillAmount uint32,
) (RateLimitResult, error) {
	shard, err := c.shardByKey(key.Key)
	if err != nil {
		return RateLimitResult{}, err
	}

	return shard.RLimitTokenBucket(ctx, key, capacity, refillAmount)
}

func (c *ShardedClient) QuotaAcquire(
	ctx context.Context,
	name string,
	amount uint32,
	clientID string,
	ttl ...uint32,
) (QuotaAcquireResult, error) {
	shard, err := c.shardByKey(name)
	if err != nil {
		return QuotaAcquireResult{}, err
	}

	return shard.QuotaAcquire(ctx, name, amount, clientID, ttl...)
}

func (c *ShardedClient) QuotaAcquireLease(
	ctx context.Context,
	name string,
	limit uint32,
	amount uint32,
	clientID string,
	ttl ...uint32,
) (QuotaAcquireResult, error) {
	shard, err := c.shardByKey(name)
	if err != nil {
		return QuotaAcquireResult{}, err
	}

	return shard.QuotaAcquireLease(ctx, name, limit, amount, clientID, ttl...)
}

func (c *ShardedClient) QuotaAcquireN(
	ctx context.Context,
	name string,
	clientID string,
	ttl ...uint32,
) (QuotaAcquireResult, error) {
	shard, err := c.shardByKey(name)
	if err != nil {
		return QuotaAcquireResult{}, err
	}

	return shard.QuotaAcquireN(ctx, name, clientID, ttl...)
}

func (c *ShardedClient) QuotaSet(ctx context.Context, name string, limit uint32) (bool, error) {
	shard, err := c.shardByKey(name)
	if err != nil {
		return false, err
	}

	return shard.QuotaSet(ctx, name, limit)
}

func (c *ShardedClient) QuotaSetN(ctx context.Context, name string, limit, clients uint32) (bool, error) {
	shard, err := c.shardByKey(name)
	if err != nil {
		return false, err
	}

	return shard.QuotaSetN(ctx, name, limit, clients)
}

func (c *ShardedClient) QuotaRelease(ctx context.Context, name, clientID string) (bool, error) {
	shard, err := c.shardByKey(name)
	if err != nil {
		return false, err
	}

	return shard.QuotaRelease(ctx, name, clientID)
}

func (c *ShardedClient) QuotaDelete(ctx context.Context, name string) (bool, error) {
	shard, err := c.shardByKey(name)
	if err != nil {
		return false, err
	}

	return shard.QuotaDelete(ctx, name)
}

func (c *ShardedClient) QuotaInfo(ctx context.Context, name string) (QuotaInfo, error) {
	shard, err := c.shardByKey(name)
	if err != nil {
		return QuotaInfo{}, err
	}

	return shard.QuotaInfo(ctx, name)
}

func (c *ShardedClient) Scan(ctx context.Context, cursor string, count uint32) (ScanResult, error) {
	return c.scan(
		ctx,
		cursor,
		count,
		func(ctx context.Context, shard *Client, cursor string, count uint32) (ScanResult, error) {
			return shard.Scan(ctx, cursor, count)
		},
	)
}

func (c *ShardedClient) PScan(ctx context.Context, prefix, cursor string, count uint32) (ScanResult, error) {
	return c.scan(
		ctx,
		cursor,
		count,
		func(ctx context.Context, shard *Client, cursor string, count uint32) (ScanResult, error) {
			return shard.PScan(ctx, prefix, cursor, count)
		},
	)
}

func (c *ShardedClient) FlushDB(ctx context.Context) (bool, error) {
	return c.boolCommandOnAllShards(ctx, func(ctx context.Context, shard *Client) (bool, error) {
		return shard.FlushDB(ctx)
	})
}

func (c *ShardedClient) Truncate(ctx context.Context) (bool, error) {
	return c.boolCommandOnAllShards(ctx, func(ctx context.Context, shard *Client) (bool, error) {
		return shard.Truncate(ctx)
	})
}

func (c *ShardedClient) Inspect(ctx context.Context, section InspectSection) ([]InspectReport, error) {
	reports := make([]InspectReport, len(c.shards))
	for i, shard := range c.shards {
		report, err := shard.Inspect(ctx, section)
		if err != nil {
			return nil, err
		}

		reports[i] = report
	}

	return reports, nil
}

func (c *ShardedClient) Stream(ctx context.Context, handle func(LimitEvent) error) error {
	return streamEvents(ctx, c.shards, func(ctx context.Context, shard *Client, handle func(LimitEvent) error) error {
		return shard.Stream(ctx, handle)
	}, handle)
}

func (c *ShardedClient) PStream(ctx context.Context, prefix string, handle func(LimitEvent) error) error {
	return streamEvents(ctx, c.shards, func(ctx context.Context, shard *Client, handle func(LimitEvent) error) error {
		return shard.PStream(ctx, prefix, handle)
	}, handle)
}

func (c *ShardedClient) QStream(ctx context.Context, handle func(QuotaEvent) error) error {
	return streamEvents(ctx, c.shards, func(ctx context.Context, shard *Client, handle func(QuotaEvent) error) error {
		return shard.QStream(ctx, handle)
	}, handle)
}

func (c *ShardedClient) QPStream(ctx context.Context, prefix string, handle func(QuotaEvent) error) error {
	return streamEvents(ctx, c.shards, func(ctx context.Context, shard *Client, handle func(QuotaEvent) error) error {
		return shard.QPStream(ctx, prefix, handle)
	}, handle)
}

func (c *ShardedClient) shardByKey(key string) (*Client, error) {
	index, err := c.shardIndexByKey(key)
	if err != nil {
		return nil, err
	}

	return c.shards[index], nil
}

func (c *ShardedClient) shardIndexByKey(key string) (int, error) {
	index := c.shardingFunc(key, len(c.shards))
	if index < 0 || index >= len(c.shards) {
		return 0, fmt.Errorf("%w: %d", ErrShardIndexOutOfRange, index)
	}

	return index, nil
}

func (c *ShardedClient) scan(
	ctx context.Context,
	cursor string,
	count uint32,
	scanShard func(context.Context, *Client, string, uint32) (ScanResult, error),
) (ScanResult, error) {
	if count == 0 {
		return scanShard(ctx, c.shards[0], ScanCursorInitial, count)
	}

	shardIndex, shardCursor, err := c.parseScanCursor(cursor)
	if err != nil {
		return ScanResult{}, err
	}

	keys := make([]ScanKey, 0, int(count))
	for shardIndex < len(c.shards) && len(keys) < int(count) {
		remaining := count - uint32(len(keys))
		result, err := scanShard(ctx, c.shards[shardIndex], shardCursor, remaining)
		if err != nil {
			return ScanResult{}, err
		}

		keys = append(keys, result.Keys...)
		if result.Cursor != ScanCursorInitial {
			return ScanResult{
				Cursor: c.makeScanCursor(shardIndex, result.Cursor),
				Keys:   keys,
			}, nil
		}

		shardIndex++
		shardCursor = ScanCursorInitial
	}

	return ScanResult{
		Cursor: c.makeScanCursor(shardIndex, ScanCursorInitial),
		Keys:   keys,
	}, nil
}

func (c *ShardedClient) parseScanCursor(cursor string) (shardIndex int, shardCursor string, err error) {
	if cursor == "" || cursor == ScanCursorInitial {
		return 0, ScanCursorInitial, nil
	}

	parts := strings.SplitN(cursor, shardedScanCursorSeparator, 2)
	if len(parts) != 2 || parts[1] == "" {
		return 0, "", ErrCorruptedResponse
	}

	shardIndex, err = strconv.Atoi(parts[0])
	if err != nil || shardIndex < 0 || shardIndex >= len(c.shards) {
		return 0, "", ErrCorruptedResponse
	}

	return shardIndex, parts[1], nil
}

func (c *ShardedClient) makeScanCursor(shardIndex int, shardCursor string) string {
	if shardIndex >= len(c.shards) {
		return ScanCursorInitial
	}

	return strconv.Itoa(shardIndex) + shardedScanCursorSeparator + shardCursor
}

func (c *ShardedClient) boolCommandOnAllShards(
	ctx context.Context,
	command func(context.Context, *Client) (bool, error),
) (bool, error) {
	result := true
	for _, shard := range c.shards {
		shardResult, err := command(ctx, shard)
		if err != nil {
			return false, err
		}

		result = result && shardResult
	}

	return result, nil
}

func streamEvents[T any](
	ctx context.Context,
	shards []*Client,
	stream func(context.Context, *Client, func(T) error) error,
	handle func(T) error,
) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, len(shards))
	var wg sync.WaitGroup
	var handleMu sync.Mutex

	for _, shard := range shards {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := stream(streamCtx, shard, func(event T) error {
				handleMu.Lock()
				defer handleMu.Unlock()

				return handle(event)
			})
			if err != nil {
				errs <- err
				cancel()
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errs)
	}()

	return firstStreamError(ctx, errs)
}

func firstStreamError(ctx context.Context, errs <-chan error) error {
	err, ok := <-errs
	if ok {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		return err
	}

	return nil
}
