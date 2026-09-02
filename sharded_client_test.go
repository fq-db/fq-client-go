package fq

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewShardedValidatesConfig(t *testing.T) {
	t.Parallel()

	_, err := NewSharded(nil, time.Minute, 1, testShardingFunc)
	require.ErrorIs(t, err, ErrNoShards)

	_, err = NewSharded([]string{"127.0.0.1:1"}, time.Minute, 1, nil)
	require.ErrorIs(t, err, ErrNilShardingFunc)
}

func TestShardedClientRoutesKeyedCommands(t *testing.T) {
	t.Parallel()

	address0, done0 := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireHello(connection, 2048, false); err != nil {
			return err
		}
		if err := requireRequestAndRespond(connection, "INCR alpha 60", "ok|1"); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, "QUOTA SET alpha-quota 10", "ok|1")
	})
	address1, done1 := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireHello(connection, 2048, false); err != nil {
			return err
		}
		if err := requireRequestAndRespond(connection, "GET beta 60", "ok|2"); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, "RLIMIT FW beta-limit 10 60", "ok|1;1;9;60")
	})

	client, err := NewSharded([]string{address0, address1}, time.Minute, 1, testShardingFunc)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done0)
		require.NoError(t, <-done1)
	}()

	value, err := client.Incr(context.Background(), CappingKey{Key: "alpha", Capping: 60})
	require.NoError(t, err)
	require.Equal(t, uint64(1), value)

	value, err = client.Get(context.Background(), CappingKey{Key: "beta", Capping: 60})
	require.NoError(t, err)
	require.Equal(t, uint64(2), value)

	changed, err := client.QuotaSet(context.Background(), "alpha-quota", 10)
	require.NoError(t, err)
	require.True(t, changed)

	limited, err := client.RLimitFixedWindow(context.Background(), LimitKey{Key: "beta-limit", Window: 60}, 10)
	require.NoError(t, err)
	require.Equal(t, RateLimitResult{
		Allowed:    true,
		Current:    1,
		Remaining:  9,
		ResetAfter: 60,
	}, limited)
}

func TestShardedClientRejectsInvalidShardIndex(t *testing.T) {
	t.Parallel()

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		return requireHello(connection, 2048, false)
	})

	client, err := NewSharded([]string{address}, time.Minute, 1, func(string, int) int {
		return 1
	})
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done)
	}()

	_, err = client.Get(context.Background(), CappingKey{Key: "key", Capping: 60})
	require.ErrorIs(t, err, ErrShardIndexOutOfRange)
}

func TestShardedClientMDelPreservesInputOrder(t *testing.T) {
	t.Parallel()

	address0, done0 := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireHello(connection, 2048, false); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, "MDEL alpha 60 gamma 60", "ok|1;0")
	})
	address1, done1 := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireHello(connection, 2048, false); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, "MDEL beta 60", "ok|1")
	})

	client, err := NewSharded([]string{address0, address1}, time.Minute, 1, testShardingFunc)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done0)
		require.NoError(t, <-done1)
	}()

	deleted, err := client.MDel(context.Background(), []CappingKey{
		{Key: "alpha", Capping: 60},
		{Key: "beta", Capping: 60},
		{Key: "gamma", Capping: 60},
	})
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, false}, deleted)
}

func TestShardedClientScanWalksShards(t *testing.T) {
	t.Parallel()

	address0, done0 := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireHello(connection, 2048, false); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, "SCAN 0 2", "ok|0;alpha;60;gamma;30")
	})
	address1, done1 := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireHello(connection, 2048, false); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, "SCAN 0 1", "ok|0;beta;60")
	})

	client, err := NewSharded([]string{address0, address1}, time.Minute, 1, testShardingFunc)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done0)
		require.NoError(t, <-done1)
	}()

	first, err := client.Scan(context.Background(), ScanCursorInitial, 2)
	require.NoError(t, err)
	require.Equal(t, "1:0", first.Cursor)
	require.Equal(t, []ScanKey{
		{Key: "alpha", Window: 60},
		{Key: "gamma", Window: 30},
	}, first.Keys)

	second, err := client.Scan(context.Background(), first.Cursor, 1)
	require.NoError(t, err)
	require.Equal(t, ScanCursorInitial, second.Cursor)
	require.Equal(t, []ScanKey{{Key: "beta", Window: 60}}, second.Keys)
}

func TestShardedClientMaintenanceRunsOnAllShards(t *testing.T) {
	t.Parallel()

	address0, done0 := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireHello(connection, 2048, false); err != nil {
			return err
		}
		if err := requireRequestAndRespond(connection, "FLUSHDB", "ok|1"); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, "TRUNCATE", "ok|1")
	})
	address1, done1 := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireHello(connection, 2048, false); err != nil {
			return err
		}
		if err := requireRequestAndRespond(connection, "FLUSHDB", "ok|1"); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, "TRUNCATE", "ok|1")
	})

	client, err := NewSharded([]string{address0, address1}, time.Minute, 1, testShardingFunc)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done0)
		require.NoError(t, <-done1)
	}()

	flushed, err := client.FlushDB(context.Background())
	require.NoError(t, err)
	require.True(t, flushed)

	truncated, err := client.Truncate(context.Background())
	require.NoError(t, err)
	require.True(t, truncated)
}

func TestShardedClientStreamMergesShards(t *testing.T) {
	t.Parallel()

	address0, done0 := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireHello(connection, 2048, false); err != nil {
			return err
		}
		request, err := readFrame(connection, 2048)
		if err != nil {
			return err
		}
		if string(request) != "STREAM" {
			return errors.New("unexpected stream request")
		}

		return writeFrame(connection, []byte("ok|alpha;60;1;59"))
	})
	address1, done1 := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireHello(connection, 2048, false); err != nil {
			return err
		}

		request, err := readFrame(connection, 2048)
		if err != nil {
			return err
		}
		if string(request) != "STREAM" {
			return errors.New("unexpected stream request")
		}

		<-time.After(time.Second)

		return nil
	})

	client, err := NewSharded([]string{address0, address1}, time.Minute, 1, testShardingFunc)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done0)
		require.NoError(t, <-done1)
	}()

	var received LimitEvent
	err = client.Stream(context.Background(), func(event LimitEvent) error {
		received = event

		return errStopStream
	})
	require.ErrorIs(t, err, errStopStream)
	require.Equal(t, LimitEvent{
		Key:        "alpha",
		Window:     60,
		Current:    1,
		ResetAfter: 59,
	}, received)
}

func testShardingFunc(key string, _ int) int {
	if stringsHasPrefix(key, "beta") {
		return 1
	}

	return 0
}

func stringsHasPrefix(value, prefix string) bool {
	return len(value) >= len(prefix) && value[:len(prefix)] == prefix
}

var errStopStream = errors.New("stop stream")
