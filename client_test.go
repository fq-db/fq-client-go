package fq

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientRateLimitCommands(t *testing.T) {
	t.Parallel()

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			return err
		}

		if err := requireRequestAndRespond(connection, "RLIMIT FW user_42 100 60", "ok|1;1;99;60"); err != nil {
			return err
		}

		if err := requireRequestAndRespond(connection, "RLIMIT SW user_42 100 60", "ok|1;2;98;59"); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, "RLIMIT TB user_42 100 10 60", "ok|1;1;99;0")
	})

	client, err := New(address, time.Minute, 1)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done)
	}()

	key := LimitKey{Key: "user_42", Window: 60}

	fw, err := client.RLimitFixedWindow(context.Background(), key, 100)
	require.NoError(t, err)
	require.Equal(t, RateLimitResult{
		Allowed:    true,
		Current:    1,
		Remaining:  99,
		ResetAfter: 60,
	}, fw)

	sw, err := client.RLimitSlidingWindow(context.Background(), key, 100)
	require.NoError(t, err)
	require.Equal(t, RateLimitResult{
		Allowed:    true,
		Current:    2,
		Remaining:  98,
		ResetAfter: 59,
	}, sw)

	tb, err := client.RLimitTokenBucket(context.Background(), key, 100, 10)
	require.NoError(t, err)
	require.Equal(t, RateLimitResult{
		Allowed:    true,
		Current:    1,
		Remaining:  99,
		ResetAfter: 0,
	}, tb)
}

func TestClientQuotaCommands(t *testing.T) {
	t.Parallel()

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			return err
		}

		if err := requireRequestAndRespond(connection, "QUOTA SET campaign_42 10", "ok|1"); err != nil {
			return err
		}

		if err := requireRequestAndRespond(connection, "QUOTA ACQ campaign_42 4 worker_a 60", "ok|1;4;4;6;60"); err != nil {
			return err
		}

		if err := requireRequestAndRespond(connection, "QUOTA INF campaign_42", "ok|10;4;6;worker_a;4;1788019260"); err != nil {
			return err
		}

		if err := requireRequestAndRespond(connection, "QUOTA REL campaign_42 worker_a", "ok|1"); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, "QUOTA DEL campaign_42", "ok|1")
	})

	client, err := New(address, time.Minute, 1)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done)
	}()

	changed, err := client.QuotaSet(context.Background(), "campaign_42", 10)
	require.NoError(t, err)
	require.True(t, changed)

	acquired, err := client.QuotaAcquire(context.Background(), "campaign_42", 4, "worker_a", 60)
	require.NoError(t, err)
	require.Equal(t, QuotaAcquireResult{
		Acquired:     true,
		Allocated:    4,
		Used:         4,
		Remaining:    6,
		ExpiresAfter: 60,
	}, acquired)

	info, err := client.QuotaInfo(context.Background(), "campaign_42")
	require.NoError(t, err)
	require.Equal(t, QuotaInfo{
		Limit:     10,
		Used:      4,
		Remaining: 6,
		Clients: []QuotaClientInfo{
			{ClientID: "worker_a", Amount: 4, ExpiresAt: 1788019260},
		},
	}, info)

	released, err := client.QuotaRelease(context.Background(), "campaign_42", "worker_a")
	require.NoError(t, err)
	require.True(t, released)

	deleted, err := client.QuotaDelete(context.Background(), "campaign_42")
	require.NoError(t, err)
	require.True(t, deleted)
}

func TestClientQuotaAcquireCommandWithoutTTL(t *testing.T) {
	t.Parallel()

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, "QUOTA ACQ campaign_42 4 worker_a", "ok|1;4;4;6;0")
	})

	client, err := New(address, time.Minute, 1)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done)
	}()

	acquired, err := client.QuotaAcquire(context.Background(), "campaign_42", 4, "worker_a")
	require.NoError(t, err)
	require.Equal(t, QuotaAcquireResult{
		Acquired:  true,
		Allocated: 4,
		Used:      4,
		Remaining: 6,
	}, acquired)
}

func TestClientQuotaAcquireLeaseCommand(t *testing.T) {
	t.Parallel()

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, "QUOTA ACQL campaign_42 10 4 worker_a 60", "ok|1;4;4;6;60")
	})

	client, err := New(address, time.Minute, 1)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done)
	}()

	acquired, err := client.QuotaAcquireLease(context.Background(), "campaign_42", 10, 4, "worker_a", 60)
	require.NoError(t, err)
	require.Equal(t, QuotaAcquireResult{
		Acquired:     true,
		Allocated:    4,
		Used:         4,
		Remaining:    6,
		ExpiresAfter: 60,
	}, acquired)
}

func TestClientQuotaSetNAndAcquireNCommands(t *testing.T) {
	t.Parallel()

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			return err
		}

		if err := requireRequestAndRespond(connection, "QUOTA SETN campaign_42 10 3", "ok|1"); err != nil {
			return err
		}

		if err := requireRequestAndRespond(connection, "QUOTA ACQN campaign_42 worker_a 60", "ok|1;3;3;7;60"); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, "QUOTA ACQN campaign_42 worker_b", "ok|1;3;6;4;0")
	})

	client, err := New(address, time.Minute, 1)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done)
	}()

	changed, err := client.QuotaSetN(context.Background(), "campaign_42", 10, 3)
	require.NoError(t, err)
	require.True(t, changed)

	first, err := client.QuotaAcquireN(context.Background(), "campaign_42", "worker_a", 60)
	require.NoError(t, err)
	require.Equal(t, QuotaAcquireResult{
		Acquired:     true,
		Allocated:    3,
		Used:         3,
		Remaining:    7,
		ExpiresAfter: 60,
	}, first)

	second, err := client.QuotaAcquireN(context.Background(), "campaign_42", "worker_b")
	require.NoError(t, err)
	require.Equal(t, QuotaAcquireResult{
		Acquired:  true,
		Allocated: 3,
		Used:      6,
		Remaining: 4,
	}, second)
}

func TestClientDatabaseMaintenanceCommands(t *testing.T) {
	t.Parallel()

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			return err
		}

		if err := requireRequestAndRespond(connection, "FLUSHDB", "ok|1"); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, "TRUNCATE", "ok|1")
	})

	client, err := New(address, time.Minute, 1)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done)
	}()

	flushed, err := client.FlushDB(context.Background())
	require.NoError(t, err)
	require.True(t, flushed)

	truncated, err := client.Truncate(context.Background())
	require.NoError(t, err)
	require.True(t, truncated)
}

func TestClientPStreamCommand(t *testing.T) {
	t.Parallel()

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			return err
		}

		request, err := readFrame(connection, 2048)
		if err != nil {
			return err
		}
		if string(request) != "PSTREAM tenant_a-" {
			return fmt.Errorf("unexpected request: %q", string(request))
		}

		return writeFrame(connection, []byte("ok|tenant_a-user_42;60;2;58"))
	})

	client, err := New(address, time.Minute, 1)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done)
	}()

	var event LimitEvent
	err = client.PStream(context.Background(), "tenant_a-", func(received LimitEvent) error {
		event = received

		return io.EOF
	})
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, LimitEvent{
		Key:        "tenant_a-user_42",
		Window:     60,
		Current:    2,
		ResetAfter: 58,
	}, event)
}

func TestClientQPStreamCommand(t *testing.T) {
	t.Parallel()

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			return err
		}

		request, err := readFrame(connection, 2048)
		if err != nil {
			return err
		}
		if string(request) != "QPSTREAM tenant_a-" {
			return fmt.Errorf("unexpected request: %q", string(request))
		}

		return writeFrame(connection, []byte("ok|acq;tenant_a-quota;client-a;4;4;6;0"))
	})

	client, err := New(address, time.Minute, 1)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done)
	}()

	var event QuotaEvent
	err = client.QPStream(context.Background(), "tenant_a-", func(received QuotaEvent) error {
		event = received

		return io.EOF
	})
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, QuotaEvent{
		Event:     "acq",
		Name:      "tenant_a-quota",
		ClientID:  "client-a",
		Amount:    4,
		Used:      4,
		Remaining: 6,
	}, event)
}

func TestClientStreamCommand(t *testing.T) {
	t.Parallel()

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			return err
		}

		request, err := readFrame(connection, 2048)
		if err != nil {
			return err
		}
		if string(request) != "STREAM" {
			return fmt.Errorf("unexpected request: %q", string(request))
		}

		return writeFrame(connection, []byte("ok|user_42;60;1;59"))
	})

	client, err := New(address, time.Minute, 1)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done)
	}()

	var event LimitEvent
	err = client.Stream(context.Background(), func(received LimitEvent) error {
		event = received

		return io.EOF
	})
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, LimitEvent{
		Key:        "user_42",
		Window:     60,
		Current:    1,
		ResetAfter: 59,
	}, event)
}

func TestClientQStreamCommand(t *testing.T) {
	t.Parallel()

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			return err
		}

		request, err := readFrame(connection, 2048)
		if err != nil {
			return err
		}
		if string(request) != "QSTREAM" {
			return fmt.Errorf("unexpected request: %q", string(request))
		}

		return writeFrame(connection, []byte("ok|rel;quota;client-a;4;0;10;0"))
	})

	client, err := New(address, time.Minute, 1)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done)
	}()

	var event QuotaEvent
	err = client.QStream(context.Background(), func(received QuotaEvent) error {
		event = received

		return io.EOF
	})
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, QuotaEvent{
		Event:     "rel",
		Name:      "quota",
		ClientID:  "client-a",
		Amount:    4,
		Remaining: 10,
	}, event)
}

func TestClientPStreamReconnectsAfterIdleConnectionClosed(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		defer func() {
			_ = listener.Close()
		}()

		connection, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			_ = connection.Close()
			done <- err
			return
		}
		request, err := readFrame(connection, 2048)
		if err != nil {
			_ = connection.Close()
			done <- err
			return
		}
		if string(request) != "PSTREAM tenant_a-" {
			_ = connection.Close()
			done <- fmt.Errorf("unexpected request: %q", string(request))
			return
		}
		_ = connection.Close()

		connection, err = listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer func() {
			_ = connection.Close()
		}()

		request, err = readFrame(connection, 2048)
		if err != nil {
			done <- err
			return
		}
		if string(request) != "PSTREAM tenant_a-" {
			done <- fmt.Errorf("unexpected request: %q", string(request))
			return
		}

		done <- writeFrame(connection, []byte("ok|tenant_a-user_42;60;1;59"))
	}()

	client, err := New(listener.Addr().String(), time.Minute, 1)
	require.NoError(t, err)
	defer client.Close()

	var event LimitEvent
	err = client.PStream(context.Background(), "tenant_a-", func(received LimitEvent) error {
		event = received

		return io.EOF
	})
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, LimitEvent{
		Key:        "tenant_a-user_42",
		Window:     60,
		Current:    1,
		ResetAfter: 59,
	}, event)
	require.NoError(t, <-done)
}

func TestClientReconnectsAfterIdleConnectionClosed(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	idleClosed := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		defer func() {
			_ = listener.Close()
		}()

		connection, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			_ = connection.Close()
			done <- err
			return
		}
		_ = connection.Close()
		close(idleClosed)

		connection, err = listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer func() {
			_ = connection.Close()
		}()

		done <- requireRequestAndRespond(connection, "GET idle-key 60", "ok|7")
	}()

	client, err := New(listener.Addr().String(), time.Minute, 1)
	require.NoError(t, err)
	defer client.Close()

	<-idleClosed

	value, err := client.Get(context.Background(), CappingKey{Key: "idle-key", Capping: 60})
	require.NoError(t, err)
	require.Equal(t, uint64(7), value)
	require.NoError(t, <-done)
}
