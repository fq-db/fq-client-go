package fq

import (
	"context"
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
