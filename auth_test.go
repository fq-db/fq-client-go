package fq

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func serveAuthenticatedConnections(t *testing.T, token string, connections int) (string, <-chan error) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	done := make(chan error, 1)

	go func() {
		defer func() { _ = listener.Close() }()

		for i := 0; i < connections; i++ {
			connection, err := listener.Accept()
			if err != nil {
				done <- err

				return
			}

			if err := requireRequestAndRespond(connection, CommandAuth+" "+token, "ok|1"); err != nil {
				_ = connection.Close()
				done <- err

				return
			}

			if i == 0 {
				if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|4096"); err != nil {
					_ = connection.Close()
					done <- err

					return
				}
			}

			if err := requireRequestAndRespond(connection, "GET k 60", "ok|7"); err != nil {
				_ = connection.Close()
				done <- err

				return
			}

			_ = connection.Close()
		}

		done <- nil
	}()

	return listener.Addr().String(), done
}

func TestClientAuthenticatesOnConnect(t *testing.T) {
	address, done := serveAuthenticatedConnections(t, "secret-token-value", 1)

	client, err := NewTCPClient(address, 4096, time.Minute, WithToken("secret-token-value"))
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	response, err := client.Send(context.Background(), []byte("GET k 60"))
	require.NoError(t, err)
	require.Equal(t, "ok|7", string(response))

	require.NoError(t, <-done)
}

func TestReconnectReauthenticates(t *testing.T) {
	address, done := serveAuthenticatedConnections(t, "secret-token-value", 2)

	client, err := NewTCPClient(address, 4096, time.Minute, WithToken("secret-token-value"))
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	response, err := client.Send(context.Background(), []byte("GET k 60"))
	require.NoError(t, err)
	require.Equal(t, "ok|7", string(response))

	require.NoError(t, client.Reconnect())

	response, err = client.Send(context.Background(), []byte("GET k 60"))
	require.NoError(t, err)
	require.Equal(t, "ok|7", string(response))

	require.NoError(t, <-done)
}

func TestAuthenticationIsSkippedWithoutToken(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	done := make(chan error, 1)

	go func() {
		defer func() { _ = listener.Close() }()

		connection, err := listener.Accept()
		if err != nil {
			done <- err

			return
		}
		defer func() { _ = connection.Close() }()

		done <- requireRequestAndRespond(connection, CommandMsgSize, "ok|4096")
	}()

	client, err := NewTCPClient(listener.Addr().String(), 4096, time.Minute)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	require.NoError(t, <-done)
}

func TestRejectedAuthenticationFailsConstruction(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = connection.Close() }()

		_ = requireRequestAndRespond(connection, CommandAuth+" wrong-token", "err|authentication failed")
	}()

	_, err = NewTCPClient(listener.Addr().String(), 4096, time.Minute, WithToken("wrong-token"))
	require.ErrorIs(t, err, ErrAuthFailed)
}

func TestTLSConfigValidation(t *testing.T) {
	_, err := TLSConfig{CertFile: "/tmp/only.crt"}.build()
	require.ErrorIs(t, err, ErrTLSKeyPairIncomplete)

	_, err = TLSConfig{MinVersion: "1.1"}.build()
	require.ErrorIs(t, err, ErrTLSUnknownMinVersion)

	config, err := TLSConfig{}.build()
	require.NoError(t, err)
	require.Nil(t, config)

	config, err = TLSConfig{SkipVerify: true}.build()
	require.NoError(t, err)
	require.True(t, config.InsecureSkipVerify)
}

func TestOptionErrorSurfacesFromConstructor(t *testing.T) {
	_, err := NewTCPClient("127.0.0.1:1", 4096, time.Second, WithTLS(TLSConfig{MinVersion: "1.1"}))
	require.ErrorIs(t, err, ErrTLSUnknownMinVersion)
}
