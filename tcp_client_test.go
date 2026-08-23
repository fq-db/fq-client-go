package fq

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTCPClient(t *testing.T) {
	t.Parallel()

	request := "hello server"
	response := "hello client"

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, request, response)
	})

	client, err := NewTCPClient(address, 2048, time.Minute)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, client.Close())
		require.NoError(t, <-done)
	}()

	buffer, err := client.Send(context.Background(), []byte(request))
	require.NoError(t, err)
	require.Equal(t, []byte(response), buffer)
}

func TestTCPIdleClientConnection(t *testing.T) {
	t.Parallel()

	request := "hello server"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			return err
		}

		received, err := readFrame(connection, 2048)
		if err != nil {
			return err
		}
		if string(received) != request {
			return fmt.Errorf("unexpected request: %q", string(received))
		}
		<-ctx.Done()

		return nil
	})

	client, err := NewTCPClient(address, 2048, time.Millisecond*50)
	require.NoError(t, err)

	_, err = client.Send(context.Background(), []byte(request))
	require.Error(t, err)
	cancel()
	require.NoError(t, client.Close())
	require.NoError(t, <-done)
}

func TestSendWithReconnectRetriesAfterUnexpectedEOF(t *testing.T) {
	t.Parallel()

	request := "hello server"
	response := "hello after reconnect"

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

		received, err := readFrame(connection, 2048)
		if err != nil {
			_ = connection.Close()
			done <- err
			return
		}
		if string(received) != request {
			_ = connection.Close()
			done <- fmt.Errorf("unexpected request: %q", string(received))
			return
		}

		if _, err := connection.Write([]byte{0, 0}); err != nil {
			_ = connection.Close()
			done <- err
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

		done <- requireRequestAndRespond(connection, request, response)
	}()

	client, err := NewTCPClient(listener.Addr().String(), 2048, time.Minute)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, client.Close())
		require.NoError(t, <-done)
	}()

	buffer, err := sendWithReconnect(context.Background(), client, []byte(request))
	require.NoError(t, err)
	require.Equal(t, []byte(response), buffer)
}

func serveFramedClient(t *testing.T, handler func(net.Conn) error) (string, <-chan error) {
	t.Helper()

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
		defer func() {
			_ = connection.Close()
		}()

		done <- handler(connection)
	}()

	return listener.Addr().String(), done
}

func requireRequestAndRespond(connection net.Conn, expectedRequest, response string) error {
	request, err := readFrame(connection, 2048)
	if err != nil {
		return err
	}
	if string(request) != expectedRequest {
		return fmt.Errorf("unexpected request: %q", string(request))
	}

	return writeFrame(connection, []byte(response))
}

func readFrame(connection net.Conn, maxMessageSize int) ([]byte, error) {
	buffer := make([]byte, maxMessageSize)

	return readFrameInto(connection, maxMessageSize, buffer)
}
