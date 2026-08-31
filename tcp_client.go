package fq

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	frameHeaderSize     = 4
	maxFramePayloadSize = 1<<32 - 1
	connectTimeout      = 5 * time.Second
)

var ErrConnClosed = errors.New("connection closed by server")

var errFrameTooLarge = errors.New("frame exceeds maximum message size")

type TCPClient struct {
	connection     net.Conn
	address        string
	maxMessageSize int
	idleTimeout    time.Duration
	bufferPool     *bytesPool
	token          string
	tlsConfig      *tls.Config
}

func NewTCPClient(
	address string,
	maxMessageSize int,
	idleTimeout time.Duration,
	opts ...Option,
) (*TCPClient, error) {
	settings, err := applyOptions(opts)
	if err != nil {
		return nil, err
	}

	c := &TCPClient{
		address:        address,
		maxMessageSize: maxMessageSize,
		idleTimeout:    idleTimeout,
		bufferPool:     newBytesPool(maxMessageSize),
		token:          settings.token,
		tlsConfig:      settings.tlsConfig,
	}

	if err := c.connect(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *TCPClient) connect() error {
	if err := c.dialAndAuthenticate(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	if err := c.getAndSetMsgSize(ctx); err != nil {
		_ = c.connection.Close()

		return fmt.Errorf("failed to set msg size: %w", err)
	}

	return nil
}

func (c *TCPClient) dialAndAuthenticate() error {
	connection, err := dial(c.address, c.tlsConfig)
	if err != nil {
		return err
	}

	c.connection = connection

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	if err := c.authenticate(ctx); err != nil {
		_ = connection.Close()

		return err
	}

	return nil
}

func (c *TCPClient) authenticate(ctx context.Context) error {
	if c.token == "" {
		return nil
	}

	request := make([]byte, 0, len(CommandAuth)+1+len(c.token))
	request = append(request, CommandAuth...)
	request = append(request, ' ')
	request = append(request, c.token...)

	resp, err := c.Send(ctx, request)
	if err != nil {
		return fmt.Errorf("send auth: %w", err)
	}

	result, err := parseResponse(resp)
	if err != nil {
		return fmt.Errorf("parse auth response: %w", err)
	}

	if result.status == ResponseStatusError {
		return fmt.Errorf("%w: %w", ErrAuthFailed, result.err)
	}

	return nil
}

func dial(address string, tlsConfig *tls.Config) (net.Conn, error) {
	if tlsConfig == nil {
		connection, err := net.Dial("tcp", address)
		if err != nil {
			return nil, fmt.Errorf("failed to dial: %w", err)
		}

		return connection, nil
	}

	connection, err := tls.Dial("tcp", address, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to dial tls: %w", err)
	}

	return connection, nil
}

func (c *TCPClient) Send(ctx context.Context, request []byte) ([]byte, error) {
	if len(request) > c.maxMessageSize {
		return nil, fmt.Errorf("request exceeds max message size (%d)", c.maxMessageSize)
	}

	if err := c.connection.SetDeadline(c.deadline(ctx)); err != nil {
		return nil, normalizeConnectionError(err)
	}

	if err := writeFrame(c.connection, request); err != nil {
		return nil, normalizeConnectionError(err)
	}

	response := c.bufferPool.Get()
	defer c.bufferPool.Put(response)

	message, err := readFrameInto(c.connection, c.maxMessageSize, response)
	if err != nil {
		return nil, normalizeConnectionError(err)
	}

	result := make([]byte, len(message))
	copy(result, message)

	return result, nil
}

func (c *TCPClient) Stream(ctx context.Context, request []byte, handle func([]byte) error) error {
	if len(request) > c.maxMessageSize {
		return fmt.Errorf("request exceeds max message size (%d)", c.maxMessageSize)
	}

	if err := c.connection.SetDeadline(c.deadline(ctx)); err != nil {
		return normalizeConnectionError(err)
	}

	if err := writeFrame(c.connection, request); err != nil {
		return normalizeConnectionError(err)
	}

	response := c.bufferPool.Get()
	defer c.bufferPool.Put(response)

	for {
		if err := c.connection.SetDeadline(c.deadline(ctx)); err != nil {
			return normalizeConnectionError(err)
		}

		message, err := readFrameInto(c.connection, c.maxMessageSize, response)
		if err != nil {
			return normalizeConnectionError(err)
		}

		result := make([]byte, len(message))
		copy(result, message)

		if err := handle(result); err != nil {
			return err
		}
	}
}

func (c *TCPClient) SendChunked(ctx context.Context, request []byte) ([]byte, error) {
	if len(request) > c.maxMessageSize {
		return nil, fmt.Errorf("request exceeds max message size (%d)", c.maxMessageSize)
	}

	if err := c.connection.SetDeadline(c.deadline(ctx)); err != nil {
		return nil, normalizeConnectionError(err)
	}

	if err := writeFrame(c.connection, request); err != nil {
		return nil, normalizeConnectionError(err)
	}

	response := c.bufferPool.Get()
	defer c.bufferPool.Put(response)

	var body []byte

	for {
		if err := c.connection.SetDeadline(c.deadline(ctx)); err != nil {
			return nil, normalizeConnectionError(err)
		}

		frame, err := readFrameInto(c.connection, c.maxMessageSize, response)
		if err != nil {
			return nil, normalizeConnectionError(err)
		}

		idx := bytes.IndexByte(frame, respDelimiter)
		if idx == -1 {
			return nil, ErrCorruptedResponse
		}

		status := string(frame[:idx])
		data := frame[idx+1:]

		switch status {
		case frameStatusNext:
			body = append(body, data...)
		case frameStatusOK:
			body = append(body, data...)

			return joinStatusAndData(frameStatusOK, body), nil
		case frameStatusError:
			return joinStatusAndData(frameStatusError, data), nil
		default:
			return nil, fmt.Errorf("unexpected frame status %q", status)
		}
	}
}

func joinStatusAndData(status string, data []byte) []byte {
	result := make([]byte, 0, len(status)+1+len(data))
	result = append(result, status...)
	result = append(result, respDelimiter)
	result = append(result, data...)

	return result
}

func normalizeConnectionError(err error) error {
	if isConnectionError(err) {
		return fmt.Errorf("%w: %w", ErrConnClosed, err)
	}

	return err
}

func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrShortWrite) ||
		errors.Is(err, net.ErrClosed) {
		return true
	}

	var netErr net.Error

	return errors.As(err, &netErr)
}

func (c *TCPClient) Close() error {
	return c.connection.Close()
}

func (c *TCPClient) SetMaxMessageSizeUnsafe(size int) {
	c.maxMessageSize = size
	c.bufferPool = newBytesPool(size)
}

func (c *TCPClient) Reconnect() error {
	_ = c.connection.Close()

	return c.dialAndAuthenticate()
}

func (c *TCPClient) deadline(ctx context.Context) time.Time {
	stdDeadline := time.Now().Add(c.idleTimeout)
	deadline, ok := ctx.Deadline()
	if ok {
		if stdDeadline.Before(deadline) {
			deadline = stdDeadline
		}
	} else {
		deadline = stdDeadline
	}

	return deadline
}

func (c *TCPClient) getAndSetMsgSize(ctx context.Context) error {
	sz, err := msgSize(ctx, c)
	if err != nil {
		return err
	}

	c.SetMaxMessageSizeUnsafe(sz)

	return nil
}

func msgSize(ctx context.Context, client *TCPClient) (int, error) {
	resp, err := client.Send(ctx, []byte(CommandMsgSize))
	if err != nil {
		return 0, fmt.Errorf("send: %w", err)
	}

	result, err := parseResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return int(result.value), nil
	case ResponseStatusError:
		return 0, result.err
	default:
		return 0, ErrUnknownRespStatus
	}
}

func readFrameInto(conn net.Conn, maxMessageSize int, buffer []byte) ([]byte, error) {
	header := make([]byte, frameHeaderSize)
	messageSize, err := readFrameSize(conn, header, maxMessageSize)
	if err != nil {
		return nil, err
	}

	message := buffer[:messageSize]
	if _, err := io.ReadFull(conn, message); err != nil {
		return nil, err
	}

	return message, nil
}

func readFrameSize(conn net.Conn, header []byte, maxMessageSize int) (int, error) {
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, err
	}

	messageSize := binary.BigEndian.Uint32(header)
	if messageSize > uint32(maxMessageSize) {
		return 0, fmt.Errorf("%w: %d > %d", errFrameTooLarge, messageSize, maxMessageSize)
	}

	return int(messageSize), nil
}

func writeFrame(conn net.Conn, payload []byte) error {
	if uint64(len(payload)) > maxFramePayloadSize {
		return fmt.Errorf("%w: %d > %d", errFrameTooLarge, len(payload), maxFramePayloadSize)
	}

	header := make([]byte, frameHeaderSize)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))

	if err := writeAll(conn, header); err != nil {
		return err
	}

	return writeAll(conn, payload)
}

func writeAll(conn net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}

		if n == 0 {
			return io.ErrShortWrite
		}

		data = data[n:]
	}

	return nil
}
