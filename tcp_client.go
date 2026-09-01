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
	"strconv"
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
	connection, err := dial(c.address, c.tlsConfig)
	if err != nil {
		return err
	}

	c.connection = connection

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	if err := c.hello(ctx); err != nil {
		_ = connection.Close()

		return err
	}

	return nil
}

func (c *TCPClient) hello(ctx context.Context) error {
	version := strconv.FormatUint(ProtocolVersion, 10)
	request := make([]byte, 0, len(CommandHello)+1+len(version)+len(" AUTH ")+len(c.token))
	request = append(request, CommandHello...)
	request = append(request, ' ')
	request = append(request, version...)
	if c.token != "" {
		request = append(request, " AUTH "...)
		request = append(request, c.token...)
	}

	resp, err := c.Send(ctx, request)
	if err != nil {
		return fmt.Errorf("send hello: %w", err)
	}

	info, err := parseHelloResponse(resp)
	if err != nil {
		return fmt.Errorf("parse hello response: %w", err)
	}

	if info.status == ResponseStatusError {
		if isProtocolCode(info.err, CodeAuthenticationFailed) {
			return fmt.Errorf("%w: %w", ErrAuthFailed, info.err)
		}

		return info.err
	}

	if info.version != ProtocolVersion {
		return fmt.Errorf("%w: negotiated version %d", ErrCorruptedResponse, info.version)
	}

	c.SetMaxMessageSizeUnsafe(info.maxMessageSize)

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

	return c.connect()
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
