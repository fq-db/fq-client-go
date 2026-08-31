package fq

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

var (
	ErrAuthFailed           = errors.New("authentication failed")
	ErrTLSKeyPairIncomplete = errors.New("CertFile and KeyFile must be set together")
	ErrTLSUnknownMinVersion = errors.New("unknown tls min version")
)

type TLSConfig struct {
	CAFile     string
	CertFile   string
	KeyFile    string
	ServerName string
	SkipVerify bool
	MinVersion string
}

func (c TLSConfig) Empty() bool {
	return c.CAFile == "" &&
		c.CertFile == "" &&
		c.KeyFile == "" &&
		c.ServerName == "" &&
		c.MinVersion == "" &&
		!c.SkipVerify
}

func (c TLSConfig) build() (*tls.Config, error) {
	if c.Empty() {
		return nil, nil
	}

	version, err := tlsMinVersion(c.MinVersion)
	if err != nil {
		return nil, err
	}

	config := &tls.Config{
		MinVersion:         version,
		ServerName:         c.ServerName,
		InsecureSkipVerify: c.SkipVerify, //nolint:gosec // opt-in through the SkipVerify field
	}

	if c.CAFile != "" {
		pool, err := certPool(c.CAFile)
		if err != nil {
			return nil, err
		}

		config.RootCAs = pool
	}

	if c.CertFile != "" || c.KeyFile != "" {
		if c.CertFile == "" || c.KeyFile == "" {
			return nil, ErrTLSKeyPairIncomplete
		}

		certificate, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load key pair: %w", err)
		}

		config.Certificates = []tls.Certificate{certificate}
	}

	return config, nil
}

func tlsMinVersion(value string) (uint16, error) {
	switch value {
	case "", "1.2":
		return tls.VersionTLS12, nil
	case "1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("%w: %q", ErrTLSUnknownMinVersion, value)
	}
}

func certPool(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ca file %q: %w", path, err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("no certificates found in %q", path)
	}

	return pool, nil
}

type clientOptions struct {
	token     string
	tlsConfig *tls.Config
}

type Option func(*clientOptions) error

func WithToken(token string) Option {
	return func(o *clientOptions) error {
		o.token = token

		return nil
	}
}

func WithTLS(config TLSConfig) Option {
	return func(o *clientOptions) error {
		built, err := config.build()
		if err != nil {
			return err
		}

		o.tlsConfig = built

		return nil
	}
}

func WithTLSConfig(config *tls.Config) Option {
	return func(o *clientOptions) error {
		o.tlsConfig = config

		return nil
	}
}

func applyOptions(opts []Option) (clientOptions, error) {
	settings := clientOptions{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}

		if err := opt(&settings); err != nil {
			return clientOptions{}, err
		}
	}

	return settings, nil
}
