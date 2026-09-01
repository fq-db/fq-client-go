//go:build integration

package fq

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	adminSecret       = "integration-admin-token"
	readWriteSecret   = "integration-rw-token-value"
	readOnlySecret    = "integration-ro-token-value"
	replicationSecret = "integration-replication-token"
)

type testPKI struct {
	CAFile         string
	ServerCertFile string
	ServerKeyFile  string
	ClientCertFile string
	ClientKeyFile  string
}

func writeSecretFile(t *testing.T, dir, name, value string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(value), 0o600))

	return path
}

func newTestPKI(t *testing.T, dir, host string) testPKI {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fq client integration CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	pki := testPKI{
		CAFile:         filepath.Join(dir, "ca.crt"),
		ServerCertFile: filepath.Join(dir, "server.crt"),
		ServerKeyFile:  filepath.Join(dir, "server.key"),
		ClientCertFile: filepath.Join(dir, "client.crt"),
		ClientKeyFile:  filepath.Join(dir, "client.key"),
	}

	writeCertPEM(t, pki.CAFile, caDER)

	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	issueLeaf(t, serverTemplate, caCert, caKey, pki.ServerCertFile, pki.ServerKeyFile)

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "fq integration client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	issueLeaf(t, clientTemplate, caCert, caKey, pki.ClientCertFile, pki.ClientKeyFile)

	return pki
}

func issueLeaf(
	t *testing.T,
	template, caCert *x509.Certificate,
	caKey *ecdsa.PrivateKey,
	certPath, keyPath string,
) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	require.NoError(t, err)

	writeCertPEM(t, certPath, der)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))
}

func writeCertPEM(t *testing.T, path string, der []byte) {
	t.Helper()

	data := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	require.NoError(t, os.WriteFile(path, data, 0o600))
}

func writeSecuredFQServerConfig(
	t *testing.T,
	workDir, serverAddress, replicationAddress string,
	pki testPKI,
) string {
	t.Helper()

	configPath := filepath.Join(workDir, "config-secured.yml")
	walDir := filepath.Join(workDir, "wal-secured")
	dumpDir := filepath.Join(workDir, "dump-secured")
	require.NoError(t, os.MkdirAll(walDir, 0o750))
	require.NoError(t, os.MkdirAll(dumpDir, 0o750))

	config := fmt.Sprintf(`
network:
  address: "%s"
  max_connections: 16
  max_message_size: 64KB
  idle_timeout: 5s
  auth:
    tokens:
      - { role: admin, token_file: "%s" }
      - { role: rw, token_file: "%s" }
      - { role: ro, token_file: "%s" }
  tls:
    cert_file: "%s"
    key_file: "%s"
    client_ca_file: "%s"
persistence:
  mode: wal_and_dump
observability:
  address: ""
wal:
  sync_commit: on
  flushing_batch_length: 16
  flushing_batch_timeout: 5ms
  queue_capacity: 16
  max_segment_size: 1MB
  data_directory: "%s"
engine:
  type: in_memory
  clean_interval: 1h
  limit_event_queue_capacity: 16
  key_index: true
dump:
  interval: 1h
  directory: "%s"
replication:
  replica_type: master
  master_address: "%s"
  sync_interval: 1s
  auth:
    token_file: "%s"
logging:
  level: error
`,
		serverAddress,
		writeSecretFile(t, workDir, "admin.token", adminSecret),
		writeSecretFile(t, workDir, "rw.token", readWriteSecret),
		writeSecretFile(t, workDir, "ro.token", readOnlySecret),
		pki.ServerCertFile,
		pki.ServerKeyFile,
		pki.CAFile,
		walDir,
		dumpDir,
		replicationAddress,
		writeSecretFile(t, workDir, "replication-secured.token", replicationSecret),
	)

	require.NoError(t, os.WriteFile(configPath, []byte(config), 0o600))

	return configPath
}

func mutualTLSOptions(pki testPKI) TLSConfig {
	return TLSConfig{
		CAFile:     pki.CAFile,
		CertFile:   pki.ClientCertFile,
		KeyFile:    pki.ClientKeyFile,
		ServerName: "localhost",
	}
}

func connectSecuredClient(t *testing.T, address, token string, tlsConfig TLSConfig) *Client {
	t.Helper()

	var client *Client
	require.Eventually(t, func() bool {
		var err error
		client, err = New(address, 3*time.Second, 2, WithToken(token), WithTLS(tlsConfig))

		return err == nil
	}, 15*time.Second, 100*time.Millisecond)

	return client
}

func TestClientAgainstSecuredFQServer(t *testing.T) {
	requireCommand(t, "go")
	if os.Getenv("FQ_SERVER_DIR") == "" {
		requireCommand(t, "git")
	}

	workDir := t.TempDir()
	t.Cleanup(func() {
		_ = makeWritable(workDir)
	})

	serverAddress := freeLocalAddress(t)
	replicationAddress := freeLocalAddress(t)

	repoDir := cloneFQServer(t, workDir)
	serverBin := buildFQServer(t, workDir, repoDir)

	pki := newTestPKI(t, workDir, "localhost")
	configPath := writeSecuredFQServerConfig(t, workDir, serverAddress, replicationAddress, pki)

	server := startFQServer(t, serverBin, configPath)
	defer server.Stop(t)

	tlsConfig := mutualTLSOptions(pki)

	t.Run("admin token works over mutual tls", func(t *testing.T) {
		client := connectSecuredClient(t, serverAddress, adminSecret, tlsConfig)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		key := CappingKey{Key: "secured-admin", Capping: 60}
		value, err := client.Incr(ctx, key)
		require.NoError(t, err)
		require.Equal(t, uint64(1), value)

		flushed, err := client.FlushDB(ctx)
		require.NoError(t, err)
		require.True(t, flushed)
	})

	t.Run("read write token cannot flush", func(t *testing.T) {
		client := connectSecuredClient(t, serverAddress, readWriteSecret, tlsConfig)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		key := CappingKey{Key: "secured-rw", Capping: 60}
		value, err := client.Incr(ctx, key)
		require.NoError(t, err)
		require.Equal(t, uint64(1), value)

		_, err = client.FlushDB(ctx)
		requireProtocolErrorCode(t, err, CodePermissionDenied)
	})

	t.Run("read only token cannot write", func(t *testing.T) {
		client := connectSecuredClient(t, serverAddress, readOnlySecret, tlsConfig)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		key := CappingKey{Key: "secured-ro", Capping: 60}

		_, err := client.Get(ctx, key)
		require.NoError(t, err)

		_, err = client.Incr(ctx, key)
		requireProtocolErrorCode(t, err, CodePermissionDenied)
	})

	t.Run("wrong token is rejected", func(t *testing.T) {
		_, err := New(serverAddress, 3*time.Second, 1, WithToken("nope-nope-nope-nope"), WithTLS(tlsConfig))
		require.ErrorIs(t, err, ErrAuthFailed)
	})

	t.Run("missing token is rejected", func(t *testing.T) {
		client, err := New(serverAddress, 3*time.Second, 1, WithTLS(tlsConfig))
		if err == nil {
			defer client.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err = client.Incr(ctx, CappingKey{Key: "anon", Capping: 60})
		}

		requireProtocolErrorCode(t, err, CodeNotAuthenticated)
	})

	t.Run("client certificate is required", func(t *testing.T) {
		withoutCert := TLSConfig{CAFile: pki.CAFile, ServerName: "localhost"}

		_, err := New(serverAddress, 3*time.Second, 1, WithToken(adminSecret), WithTLS(withoutCert))
		require.Error(t, err)
	})

	t.Run("plaintext connection is refused", func(t *testing.T) {
		_, err := New(serverAddress, 3*time.Second, 1, WithToken(adminSecret))
		require.Error(t, err)
	})

	t.Run("reconnect stays authenticated", func(t *testing.T) {
		client := connectSecuredClient(t, serverAddress, adminSecret, tlsConfig)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		key := CappingKey{Key: "secured-reconnect", Capping: 60}
		_, err := client.Incr(ctx, key)
		require.NoError(t, err)

		conn, err := client.pool.GetConnection()
		require.NoError(t, err)
		require.NoError(t, conn.Reconnect())
		client.pool.ReleaseConnection(conn)

		value, err := client.Incr(ctx, key)
		require.NoError(t, err)
		require.Equal(t, uint64(2), value)
	})
}

func requireProtocolErrorCode(t *testing.T, err error, code ErrorCode) {
	t.Helper()

	var protocolErr *ProtocolError
	require.ErrorAs(t, err, &protocolErr)
	require.Equal(t, code, protocolErr.Code)
}
