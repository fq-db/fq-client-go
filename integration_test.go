//go:build integration

package fq

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const defaultFQServerRepo = "https://github.com/rom8726/fq.git"

func TestClientAgainstRealFQServer(t *testing.T) {
	requireCommand(t, "git")
	requireCommand(t, "go")

	workDir := t.TempDir()
	t.Cleanup(func() {
		_ = makeWritable(workDir)
	})

	serverAddress := freeLocalAddress(t)
	replicationAddress := freeLocalAddress(t)

	repoDir := cloneFQServer(t, workDir)
	serverBin := buildFQServer(t, workDir, repoDir)
	configPath := writeFQServerConfig(t, workDir, serverAddress, replicationAddress)

	server := startFQServer(t, serverBin, configPath)
	defer server.Stop(t)

	client := connectRealClient(t, serverAddress)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	t.Run("counters", func(t *testing.T) {
		key := CappingKey{Key: "integration-key", Capping: 60}
		value, err := client.Incr(ctx, key)
		require.NoError(t, err)
		require.Equal(t, uint64(1), value)

		value, err = client.Incr(ctx, key)
		require.NoError(t, err)
		require.Equal(t, uint64(2), value)

		value, err = client.Get(ctx, key)
		require.NoError(t, err)
		require.Equal(t, uint64(2), value)

		other := CappingKey{Key: "integration-other", Capping: 60}
		_, err = client.Incr(ctx, other)
		require.NoError(t, err)

		deleted, err := client.MDel(ctx, []CappingKey{key, other})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true}, deleted)

		value, err = client.Get(ctx, key)
		require.NoError(t, err)
		require.Equal(t, uint64(0), value)
	})

	t.Run("rlimit", func(t *testing.T) {
		t.Run("fixed_window", func(t *testing.T) {
			limitKey := LimitKey{Key: "integration-limit", Window: 60}
			fw, err := client.RLimitFixedWindow(ctx, limitKey, 2)
			require.NoError(t, err)
			require.Equal(t, RateLimitResult{
				Allowed:   true,
				Current:   1,
				Remaining: 1,
			}, rateLimitResultWithoutResetAfter(fw))

			fw, err = client.RLimitFixedWindow(ctx, limitKey, 2)
			require.NoError(t, err)
			require.Equal(t, RateLimitResult{
				Allowed:   true,
				Current:   2,
				Remaining: 0,
			}, rateLimitResultWithoutResetAfter(fw))

			fw, err = client.RLimitFixedWindow(ctx, limitKey, 2)
			require.NoError(t, err)
			require.Equal(t, RateLimitResult{
				Allowed:   false,
				Current:   2,
				Remaining: 0,
			}, rateLimitResultWithoutResetAfter(fw))
			require.LessOrEqual(t, fw.ResetAfter, limitKey.Window)
		})

		t.Run("sliding_window", func(t *testing.T) {
			limitKey := LimitKey{Key: "integration-sliding", Window: 60}
			sw, err := client.RLimitSlidingWindow(ctx, limitKey, 2)
			require.NoError(t, err)
			require.Equal(t, RateLimitResult{
				Allowed:   true,
				Current:   1,
				Remaining: 1,
			}, rateLimitResultWithoutResetAfter(sw))

			sw, err = client.RLimitSlidingWindow(ctx, limitKey, 2)
			require.NoError(t, err)
			require.Equal(t, RateLimitResult{
				Allowed:   true,
				Current:   2,
				Remaining: 0,
			}, rateLimitResultWithoutResetAfter(sw))

			sw, err = client.RLimitSlidingWindow(ctx, limitKey, 2)
			require.NoError(t, err)
			require.Equal(t, RateLimitResult{
				Allowed:   false,
				Current:   2,
				Remaining: 0,
			}, rateLimitResultWithoutResetAfter(sw))
			require.LessOrEqual(t, sw.ResetAfter, limitKey.Window)
		})

		t.Run("token_bucket", func(t *testing.T) {
			limitKey := LimitKey{Key: "integration-bucket", Window: 60}
			tb, err := client.RLimitTokenBucket(ctx, limitKey, 2, 1)
			require.NoError(t, err)
			require.Equal(t, RateLimitResult{
				Allowed:   true,
				Current:   1,
				Remaining: 1,
			}, rateLimitResultWithoutResetAfter(tb))

			tb, err = client.RLimitTokenBucket(ctx, limitKey, 2, 1)
			require.NoError(t, err)
			require.Equal(t, RateLimitResult{
				Allowed:   true,
				Current:   2,
				Remaining: 0,
			}, rateLimitResultWithoutResetAfter(tb))

			tb, err = client.RLimitTokenBucket(ctx, limitKey, 2, 1)
			require.NoError(t, err)
			require.Equal(t, RateLimitResult{
				Allowed:   false,
				Current:   2,
				Remaining: 0,
			}, rateLimitResultWithoutResetAfter(tb))
			require.LessOrEqual(t, tb.ResetAfter, limitKey.Window)
		})
	})
}

func rateLimitResultWithoutResetAfter(result RateLimitResult) RateLimitResult {
	result.ResetAfter = 0

	return result
}

type fqServerProcess struct {
	cancel context.CancelFunc
	cmd    *exec.Cmd
	done   chan error
	output *bytes.Buffer
}

func (p *fqServerProcess) Stop(t *testing.T) {
	t.Helper()

	if p.cmd.Process != nil {
		_ = p.cmd.Process.Signal(os.Interrupt)
	}

	select {
	case err := <-p.done:
		if err != nil {
			t.Logf("fq server exited with error: %v\n%s", err, p.output.String())
		}
	case <-time.After(5 * time.Second):
		p.cancel()
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
		select {
		case <-p.done:
		case <-time.After(time.Second):
		}
		t.Fatalf("fq server did not stop gracefully\n%s", p.output.String())
	}
}

func cloneFQServer(t *testing.T, workDir string) string {
	t.Helper()

	repoURL := getenvDefault("FQ_SERVER_REPO", defaultFQServerRepo)
	repoRef := os.Getenv("FQ_SERVER_REF")
	repoDir := filepath.Join(workDir, "fq")

	args := []string{"clone", "--depth", "1"}
	if repoRef != "" {
		args = append(args, "--branch", repoRef)
	}
	args = append(args, repoURL, repoDir)

	runCommand(t, workDir, nil, "git", args...)

	return repoDir
}

func buildFQServer(t *testing.T, workDir, repoDir string) string {
	t.Helper()

	serverBin := filepath.Join(workDir, "fq-server")
	env := append(os.Environ(),
		"GOCACHE="+filepath.Join(workDir, "gocache"),
		"GOMODCACHE="+filepath.Join(workDir, "gomodcache"),
	)

	runCommand(t, repoDir, env, "go", "build", "-o", serverBin, "./cmd/fq")

	return serverBin
}

func writeFQServerConfig(t *testing.T, workDir, serverAddress, replicationAddress string) string {
	t.Helper()

	configPath := filepath.Join(workDir, "config.yml")
	walDir := filepath.Join(workDir, "wal")
	dumpDir := filepath.Join(workDir, "dump")
	require.NoError(t, os.MkdirAll(walDir, 0o750))
	require.NoError(t, os.MkdirAll(dumpDir, 0o750))

	config := fmt.Sprintf(`
network:
  address: "%s"
  max_connections: 16
  max_message_size: 64KB
  idle_timeout: 5s
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
dump:
  interval: 1h
  directory: "%s"
replication:
  replica_type: master
  master_address: "%s"
  sync_interval: 1s
logging:
  level: error
`, serverAddress, walDir, dumpDir, replicationAddress)

	require.NoError(t, os.WriteFile(configPath, []byte(config), 0o600))

	return configPath
}

func startFQServer(t *testing.T, serverBin, configPath string) *fqServerProcess {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, serverBin, configPath)
	output := &bytes.Buffer{}
	cmd.Stdout = output
	cmd.Stderr = output

	require.NoError(t, cmd.Start())

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	return &fqServerProcess{
		cancel: cancel,
		cmd:    cmd,
		done:   done,
		output: output,
	}
}

func connectRealClient(t *testing.T, address string) *Client {
	t.Helper()

	var client *Client
	require.Eventually(t, func() bool {
		var err error
		client, err = New(address, time.Second, 2)

		return err == nil
	}, 15*time.Second, 100*time.Millisecond)

	return client
}

func freeLocalAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, listener.Close())
	}()

	return listener.Addr().String()
}

func runCommand(t *testing.T, dir string, env []string, name string, args ...string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	if env != nil {
		cmd.Env = env
	}

	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s %v\n%s", name, args, string(output))
}

func requireCommand(t *testing.T, name string) {
	t.Helper()

	_, err := exec.LookPath(name)
	require.NoError(t, err)
}

func getenvDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func makeWritable(root string) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			return os.Chmod(path, 0o700)
		}

		return os.Chmod(path, 0o600)
	})
}
