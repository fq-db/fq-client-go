package fq

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientInspectCommand(t *testing.T) {
	t.Parallel()

	replicaID := "replica-1"
	syncCommit := "off"

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			return err
		}

		request, err := readFrame(connection, 2048)
		if err != nil {
			return err
		}
		if string(request) != "INSPECT" {
			return fmt.Errorf("unexpected request: %q", string(request))
		}

		if err := writeFrame(connection, []byte(`nxt|{"section":"summary","ts":1788110417,`)); err != nil {
			return err
		}
		if err := writeFrame(connection, []byte(`nxt|"instance":{"role":"slave","replica_id":"replica-1"},`)); err != nil {
			return err
		}

		return writeFrame(connection, []byte(`ok|"persistence":{"mode":"wal_and_dump","sync_commit":"off"}}`))
	})

	client, err := New(address, time.Minute, 1)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done)
	}()

	report, err := client.Inspect(context.Background(), InspectSectionSummary)
	require.NoError(t, err)
	require.Equal(t, InspectReport{
		Section: "summary",
		TS:      1788110417,
		Instance: &InstanceInfo{
			Role:      "slave",
			ReplicaID: &replicaID,
		},
		Persistence: &PersistenceInfo{
			Mode:       "wal_and_dump",
			SyncCommit: &syncCommit,
		},
	}, report)
}

func TestClientInspectCommandWithSection(t *testing.T) {
	t.Parallel()

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			return err
		}

		return requireRequestAndRespond(
			connection, "INSPECT ENGINE", `ok|{"section":"engine","ts":1788110417,"engine":{"partitions":4,"counters":51}}`,
		)
	})

	client, err := New(address, time.Minute, 1)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done)
	}()

	report, err := client.Inspect(context.Background(), InspectSectionEngine)
	require.NoError(t, err)
	require.Equal(t, InspectReport{
		Section: "engine",
		TS:      1788110417,
		Engine: &EngineInfo{
			Partitions: 4,
			Counters:   51,
		},
	}, report)
}

func TestClientInspectCommandError(t *testing.T) {
	t.Parallel()

	address, done := serveFramedClient(t, func(connection net.Conn) error {
		if err := requireRequestAndRespond(connection, CommandMsgSize, "ok|2048"); err != nil {
			return err
		}

		return requireRequestAndRespond(connection, "INSPECT BOGUS", `err|unknown inspect section: "BOGUS"`)
	})

	client, err := New(address, time.Minute, 1)
	require.NoError(t, err)
	defer func() {
		client.Close()
		require.NoError(t, <-done)
	}()

	_, err = client.Inspect(context.Background(), "BOGUS")
	require.EqualError(t, err, `unknown inspect section: "BOGUS"`)
}
