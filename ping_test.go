package ch

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/ClickHouse/ch-go/proto"
)

func TestClient_Ping(t *testing.T) {
	t.Parallel()
	require.NoError(t, Conn(t).Ping(context.Background()))
}

func TestClientPingClosesOnCanceledWrite(t *testing.T) {
	clientConn, peer := net.Pipe()
	t.Cleanup(func() { require.NoError(t, clientConn.Close()) })
	t.Cleanup(func() { require.NoError(t, peer.Close()) })
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(10*time.Second)))

	conn := &notifyWriteConn{Conn: clientConn, started: make(chan struct{})}
	c := &Client{
		conn:   conn,
		lg:     zap.NewNop(),
		writer: proto.NewWriter(conn, new(proto.Buffer)),
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Ping(ctx) }()
	select {
	case <-conn.started:
		cancel()
	case err := <-done:
		t.Fatalf("ping returned before writing: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("ping did not start writing")
	}

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("ping did not return after cancellation")
	}

	require.True(t, c.IsClosed())
	_, err := peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}

func TestClientPingClosesWhenPongTimesOut(t *testing.T) {
	conn, peer := net.Pipe()
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	t.Cleanup(func() { require.NoError(t, peer.Close()) })
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(10*time.Second)))

	c := &Client{
		conn:        conn,
		lg:          zap.NewNop(),
		writer:      proto.NewWriter(conn, new(proto.Buffer)),
		reader:      proto.NewReader(conn),
		readTimeout: time.Second,
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Ping(ctx) }()

	var packet [1]byte
	_, err := io.ReadFull(peer, packet[:])
	require.NoError(t, err)
	require.Equal(t, byte(proto.ClientCodePing), packet[0])

	select {
	case err := <-done:
		var timeout net.Error
		require.ErrorAs(t, err, &timeout)
		require.True(t, timeout.Timeout(), "expected a read timeout, got %v", err)
	case <-ctx.Done():
		t.Fatal("ping did not return after the read timeout")
	}

	require.True(t, c.IsClosed())
	_, err = peer.Read(packet[:])
	require.ErrorIs(t, err, io.EOF)
}
