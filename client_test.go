package ch

import (
	"context"
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/ClickHouse/ch-go/cht"
	"github.com/ClickHouse/ch-go/internal/gold"
	pkgVersion "github.com/ClickHouse/ch-go/internal/version"
	"github.com/ClickHouse/ch-go/proto"
)

func TestMain(m *testing.M) {
	// Explicitly registering flags for golden files.
	gold.Init()

	os.Exit(m.Run())
}

func ConnOpt(t testing.TB, opt Options) *Client {
	t.Helper()

	ctx := context.Background()
	server := cht.New(t)

	if opt.Logger == nil {
		opt.Logger = zaptest.NewLogger(t)
	}

	opt.Address = server.TCP
	client, err := Dial(ctx, opt)
	require.NoError(t, err)

	t.Log("Connected", client.ServerInfo())
	t.Cleanup(func() {
		if !client.IsClosed() {
			require.NoError(t, client.Close())
		}
	})

	return client
}

func Conn(t testing.TB) *Client {
	return ConnOpt(t, Options{})
}

func TestFormatClientName(t *testing.T) {
	metadata := expectedClientMetadata()

	require.Equal(t, "clickhouse/ch-go "+metadata, formatClientName("", pkgVersion.Value{}))
	require.Equal(t, "clickhouse/ch-go (alpha.1) "+metadata, formatClientName("", pkgVersion.Value{Name: "alpha.1"}))
	require.Equal(
		t,
		"storage/v1.85.2 clickhouse/ch-go/0.71.0 "+metadata,
		formatClientName("storage/v1.85.2", pkgVersion.Value{Raw: "v0.71.0", Major: 0, Minor: 71, Patch: 0}),
	)
	require.Equal(
		t,
		"kafka-consumer/v1.85.2 clickhouse/ch-go/0.71.0 "+metadata,
		formatClientName("kafka-consumer/v1.85.2", pkgVersion.Value{Major: 0, Minor: 71, Patch: 0}),
	)
}

func expectedClientMetadata() string {
	return "(lv:go/" + strings.TrimPrefix(runtime.Version(), "go") + "; os:" + runtime.GOOS + ")"
}

func SkipNoFeature(t *testing.T, client *Client, feature proto.Feature) {
	if !client.ServerInfo().Has(feature) {
		t.Skipf("Skipping (feature %q not supported)", feature)
	}
}

func TestDial(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		conn := Conn(t)
		require.NoError(t, conn.Ping(context.Background()))
	})
	t.Run("Closed", func(t *testing.T) {
		ctx := context.Background()
		server := cht.New(t)
		conn, err := Dial(ctx, Options{
			Address: server.TCP,
		})
		require.NoError(t, err)
		require.NoError(t, conn.Ping(ctx))
		require.NoError(t, conn.Close())
		require.ErrorIs(t, conn.Ping(ctx), ErrClosed)
		require.ErrorIs(t, conn.Do(ctx, Query{}), ErrClosed)
	})
	t.Run("DatabaseNotFound", func(t *testing.T) {
		ctx := context.Background()
		server := cht.New(t)
		client, err := Dial(ctx, Options{
			Address:  server.TCP,
			Database: "bad",
		})
		if IsErr(err, proto.ErrUnknownDatabase) {
			t.Skip("got error during handshake")
		}
		require.NoError(t, err)
		err = client.Do(ctx, Query{
			Body:   "SELECT 1",
			Result: discardResult(),
		})
		require.True(t, IsErr(err, proto.ErrUnknownDatabase))
	})
}

func TestExceptionUnwrap(t *testing.T) {
	flat := &Exception{
		Code:    proto.ErrReadonly,
		Name:    "foo",
		Message: "bar",
	}

	if !errors.Is(flat, proto.ErrReadonly) {
		t.Fatal("flat exception must be the error code it represents")
	}
}
