package chpool

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/cht"
	"github.com/ClickHouse/ch-go/proto"
)

func PoolConnOpt(t testing.TB, opt Options) *Pool {
	t.Helper()

	ctx := context.Background()
	server := cht.New(t)

	if opt.ClientOptions.Logger == nil {
		opt.ClientOptions.Logger = zaptest.NewLogger(t)
	}

	opt.ClientOptions.Address = server.TCP
	pool, err := Dial(ctx, opt)
	require.NoError(t, err)

	t.Cleanup(func() {
		pool.Close()
	})

	return pool
}

func PoolConn(t testing.TB) *Pool {
	return PoolConnOpt(t, Options{})
}

type IDo interface {
	Do(ctx context.Context, q ch.Query) (err error)
}

func testDo(t *testing.T, do IDo) {
	var (
		numbers int
		data    proto.ColUInt64
	)

	err := do.Do(context.Background(), ch.Query{
		Body: "SELECT number FROM system.numbers LIMIT 10",
		OnResult: func(ctx context.Context, b proto.Block) error {
			numbers += len(data)
			return nil
		},
		Result: proto.Results{
			{Name: "number", Data: &data},
		},
	})

	require.NoError(t, err)
	require.Equal(t, 10, numbers)
}

func waitForReleaseToComplete() {
	time.Sleep(500 * time.Millisecond)
}
