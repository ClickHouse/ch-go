package cht_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch"
	"github.com/go-faster/ch/internal/cht"
	"github.com/go-faster/ch/internal/proto"
)

func TestRun(t *testing.T) {
	ctx := context.Background()
	server := cht.Connect(t)

	client, err := ch.Dial(ctx, server.TCP, ch.Options{})
	require.NoError(t, err)
	t.Log("Connected", client.ServerInfo(), client.Location())

	// Sending query.
	require.NoError(t, client.SendQuery(ctx, "CREATE TABLE test_table (id UInt64) ENGINE = MergeTree ORDER BY id", "1"))

	p, err := client.Packet()
	require.NoError(t, err)

	switch p {
	case proto.ServerCodeEndOfStream: // expected
		t.Log("Query sent")
	default:
		t.Fatal("unexpected server code", p)
	}

	// Select
	require.NoError(t, client.SendQuery(ctx, "SELECT 1 AS one", "2"))

	var fetched bool

Fetch:
	for {
		p, err = client.Packet()
		require.NoError(t, err)

		switch p {
		case proto.ServerCodeData:
			t.Log("Data received")
			b, err := client.Block()
			require.NoError(t, err)

			if fetched && b.Columns == 0 && b.Rows == 0 {
				t.Log("End of data")
				break Fetch
			}

			t.Log(b, b.Info)

			require.Equal(t, 1, b.Columns)
			require.Len(t, b.Data, 1)

			c := b.Data[0]
			require.Equal(t, "one", c.Name)
			require.Equal(t, proto.ColumnTypeUInt8, c.Type)

			if b.Rows > 0 {
				br := proto.NewReader(bytes.NewReader(c.Data))
				v, err := br.UInt8()

				require.NoError(t, err)
				require.Equal(t, byte(1), v)
				t.Log("Fetched", c.Name, "=", v)

				fetched = true
			}
		case proto.ServerCodeProgress:
			p, err := client.Progress()
			require.NoError(t, err)

			t.Logf("%+v", p)
		case proto.ServerCodeProfile:
			p, err := client.Profile()
			require.NoError(t, err)

			t.Logf("%+v", p)
		case proto.ServerCodeEndOfStream:
			break Fetch
		default:
			t.Fatal("unexpected server code", p)
		}
	}

	require.NoError(t, client.Close())
}
