package ch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/proto"
)

func TestQueryParameters(t *testing.T) {
	conn := Conn(t)
	if !conn.ServerInfo().Has(proto.FeatureParameters) {
		t.Skip("Skipping (not supported)")
	}
	ctx := context.Background()
	require.NoError(t, conn.Do(ctx, Query{
		Body: "select {num:UInt8} v, {str:String} s",
		Parameters: Parameters(map[string]any{
			"num": 100,
			"str": "foo",
		}),
		Result: discardResult(),
	}))
}
