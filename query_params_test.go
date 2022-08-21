package ch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/proto"
)

func TestQueryParameters(t *testing.T) {
	conn := ConnOpt(t, Options{QuotaKey: ""})
	if !conn.ServerInfo().Has(proto.FeatureParameters) {
		t.Skip("Skipping (not supported)")
	}
	ctx := context.Background()
	data := new(proto.ColUInt8)
	require.NoError(t, conn.Do(ctx, Query{
		Body: "select {num:UInt8} v",
		Parameters: []proto.Parameter{
			{Key: "num", Value: `'1'`},
		},
		Result: proto.Results{
			{Name: "v", Data: data},
		},
	}))
}
