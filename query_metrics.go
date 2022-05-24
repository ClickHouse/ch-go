package ch

import "context"

type (
	ctxQueryKey  struct{}
	queryMetrics struct {
		ColumnsReceived int
		RowsReceived    int
		BlocksReceived  int
		BlocksSent      int
		Rows            int
		Bytes           int
	}
)

func (c *Client) metricsInc(ctx context.Context, delta queryMetrics) {
	if !c.otel {
		return
	}
	v, ok := ctx.Value(ctxQueryKey{}).(*queryMetrics)
	if !ok {
		return
	}

	v.Bytes += delta.Bytes
	v.Rows += delta.Rows
	v.RowsReceived += delta.RowsReceived
	v.BlocksReceived += delta.BlocksReceived
	v.BlocksSent += delta.BlocksSent

	if delta.ColumnsReceived > 0 {
		v.ColumnsReceived = delta.ColumnsReceived
	}
}
