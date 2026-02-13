package ch

import (
	"context"
	"sync"
)

type (
	ctxQueryKey  struct{}
	queryMetrics struct {
		ColumnsReceived int
		RowsReceived    int
		BlocksReceived  int
		BlocksSent      int
		Rows            int
		Bytes           int
		Lock            sync.Mutex
	}
	queryMetricsDelta struct {
		ColumnsReceived int
		RowsReceived    int
		BlocksReceived  int
		BlocksSent      int
		Rows            int
		Bytes           int
	}
)

func (q *queryMetrics) Observe(delta queryMetricsDelta) {
	q.Lock.Lock()
	defer q.Lock.Unlock()

	q.Bytes += delta.Bytes
	q.Rows += delta.Rows
	q.RowsReceived += delta.RowsReceived
	q.BlocksReceived += delta.BlocksReceived
	q.BlocksSent += delta.BlocksSent

	if delta.ColumnsReceived > 0 {
		q.ColumnsReceived = delta.ColumnsReceived
	}
}

func (c *Client) metricsInc(ctx context.Context, delta queryMetricsDelta) {
	if !c.otel {
		return
	}
	v, ok := ctx.Value(ctxQueryKey{}).(*queryMetrics)
	if !ok {
		return
	}
	v.Observe(delta)
}
