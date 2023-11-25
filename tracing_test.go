package ch

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ClickHouse/ch-go/proto"
)

type randomIDGenerator struct {
	sync.Mutex
	rand *rand.Rand
}

// NewSpanID returns a non-zero span ID from a randomly-chosen sequence.
func (gen *randomIDGenerator) NewSpanID(_ context.Context, _ trace.TraceID) (sid trace.SpanID) {
	gen.Lock()
	defer gen.Unlock()
	gen.rand.Read(sid[:])
	return sid
}

// NewIDs returns a non-zero trace ID and a non-zero span ID from a
// randomly-chosen sequence.
func (gen *randomIDGenerator) NewIDs(_ context.Context) (tid trace.TraceID, sid trace.SpanID) {
	gen.Lock()
	defer gen.Unlock()
	gen.rand.Read(tid[:])
	gen.rand.Read(sid[:])
	return tid, sid
}

func TestClient_Do_tracing(t *testing.T) {
	ctx := context.Background()
	exporter := tracetest.NewInMemoryExporter()
	randSource := rand.NewSource(15)
	tp := tracesdk.NewTracerProvider(
		// Using deterministic random ids.
		tracesdk.WithIDGenerator(&randomIDGenerator{
			rand: rand.New(randSource),
		}),
		tracesdk.WithBatcher(exporter,
			tracesdk.WithBatchTimeout(0), // instant
		),
	)
	conn := ConnOpt(t, Options{
		ProtocolVersion:              54454,
		OpenTelemetryInstrumentation: true,
		TracerProvider:               tp,
		Settings: []Setting{
			{
				Key:       "send_logs_level",
				Value:     "trace",
				Important: true,
			},
		},
	})

	if v := conn.ServerInfo(); (v.Major < 22) || (v.Major == 22 && v.Minor < 2) {
		t.Skip("Skipping (not supported)")
	}

	// Should record trace and spans.
	var traceID trace.TraceID
	require.NoError(t, conn.Do(ctx, Query{
		Body:   "SELECT 1",
		Result: discardResult(),
		OnLogs: func(ctx context.Context, logs []Log) error {
			sc := trace.SpanContextFromContext(ctx)
			traceID = sc.TraceID()
			for _, l := range logs {
				t.Log(l.Text, sc.TraceID(), sc.SpanID())
			}
			return nil
		},
	}))

	require.True(t, traceID.IsValid(), "trace id not registered")

	// Force flushing.
	require.NoError(t, tp.ForceFlush(ctx))
	spans := exporter.GetSpans()
	require.NotEmpty(t, spans)
	require.NoError(t, conn.Do(ctx, Query{Body: "system flush logs"}))

	var total proto.ColUInt64
	require.NoError(t, conn.Do(ctx, Query{
		Body: fmt.Sprintf("SELECT count() as total FROM system.opentelemetry_span_log WHERE lower(hex(trace_id)) = '%s'", traceID),
		Result: proto.Results{
			{Name: "total", Data: &total},
		},
	}))

	require.Greater(t, total.Row(0), uint64(1), "spans should be recorded")

	var traceIDs proto.ColUUID
	require.NoError(t, conn.Do(ctx, Query{
		Body: fmt.Sprintf("SELECT trace_id FROM system.opentelemetry_span_log WHERE lower(hex(trace_id)) = '%s' LIMIT 1", traceID),
		Result: proto.Results{
			{Name: "trace_id", Data: &traceIDs},
		},
	}))
	require.Equal(t, traceIDs[0][:], traceID[:])
}
