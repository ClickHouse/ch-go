package ch

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/go-faster/city"
	"github.com/go-faster/errors"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/ch-go/otelch"
	"github.com/ClickHouse/ch-go/proto"
)

// cancelQuery cancels current query.
func (c *Client) cancelQuery() error {
	c.lg.Warn("Cancel query")

	const cancelDeadline = time.Second * 1
	ctx, cancel := context.WithTimeout(context.Background(), cancelDeadline)
	defer cancel()

	// Not using c.buf to prevent data race.
	b := proto.Buffer{
		Buf: make([]byte, 1),
	}
	proto.ClientCodeCancel.Encode(&b)
	if err := c.flushBuf(ctx, &b); err != nil {
		return errors.Wrap(err, "flush")
	}

	// Closing connection to prevent further queries.
	if err := c.Close(); err != nil {
		return errors.Wrap(err, "close")
	}

	return nil
}

func (c *Client) querySettings(q Query) []proto.Setting {
	var result []proto.Setting
	for _, s := range c.settings {
		result = append(result, proto.Setting{
			Key:       s.Key,
			Value:     s.Value,
			Important: s.Important,
		})
	}
	for _, s := range q.Settings {
		result = append(result, proto.Setting{
			Key:       s.Key,
			Value:     s.Value,
			Important: s.Important,
		})
	}
	return result
}

// sendQuery starts query.
func (c *Client) sendQuery(ctx context.Context, q Query) error {
	if ce := c.lg.Check(zap.DebugLevel, "sendQuery"); ce != nil {
		ce.Write(
			zap.String("query", q.Body),
			zap.String("query_id", q.QueryID),
		)
	}
	if c.IsClosed() {
		return ErrClosed
	}
	c.encode(proto.Query{
		ID:          q.QueryID,
		Body:        q.Body,
		Secret:      q.Secret,
		Stage:       proto.StageComplete,
		Compression: c.compression,
		Settings:    c.querySettings(q),
		Parameters:  q.Parameters,
		Info: proto.ClientInfo{
			ProtocolVersion: c.protocolVersion,
			Major:           c.version.Major,
			Minor:           c.version.Minor,
			Patch:           c.version.Patch,
			Interface:       proto.InterfaceTCP,
			Query:           proto.ClientQueryInitial,

			InitialUser:    q.InitialUser,
			InitialQueryID: q.QueryID,
			InitialAddress: c.conn.LocalAddr().String(),
			OSUser:         "",
			ClientHostname: "",
			ClientName:     c.version.Name,

			Span:     trace.SpanContextFromContext(ctx),
			QuotaKey: q.QuotaKey,
		},
	})

	// Encoding external data if provided.
	if len(q.ExternalData) > 0 {
		if q.ExternalTable == "" {
			// Resembling behavior of clickhouse-client.
			q.ExternalTable = "_data"
		}
		if err := c.encodeBlock(ctx, q.ExternalTable, q.ExternalData); err != nil {
			return errors.Wrap(err, "external data")
		}
	}
	// End of external data.
	if err := c.encodeBlankBlock(ctx); err != nil {
		return errors.Wrap(err, "external data end")
	}

	return nil
}

// Query to ClickHouse.
type Query struct {
	// Body of query, like "SELECT 1".
	Body string
	// QueryID is ID of query, defaults to new UUIDv4.
	QueryID string
	// QuotaKey of query, optional.
	QuotaKey string

	// Input columns for INSERT operations.
	Input proto.Input
	// OnInput is called to allow ingesting more data to Input.
	//
	// The io.EOF reports that no more input should be ingested.
	//
	// Optional, single block is ingested from Input if not provided,
	// but query will fail if Input is set but has zero rows.
	OnInput func(ctx context.Context) error

	// Result columns for SELECT operations.
	Result proto.Result
	// OnResult is called when Result is filled with result block.
	//
	// Optional, but query will fail of more than one block is received
	// and no OnResult is provided.
	OnResult func(ctx context.Context, block proto.Block) error

	// OnProgress is optional progress handler. The progress value contain
	// difference, so progress should be accumulated if needed.
	OnProgress func(ctx context.Context, p proto.Progress) error
	// OnProfile is optional handler for profiling data.
	OnProfile func(ctx context.Context, p proto.Profile) error
	// OnProfileEvent is optional handler for profiling event stream data.
	OnProfileEvent func(ctx context.Context, e ProfileEvent) error
	// OnLog is optional handler for server log entry.
	OnLog func(ctx context.Context, l Log) error

	// Settings are optional query-scoped settings. Can override client settings.
	Settings []Setting

	// EXPERIMENTAL: parameters for query.
	Parameters []proto.Parameter

	// Secret is optional inter-server per-cluster secret for Distributed queries.
	//
	// See https://clickhouse.com/docs/en/engines/table-engines/special/distributed/#distributed-clusters
	Secret string

	// InitialUser is optional initial user for Distributed queries.
	InitialUser string

	// ExternalData is optional data for server to load.
	//
	// https://clickhouse.com/docs/en/engines/table-engines/special/external-data/
	ExternalData []proto.InputColumn
	// ExternalTable name. Defaults to _data.
	ExternalTable string
}

// CorruptedDataErr means that provided hash mismatch with calculated.
type CorruptedDataErr struct {
	Actual    city.U128
	Reference city.U128
	RawSize   int
	DataSize  int
}

func (c *CorruptedDataErr) Error() string {
	return fmt.Sprintf("corrupted data: %s (actual), %s (reference), compressed size: %d, data size: %d",
		compress.FormatU128(c.Actual), compress.FormatU128(c.Reference), c.RawSize, c.DataSize,
	)
}

type decodeOptions struct {
	Handler         func(ctx context.Context, b proto.Block) error
	Result          proto.Result
	ProtocolVersion int
	Compressible    bool
}

func (c *Client) decodeBlock(ctx context.Context, opt decodeOptions) error {
	if opt.ProtocolVersion == 0 {
		opt.ProtocolVersion = c.protocolVersion
	}
	if proto.FeatureTempTables.In(opt.ProtocolVersion) {
		v, err := c.reader.Str()
		if err != nil {
			return errors.Wrap(err, "temp table")
		}
		if v != "" {
			return errors.Errorf("unexpected temp table %q", v)
		}
	}
	var block proto.Block
	if c.compression == proto.CompressionEnabled && opt.Compressible {
		c.reader.EnableCompression()
		defer c.reader.DisableCompression()
	}
	if err := block.DecodeBlock(c.reader, opt.ProtocolVersion, opt.Result); err != nil {
		var badData *compress.CorruptedDataErr
		if errors.As(err, &badData) {
			// Returning wrapped exported error to allow user matching.
			exportedErr := CorruptedDataErr(*badData)
			return errors.Wrap(&exportedErr, "bad block")
		}
		return errors.Wrap(err, "decode block")
	}
	if ce := c.lg.Check(zap.DebugLevel, "Block"); ce != nil {
		ce.Write(
			zap.Int("rows", block.Rows),
			zap.Int("columns", block.Columns),
		)
	}
	if block.End() {
		return nil
	}
	c.metricsInc(ctx, queryMetrics{
		BlocksReceived:  1,
		RowsReceived:    block.Rows,
		ColumnsReceived: block.Columns,
	})
	if err := opt.Handler(ctx, block); err != nil {
		return errors.Wrap(err, "handler")
	}
	return nil
}

// encodeBlock encodes data block into buf, performing compression if needed.
//
// If input length is zero, blank block will be encoded, which is special case
// for "end of data".
func (c *Client) encodeBlock(ctx context.Context, tableName string, input []proto.InputColumn) error {
	proto.ClientCodeData.Encode(c.buf)
	clientData := proto.ClientData{
		// External data table name.
		// https://clickhouse.com/docs/en/engines/table-engines/special/external-data/
		TableName: tableName,
	}
	clientData.EncodeAware(c.buf, c.protocolVersion)

	// Saving offset of compressible data.
	start := len(c.buf.Buf)
	b := proto.Block{
		Columns: len(input),
	}
	if len(input) > 0 {
		c.metricsInc(ctx, queryMetrics{BlocksSent: 1})
		b.Rows = input[0].Data.Rows()
		b.Info = proto.BlockInfo{
			// TODO(ernado): investigate and document
			BucketNum: -1,
		}
	}
	if err := b.EncodeBlock(c.buf, c.protocolVersion, input); err != nil {
		return errors.Wrap(err, "encode")
	}

	// Performing compression.
	//
	// Note: only blocks are compressed.
	// See "Compressible" method of server or client code for reference.
	if c.compression == proto.CompressionEnabled {
		data := c.buf.Buf[start:]
		if err := c.compressor.Compress(c.compressionMethod, data); err != nil {
			return errors.Wrap(err, "compress")
		}
		c.buf.Buf = append(c.buf.Buf[:start], c.compressor.Data...)
	}

	return nil
}

// encodeBlankBlock encodes block with zero columns and rows which is special
// case for "end of data".
func (c *Client) encodeBlankBlock(ctx context.Context) error {
	return c.encodeBlock(ctx, "", nil)
}

func (c *Client) sendInput(ctx context.Context, info proto.ColInfoInput, q Query) error {
	if len(q.Input) == 0 {
		return nil
	}
	// Handling input columns that require inference, e.g. enums.
	for _, v := range info {
		for _, inCol := range q.Input {
			infer, ok := inCol.Data.(proto.Inferable)
			if !ok || inCol.Name != v.Name {
				continue
			}
			c.lg.Debug("Inferring column",
				zap.String("column.name", v.Name),
				zap.Stringer("column.type", v.Type),
			)
			if err := infer.Infer(v.Type); err != nil {
				return errors.Wrapf(err, "infer %q", inCol.Name)
			}
		}
	}
	var (
		rows = q.Input[0].Data.Rows()
		f    = q.OnInput
	)
	if f != nil && rows == 0 {
		// Fetching initial input if no rows provided.
		if err := f(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				goto End // initial input was blank
			}
			return errors.Wrap(err, "input")
		}
	}
	// Streaming input to ClickHouse server.
	//
	// NB: atomicity is guaranteed only within single block.
	for {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "context")
		}
		if err := c.encodeBlock(ctx, "", q.Input); err != nil {
			return errors.Wrap(err, "write block")
		}
		if f == nil {
			// No callback, single block.
			break
		}
		// Flushing the buffer to prevent high memory consumption.
		if err := c.flush(ctx); err != nil {
			return errors.Wrap(err, "flush")
		}
		if err := f(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				// No more data.
				if tailRows := q.Input[0].Data.Rows(); tailRows > 0 {
					// Write data tail on next tick and break.
					//
					// This is required to resemble io.Reader behavior.
					if ce := c.lg.Check(zap.DebugLevel, "Writing tail of input data (not empty and io.EOF)"); ce != nil {
						ce.Write(
							zap.Int("rows", tailRows),
						)
					}
					f = nil
					continue
				}

				break
			}
			// ClickHouse server persists blocks after receive.
			return errors.Wrap(err, "next input (server already persisted previous blocks)")
		}
	}
End:
	// End of input stream.
	//
	// Encoding that there are no more data.
	if err := c.encodeBlankBlock(ctx); err != nil {
		return errors.Wrap(err, "write end of data")
	}

	return nil
}

func (c *Client) resultHandler(q Query) func(ctx context.Context, b proto.Block) error {
	if q.OnResult != nil {
		return q.OnResult
	}
	first := true
	return func(ctx context.Context, block proto.Block) error {
		if !first {
			return errors.New("no OnResult provided")
		}
		if block.Rows > 0 {
			// Server can send block with zero rows on start,
			// providing a way to check column metadata.
			first = false
		}
		return nil
	}
}

type (
	ProfileEvent     = proto.ProfileEvent
	ProfileEventType = proto.ProfileEventType
	Log              = proto.Log
)

func (c *Client) handlePacket(ctx context.Context, p proto.ServerCode, q Query) error {
	switch p {
	case proto.ServerCodeException:
		e, err := c.exception()
		if err != nil {
			return errors.Wrap(err, "decode exception")
		}
		return e
	case proto.ServerCodeProgress:
		p, err := c.progress()
		if err != nil {
			return errors.Wrap(err, "progress")
		}
		c.metricsInc(ctx, queryMetrics{Rows: int(p.Rows), Bytes: int(p.Bytes)})
		if ce := c.lg.Check(zap.DebugLevel, "Progress"); ce != nil {
			ce.Write(
				zap.Uint64("rows", p.Rows),
				zap.Uint64("total_rows", p.TotalRows),
				zap.Uint64("bytes", p.Bytes),
				zap.Uint64("wrote_bytes", p.WroteBytes),
				zap.Uint64("wrote_rows", p.WroteRows),
			)
		}
		if f := q.OnProgress; f != nil {
			if err := f(ctx, p); err != nil {
				return errors.Wrap(err, "progress")
			}
		}
		return nil
	case proto.ServerCodeProfile:
		p, err := c.profile()
		if err != nil {
			return errors.Wrap(err, "profile")
		}
		if ce := c.lg.Check(zap.DebugLevel, "Profile"); ce != nil {
			ce.Write(
				zap.Uint64("rows", p.Rows),
				zap.Uint64("bytes", p.Bytes),
				zap.Uint64("blocks", p.Blocks),
			)
		}
		if f := q.OnProfile; f != nil {
			if err := f(ctx, p); err != nil {
				return errors.Wrap(err, "profile")
			}
		}
		return nil
	case proto.ServerCodeTableColumns:
		// Ignoring for now.
		var info proto.TableColumns
		if err := c.decode(&info); err != nil {
			return errors.Wrap(err, "table columns")
		}
		return nil
	case proto.ServerProfileEvents:
		var data proto.ProfileEvents
		onResult := func(ctx context.Context, b proto.Block) error {
			events, err := data.All()
			if err != nil {
				return errors.Wrap(err, "events")
			}
			for _, e := range events {
				if ce := c.lg.Check(zap.DebugLevel, "ProfileEvent"); ce != nil {
					ce.Write(
						zap.Time("event.time", e.Time),
						zap.String("event.host_name", e.Host),
						zap.Uint64("event.thread_id", e.ThreadID),
						zap.Stringer("event.type", e.Type),
						zap.String("event.name", e.Name),
						zap.Int64("event.value", e.Value),
					)
				}
				if f := q.OnProfileEvent; f != nil {
					if err := f(ctx, e); err != nil {
						return errors.Wrap(err, "profile event")
					}
				}
			}
			return nil
		}
		if err := c.decodeBlock(ctx, decodeOptions{
			Handler:      onResult,
			Compressible: p.Compressible(),
			Result:       data.Result(),
			// ProtocolVersion: 54451,
		}); err != nil {
			return errors.Wrap(err, "decode block")
		}
		return nil
	case proto.ServerCodeLog:
		var data proto.Logs
		onResult := func(ctx context.Context, b proto.Block) error {
			for _, l := range data.All() {
				if ce := c.lg.Check(zap.DebugLevel, "Profile"); ce != nil {
					ce.Write(
						zap.Time("event_time", l.Time),
						zap.String("host", l.Host),
						zap.String("query_id", l.QueryID),
						zap.Uint64("thread_id", l.ThreadID),
						zap.Int8("priority", l.Priority),
						zap.String("source", l.Source),
						zap.String("text", l.Text),
					)
				}
				if f := q.OnLog; f != nil {
					if err := f(ctx, l); err != nil {
						return errors.Wrap(err, "log")
					}
				}
			}
			return nil
		}
		if err := c.decodeBlock(ctx, decodeOptions{
			Handler:      onResult,
			Compressible: p.Compressible(),
			Result:       data.Result(),
		}); err != nil {
			return errors.Wrap(err, "decode block")
		}
		return nil
	default:
		return errors.Errorf("unexpected packet %q", p)
	}
}

// Do performs Query on ClickHouse server.
func (c *Client) Do(ctx context.Context, q Query) (err error) {
	if c.IsClosed() {
		return ErrClosed
	}
	if len(q.Parameters) > 0 && !proto.FeatureParameters.In(c.protocolVersion) {
		return errors.Errorf("query parameters are not supported in protocol version %d, upgrade server %q",
			c.protocolVersion, c.server,
		)
	}
	if q.QueryID == "" {
		q.QueryID = uuid.New().String()
	}
	if c.otel {
		newCtx, span := c.tracer.Start(ctx, "Do",
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				semconv.DBSystemKey.String("clickhouse"),
				semconv.DBStatementKey.String(q.Body),
				semconv.DBUserKey.String(c.info.User),
				semconv.DBNameKey.String(c.info.Database),
				otelch.ProtocolVersion(c.protocolVersion),
				otelch.QuotaKey(q.QuotaKey),
				otelch.QueryID(q.QueryID),
			),
		)
		m := new(queryMetrics)
		ctx = context.WithValue(newCtx, ctxQueryKey{}, m)
		defer func() {
			span.SetAttributes(
				otelch.BlocksSent(m.BlocksSent),
				otelch.BlocksReceived(m.BlocksReceived),
				otelch.RowsReceived(m.RowsReceived),
				otelch.ColumnsReceived(m.ColumnsReceived),
				otelch.Rows(m.Rows),
				otelch.Bytes(m.Bytes),
			)
			if err != nil {
				span.RecordError(err)
				status := "Failed"
				var exc *Exception
				if errors.As(err, &exc) {
					status = exc.Name
					span.SetAttributes(
						otelch.ErrorCode(int(exc.Code)),
						otelch.ErrorName(exc.Name),
					)
				}
				span.SetStatus(codes.Error, status)
			} else {
				span.SetStatus(codes.Ok, "")
			}
			span.End()
		}()
	}
	g, ctx := errgroup.WithContext(ctx)
	done := make(chan struct{})
	var (
		gotException atomic.Bool
		colInfo      chan proto.ColInfoInput
	)
	if q.Result == nil && len(q.Input) > 0 {
		// Handling input column type inference, e.g. enums.
		result := proto.ColInfoInput{}
		q.Result = &result
		colInfo = make(chan proto.ColInfoInput, 1)
		q.OnResult = func(ctx context.Context, block proto.Block) error {
			c.lg.Debug("Received column info")
			for _, v := range result {
				c.lg.Debug("Column",
					zap.String("column.name", v.Name),
					zap.Stringer("column.type", v.Type),
				)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case colInfo <- result:
				return nil
			}
		}
	}
	g.Go(func() error {
		// Sending data.
		if err := c.sendQuery(ctx, q); err != nil {
			return errors.Wrap(err, "send query")
		}
		if err := c.flush(ctx); err != nil {
			return errors.Wrap(err, "flush")
		}
		var info proto.ColInfoInput
		if colInfo != nil {
			c.lg.Debug("Waiting for column info")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v := <-colInfo:
				info = v
			}
		}
		if err := c.sendInput(ctx, info, q); err != nil {
			return errors.Wrap(err, "send input")
		}
		if err := c.flush(ctx); err != nil {
			return errors.Wrap(err, "flush")
		}
		return nil
	})
	g.Go(func() error {
		// Receiving query result, data and telemetry.
		defer close(done)
		if colInfo != nil {
			defer close(colInfo)
		}
		onResult := c.resultHandler(q)
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			code, err := c.packet(ctx)
			if err != nil {
				var opErr *net.OpError
				if errors.As(err, &opErr) && opErr.Timeout() {
					continue
				}
				return errors.Wrap(err, "packet")
			}
			switch code {
			case proto.ServerCodeData:
				if err := c.decodeBlock(ctx, decodeOptions{
					Handler:      onResult,
					Result:       q.Result,
					Compressible: code.Compressible(),
				}); err != nil {
					return errors.Wrap(err, "decode block")
				}
			case proto.ServerCodeEndOfStream:
				return nil
			default:
				if err := c.handlePacket(ctx, code, q); err != nil {
					if IsException(err) {
						// Prevent query cancellation on exception.
						gotException.Store(true)
					}
					return errors.Wrap(err, "handle packet")
				}
			}
		}
	})
	g.Go(func() error {
		<-done
		// Handling query cancellation if needed.
		if ctx.Err() != nil && !gotException.Load() {
			err := multierr.Append(ctx.Err(), c.cancelQuery())
			return errors.Wrap(err, "canceled")
		}
		return nil
	})
	return g.Wait()
}
