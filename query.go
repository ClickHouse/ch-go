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
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/ch/internal/compress"
	"github.com/go-faster/ch/proto"
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
	c.encode(proto.Query{
		ID:          q.QueryID,
		Body:        q.Body,
		Secret:      "",
		Stage:       proto.StageComplete,
		Compression: c.compression,
		Settings:    c.querySettings(q),
		Info: proto.ClientInfo{
			ProtocolVersion: c.protocolVersion,
			Major:           c.info.Major,
			Minor:           c.info.Minor,
			Patch:           0,
			Interface:       proto.InterfaceTCP,
			Query:           proto.ClientQueryInitial,

			InitialUser:    "",
			InitialQueryID: q.QueryID,
			InitialAddress: c.conn.LocalAddr().String(),
			OSUser:         "",
			ClientHostname: "",
			ClientName:     c.info.Name,

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
		if err := c.encodeBlock(q.ExternalTable, q.ExternalData); err != nil {
			return errors.Wrap(err, "external data")
		}
	}
	if err := c.encodeBlankBlock(); err != nil {
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
	Handler      func(ctx context.Context, b proto.Block) error
	Result       proto.Result
	Compressible bool
}

func (c *Client) decodeBlock(ctx context.Context, opt decodeOptions) error {
	if proto.FeatureTempTables.In(c.protocolVersion) {
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
	if err := block.DecodeBlock(c.reader, c.protocolVersion, opt.Result); err != nil {
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
	if err := opt.Handler(ctx, block); err != nil {
		return errors.Wrap(err, "handler")
	}
	return nil
}

// encodeBlock encodes data block into buf, performing compression if needed.
//
// If input length is zero, blank block will be encoded, which is special case
// for "end of data".
func (c *Client) encodeBlock(tableName string, input []proto.InputColumn) error {
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
func (c *Client) encodeBlankBlock() error {
	return c.encodeBlock("", nil)
}

func (c *Client) sendInput(ctx context.Context, q Query) error {
	if len(q.Input) == 0 {
		return nil
	}
	var (
		rows = q.Input[0].Data.Rows()
		f    = q.OnInput
	)
	if f != nil && rows == 0 {
		// Fetching initial input if no rows provided.
		if err := f(ctx); err != nil {
			// Not handling io.EOF here because input is expected.
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
		if err := c.encodeBlock("", q.Input); err != nil {
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
				break
			}
			// ClickHouse server persists blocks after receive.
			return errors.Wrap(err, "next input (server already persisted previous blocks)")
		}
	}
	// End of input stream.
	//
	// Encoding that there are no more data.
	if err := c.encodeBlankBlock(); err != nil {
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

// Log from server.
type Log struct {
	Time     time.Time
	Host     string
	QueryID  string
	ThreadID uint64
	Priority int8
	Source   string
	Text     string
}

//go:generate go run github.com/dmarkham/enumer -type ProfileEventType -trimprefix Profile -output profile_enum.go

type ProfileEventType byte

const (
	ProfileIncrement ProfileEventType = 1
	ProfileGauge     ProfileEventType = 2
)

// ProfileEvent is detailed profiling event from Server.
type ProfileEvent struct {
	ThreadID uint64
	Host     string
	Time     time.Time
	Type     ProfileEventType
	Name     string
	Value    int64
}

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
	case proto.ServerCodeTableColumns:
		// Ignoring for now.
		var info proto.TableColumns
		if err := c.decode(&info); err != nil {
			return errors.Wrap(err, "table columns")
		}
	case proto.ServerProfileEvents:
		/*
			auto profile_event_type = std::make_shared<DataTypeEnum8>(
			    DataTypeEnum8::Values
			    {
			        { "increment", static_cast<Int8>(INCREMENT)},
			        { "gauge",     static_cast<Int8>(GAUGE)},
			    });
			NamesAndTypesList column_names_and_types = {
			    { "host_name",    std::make_shared<DataTypeString>()   },
			    { "current_time", std::make_shared<DataTypeDateTime>() },
			    { "thread_id",    std::make_shared<DataTypeUInt64>()   },
			    { "type",         profile_event_type                   },
			    { "name",         std::make_shared<DataTypeString>()   },
			    { "value",        std::make_shared<DataTypeUInt64>()   },
			}
		*/
		var (
			evHost     proto.ColStr
			evTime     proto.ColDateTime
			evThreadID proto.ColUInt64
			evType     proto.ColInt8
			evName     proto.ColStr
			evValue    proto.ColAuto // UInt64 or Int64 depending on version
		)
		onResult := func(ctx context.Context, b proto.Block) error {
			for i := range evTime {
				e := ProfileEvent{
					Time:     evTime[i].Time(),
					Host:     evHost.Row(i),
					ThreadID: evThreadID[i],
					Type:     ProfileEventType(evType[i]),
					Name:     evName.Row(i),
				}
				switch d := evValue.Data.(type) {
				case *proto.ColInt64:
					e.Value = (*d)[i]
				case *proto.ColUInt64:
					e.Value = int64((*d)[i])
				default:
					return errors.Errorf("unexpected type %q for metric column", d.Type())
				}
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
						return errors.Wrap(err, "log")
					}
				}
			}
			return nil
		}
		if err := c.decodeBlock(ctx, decodeOptions{
			Handler:      onResult,
			Compressible: p.Compressible(),
			Result: proto.Results{
				{Name: "host_name", Data: &evHost},
				{Name: "current_time", Data: &evTime},
				{Name: "thread_id", Data: &evThreadID},
				{Name: "type", Data: &evType},
				{Name: "name", Data: &evName},
				{Name: "value", Data: &evValue},
			},
		}); err != nil {
			return errors.Wrap(err, "decode block")
		}
		return nil
	case proto.ServerCodeLog:
		var (
			eventTime      proto.ColDateTime
			eventTimeMicro proto.ColUInt32
			eventHostName  proto.ColStr
			eventQueryID   proto.ColStr
			eventThreadID  proto.ColUInt64
			eventPriority  proto.ColInt8
			eventSource    proto.ColStr
			eventText      proto.ColStr
		)
		onResult := func(ctx context.Context, b proto.Block) error {
			for i := range eventTime {
				l := Log{
					Time:     eventTime[i].Time(),
					Host:     eventHostName.Row(i),
					QueryID:  eventQueryID.Row(i),
					ThreadID: eventThreadID[i],
					Priority: eventPriority[i],
					Source:   eventSource.Row(i),
					Text:     eventText.Row(i),
				}
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
			Result: proto.Results{
				{Name: "event_time", Data: &eventTime},
				{Name: "event_time_microseconds", Data: &eventTimeMicro},
				{Name: "host_name", Data: &eventHostName},
				{Name: "query_id", Data: &eventQueryID},
				{Name: "thread_id", Data: &eventThreadID},
				{Name: "priority", Data: &eventPriority},
				{Name: "source", Data: &eventSource},
				{Name: "text", Data: &eventText},
			},
		}); err != nil {
			return errors.Wrap(err, "decode block")
		}
		return nil
	default:
		return errors.Errorf("unexpected packet %q", p)
	}

	return nil
}

// Query performs Query on ClickHouse server.
func (c *Client) Query(ctx context.Context, q Query) error {
	if q.QueryID == "" {
		q.QueryID = uuid.New().String()
	}
	g, ctx := errgroup.WithContext(ctx)
	done := make(chan struct{})
	var gotException atomic.Bool
	g.Go(func() error {
		// Sending data.
		if err := c.sendQuery(ctx, q); err != nil {
			return errors.Wrap(err, "send query")
		}
		if err := c.sendInput(ctx, q); err != nil {
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
