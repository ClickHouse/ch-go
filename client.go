package ch

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/ClickHouse/ch-go/compress"
	pkgVersion "github.com/ClickHouse/ch-go/internal/version"
	"github.com/ClickHouse/ch-go/otelch"
	"github.com/ClickHouse/ch-go/proto"
)

// Client implements ClickHouse binary protocol client on top of
// single TCP connection.
type Client struct {
	lg       *zap.Logger
	conn     net.Conn
	mux      sync.Mutex
	buf      *proto.Buffer
	reader   *proto.Reader
	info     proto.ClientHello
	server   proto.ServerHello
	version  clientVersion
	quotaKey string

	otel   bool
	tracer trace.Tracer
	meter  metric.Meter

	// TCP Binary protocol version.
	protocolVersion int

	// compressor performs block compression,
	// see encodeBlock.
	compressor        *compress.Writer
	compression       proto.Compression
	compressionMethod compress.Method

	settings []Setting
}

// Setting to send to server.
type Setting struct {
	Key, Value string
	Important  bool
}

// SettingInt returns Setting with integer value v.
func SettingInt(k string, v int) Setting {
	return Setting{
		Key:       k,
		Value:     strconv.Itoa(v),
		Important: true,
	}
}

// ServerInfo returns server information.
func (c *Client) ServerInfo() proto.ServerHello { return c.server }

// ErrClosed means that client was already closed.
var ErrClosed = errors.New("client is closed")

// Close closes underlying connection and frees all resources,
// rendering Client to unusable state.
func (c *Client) Close() error {
	if c.conn == nil {
		return ErrClosed
	}
	defer func() {
		c.buf = nil
		c.reader = nil
		c.conn = nil
	}()
	if err := c.conn.Close(); err != nil {
		return errors.Wrap(err, "conn")
	}

	return nil
}

// IsClosed indicates that connection is closed.
func (c *Client) IsClosed() bool {
	return c.conn == nil
}

// Exception is server-side error.
type Exception struct {
	Code    proto.Error
	Name    string
	Message string
	Stack   string
	Next    []Exception // non-nil only for top exception
}

func (e *Exception) IsCode(codes ...proto.Error) bool {
	if e == nil || len(codes) == 0 {
		return false
	}
	for _, c := range codes {
		if e.Code == c {
			return true
		}
	}
	return false
}

func (e *Exception) Error() string {
	msg := strings.TrimPrefix(e.Message, e.Name+":")
	msg = strings.TrimSpace(msg)
	return fmt.Sprintf("%s: %s: %s", e.Code, e.Name, msg)
}

// AsException finds first *Exception in err chain.
func AsException(err error) (*Exception, bool) {
	var e *Exception
	if !errors.As(err, &e) {
		return nil, false
	}
	return e, true
}

// IsErr reports whether err is error with provided exception codes.
func IsErr(err error, codes ...proto.Error) bool {
	if e, ok := AsException(err); ok {
		return e.IsCode(codes...)
	}
	return false
}

// IsException reports whether err is Exception.
func IsException(err error) bool {
	_, ok := AsException(err)
	return ok
}

// Exception reads exception from server.
func (c *Client) exception() (*Exception, error) {
	var list []proto.Exception
	for {
		var ex proto.Exception
		if err := c.decode(&ex); err != nil {
			return nil, errors.Wrap(err, "decode")
		}

		list = append(list, ex)
		if !ex.Nested {
			break
		}
	}
	top := list[0]
	e := &Exception{
		Code:    top.Code,
		Name:    top.Name,
		Message: top.Message,
		Stack:   top.Stack,
	}
	for _, next := range list[1:] {
		e.Next = append(e.Next, Exception{
			Code:    next.Code,
			Name:    next.Name,
			Message: next.Message,
			Stack:   next.Stack,
		})
	}
	return e, nil
}

func (c *Client) decode(v proto.AwareDecoder) error {
	return v.DecodeAware(c.reader, c.protocolVersion)
}

func (c *Client) progress() (proto.Progress, error) {
	var p proto.Progress

	if err := c.decode(&p); err != nil {
		return proto.Progress{}, errors.Wrap(err, "decode")
	}

	return p, nil
}

func (c *Client) profile() (proto.Profile, error) {
	var p proto.Profile

	if err := c.decode(&p); err != nil {
		return proto.Profile{}, errors.Wrap(err, "decode")
	}

	return p, nil
}

// packet reads server code.
func (c *Client) packet(ctx context.Context) (proto.ServerCode, error) {
	const defaultTimeout = time.Second * 3

	deadline := time.Now().Add(defaultTimeout)
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	}
	if !deadline.IsZero() {
		if err := c.conn.SetReadDeadline(deadline); err != nil {
			return 0, errors.Wrap(err, "set read deadline")
		}
		defer func() {
			// Reset deadline.
			_ = c.conn.SetReadDeadline(time.Time{})
		}()
	}

	n, err := c.reader.UVarInt()
	if err != nil {
		return 0, errors.Wrap(err, "uvarint")
	}

	code := proto.ServerCode(n)
	if ce := c.lg.Check(zap.DebugLevel, "Packet"); ce != nil {
		ce.Write(
			zap.Uint64("packet_code", n),
			zap.Stringer("packet", code),
		)
	}
	if !code.IsAServerCode() {
		return 0, errors.Errorf("bad server packet type %d", n)
	}

	return code, nil
}

func (c *Client) flushBuf(ctx context.Context, b *proto.Buffer) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "context")
	}
	if len(b.Buf) == 0 {
		// Nothing to flush.
		return nil
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetWriteDeadline(deadline); err != nil {
			return errors.Wrap(err, "set write deadline")
		}
		defer c.conn.SetWriteDeadline(time.Time{})
	}
	n, err := c.conn.Write(b.Buf)
	if err != nil {
		return errors.Wrap(err, "write")
	}
	if n != len(b.Buf) {
		return errors.Wrap(io.ErrShortWrite, "wrote less than expected")
	}
	if ce := c.lg.Check(zap.DebugLevel, "Flush"); ce != nil {
		ce.Write(zap.Int("bytes", n))
	}
	b.Reset()
	return nil
}

func (c *Client) flush(ctx context.Context) error {
	return c.flushBuf(ctx, c.buf)
}

func (c *Client) encode(v proto.AwareEncoder) {
	v.EncodeAware(c.buf, c.protocolVersion)
}

//go:generate go run github.com/dmarkham/enumer -transform snake_upper -type Compression -trimprefix Compression -output compression_enum.go

// Compression setting.
//
// Trade bandwidth for CPU.
type Compression byte

const (
	// CompressionDisabled disables compression. Lowest CPU overhead.
	CompressionDisabled Compression = iota
	// CompressionLZ4 enables LZ4 compression for data. Medium CPU overhead.
	CompressionLZ4
	// CompressionZSTD enables ZStandard compression. High CPU overhead.
	CompressionZSTD
	// CompressionNone uses no compression but data has checksums.
	CompressionNone
)

// Options for Client. Zero value is valid.
type Options struct {
	Logger      *zap.Logger // defaults to Nop.
	Address     string      // 127.0.0.1:9000
	Database    string      // "default"
	User        string      // "default"
	Password    string      // blank string by default
	QuotaKey    string      // blank string by default
	Compression Compression // disabled by default
	Settings    []Setting   // none by default

	Dialer      Dialer        // defaults to net.Dialer
	DialTimeout time.Duration // defaults to 1s
	TLS         *tls.Config   // no TLS is used by default

	ProtocolVersion  int           // force protocol version, optional
	HandshakeTimeout time.Duration // longer lasting handshake is a case for ClickHouse cloud idle instances, defaults to 5m

	// Additional OpenTelemetry instrumentation that will capture query body
	// and other parameters.
	//
	// Note: OpenTelemetry context propagation works without this option too.
	OpenTelemetryInstrumentation bool
	TracerProvider               trace.TracerProvider
	MeterProvider                metric.MeterProvider

	meter  metric.Meter
	tracer trace.Tracer
}

// Defaults for connection.
const (
	DefaultDatabase         = "default"
	DefaultUser             = "default"
	DefaultHost             = "127.0.0.1"
	DefaultPort             = 9000
	DefaultDialTimeout      = 1 * time.Second
	DefaultHandshakeTimeout = 300 * time.Second
)

func (o *Options) setDefaults() {
	if o.ProtocolVersion == 0 {
		o.ProtocolVersion = proto.Version
	}
	if o.HandshakeTimeout == 0 {
		o.HandshakeTimeout = DefaultHandshakeTimeout
	}
	if o.Database == "" {
		o.Database = DefaultDatabase
	}
	if o.User == "" {
		o.User = DefaultUser
	}
	if o.Logger == nil {
		o.Logger = zap.NewNop()
	}
	if o.Address == "" {
		o.Address = net.JoinHostPort(DefaultHost, strconv.Itoa(DefaultPort))
	}
	if o.DialTimeout == 0 {
		o.DialTimeout = DefaultDialTimeout
	}
	if o.Dialer == nil {
		o.Dialer = &net.Dialer{
			Timeout: o.DialTimeout,
		}
	}
	if o.MeterProvider == nil {
		o.MeterProvider = metric.NewNoopMeterProvider()
	}
	if o.TracerProvider == nil {
		o.TracerProvider = otel.GetTracerProvider()
	}
	if o.meter == nil {
		o.meter = o.MeterProvider.Meter(otelch.Name)
	}
	if o.tracer == nil {
		o.tracer = o.TracerProvider.Tracer(otelch.Name,
			trace.WithInstrumentationVersion(otelch.SemVersion()),
		)
	}
}

type clientVersion struct {
	Name  string
	Major int
	Minor int
	Patch int
}

// Connect performs handshake with ClickHouse server and initializes
// application level connection.
func Connect(ctx context.Context, conn net.Conn, opt Options) (*Client, error) {
	opt.setDefaults()

	pkg := pkgVersion.Get()
	ver := clientVersion{
		Name:  proto.Name,
		Major: pkg.Major,
		Minor: pkg.Minor,
		Patch: pkg.Patch,
	}
	if pkg.Name != "" {
		ver.Name = fmt.Sprintf("%s (%s)", proto.Name, pkg.Name)
	}
	if opt.OpenTelemetryInstrumentation {
		newCtx, span := opt.tracer.Start(ctx, "Connect",
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				semconv.DBNameKey.String(opt.Database),
			),
		)
		ctx = newCtx
		defer span.End()
	}
	c := &Client{
		conn:     conn,
		buf:      new(proto.Buffer),
		reader:   proto.NewReader(conn),
		settings: opt.Settings,
		lg:       opt.Logger,
		otel:     opt.OpenTelemetryInstrumentation,
		tracer:   opt.tracer,
		meter:    opt.meter,
		quotaKey: opt.QuotaKey,

		compressor: compress.NewWriter(),

		version:         ver,
		protocolVersion: opt.ProtocolVersion,
		info: proto.ClientHello{
			Name:  ver.Name,
			Major: ver.Major,
			Minor: ver.Minor,

			ProtocolVersion: opt.ProtocolVersion,

			Database: opt.Database,
			User:     opt.User,
			Password: opt.Password,
		},
	}
	switch opt.Compression {
	case CompressionLZ4:
		c.compression = proto.CompressionEnabled
		c.compressionMethod = compress.LZ4
	case CompressionZSTD:
		c.compression = proto.CompressionEnabled
		c.compressionMethod = compress.ZSTD
	case CompressionNone:
		c.compression = proto.CompressionEnabled
		c.compressionMethod = compress.None
	default:
		c.compression = proto.CompressionDisabled
	}

	handshakeCtx, cancel := context.WithTimeout(ctx, opt.HandshakeTimeout)
	defer cancel()
	if err := c.handshake(handshakeCtx); err != nil {
		return nil, errors.Wrap(err, "handshake")
	}

	return c, nil
}

// A Dialer dials using a context.
type Dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// Dial dials requested address and establishes TCP connection to ClickHouse
// server, performing handshake.
func Dial(ctx context.Context, opt Options) (c *Client, err error) {
	opt.setDefaults()

	if opt.OpenTelemetryInstrumentation {
		newCtx, span := opt.tracer.Start(ctx, "Dial",
			trace.WithSpanKind(trace.SpanKindClient),
		)
		ctx = newCtx
		defer func() {
			if err != nil {
				span.RecordError(err)
			}
			span.End()
		}()
	}

	if opt.TLS != nil {
		netDialer := &net.Dialer{
			Timeout: opt.DialTimeout,
		}
		if opt.Dialer != nil {
			d, ok := opt.Dialer.(*net.Dialer)
			if !ok {
				return nil, errors.Errorf("tls dialer should be *net.Dialer, got %T", opt.Dialer)
			}
			netDialer = d
		}
		opt.Dialer = &tls.Dialer{
			NetDialer: netDialer,
			Config:    opt.TLS,
		}
	}

	conn, err := opt.Dialer.DialContext(ctx, "tcp", opt.Address)
	if err != nil {
		return nil, errors.Wrap(err, "dial")
	}

	client, err := Connect(ctx, conn, opt)
	if err != nil {
		return nil, errors.Wrap(err, "connect")
	}

	return client, nil
}
