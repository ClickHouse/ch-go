package ch

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/go-faster/ch/internal/compress"
	"github.com/go-faster/ch/proto"
)

// Client implements ClickHouse binary protocol client on top of
// single TCP connection.
type Client struct {
	lg     *zap.Logger
	conn   net.Conn
	mux    sync.Mutex
	buf    *proto.Buffer
	reader *proto.Reader
	info   proto.ClientHello
	server proto.ServerHello
	tz     *time.Location

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

// serverInfo returns server information.
func (c *Client) serverInfo() proto.ServerHello { return c.server }

// Location returns current server timezone.
func (c *Client) Location() *time.Location { return c.tz }

// Close closes underlying connection and frees all resources,
// rendering Client to unusable state.
func (c *Client) Close() error {
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
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetWriteDeadline(deadline); err != nil {
			return errors.Wrap(err, "set write deadline")
		}
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

// Options for Client.
type Options struct {
	Logger      *zap.Logger
	Database    string
	User        string
	Password    string
	Settings    []Setting
	Compression Compression
}

func (o *Options) setDefaults() {
	if o.Database == "" {
		o.Database = "default"
	}
	if o.User == "" {
		o.User = "default"
	}
	if o.Logger == nil {
		o.Logger = zap.NewNop()
	}
}

// Connect performs handshake with ClickHouse server and initializes
// application level connection.
func Connect(ctx context.Context, conn net.Conn, opt Options) (*Client, error) {
	opt.setDefaults()

	c := &Client{
		conn:     conn,
		buf:      new(proto.Buffer),
		reader:   proto.NewReader(conn),
		settings: opt.Settings,
		lg:       opt.Logger,

		compressor: compress.NewWriter(),

		protocolVersion: proto.Version,
		info: proto.ClientHello{
			Name:  proto.Name,
			Major: proto.Major,
			Minor: proto.Minor,

			ProtocolVersion: proto.Version,

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
	if err := c.handshake(ctx); err != nil {
		return nil, errors.Wrap(err, "handshake")
	}
	if c.server.Timezone != "" {
		loc, err := time.LoadLocation(c.server.Timezone)
		if err != nil {
			return nil, errors.Wrap(err, "load timezone")
		}
		c.tz = loc
	}

	return c, nil
}

// Dial dials requested address and establishes TCP connection to ClickHouse
// server, performing handshake.
func Dial(ctx context.Context, addr string, opt Options) (*Client, error) {
	var d net.Dialer

	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, errors.Wrap(err, "dial")
	}

	c, err := Connect(ctx, conn, opt)
	if err != nil {
		return nil, errors.Wrap(err, "connect")
	}

	return c, nil
}
