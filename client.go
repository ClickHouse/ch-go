// Package ch implements ClickHouse client.
package ch

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/go-faster/errors"

	"github.com/go-faster/ch/internal/proto"
)

// Client implements ClickHouse binary protocol client on top of
// single TCP connection.
type Client struct {
	conn   net.Conn
	buf    *proto.Buffer
	reader *proto.Reader
	info   proto.ClientHello
	server proto.ServerHello
	tz     *time.Location

	compression proto.Compression
	settings    []Setting
}

// Setting to send to server.
type Setting struct {
	Key, Value string
	Important  bool
}

// ServerInfo returns server information.
func (c *Client) ServerInfo() proto.ServerHello { return c.server }

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

func (e Exception) String() string {
	return fmt.Sprintf("%s %s", e.Code, e.Name)
}

// Exception reads exception from server.
func (c *Client) Exception() (*Exception, error) {
	var list []proto.Exception
	for {
		var ex proto.Exception
		if err := ex.Decode(c.reader); err != nil {
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

// Packet reads server code.
func (c *Client) Packet() (proto.ServerCode, error) {
	n, err := c.reader.Uvarint()
	if err != nil {
		return 0, errors.Wrap(err, "uvarint")
	}

	code := proto.ServerCode(n)
	if !code.IsAServerCode() {
		return 0, errors.Errorf("bad server Packet type %d", n)
	}

	return code, nil
}

func (c *Client) flush(ctx context.Context) error {
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetWriteDeadline(deadline); err != nil {
			return errors.Wrap(err, "set write deadline")
		}
	}
	n, err := c.conn.Write(c.buf.Buf)
	if err != nil {
		return errors.Wrap(err, "write")
	}
	if n != len(c.buf.Buf) {
		return errors.Wrap(io.ErrShortWrite, "wrote less than expected")
	}

	c.buf.Reset()
	return nil
}

func (c *Client) encode(v proto.AwareEncoder) {
	v.EncodeAware(c.buf, c.info.ProtocolVersion)
}

// Options for Client.
type Options struct {
	Database string
	User     string
	Password string
	Settings []Setting
}

func (o *Options) setDefaults() {
	if o.Database == "" {
		o.Database = "default"
	}
	if o.User == "" {
		o.User = "default"
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

		info: proto.ClientHello{
			Name:            proto.Name,
			Major:           proto.Major,
			Minor:           proto.Minor,
			ProtocolVersion: proto.Revision,

			Database: opt.Database,
			User:     opt.User,
			Password: opt.Password,
		},
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
