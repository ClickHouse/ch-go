package ch

import (
	"context"
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
}

func (c *Client) ServerInfo() proto.ServerHello { return c.server }
func (c *Client) Location() *time.Location      { return c.tz }

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

func (c *Client) packet() (proto.ServerCode, error) {
	n, err := c.reader.Uvarint()
	if err != nil {
		return 0, errors.Wrap(err, "uvarint")
	}

	code := proto.ServerCode(n)
	if !code.IsAServerCode() {
		return 0, errors.Errorf("bad server packet type %d", n)
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

type Options struct {
	Database string
	User     string
	Password string
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
		conn:   conn,
		buf:    new(proto.Buffer),
		reader: proto.NewReader(conn),

		info: proto.ClientHello{
			Name:     proto.Name,
			Major:    proto.Major,
			Minor:    proto.Minor,
			Revision: proto.Revision,

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
