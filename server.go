package ch

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/ch-go/proto"
)

// Server is basic ClickHouse server.
type Server struct {
	lg    *zap.Logger
	tz    *time.Location
	conn  atomic.Uint64
	ver   int
	onErr func(err error)
}

// ServerOptions wraps possible Server configuration.
type ServerOptions struct {
	Logger   *zap.Logger
	Timezone *time.Location
	OnError  func(err error)
}

// NewServer returns new ClickHouse Server.
func NewServer(opt ServerOptions) *Server {
	if opt.Logger == nil {
		opt.Logger = zap.NewNop()
	}
	if opt.Timezone == nil {
		opt.Timezone = time.UTC
	}
	if opt.OnError == nil {
		opt.OnError = func(err error) {}
	}
	return &Server{
		lg:    opt.Logger,
		tz:    opt.Timezone,
		ver:   proto.Version,
		onErr: opt.OnError,
	}
}

// ServerConn wraps Server connection.
type ServerConn struct {
	lg     *zap.Logger
	tz     *time.Location
	conn   net.Conn
	buf    *proto.Buffer
	reader *proto.Reader
	client proto.ClientHello
	info   proto.ServerHello
	ver    int

	// compressor performs block compression,
	// see encodeBlock.
	compressor *compress.Writer

	settings []Setting
}

func (c *ServerConn) packet() (proto.ClientCode, error) {
	n, err := c.reader.UVarInt()
	if err != nil {
		return 0, errors.Wrap(err, "uvarint")
	}

	code := proto.ClientCode(n)
	if ce := c.lg.Check(zap.DebugLevel, "Packet"); ce != nil {
		ce.Write(
			zap.Uint64("packet_code_raw", n),
			zap.Stringer("packet_code", code),
		)
	}
	if !code.IsAClientCode() {
		return 0, errors.Errorf("bad client packet type %d", n)
	}

	return code, nil
}

func (c *ServerConn) handshake() error {
	p, err := c.packet()
	if err != nil {
		return errors.Wrap(err, "packet")
	}
	if p != proto.ClientCodeHello {
		return errors.Errorf("unexpected packet %q", p)
	}
	if err := c.client.Decode(c.reader); err != nil {
		return errors.Wrap(err, "decode hello")
	}
	c.ver = c.client.ProtocolVersion
	c.info.EncodeAware(c.buf, c.ver)
	if err := c.flush(); err != nil {
		return errors.Wrap(err, "flush")
	}

	_ = c.compressor // hack
	_ = c.settings   // hack

	return nil
}

func (c *ServerConn) flush() error {
	n, err := c.conn.Write(c.buf.Buf)
	if err != nil {
		return errors.Wrap(err, "write")
	}
	if n != len(c.buf.Buf) {
		return errors.Wrap(io.ErrShortWrite, "wrote less than expected")
	}
	if ce := c.lg.Check(zap.DebugLevel, "Flush"); ce != nil {
		ce.Write(zap.Int("bytes", n))
	}
	c.buf.Reset()
	return nil
}

func (c *ServerConn) handlePacket(p proto.ClientCode) error {
	switch p {
	case proto.ClientCodePing:
		return c.handlePing()
	case proto.ClientCodeQuery:
		return c.handleQuery()
	default:
		return errors.Errorf("%q not implemented", p)
	}
}

func (c *ServerConn) handlePing() error {
	proto.ServerCodePong.Encode(c.buf)
	return c.flush()
}

func (c *ServerConn) handleClientData(ctx context.Context, q proto.Query) error {
	var data proto.ClientData
	if err := data.DecodeAware(c.reader, c.ver); err != nil {
		return errors.Wrap(err, "decode")
	}

	var block proto.Block
	if err := block.DecodeBlock(c.reader, c.ver, nil); err != nil {
		return errors.Wrap(err, "decode block")
	}

	if block.Rows > 0 || block.Columns > 0 {
		return errors.New("input not implemented")
	}

	_ = ctx
	_ = q

	return nil
}

func (c *ServerConn) handleQuery() error {
	c.lg.Debug("Decoding query", zap.Int("v", c.ver))

	deadline := time.Now().Add(time.Second * 1)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	go func() {
		ticker := time.NewTicker(time.Millisecond * 100)
		defer ticker.Stop()
		defer func() {
			_ = c.conn.SetReadDeadline(time.Time{})
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = c.conn.SetReadDeadline(deadline)
			}
		}
	}()

	var q proto.Query
	if err := q.DecodeAware(c.reader, c.ver); err != nil {
		return errors.Wrap(err, "decode")
	}

	lg := c.lg.With(zap.String("query_id", q.ID))

Ingest:
	for {
		lg.Debug("Reading packet")
		p, err := c.packet()
		if err != nil {
			return errors.Wrap(err, "packet")
		}
		switch p {
		case proto.ClientCodeData:
			if err := c.handleClientData(ctx, q); err != nil {
				return errors.Wrap(err, "client data")
			}
			break Ingest
		default:
			return errors.Errorf("unexpected packet %q", p)
		}
	}

	proto.ServerCodeEndOfStream.Encode(c.buf)
	if err := c.flush(); err != nil {
		return errors.Wrap(err, "flush")
	}

	return nil
}

// Handle connection.
func (c *ServerConn) Handle() error {
	if err := c.handshake(); err != nil {
		return errors.Wrap(err, "handshake")
	}
	for {
		p, err := c.packet()
		if err != nil {
			return errors.Wrap(err, "packet")
		}
		if err := c.handlePacket(p); err != nil {
			return errors.Wrapf(err, "handle %q", p)
		}
	}
}

func (s *Server) handle(conn net.Conn) error {
	lg := s.lg.With(
		zap.Uint64("conn", s.conn.Inc()),
	)
	lg.Info("Connected",
		zap.String("addr", conn.RemoteAddr().String()),
	)
	sConn := &ServerConn{
		lg:     lg,
		conn:   conn,
		ver:    s.ver,
		buf:    new(proto.Buffer),
		reader: proto.NewReader(conn),
		client: proto.ClientHello{},
		info: proto.ServerHello{
			Name:     "CH",
			Revision: s.ver,
		},
		tz:         time.UTC,
		compressor: compress.NewWriter(),
	}
	return sConn.Handle()
}

// Serve connections on net.Listener.
func (s *Server) Serve(ln net.Listener) error {
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		c, err := ln.Accept()
		if err != nil {
			return errors.Wrap(err, "accept")
		}
		wg.Add(1)
		go func() {
			defer func() {
				_ = c.Close()
			}()
			defer wg.Done()
			if err := s.handle(c); err != nil && !errors.Is(err, io.EOF) {
				s.lg.Error("Handle", zap.Error(err))
				s.onErr(err)
			}
		}()
	}
}
