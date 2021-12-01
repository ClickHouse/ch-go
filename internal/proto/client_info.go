package proto

import (
	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/trace"
)

//go:generate go run github.com/dmarkham/enumer -type Interface -trimprefix Interface -output client_info_interface.go

// Interface is interface of client.
type Interface byte

// Possible interfaces.
const (
	InterfaceTCP  Interface = 1
	InterfaceHTTP Interface = 2
)

//go:generate go run github.com/dmarkham/enumer -type ClientQueryKind -trimprefix ClientQueryKind -output client_info_query.go

// ClientQueryKind is kind of query.
type ClientQueryKind byte

// Possible query kinds.
const (
	ClientQueryNone      ClientQueryKind = 0
	ClientQueryInitial   ClientQueryKind = 1
	ClientQuerySecondary ClientQueryKind = 2
)

// ClientInfo message.
type ClientInfo struct {
	ProtocolVersion int

	Major int
	Minor int
	Patch int

	Interface Interface
	Query     ClientQueryKind

	InitialUser    string
	InitialQueryID string
	InitialAddress string
	InitialTime    int64

	OSUser         string
	ClientHostname string
	ClientName     string

	Span trace.SpanContext

	QuotaKey         string
	DistributedDepth int
}

// EncodeAware encodes to buffer revision-aware.
func (c ClientInfo) EncodeAware(b *Buffer, revision int) {
	b.PutByte(byte(c.Query))

	b.PutString(c.InitialUser)
	b.PutString(c.InitialQueryID)
	b.PutString(c.InitialAddress)
	if FeatureQueryStartTime.In(revision) {
		b.PutInt64(c.InitialTime)
	}

	b.PutByte(byte(c.Interface))

	b.PutString(c.OSUser)
	b.PutString(c.ClientHostname)
	b.PutString(c.ClientName)

	b.PutInt(c.Major)
	b.PutInt(c.Minor)
	b.PutInt(c.ProtocolVersion)

	if FeatureQuotaKeyInClientInfo.In(revision) {
		b.PutString(c.QuotaKey)
	}
	if FeatureDistributedDepth.In(revision) {
		b.PutInt(c.DistributedDepth)
	}
	if FeatureVersionPatch.In(revision) && c.Interface == InterfaceTCP {
		b.PutInt(c.Patch)
	}
	if FeatureOpenTelemetry.In(revision) {
		if c.Span.IsValid() {
			b.PutByte(1)
			{
				v := c.Span.TraceID()
				b.Buf = append(b.Buf, v[:]...)
			}
			{
				v := c.Span.SpanID()
				b.Buf = append(b.Buf, v[:]...)
			}
			b.PutString(c.Span.TraceState().String())
			b.PutByte(byte(c.Span.TraceFlags()))
		} else {
			// No OTEL data.
			b.PutByte(0)
		}
	}
}

func (c *ClientInfo) DecodeAware(r *Reader, revision int) error {
	{
		v, err := r.UInt8()
		if err != nil {
			return errors.Wrap(err, "query kind")
		}
		c.Query = ClientQueryKind(v)
		if !c.Query.IsAClientQueryKind() {
			return errors.Errorf("unknown query kind %d", v)
		}
	}
	{
		v, err := r.Str()
		if err != nil {
			return errors.Wrap(err, "initial user")
		}
		c.InitialUser = v
	}
	{
		v, err := r.Str()
		if err != nil {
			return errors.Wrap(err, "initial query id")
		}
		c.InitialQueryID = v
	}
	{
		v, err := r.Str()
		if err != nil {
			return errors.Wrap(err, "initial address")
		}
		c.InitialAddress = v
	}

	if FeatureQueryStartTime.In(revision) {
		// Microseconds.
		v, err := r.Int64()
		if err != nil {
			return errors.Wrap(err, "query start time")
		}
		c.InitialTime = v
	}

	{
		v, err := r.UInt8()
		if err != nil {
			return errors.Wrap(err, "interface")
		}
		c.Interface = Interface(v)
		if !c.Interface.IsAInterface() {
			return errors.Errorf("unknown interface %d", v)
		}

		// TODO(ernado): support HTTP
		if c.Interface != InterfaceTCP {
			return errors.New("only tcp interface is supported")
		}
	}

	{
		v, err := r.Str()
		if err != nil {
			return errors.Wrap(err, "os user")
		}
		c.OSUser = v
	}
	{
		v, err := r.Str()
		if err != nil {
			return errors.Wrap(err, "client hostname")
		}
		c.ClientHostname = v
	}
	{
		v, err := r.Str()
		if err != nil {
			return errors.Wrap(err, "client name")
		}
		c.ClientName = v
	}

	{
		v, err := r.Int()
		if err != nil {
			return errors.Wrap(err, "major version")
		}
		c.Major = v
	}
	{
		v, err := r.Int()
		if err != nil {
			return errors.Wrap(err, "minor version")
		}
		c.Minor = v
	}
	{
		v, err := r.Int()
		if err != nil {
			return errors.Wrap(err, "protocol version")
		}
		c.ProtocolVersion = v
	}

	if FeatureQuotaKeyInClientInfo.In(revision) {
		v, err := r.Str()
		if err != nil {
			return errors.Wrap(err, "quota key")
		}
		c.QuotaKey = v
	}
	if FeatureDistributedDepth.In(revision) {
		v, err := r.Int()
		if err != nil {
			return errors.Wrap(err, "distributed depth")
		}
		c.DistributedDepth = v
	}
	if FeatureVersionPatch.In(revision) && c.Interface == InterfaceTCP {
		v, err := r.Int()
		if err != nil {
			return errors.Wrap(err, "patch version")
		}
		c.Patch = v
	}
	if FeatureOpenTelemetry.In(revision) {
		v, err := r.Bool()
		if err != nil {
			return errors.Wrap(err, "open telemetry start")
		}
		if v {
			return errors.New("open telemetry not implemented")
		}
	}

	return nil
}
