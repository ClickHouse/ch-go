package proto

import (
	"go.opentelemetry.io/otel/trace"
)

//go:generate go run github.com/dmarkham/enumer -type ClientInterface -trimprefix ClientInterface -output client_info_interface.go

// ClientInterface is interface of client.
type ClientInterface byte

// Possible interfaces.
const (
	ClientInterfaceTCP  ClientInterface = 1
	ClientInterfaceHTTP ClientInterface = 2
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
	Revision int
	Major    int
	Minor    int
	Patch    int

	Interface ClientInterface
	Query     ClientQueryKind

	InitialUser    string
	InitialQueryID string
	InitialAddress string

	OSUser         string
	ClientHostname string
	ClientName     string

	Span trace.SpanContext

	QuotaKey string
}

// EncodeAware encodes to buffer revision-aware.
func (c ClientInfo) EncodeAware(b *Buffer, revision int) {
	b.PutByte(byte(c.Query))

	b.PutString(c.InitialUser)
	b.PutString(c.InitialQueryID)
	b.PutString(c.InitialAddress)

	b.PutByte(byte(c.Interface))

	b.PutString(c.OSUser)
	b.PutString(c.ClientHostname)
	b.PutString(c.ClientName)

	b.PutInt(c.Major)
	b.PutInt(c.Minor)
	b.PutInt(c.Revision)

	if FeatureQuotaKeyInClientInfo.In(revision) {
		b.PutString(c.QuotaKey)
	}
	if FeatureVersionPatch.In(revision) {
		b.PutInt(c.Patch)
	}
	if FeatureOpenTelemetry.In(revision) {
		if c.Span.IsValid() {
			b.PutInt(1)
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
			b.PutInt(0)
		}
	}
}
