package otelch

import (
	"go.opentelemetry.io/otel/attribute"
)

const (
	QueryIDKey         = attribute.Key("ch.query.id")
	QuotaKeyKey        = attribute.Key("ch.quota.key")
	ProtocolVersionKey = attribute.Key("ch.protocol.version")
	ServerNameKey      = attribute.Key("ch.server.name")
	ErrorCodeKey       = attribute.Key("ch.error.code")
	ErrorNameKey       = attribute.Key("ch.error.name")
)

// QueryID attribute.
func QueryID(v string) attribute.KeyValue {
	return attribute.KeyValue{
		Key:   QueryIDKey,
		Value: attribute.StringValue(v),
	}
}

// QuotaKey attribute.
func QuotaKey(v string) attribute.KeyValue {
	return attribute.KeyValue{
		Key:   QuotaKeyKey,
		Value: attribute.StringValue(v),
	}
}

// ProtocolVersion attribute.
func ProtocolVersion(v int) attribute.KeyValue {
	return attribute.KeyValue{
		Key:   ProtocolVersionKey,
		Value: attribute.IntValue(v),
	}
}

// ErrorCode attribute.
func ErrorCode(v int) attribute.KeyValue {
	return attribute.KeyValue{
		Key:   ErrorCodeKey,
		Value: attribute.IntValue(v),
	}
}

// ErrorName attribute.
func ErrorName(v string) attribute.KeyValue {
	return attribute.KeyValue{
		Key:   ErrorNameKey,
		Value: attribute.StringValue(v),
	}
}

// ServerName attribute.
func ServerName(v string) attribute.KeyValue {
	return attribute.KeyValue{
		Key:   ServerNameKey,
		Value: attribute.StringValue(v),
	}
}
