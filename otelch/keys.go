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
	BlocksSentKey      = attribute.Key("ch.blocks_sent")
	BlocksReceivedKey  = attribute.Key("ch.blocks_received")
	ColumnsReceivedKey = attribute.Key("ch.columns_received")
	RowsReceivedKey    = attribute.Key("ch.rows_received")
	RowsKey            = attribute.Key("ch.rows")
	BytesKey           = attribute.Key("ch.bytes")
)

// BlocksSent is cumulative blocks sent count during query execution.
func BlocksSent(v int) attribute.KeyValue {
	return attribute.KeyValue{
		Key:   BlocksSentKey,
		Value: attribute.IntValue(v),
	}
}

// BlocksReceived is cumulative received sent count during query execution.
func BlocksReceived(v int) attribute.KeyValue {
	return attribute.KeyValue{
		Key:   BlocksReceivedKey,
		Value: attribute.IntValue(v),
	}
}

// RowsReceived is cumulative rows received count during query execution.
func RowsReceived(v int) attribute.KeyValue {
	return attribute.KeyValue{
		Key:   RowsReceivedKey,
		Value: attribute.IntValue(v),
	}
}

// ColumnsReceived is count of columns in result.
func ColumnsReceived(v int) attribute.KeyValue {
	return attribute.KeyValue{
		Key:   ColumnsReceivedKey,
		Value: attribute.IntValue(v),
	}
}

// Rows is cumulative rows processed count during query execution.
func Rows(v int) attribute.KeyValue {
	return attribute.KeyValue{
		Key:   RowsKey,
		Value: attribute.IntValue(v),
	}
}

// Bytes is cumulative bytes processed count during query execution.
func Bytes(v int) attribute.KeyValue {
	return attribute.KeyValue{
		Key:   BytesKey,
		Value: attribute.IntValue(v),
	}
}

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
