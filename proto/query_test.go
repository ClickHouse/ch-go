package proto

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/ch/internal/gold"
)

const queryCreateDatabaseHex = "012432336164326330372d32663" +
	"6382d343030352d396261632d646138663436376264" +
	"64336201002432336164326330372d326636382d343" +
	"030352d396261632d64613866343637626464336209" +
	"302e302e302e303a300000000000000000010665726" +
	"e61646f056e657875730b436c69636b486f757365201" +
	"50bb2a90300000400000002001543524541544520444" +
	"154414241534520746573743b"

const queryProtoVersion = 54450

var queryCreateDatabase = Query{
	ID:          "23ad2c07-2f68-4005-9bac-da8f467bdd3b",
	Body:        "CREATE DATABASE test;",
	Secret:      "",
	Stage:       StageComplete,
	Compression: CompressionDisabled,
	Info: ClientInfo{
		ProtocolVersion: queryProtoVersion,
		Major:           21,
		Minor:           11,
		Patch:           4,
		Interface:       InterfaceTCP,
		Query:           ClientQueryInitial,

		InitialUser:      "",
		InitialQueryID:   "23ad2c07-2f68-4005-9bac-da8f467bdd3b",
		InitialAddress:   "0.0.0.0:0",
		OSUser:           "ernado",
		ClientHostname:   "nexus",
		ClientName:       "ClickHouse ",
		Span:             trace.SpanContext{},
		QuotaKey:         "",
		DistributedDepth: 0,
	},
}

func TestQuery_DecodeAware(t *testing.T) {
	data, err := hex.DecodeString(queryCreateDatabaseHex)
	require.NoError(t, err)

	var q Query

	r := NewReader(bytes.NewReader(data))
	v, err := r.UVarInt()
	require.NoError(t, err)
	require.Equal(t, ClientCodeQuery, ClientCode(v))

	require.NoError(t, q.DecodeAware(r, 54450))
	require.Equal(t, q.Body, "CREATE DATABASE test;")
	require.Equal(t, queryCreateDatabase, q)

	t.Logf("%+v", q)
}

func TestQuery_EncodeAware(t *testing.T) {
	buf := new(Buffer)
	queryCreateDatabase.EncodeAware(buf, 54450)
	gold.Bytes(t, buf.Buf, "query_create_db")

	r := NewReader(bytes.NewReader(buf.Buf))
	v, err := r.UVarInt()
	require.NoError(t, err)
	require.Equal(t, ClientCodeQuery, ClientCode(v))

	var dec Query
	assert.NoError(t, dec.DecodeAware(r, 54450))
	assert.Equal(t, dec.Body, "CREATE DATABASE test;")
	assert.Equal(t, queryCreateDatabase, dec)
}
