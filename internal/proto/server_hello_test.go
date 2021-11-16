package proto

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServerHello_Decode(t *testing.T) {
	data, err := hex.DecodeString("11436c69636b486f75736520736572766572150bb2a9030d4575726f70652f4d6f73636f7705616c70686103")
	require.NoError(t, err)

	r := NewReader(bytes.NewReader(data))
	var v ServerHello
	require.NoError(t, v.Decode(r))
	require.Equal(t, ServerHello{
		Name:        "ClickHouse server",
		Major:       21,
		Minor:       11,
		Patch:       3,
		Revision:    54450,
		Timezone:    "Europe/Moscow",
		DisplayName: "alpha",
	}, v)
}

func BenchmarkServerHello_Decode(b *testing.B) {
	var raw Buffer
	raw.PutString("ClickHouse server")
	raw.PutInt(21)
	raw.PutInt(11)
	raw.PutInt(54450)
	raw.PutString("Europe/Moscow")
	raw.PutString("alpha")
	raw.PutInt(3)

	b.Logf("%x", raw.Buf)

	buf := new(bytes.Reader)

	r := NewReader(buf)

	b.Run("Struct", func(b *testing.B) {
		b.SetBytes(int64(len(raw.Buf)))
		b.ReportAllocs()
		var serverHello ServerHello
		for i := 0; i < b.N; i++ {
			buf.Reset(raw.Buf)
			r.s.Reset(buf)

			if err := serverHello.Decode(r); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Raw", func(b *testing.B) {
		b.SetBytes(int64(len(raw.Buf)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf.Reset(raw.Buf)
			r.s.Reset(buf)

			name, err := r.StrRaw()
			if err != nil {
				b.Fatal(err)
			}

			major, err := r.Int()
			if err != nil {
				b.Fatal(err)
			}
			minor, err := r.Int()
			if err != nil {
				b.Fatal(err)
			}
			revision, err := r.Int()
			if err != nil {
				b.Fatal(err)
			}

			if FeatureTimezone.In(revision) {
				v, err := r.StrRaw()
				if err != nil {
					b.Fatal(err)
				}
				_ = v
			}
			if FeatureDisplayName.In(revision) {
				v, err := r.StrRaw()
				if err != nil {
					b.Fatal(err)
				}
				_ = v
			}
			if FeatureVersionPatch.In(revision) {
				v, err := r.Int()
				if err != nil {
					b.Fatal(err)
				}
				_ = v
			}

			_ = name
			_ = major
			_ = minor
			_ = revision
		}
	})
}
