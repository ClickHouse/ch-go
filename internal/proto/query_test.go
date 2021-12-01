package proto

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuery_DecodeAware(t *testing.T) {
	data, err := hex.DecodeString(`012432336164326330372d326636382d343030352d396261632d64613866343637626464336201002432336164326330372d326636382d343030352d396261632d64613866343637626464336209302e302e302e303a300000000000000000010665726e61646f056e657875730b436c69636b486f75736520150bb2a90300000400000002001543524541544520444154414241534520746573743b0200010002ffffffff000000`)
	require.NoError(t, err)

	var q Query

	r := NewReader(bytes.NewReader(data))
	v, err := r.Uvarint()
	require.NoError(t, err)
	require.Equal(t, ClientCodeQuery, ClientCode(v))

	require.NoError(t, q.DecodeAware(r, int(FeatureQueryStartTime)))
	require.Equal(t, q.Body, "CREATE DATABASE test;")

	t.Logf("%+v", q)
}
