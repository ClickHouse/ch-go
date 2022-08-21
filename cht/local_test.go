package cht

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func colStr(data []string) proto.ColStr {
	var v proto.ColStr
	for _, s := range data {
		v.Append(s)
	}
	return v
}

func TestLocalNativeDump(t *testing.T) {
	bin := BinOrSkip(t)
	t.Parallel()

	ctx := context.Background()
	srv := New(t)
	db, err := ch.Dial(ctx, ch.Options{Address: srv.TCP})
	require.NoError(t, err)
	info := db.ServerInfo()
	require.NoError(t, db.Close())

	if info.Major < 22 {
		t.Skip("Skipping versions before v22")
	}

	// Testing clickhouse-local.
	buf := new(proto.Buffer)
	b := proto.Block{Rows: 2, Columns: 2}
	require.NoError(t, b.EncodeRawBlock(buf, 54451, []proto.InputColumn{
		{Name: "title", Data: colStr([]string{"Foo", "Bar"})},
		{Name: "data", Data: proto.ColInt64{1, 2}},
	}), "encode")

	dir := t.TempDir()
	inFile := filepath.Join(dir, "data.native")
	require.NoError(t, os.WriteFile(inFile, buf.Buf, 0600), "write file")

	cmd := exec.Command(bin, "local",
		"--logger.console",
		"--log-level", "trace",
		"--file", inFile,
		"--input-format", "Native",
		"--output-format", "JSON",
		"--query", "SELECT * FROM table",
	)
	out := new(bytes.Buffer)
	errOut := new(bytes.Buffer)
	cmd.Stdout = out
	cmd.Stderr = errOut

	t.Log(cmd.Args)
	require.NoError(t, cmd.Run(), "run: %s", errOut)
	t.Log(errOut)

	v := struct {
		Rows int `json:"rows"`
		Data []struct {
			Title string `json:"title"`
			Data  int    `json:"data,string"`
		}
	}{}
	require.NoError(t, json.Unmarshal(out.Bytes(), &v), "json")
	assert.Equal(t, 2, v.Rows)
	if assert.Len(t, v.Data, 2) {
		for i, r := range []struct {
			Title string `json:"title"`
			Data  int    `json:"data,string"`
		}{
			{"Foo", 1},
			{"Bar", 2},
		} {
			assert.Equal(t, r, v.Data[i])
		}
	}
}
