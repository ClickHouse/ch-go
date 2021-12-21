package proto

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDump(t *testing.T) {
	// Testing decoding of Native format dump.
	//
	// CREATE TABLE test_dump (id Int8, v String)
	//   ENGINE = MergeTree()
	// ORDER BY id;
	//
	// SELECT * FROM test_dump
	//   ORDER BY id
	// INTO OUTFILE 'test_dump_native.raw' FORMAT Native;
	data, err := os.ReadFile(filepath.Join("_testdata", "test_dump_native.raw"))
	require.NoError(t, err)
	var (
		dec    Block
		ids    ColInt8
		values ColStr
	)
	require.NoError(t, dec.DecodeRawBlock(
		NewReader(bytes.NewReader(data)),
		Results{
			{Name: "id", Data: &ids},
			{Name: "v", Data: &values},
		}),
	)
}
