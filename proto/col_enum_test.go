package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColEnum_ParseEscapesAndDelimiters(t *testing.T) {
	t.Run("EscapedCharacters", func(t *testing.T) {
		var col ColEnum
		colType := ColumnType(`Enum8('a\'b' = 1, 'line\nbreak' = 2, 'back\\' = 3, 'q\'' = 4, 'tabs\t\r\0\b\f\v\a\e' = 5, 'hex\x41' = 6)`)
		require.NoError(t, col.Infer(colType))

		want := []string{
			"a'b",
			"line\nbreak",
			"back\\",
			"q'",
			"tabs\t\r\x00\b\f\v\a\x1b",
			"hexA",
		}
		col.AppendArr(want)
		require.NoError(t, col.Prepare())

		var buf Buffer
		col.EncodeColumn(&buf)

		var dec ColEnum
		require.NoError(t, dec.Infer(colType))
		require.NoError(t, dec.DecodeColumn(buf.Reader(), len(want)))
		require.Equal(t, want, dec.Values)
	})

	t.Run("CommasEqualsAndParens", func(t *testing.T) {
		var col ColEnum
		colType := ColumnType(`Enum8('x,y' = 1, 'p=q' = 2, 'a,b=c\'d' = -5, '' = 0, '(z)' = 7, 'it''s' = 8)`)
		require.NoError(t, col.Infer(colType))

		want := []string{"x,y", "p=q", "a,b=c'd", "", "(z)", "it's"}
		col.AppendArr(want)
		require.NoError(t, col.Prepare())

		var buf Buffer
		col.EncodeColumn(&buf)

		var dec ColEnum
		require.NoError(t, dec.Infer(colType))
		require.NoError(t, dec.DecodeColumn(buf.Reader(), len(want)))
		require.Equal(t, want, dec.Values)
	})

	t.Run("Enum16ViaColAuto", func(t *testing.T) {
		colType := ColumnType(`Enum16('k1,k2' = -300, 'a=b\\c' = 300)`)
		var auto ColAuto
		require.NoError(t, auto.Infer(colType))

		col, ok := auto.Data.(*ColEnum)
		require.True(t, ok)

		want := []string{"k1,k2", "a=b\\c"}
		col.AppendArr(want)
		require.NoError(t, col.Prepare())

		var buf Buffer
		col.EncodeColumn(&buf)

		var dec ColAuto
		require.NoError(t, dec.Infer(colType))
		require.NoError(t, dec.DecodeColumn(buf.Reader(), len(want)))
		decEnum, ok := dec.Data.(*ColEnum)
		require.True(t, ok)
		require.Equal(t, want, decEnum.Values)
	})

	t.Run("InvalidDefinitions", func(t *testing.T) {
		for _, tc := range []ColumnType{
			"Enum8()",
			"Enum8('unterminated = 1)",
			"Enum8('dangling_quote\\')",
			"Enum8('missing_eq' 1)",
			"Enum8('bad_hex\\xZZ' = 1)",
			"Enum8('not_an_int' = abc)",
			"Enum8('sign_only' = -)",
			"Enum8('overflow' = 200)",
			"Enum8('trailing_comma' = 1, )",
			"Enum8('extra_token' = 1 foo)",
			"Enum8(unquoted = 1)",
		} {
			var col ColEnum
			require.Error(t, col.Infer(tc), "expected error for %s", tc)
		}

		_, err := parseEnumDefs("'a\\", 8)
		require.Error(t, err)
	})
}
