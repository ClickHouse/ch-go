package ch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/proto"
)

func TestQueryParameters(t *testing.T) {
	conn := Conn(t)
	SkipNoFeature(t, conn, proto.FeatureParameters)
	ctx := context.Background()

	var (
		num                proto.ColUInt8
		str                proto.ColStr
		quote              proto.ColStr
		escapedRaw         proto.ColStr
		escapedInterpreted proto.ColStr
		literalBackslash   proto.ColStr
	)
	require.NoError(t, conn.Do(ctx, Query{
		Body: `SELECT
			{num:UInt8},
			{str:String},
			{quote:String},
			{escaped_raw:String},
			{escaped_interpreted:String},
			{literal_backslash:String}`,
		Parameters: Parameters(map[string]any{
			"num":                 100,
			"str":                 "foo",
			"quote":               "it's",
			"escaped_raw":         `line 1\nline 2\tend`,
			"escaped_interpreted": "line 1\\nline 2\\tend",
			"literal_backslash":   `line 1\\nline 2`,
		}),
		Result: proto.Results{
			{Data: &num},
			{Data: &str},
			{Data: &quote},
			{Data: &escapedRaw},
			{Data: &escapedInterpreted},
			{Data: &literalBackslash},
		},
	}))
	require.Equal(t, uint8(100), num.Row(0))
	require.Equal(t, "foo", str.Row(0))
	require.Equal(t, "it's", quote.Row(0))
	require.Equal(t, "line 1\nline 2\tend", escapedRaw.Row(0))
	require.Equal(t, "line 1\nline 2\tend", escapedInterpreted.Row(0))
	require.Equal(t, `line 1\nline 2`, literalBackslash.Row(0))

	t.Run("escaped string values", func(t *testing.T) {
		cases := []struct {
			name  string
			value string
			want  string
		}{
			{"raw literal with escapes", `line 1\nline 2\tend`, "line 1\nline 2\tend"},
			{"interpreted literal with escaped backslashes", "line 1\\nline 2\\tend", "line 1\nline 2\tend"},
			{"raw literal with literal backslashes", `line 1\\nline 2\\tend`, `line 1\nline 2\tend`},
			{"interpreted literal with literal backslashes", "line 1\\\\nline 2\\\\tend", `line 1\nline 2\tend`},
			{"single quote", "it's", "it's"},
			// Escaped-format `\\` is a literal backslash; a lone `\b` would be backspace.
			{"literal backslash", `a\\b`, `a\b`},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var got proto.ColStr
				err := conn.Do(ctx, Query{
					Body:       "SELECT {value:String}",
					Parameters: Parameters(map[string]any{"value": tc.value}),
					Result:     proto.Results{{Data: &got}},
				})
				require.NoError(t, err)
				require.Equal(t, tc.want, got.Row(0))
			})
		}

		for name, value := range map[string]string{
			"actual newline": "line 1\nline 2",
			"actual tab":     "column 1\tcolumn 2",
		} {
			t.Run("reject "+name, func(t *testing.T) {
				conn := Conn(t)
				err := conn.Do(ctx, Query{
					Body:       "SELECT {value:String}",
					Parameters: Parameters(map[string]any{"value": value}),
					Result:     discardResult(),
				})
				require.Error(t, err, "value %q should be rejected", value)
			})
		}
	})
}
