package ch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatParameterValue(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  string
	}{
		{"plain string", "foo", `'foo'`},
		{"integer", 100, `'100'`},
		{"single quote", "it's", `'it\'s'`},
		{"backslash", `a\b`, `'a\\b'`},
		{"backslash before quote", `a\'b`, `'a\\\'b'`},
		{"escaped newline text", `line 1\nline 2`, `'line 1\\nline 2'`},
		{"literal backslash-n text", `line 1\\nline 2`, `'line 1\\\\nline 2'`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, formatParameterValue(tc.value))
		})
	}
}

func TestParametersEscapesQuotesAndBackslashes(t *testing.T) {
	got := Parameters(map[string]any{
		"q": "it's",
		"b": `a\b`,
		"n": 42,
	})
	require.Len(t, got, 3)
	// Parameters sorts by key.
	assert.Equal(t, "b", got[0].Key)
	assert.Equal(t, `'a\\b'`, got[0].Value)
	assert.Equal(t, "n", got[1].Key)
	assert.Equal(t, `'42'`, got[1].Value)
	assert.Equal(t, "q", got[2].Key)
	assert.Equal(t, `'it\'s'`, got[2].Value)
}
