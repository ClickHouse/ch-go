package ch

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
)

// parameterQuoteEscaper escapes a value for a quoted Field dump. The server
// restores custom settings / query parameters with readQuoted, so bare
// backslashes and single quotes must be escaped before the value is wrapped
// in quotes. Control characters are intentionally left alone: callers still
// pass ClickHouse Escaped-format text (e.g. `\n` for a newline).
var parameterQuoteEscaper = strings.NewReplacer(
	`\`, `\\`,
	`'`, `\'`,
)

// formatParameterValue formats v as a quoted Field dump for Query.Parameters.
func formatParameterValue(v any) string {
	return "'" + parameterQuoteEscaper.Replace(fmt.Sprint(v)) + "'"
}

// Parameters is a helper for building Query.Parameters.
//
// Each value is formatted with fmt and encoded as a quoted Field dump
// (`'...'`), escaping `\` and `'` so the server's readQuoted step restores the
// original text. ClickHouse then parses that text as the placeholder's
// declared type using Escaped format, so backslash escapes such as `\n` and
// `\t` are still interpreted there.
//
// Pass Escaped-format text (not a raw Go string with actual newlines). For
// example, use `line 1\nline 2` to receive a newline, or `line 1\\nline 2`
// to receive the literal characters \ and n. Actual tab/newline bytes are
// rejected by the server.
//
// EXPERIMENTAL.
func Parameters(m map[string]any) []proto.Parameter {
	var out []proto.Parameter
	for k, v := range m {
		out = append(out, proto.Parameter{
			Key:   k,
			Value: formatParameterValue(v),
		})
	}
	// Sorting to make output deterministic.
	sort.Slice(out, func(i, j int) bool {
		return out[i].Key < out[j].Key
	})

	return out
}
