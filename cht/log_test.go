package cht

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLog(t *testing.T) {
	t.Run("Cut", func(t *testing.T) {
		for _, tc := range []struct {
			Input  string
			Left   string
			Right  string
			Output string
		}{
			{},
			{
				Input: "123214] [12345]][]",
				Left:  "[", Right: "]",
				Output: "12345",
			},
			{
				Input: "[ 591781 ] {} <Debug> TCP-Session: b75e863b-c0c0-4b7b-b75e-863bc",
				Left:  "{", Right: "}",
				Output: "",
			},
			{
				Input: "[ 591781 ] {} <Debug> TCP-Session: b75e863b-c0c0-4b7b-b75e-863bc",
				Left:  "<", Right: ">",
				Output: "Debug",
			},
			{
				Input: "2022.01.19 16:58:24.256488 [ 591781 ] {} <Trace> ContextAccess (default): List of all grants including implicit",
				Left:  ">", Right: ":",
				Output: "ContextAccess (default)",
			},
			{
				Input: "{f9464441-7023-4df5-89e5-8d16ea6aa2dd} <Information> executeQuery: Read 2 rows, 204.00 B in 0.000814392 sec., 2455 rows/sec., 244.62 KiB/sec.",
				Left:  "{", Right: "}",
				Output: "f9464441-7023-4df5-89e5-8d16ea6aa2dd",
			},
			{
				Input: "[ 591781 ] {} <Debug> TCP-Session: b75e863b-c0c0-4b7b-b75e-863bc",
				Left:  "[", Right: "]",
				Output: "591781",
			},
		} {
			if s := cut(tc.Input, tc.Left, tc.Right); s != tc.Output {
				t.Errorf("cut(%s, %s, %s) %s != %s (expected)",
					tc.Input, tc.Left, tc.Right, s, tc.Output,
				)
			}
		}
	})
	t.Run("Parse", func(t *testing.T) {
		for _, tc := range []struct {
			Input string
			Entry LogEntry
		}{
			{},
			{
				Input: "2022.01.19 16:58:24.257025 [ 591781 ] {f9464441-7023-4df5-89e5-8d16ea6aa2dd} <Information> executeQuery: Read 2 rows, 204.00 B in 0.000814392 sec., 2455 rows/sec., 244.62 KiB/sec.",
				Entry: LogEntry{
					QueryID:  "f9464441-7023-4df5-89e5-8d16ea6aa2dd",
					Name:     "executeQuery",
					Message:  "Read 2 rows, 204.00 B in 0.000814392 sec., 2455 rows/sec., 244.62 KiB/sec.",
					Severity: "Information",
					ThreadID: 591781,
				},
			},
			{
				Input: "2022.01.19 16:58:24.256479 [ 591781 ] {} <Trace> ContextAccess (default): Settings: readonly=0, allow_ddl=true, allow_introspection_functions=false",
				Entry: LogEntry{
					Name:     "ContextAccess (default)",
					Message:  "Settings: readonly=0, allow_ddl=true, allow_introspection_functions=false",
					Severity: "Trace",
					ThreadID: 591781,
				},
			},
		} {
			assert.Equal(t, tc.Entry, parseLog(tc.Input), "input: %q", tc.Input)
		}
	})
}
