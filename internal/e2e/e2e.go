// Package e2e implements end to end testing utilities.
package e2e

import (
	"os"
	"strconv"
	"testing"
)

// Env variable for E2E tests.
const Env = "CH_E2E"

type Status byte

const (
	NotSet   Status = iota // N/A
	Enabled                // explicitly enabled
	Disabled               // explicitly disabled
)

// Get reports current end-to-end status.
func Get(tb testing.TB) Status {
	tb.Helper()
	s, ok := os.LookupEnv(Env)
	if !ok || s == "" {
		return NotSet
	}
	v, err := strconv.ParseBool(s)
	if err != nil {
		tb.Fatalf("E2E: %s=%s is invalid: %v", Env, s, err)
	}
	if v {
		return Enabled
	}
	return Disabled
}
