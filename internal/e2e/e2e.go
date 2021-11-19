// Package e2e implements end to end testing utilities.
package e2e

import (
	"os"
	"strconv"
	"testing"
)

// Env variable for E2E tests.
const Env = "CH_E2E"

// Skip test if Env is not set to 1, t, T, TRUE, true, True.
func Skip(tb testing.TB) {
	tb.Helper()

	s, ok := os.LookupEnv(Env)
	if !ok {
		tb.Skipf("E2E: %s not set, skipping", Env)
	}

	v, err := strconv.ParseBool(s)
	if err != nil {
		tb.Fatalf("E2E: %s=%s is invalid: %v", Env, s, err)
	}

	if !v {
		tb.Skipf("E2E: %s=%s (%v), skipping", Env, s, v)
	}
}
