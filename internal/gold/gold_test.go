package gold_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/internal/gold"
)

func TestReadFile(t *testing.T) {
	const (
		name = "hello.txt"
		text = "Hello, world!"
	)
	require.Equal(t, text, strings.TrimSpace(string(gold.ReadFile(t, name))))
}
