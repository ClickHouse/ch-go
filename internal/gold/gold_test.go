package gold_test

import (
	"os"
	"testing"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestStr(t *testing.T) {
	gold.Str(t, "Hello, world!\n", "hello.txt")
}

func TestBytes(t *testing.T) {
	gold.Bytes(t, append([]byte{1, 2, 3}, "Hi!"...))
}

func TestMain(m *testing.M) {
	// Explicitly registering flags for golden files.
	gold.Init()

	os.Exit(m.Run())
}
