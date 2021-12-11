package ch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClient_Ping(t *testing.T) {
	t.Parallel()
	require.NoError(t, Conn(t).Ping(context.Background()))
}
