package ch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/proto"
)

func TestDial_Exception(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, err := Dial(ctx, withServer(Options{
		Password: "invalid_password",
	}, testServer(t)))

	var e *Exception
	require.Nil(t, client)
	require.ErrorAs(t, err, &e)
	require.Truef(t, IsErr(err, proto.ErrAuthenticationFailed, proto.ErrRequiredPassword), "got %v", err)
}
