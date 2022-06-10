package ch

import (
	"context"
	"io"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	"github.com/ClickHouse/ch-go/proto"
)

func TestError(t *testing.T) {
	err := errors.Wrap(multierr.Append(
		errors.Wrap(io.EOF, "foo"),
		errors.Wrap(context.Canceled, "bar"),
	), "parent")

	t.Log(err)
	assert.ErrorIs(t, err, io.EOF)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestException_Error(t *testing.T) {
	err := errors.Wrap(multierr.Append(
		errors.Wrap(&Exception{
			Code: proto.ErrBadArguments,
		}, "foo"),
		errors.Wrap(context.Canceled, "bar"),
	), "parent")

	var ex *Exception
	require.ErrorAs(t, err, &ex)

	require.True(t, IsException(err), "IsException should be true")
	e, ok := AsException(err)
	require.True(t, ok)
	require.NotNil(t, e)

	require.True(t, IsErr(err, proto.ErrBadArguments))
	require.False(t, IsErr(err, proto.ErrTableIsDropped))
	require.False(t, IsErr(io.EOF, proto.ErrBadArguments))
}
