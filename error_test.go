package ch

import (
	"context"
	"io"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/multierr"
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
