package wrappers

import (
	"bytes"
	"io"
)

// BytesReadCloser wraps a bytes.Reader and provides a noop Close method.
type BytesReadCloser struct {
  *bytes.Reader
  io.Closer
}

// Close is a noop that always returns nil.
func (r *BytesReadCloser) Close() error {
  return nil
}

// NewBytesReadCloser returns a wrapper around the specified reader.
func NewBytesReadCloser(r *bytes.Reader) *BytesReadCloser {
  return &BytesReadCloser{Reader: r}
}
