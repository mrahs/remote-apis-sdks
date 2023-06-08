package casng

import (
	"context"
	"errors"
	"io"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
)

type downloader struct{}

// Download a list into paths.
func (d *downloader) Download(ctx context.Context, spec map[digest.Digest]impath.Relative, root impath.Absolute) (Stats, error) {
	return Stats{}, errors.New("not yet implemented")
}

// Download a tree into path.
func (d *downloader) DownloadTree(ctx context.Context, digest digest.Digest, root impath.Absolute) (Stats, error) {
	return Stats{}, errors.New("not yet implemented")
}

// Download digests into memory.
func (d *downloader) Read(ctx context.Context, digests ...digest.Digest) (map[digest.Digest][]byte, Stats, error) {
	return nil, Stats{}, errors.New("not yet implemented")
}

// Stream bytes to the writer.
func (d *downloader) ReadBytes(ctx context.Context, name string, offset int64, limit int64, writer io.Writer) (Stats, error) {
	return Stats{}, errors.New("not yet implemented")
}

