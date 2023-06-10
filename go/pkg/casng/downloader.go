package casng

import (
	"context"
	"errors"
	"io"
	"io/fs"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
)

type DownloadRequest struct {
	Digest digest.Digest
	Path   impath.Absolute
	Mode   fs.FileMode
}

type Downloader struct{}

func NewDownloader() *Downloader {
	return &Downloader{}
}

func (d *Downloader) Download(ctx context.Context, reqs ...DownloadRequest) (Stats, error) {
	return Stats{}, errors.New("not yet implemented")
}

func (d *Downloader) DownloadTree(ctx context.Context, digest digest.Digest, root impath.Absolute) (Stats, error) {
	return Stats{}, errors.New("not yet implemented")
}

func (d *Downloader) Read(ctx context.Context, digests ...digest.Digest) (map[digest.Digest][]byte, Stats, error) {
	return nil, Stats{}, errors.New("not yet implemented")
}

func (d *Downloader) ReadBytes(ctx context.Context, name string, offset int64, limit int64, writer io.Writer) (Stats, error) {
	return Stats{}, errors.New("not yet implemented")
}
