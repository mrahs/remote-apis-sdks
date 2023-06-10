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
	// Mode is used to tell if this request if for a directory, Mode.IsDir(), in which case DownloadTree is used.
	// Otherwise, if it must be a symlink (fs.ModeSymlink is set) or a regular file (Mode.IsRegular() is true) and only Mode.Perm() is used.
	Mode fs.FileMode
}

type BatchingDownloader struct {
	*downloader
}

type downloader struct{}

func NewDownloader() *BatchingDownloader {
	return &BatchingDownloader{&downloader{}}
}

func (d *BatchingDownloader) Download(ctx context.Context, reqs ...DownloadRequest) (Stats, error) {
	return Stats{}, errors.New("not yet implemented")
}

func (d *BatchingDownloader) DownloadTree(ctx context.Context, digest digest.Digest, root impath.Absolute) (Stats, error) {
	// Should handle multiple copies (paths) for the same digest.
	// Symlinks should be written after the targets?
	// Should clear existing directories and overwrite files.
	// Should update fileNodeCache.
	return Stats{}, errors.New("not yet implemented")
}

func (d *BatchingDownloader) Read(ctx context.Context, digests ...digest.Digest) (map[digest.Digest][]byte, Stats, error) {
	return nil, Stats{}, errors.New("not yet implemented")
}

func (d *BatchingDownloader) ReadBytes(ctx context.Context, name string, offset int64, limit int64, writer io.Writer) (Stats, error) {
	return Stats{}, errors.New("not yet implemented")
}
