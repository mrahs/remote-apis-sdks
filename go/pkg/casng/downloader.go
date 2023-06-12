package casng

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	bspb "google.golang.org/genproto/googleapis/bytestream"
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

type downloader struct {
	cas          repb.ContentAddressableStorageClient
	byteStream   bspb.ByteStreamClient
	instanceName string

	streamRPCCfg GRPCConfig

	// gRPC throttling controls.
	streamThrottle *throttler // Controls concurrent calls to the byte streaming API.

	// IO controls.
	ioCfg        IOConfig
	buffers      sync.Pool
	zstdEncoders sync.Pool
}

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

func (d *BatchingDownloader) ReadBytes(ctx context.Context, name string, offset int64, limit int64, writer io.Writer) (digest.Digest, Stats, error) {
	contextmd.Infof(ctx, log.Level(1), "[casng] download.read_bytes: name=%s, offset=%d, limit=%d", name, offset, limit)
	defer contextmd.Infof(ctx, log.Level(1), "[casng] upload.write_bytes.done: name=%s, offset=%d, limit=%d", name, offset, limit)
	if log.V(3) {
		startTime := time.Now()
		defer func() {
			log.Infof("[casng] download.read_bytes.duration: start=%d, end=%d, name=%s, chunk_size=%d", startTime.UnixNano(), time.Now().UnixNano(), name, d.ioCfg.BufferSize)
		}()
	}
	var stats Stats
	var digest digest.Digest

	// TODO: create a pipe for digestion.
	// TODO: create a pipe for decompression.

	ctx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()

	stream, errStream := d.byteStream.Read(ctx, &bspb.ReadRequest{
		ResourceName: name,
		ReadOffset:   offset,
		ReadLimit:    limit,
	})
	if errStream != nil {
		return digest, stats, errors.Join(ErrGRPC, errStream)
	}

	// buf slice is never resliced which makes it safe to use a pointer-like type.
	buf := d.buffers.Get().([]byte)
	defer d.buffers.Put(buf)

	var err error
	for {
		var resp *bspb.ReadResponse
		errStream := retry.WithPolicy(ctx, d.streamRPCCfg.RetryPredicate, d.streamRPCCfg.RetryPolicy, func() error {
			timer := time.NewTimer(d.streamRPCCfg.Timeout)
			// Ensure the timer goroutine terminates if Recv does not timeout.
			success := make(chan struct{})
			defer close(success)
			go func() {
				select {
				case <-timer.C:
					ctxCancel() // Cancel the stream to allow Recv to return.
				case <-success:
				}
			}()
			r, errStream := stream.Recv()
			if r != nil {
				stats.TotalBytesMoved += int64(len(r.Data))
			}
			resp = r
			return errStream
		})

		if resp != nil {
			stats.LogicalBytesMoved += int64(len(resp.Data))
		}

		if errStream == io.EOF {
			break
		}
		if errStream != nil {
			err = errors.Join(ErrGRPC, errStream, err)
			break
		}

		n, errWrite := writer.Write(resp.Data)
		if errWrite != nil {
			err = errors.Join(ErrIO, errWrite, err)
			break
		}
		if n < len(resp.Data) {
			err = errors.Join(ErrIO, fmt.Errorf("received %d bytes, but only %d were written", len(resp.Data), n), err)
			break
		}
	}

	// TODO: update stats based on compression.
	stats.LogicalBytesStreamed = stats.LogicalBytesMoved
	if err == nil {
		stats.StreamedCount = 1
	}
	return digest, stats, err
}
