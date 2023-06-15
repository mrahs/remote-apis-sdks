package casng

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"github.com/klauspost/compress/zstd"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func MakeReadResourceName(instanceName, hash string, size int64) string {
	return fmt.Sprintf("%s/blobs/%s/%d", instanceName, hash, size)
}

func MakeCompressedReadResourceName(instanceName, hash string, size int64) string {
	return fmt.Sprintf("%s/compressed-blobs/zstd/%s/%d", instanceName, hash, size)
}

func IsCompressedReadResourceName(name string) bool {
	return strings.Contains(name, "compressed-blobs/zstd")
}

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
	zstdDecoders sync.Pool

	ctx context.Context
}

func NewDownloader(
	ctx context.Context, cas repb.ContentAddressableStorageClient, byteStream bspb.ByteStreamClient, instanceName string,
	queryCfg, uploadCfg, streamCfg GRPCConfig, ioCfg IOConfig,
) (*BatchingDownloader, error) {
	if cas == nil || byteStream == nil {
		return nil, ErrNilClient
	}
	if err := validateGrpcConfig(&queryCfg); err != nil {
		return nil, err
	}
	if err := validateGrpcConfig(&uploadCfg); err != nil {
		return nil, err
	}
	if err := validateGrpcConfig(&streamCfg); err != nil {
		return nil, err
	}
	if err := validateIOConfig(&ioCfg); err != nil {
		return nil, err
	}
	d := &downloader{
		ctx: ctx,

		cas:          cas,
		byteStream:   byteStream,
		instanceName: instanceName,

		streamRPCCfg: streamCfg,

		streamThrottle: newThrottler(int64(streamCfg.ConcurrentCallsLimit)),

		ioCfg: ioCfg,
		zstdDecoders: sync.Pool{
			New: func() any {
				// Providing a nil writer implies that the decoder needs to be (re)initilaized with a reader using dec.Reset(r) before using it.
				dec, _ := zstd.NewReader(nil)
				return dec
			},
		},
	}
	return &BatchingDownloader{d}, nil
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

// ReadBytes downloads the blob asscoated with the resource name, starting at offset and up to limit, and writes it to writer.
//
// offset must be positive and less than the size of the blob.
// limit must be positive. A value of 0 means no limit.
// Returns the digest of the downloaded bytes and download stats.
func (d *BatchingDownloader) ReadBytes(ctx context.Context, name string, offset int64, limit int64, writer io.Writer) (digest.Digest, Stats, error) {
	contextmd.Infof(ctx, log.Level(1), "[casng] download.read_bytes: name=%s, offset=%d, limit=%d", name, offset, limit)
	defer contextmd.Infof(ctx, log.Level(1), "[casng] upload.write_bytes.done: name=%s, offset=%d, limit=%d", name, offset, limit)

	startTime := time.Now()
	if !d.streamThrottle.acquire(ctx) {
		return digest.Digest{}, Stats{}, ctx.Err()
	}
	defer d.streamThrottle.release()
	log.V(3).Infof("[casng] download.read_bytes.throttle.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())

	if log.V(3) {
		startTime := time.Now()
		defer func() {
			log.Infof("[casng] download.read_bytes.duration: start=%d, end=%d, name=%s", startTime.UnixNano(), time.Now().UnixNano(), name)
		}()
	}

	var wg sync.WaitGroup
	var dg digest.Digest
	var stats Stats
	var w io.WriteCloser

	// Digest and pipe a copy of the bytes to the client's writer.
	var errDg error
	prDg, pwDg := io.Pipe()
	readerDg := io.TeeReader(prDg, writer)
	wg.Add(1)
	go func() {
		defer wg.Done()
		dg, errDg = digest.NewFromReader(readerDg)
	}()

	// By default, bytes are written to the digster's writer, which pipes them to the client's.
	w = pwDg

	// If compression is on, plug in the decoder via a pipe.
	var errDec error
	var nRawBytes int64 // Track the actual number of written raw bytes.
	var withDecompression bool
	if IsCompressedReadResourceName(name) {
		contextmd.Infof(ctx, log.Level(1), "[casng] download.read_bytes.decompressing: name=%s", name)
		withDecompression = true
		prDec, pwDec := io.Pipe()

		dec := d.zstdDecoders.Get().(*zstd.Decoder)
		defer d.zstdDecoders.Put(dec)
		// (Re)initialize the encoder with this reader.
		errDec = dec.Reset(prDec)
		if errDec != nil {
			return dg, stats, errors.Join(ErrIO, errDec)
		}
		// Get it going.
		wg.Add(1)
		go func() {
			defer wg.Done()

			nRawBytes, errDec = dec.WriteTo(pwDg)
			// Closing the decoder is necessary to flush remaining bytes.
			dec.Close()
			if errors.Is(errDec, io.ErrClosedPipe) {
				// pr was closed first, which means the actual error is on that end.
				errDec = nil
			}
			if errClose := pwDg.Close(); errClose != nil {
				errDec = errors.Join(ErrIO, errClose)
			}
		}()

		// The bytes should be written to the decoder, which writres the decoded bytes to the digster's writer, which pipes them to the client's.
		w = pwDec
	}

	ctx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()

	stream, errStream := d.byteStream.Read(ctx, &bspb.ReadRequest{
		ResourceName: name,
		ReadOffset:   offset,
		ReadLimit:    limit,
	})
	if errStream != nil {
		return dg, stats, errors.Join(ErrGRPC, errStream)
	}

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
			stats.EffectiveBytesMoved += int64(len(resp.Data))
		}

		n, errWrite := w.Write(resp.Data)
		if errWrite != nil {
			err = errors.Join(ErrIO, errWrite, err)
			break
		}
		if n < len(resp.Data) {
			err = errors.Join(ErrIO, fmt.Errorf("received %d bytes, but only %d were written", len(resp.Data), n), err)
			break
		}

		if errStream == io.EOF {
			break
		}
		if errStream != nil {
			err = errors.Join(ErrGRPC, errStream, err)
			break
		}
	}

	// Close own writer to ensure the digester (and the decoder, if any) to terminate.
	if errClose := w.Close(); errClose != nil {
		err = errors.Join(ErrIO, errClose, err)
	}

	// The decoder and the digester should terminate on EOF or another error.
	wg.Wait()
	if errDec != nil {
		err = errors.Join(ErrCompression, errDec, err)
	}
	if errDg != nil {
		err = errors.Join(ErrIO, errDg, err)
	}

	stats.LogicalBytesMoved = stats.EffectiveBytesMoved
	if withDecompression {
		// nRawBytes may be smaller than compressed bytes (additional headers without effective compression).
		stats.LogicalBytesMoved = nRawBytes
	}
	stats.LogicalBytesStreamed = stats.LogicalBytesMoved
	if err == nil {
		stats.StreamedCount = 1
	}
	return dg, stats, err
}
