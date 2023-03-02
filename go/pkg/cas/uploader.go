package cas

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/blob"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/exppath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	rpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/support/bundler"
)

var ErrNegativeLimit = errors.New("limit value must be >= 0")
var ErrZeroOrNegativeLimit = errors.New("limit value must be > 0")
var ErrNilClient = errors.New("client cannot be nil")

// MakeWriteResourceName returns a valid resource name for writing an uncompressed blob.
func MakeWriteResourceName(instanceName, hash string, size int64) string {
	return fmt.Sprintf("%s/uploads/%s/blobs/%s/%d", instanceName, uuid.New(), hash, size)
}

// MakeCompressedWriteResourceName returns a valid resource name for writing a compressed blob.
func MakeCompressedWriteResourceName(instanceName, hash string, size int64) string {
	return fmt.Sprintf("%s/uploads/%s/compressed-blobs/zstd/%s/%d", instanceName, uuid.New(), hash, size)
}

// PrepareActionResult computes a list of blobs and constructs an ActionResult ready to be inspected or uploaded.
func PrepareActionResult(
	ctx context.Context, execRoot exppath.Abs, workDir exppath.Rel, paths []exppath.Rel,
	symlinkOpts symlinkopts.Opts, cache filemetadata.Cache) (map[digest.Digest]blob.Blob, repb.ActionResult, error) {
	panic("not yet implemented")
}

// MerkleTree computes a merkle tree from the specified InputSpec and returns a list of blobs ready to be inspected or uploaded.
func MerkleTree(
	ctx context.Context, execRoot exppath.Abs, workDir exppath.Rel, remoteWorkDir exppath.Rel,
	spec command.InputSpec, cache filemetadata.Cache) (digest.Digest, []blob.Blob, Stats, error) {
	panic("not yet implemented")
}

// BatchingUploader provides a simple imperative API to upload to the CAS.
type BatchingUploader interface {
	// MissingBlobs queries the CAS for the specified digests and returns a slice of the missing ones.
	MissingBlobs(context.Context, []digest.Digest) ([]digest.Digest, error)

	// Upload deduplicates the specified blobs (unified uploads) and only uploads the ones that are not already in the CAS.
	// Large files are streamed while others are batched together.
	// Returns a slice of the digests of the uploaded blobs, excluding the ones that already exist in the CAS.
	Upload(context.Context, []blob.Blob, symlinkopts.Opts, exppath.Predicate) ([]digest.Digest, Stats, error)

	// WriteBytes uploads all of the specified bytes directly to the specified resource name starting remotely at the specified offset.
	// If finish is true, the server is notified to finalize the resource name and no further writes are allowed.
	WriteBytes(ctx context.Context, name string, bytes []byte, offset int64, finish bool) (Stats, error)
}

// StreamingUploader provides a concurrency friendly API to upload to the CAS.
type StreamingUploader interface {
	// MissingBlobs is a blocking call that queries the CAS for incoming digests.
	// It returns when the context is done, the input channel is closed, or a fatal error occurs.
	// The returned error is either from the context or an error that necessitates terminating the call.
	// Errors related to particular items are reported as part of their result.
	// During the lifetime of the call, digests are cached to avoid duplicate queries.
	MissingBlobs(context.Context, <-chan digest.Digest) (<-chan MissingBlobsResponse, error)

	// Upload is a blocking call that uploads incoming blobs to the CAS.
	// It returns when the context is done, the input channel is closed, or a fatal error occurs.
	// The returned error is either from the context or an error that necessitates terminating the call.
	// Errors related to particular items are reported as part of their result.
	// During the lifetime of the call, digests are cached to avoid duplicate uploads (unified uploads).
	Upload(context.Context, <-chan blob.Blob, symlinkopts.Opts, exppath.Predicate) (<-chan UploadResponse, error)

	// WriteBytes is a blocking call that uploads incoming bytes to the CAS at the specified resource name starting remotely at the specified offset.
	// It returns when the context is done, the input channel is closed, or a fatal error occurs.
	// Each outgoing integer value corresponds with an incoming item and represents the total bytes moved over the wire while streaming the bytes. This may be higher (retries) or lower (interrupted) than the number of incoming bytes.
	// If finish is true, the server is notified to finalize the resource name and no further writes are allowed.
	WriteBytes(ctx context.Context, name string, bytesChan <-chan []byte, offset int64, finish bool) (<-chan int64, error)
}

// MissingBlobsResponse represents a query result for a single digest.
type MissingBlobsResponse struct {
	Digest  digest.Digest
	Missing bool
	Err     error
}

// UploadResponse represents an upload result for a single blob (which may represent a tree of files).
type UploadResponse struct {
	Digest digest.Digest
	Stats  Stats
	Err    error
}

// uploader represents the state of an uploader implementation.
type uploaderv2 struct {
	cas rpc.ContentAddressableStorageClient

	queryRpcConfig  RPCCfg
	uploadRpcConfig RPCCfg
	streamRpcConfig RPCCfg

	// Throttling controls.
	querySem    *semaphore.Weighted
	uploadSem   *semaphore.Weighted
	streamSem   *semaphore.Weighted
	retryPolicy retry.BackoffPolicy

	// Batching controls.
	queryBundler  *bundler.Bundler
	uploadBundler *bundler.Bundler

	// IO controls.
	ioCfg        IOCfg
	zstdEncoders sync.Pool
}

// batchingUplodaer implements the corresponding interface.
type batchingUploader struct {
	*uploaderv2
}

// streamingUploader implements the corresponding interface.
type streamingUploader struct {
	*uploaderv2
}

// MissingBlobs queries the CAS for the specified digests and returns a slice of the missing ones.
func (u *batchingUploader) MissingBlobs(ctx context.Context, digests []digest.Digest) ([]digest.Digest, error) {
	panic("not yet implemented")
}

// Upload deduplicates the specified blobs (unified uploads) and only uploads the ones that are not already in the CAS.
// Large files are streamed while others are batched together.
// Returns a slice of the digests of the uploaded blobs, excluding the ones that already exist in the CAS.
func (u *batchingUploader) Upload(ctx context.Context, blobs []blob.Blob, symlinkOpts symlinkopts.Opts, predicate exppath.Predicate) ([]digest.Digest, Stats, error) {
	panic("not yet implemented")
}

// WriteBytes uploads all of the specified bytes directly to the specified resource name starting remotely at the specified offset.
// If finish is true, the server is notified to finalize the resource name and no further writes are allowed.
func (u *batchingUploader) WriteBytes(ctx context.Context, name string, bytes []byte, offset int64, finish bool) (Stats, error) {
	panic("not yet implemented")
}

// MissingBlobs is a blocking call that queries the CAS for incoming digests.
// It returns when the context is done, the input channel is closed, or a fatal error occurs.
// The returned error is either from the context or an error that necessitates terminating the call.
// Errors related to particular items are reported as part of their result.
// During the lifetime of the call, digests are cached to avoid duplicate queries.
func (u *streamingUploader) MissingBlobs(context.Context, <-chan digest.Digest) (<-chan MissingBlobsResponse, error) {
	panic("not yet implemented")
}

// Upload is a blocking call that uploads incoming blobs to the CAS.
// It returns when the context is done, the input channel is closed, or a fatal error occurs.
// The returned error is either from the context or an error that necessitates terminating the call.
// Errors related to particular items are reported as part of their result.
// During the lifetime of the call, digests are cached to avoid duplicate uploads (unified uploads).
func (u *streamingUploader) Upload(context.Context, <-chan blob.Blob, symlinkopts.Opts, exppath.Predicate) (<-chan UploadResponse, error) {
	panic("not yet implemented")
}

// WriteBytes is a blocking call that uploads incoming bytes to the CAS at the specified resource name starting remotely at the specified offset.
// It returns when the context is done, the input channel is closed, or a fatal error occurs.
// Each outgoing integer value corresponds with an incoming item and represents the total bytes moved over the wire while streaming the bytes. This may be higher (retries) or lower (interrupted) than the number of incoming bytes.
// If finish is true, the server is notified to finalize the resource name and no further writes are allowed.
func (u *streamingUploader) WriteBytes(ctx context.Context, name string, bytesChan <-chan []byte, offset int64, finish bool) (<-chan int64, error) {
	panic("not yet implemented")
}

func isValidRpcCfg(cfg *RPCCfg) error {
	if cfg.ConcurrentCallsLimit < 1 || cfg.ItemsLimit < 1 || cfg.BytesLimit < 1 {
		return ErrZeroOrNegativeLimit
	}
	return nil
}

func isValidIOCfg(cfg *IOCfg) error {
	if cfg.OpenFilesLimit < 1 || cfg.OpenLargeFilesLimit < 1 || cfg.BufferSize < 1 {
		return ErrZeroOrNegativeLimit
	}
	if cfg.SmallFileSizeThreshold < 0 || cfg.LargeFileSizeThreshold < 0 || cfg.CompressionSizeThreshold < 0 {
		return ErrNegativeLimit
	}
	return nil
}

func newUploaderv2(
	cas rpc.ContentAddressableStorageClient,
	queryCfg, uploadCfg, streamCfg RPCCfg, ioCfg IOCfg,
	retryPolicy retry.BackoffPolicy) (*uploaderv2, error) {

	if cas == nil {
		return nil, ErrNilClient
	}
	if err := isValidRpcCfg(&queryCfg); err != nil {
		return nil, err
	}
	if err := isValidRpcCfg(&uploadCfg); err != nil {
		return nil, err
	}
	if err := isValidRpcCfg(&streamCfg); err != nil {
		return nil, err
	}
	if err := isValidIOCfg(&ioCfg); err != nil {
		return nil, err
	}

	zeroBlob := &blob.Blob{}

	// TODO: bundler callback
	queryBundler := bundler.NewBundler(zeroBlob, func(items interface{}) {})
	queryBundler.DelayThreshold = time.Second
	queryBundler.BundleCountThreshold = queryCfg.ItemsLimit
	queryBundler.BundleByteThreshold = queryCfg.BytesLimit
	queryBundler.BufferedByteLimit = queryCfg.BytesLimit

	// TODO: bundler callback
	uploadBundler := bundler.NewBundler(zeroBlob, func(items interface{}) {})
	uploadBundler.DelayThreshold = time.Second
	uploadBundler.BundleCountThreshold = uploadCfg.ItemsLimit
	uploadBundler.BundleByteThreshold = uploadCfg.BytesLimit
	uploadBundler.BufferedByteLimit = uploadCfg.BytesLimit

	return &uploaderv2{
		cas: cas,

		queryRpcConfig:  queryCfg,
		uploadRpcConfig: uploadCfg,
		streamRpcConfig: streamCfg,

		querySem:    semaphore.NewWeighted(int64(queryCfg.ConcurrentCallsLimit)),
		uploadSem:   semaphore.NewWeighted(int64(uploadCfg.ConcurrentCallsLimit)),
		streamSem:   semaphore.NewWeighted(int64(streamCfg.ConcurrentCallsLimit)),
		retryPolicy: retryPolicy,

		queryBundler:  queryBundler,
		uploadBundler: uploadBundler,

		ioCfg: ioCfg,
		zstdEncoders: sync.Pool{
			New: func() interface{} {
				enc, _ := zstd.NewWriter(nil)
				return enc
			},
		},
	}, nil
}

// NewBatchingUploader creates a new instance of the batching uploader interface.
func NewBatchingUploader(
	cas rpc.ContentAddressableStorageClient,
	queryCfg, uploadCfg, streamCfg RPCCfg, ioCfg IOCfg,
	retryPolicy retry.BackoffPolicy) (BatchingUploader, error) {
	uploader, err := newUploaderv2(cas, queryCfg, uploadCfg, streamCfg, ioCfg, retryPolicy)
	if err != nil {
		return nil, err
	}
	return &batchingUploader{uploaderv2: uploader}, nil
}

// NewStreamingUploader creates a new instance of the streaming uploader interface.
func NewStreamingUploader(
	cas rpc.ContentAddressableStorageClient,
	queryCfg, uploadCfg, streamCfg RPCCfg, ioCfg IOCfg,
	retryPolicy retry.BackoffPolicy) (StreamingUploader, error) {
	uploader, err := newUploaderv2(cas, queryCfg, uploadCfg, streamCfg, ioCfg, retryPolicy)
	if err != nil {
		return nil, err
	}
	return &streamingUploader{uploaderv2: uploader}, nil
}
