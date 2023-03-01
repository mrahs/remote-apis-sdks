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
	"github.com/pborman/uuid"
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/support/bundler"
)

const (
	// MegaByte is 1_048_576 bytes.
	MegaByte = 1024 * 1024

	// DefaultGRPCConcurrentCallsLimit is set arbitrarily to a power of 2.
	DefaultGRPCConcurrentCallsLimit = 256

	// DefaultGRPCBytesLimit is the same as the default gRPC request size limit.
	// See: https://pkg.go.dev/google.golang.org/grpc#MaxCallRecvMsgSize
	DefaultGRPCBytesLimit = 4 * MegaByte

	// DefaultGRPCItemsLimit is a 10th of the max.
	DefaultGRPCItemsLimit = 1000

	// MaxGRPCItems is heuristcally (with Google's RBE) set to 10k.
	MaxGRPCItems = 10_000

	// DefaultRPCTimeout is arbitrarily set to what is reasonable for a large action.
	DefaultRPCTimeout = time.Minute

	// DefaultOpenFilesLimit is based on GCS recommendations.
	// See: https://cloud.google.com/compute/docs/disks/optimizing-pd-performance#io-queue-depth
	DefaultOpenFilesLimit = 32

	// DefaultOpenLargeFilesLimit is arbitrarily set.
	DefaultOpenLargeFilesLimit = 2

	// DefaultCompressionSizeThreshold is disabled by default.
	DefaultCompressionSizeThreshold = -1

	// BufferSize is based on GCS recommendations.
	// See: https://cloud.google.com/compute/docs/disks/optimizing-pd-performance#io-size
	BufferSize = 4 * MegaByte
)

// RPCCfg specifies the configuration for a gRPC endpoint.
type RPCCfg struct {
	// ConcurrentCallsLimit sets the upper bound of concurrent calls.
	ConcurrentCallsLimit int

	// BytesLimit sets the upper bound for the size of each request.
	BytesLimit int

	// ItemsLimit sets the upper bound for the number of items per request.
	ItemsLimit int

	// Timeout sets the upper bound of the total time spent processing a request.
	// This does not take into account the time it takes to abort the request.
	Timeout time.Duration
}

// IOCfg specifies the configuration for IO operations.
type IOCfg struct {
	// OpenFilesLimit sets the upper bound for the number of files being simultanuously processed.
	OpenFilesLimit int

	// OpenLargeFilesLimit sets the upper bound for the number of large files being simultanuously processed.
	// Open large files count towards OpenFilesLimit. I.e. thef following inequality is always effectively true:
	// OpenFilesLimit >= OpenLargeFilesLimit
	OpenLargeFilesLimit int

	// SmallFileSizeThreshold sets the upper bound (inclusive) for the file size to be considered a small file.
	// Such files are buffered entirely in memory.
	SmallFileSizeThreshold int64

	// LargeFileSizeThreshold sets the lower bound (inclusive) for the file size to be considered a large file.
	// Such files are uploaded in chunks using the file streaming API.
	LargeFileSizeThreshold int64

	// CompressionSizeThreshold sets the lower bound for the chunk size before it is subject to compression.
	// A value of 0 enables compression for any chunk size, while a negative value disables compression entirely.
	CompressionSizeThreshold int64

	// BufferSize sets the buffer size for IO read/write operations.
	BufferSize int64

	// OptimizeForDiskLocality enables sorting files by path before they are written to disk to optimize for disk locality (files under the same directory are located close to each other on disk).
	OptimizeForDiskLocality bool

	// Cache is a read/write cache for file metadata.
	Cache filemetadata.Cache
}

// Stats represents potential metrics reported by various methods.
// Not all fields are populated by every method.
type Stats struct {
	// BytesRequesetd is the total number of bytes in a request.
	// It does not necessarily equal the total number of bytes uploaded/downloaded.
	BytesRequesetd int64

	// BytesMoved is the total number of bytes moved over the wire.
	// It may be larger than (retries) or smaller than (cache hits or partial response) the requested bytes.
	BytesMoved int64

	// LogicalBytesMoved is the total number of bytes for the processed items, regardless what actually moved over the wire.
	// It cannot be larger than the requested bytes, but may be smaller in case of a partial response.
	LogicalBytesMoved int64

	// BytesCached is the total number of bytes not moved over the wire due to caching (either remotely or locally).
	BytesCached int64

	// BytesStreamed is the total number of bytes moved by the streaming API (large files).
	// It may be larger than (retries) or smaller than (cache hits or partial response) than the requested size.
	BytesStreamed int64

	// BytesBatched is the total number of bytes moved by the batching API (small and medium files).
	// It may be larger than (retries) or smaller than (cache hits or partial response) the requested size.
	BytesBatched int64

	// InputFileCount is the number of processed regular files.
	InputFileCount int64

	// InputDirCount is the number of processed directories.
	InputDirCount int64

	// InputSymlinkCount is the number of processed symlinks.
	InputSymlinkCount int64

	// CacheHitCount is the number of cache hits.
	CacheHitCount int64

	// CacheMissCount is the number of cache misses.
	CacheMissCount int64

	// DigestCount is the number of processed digests.
	DigestCount int64

	// BatchedCount is the number of small and medium files processed.
	BatchedCount int64

	// StreamedCount is the number of large files processed.
	StreamedCount int64
}

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
	MissingBlobs(context.Context, <-chan digest.Digest) (chan<- MissingBlobsResponse, error)

	// Upload is a blocking call that uploads incoming blobs to the CAS.
	// It returns when the context is done, the input channel is closed, or a fatal error occurs.
	// The returned error is either from the context or an error that necessitates terminating the call.
	// Errors related to particular items are reported as part of their result.
	// During the lifetime of the call, digests are cached to avoid duplicate uploads (unified uploads).
	Upload(context.Context, <-chan blob.Blob, symlinkopts.Opts, exppath.Predicate) (chan<- UploadResponse, error)

	// WriteBytes is a blocking call that uploads incoming bytes to the CAS at the specified resource name starting remotely at the specified offset.
	// It returns when the context is done, the input channel is closed, or a fatal error occurs.
	// Each outgoing integer value corresponds with an incoming item and represents the total bytes moved over the wire while streaming the bytes. This may be higher (retries) or lower (interrupted) than the number of incoming bytes.
	// If finish is true, the server is notified to finalize the resource name and no further writes are allowed.
	WriteBytes(ctx context.Context, name string, bytesChan <-chan []byte, offset int64, finish bool) (chan<- int64, error)
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
	queryRpcConfig  RPCConfig
	uploadRpcConfig RPCConfig
	streamRpcConfig RPCConfig

	// Throttling controls.
	querySem  semaphore.Weighted
	uploadSem semaphore.Weighted
	streamSem semaphore.Weighted

	// Batching controls.
	queryBundler  bundler.Bundler
	uploadBundler bundler.Bundler

	// Compression controls.
	zstdEncoders sync.Pool
}

// batchingUplodaer implements the corresponding interface.
type batchingUploader struct {
	uploader
}

// streamingUploader implements the corresponding interface.
type streamingUploader struct {
	uploader
}

func NewBatchingUploader(
	cas rpc.ContentAddressableStorageClient,
	queryCfg, uploadCfg, streamCfg RPCCfg, ioCfg IOCfg,
	retryPolicy retry.BackoffPolicy) BatchingUploader {
	panic("not yet implemented")
}

func NewStreamingUploader(
	cas rpc.ContentAddressableStorageClient,
	queryCfg, uploadCfg, streamCfg RPCCfg, ioCfg IOCfg,
	retryPolicy retry.BackoffPolicy) StreamingUploader {
	panic("not yet implemented")
}
