package cas

import (
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
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
	// Must be > 0.
	ConcurrentCallsLimit int

	// BytesLimit sets the upper bound for the size of each request.
	// Must be > 0.
	// This is defined as int rather than int64 because gRPC uses int for its limit.
	BytesLimit int

	// ItemsLimit sets the upper bound for the number of items per request.
	// Must be > 0.
	ItemsLimit int

	// BundleTimeout sets the maximum duration a call is held while bundling.
	// Bundling is used to ammortize the cost of a gRPC call over time. Instead of sending
	// many requests with few items, bunlding attempt to maximize the number of items sent in a single request.
	// This includes waiting for a bit to see if more items are requested.
	BundleTimeout time.Duration

	// Timeout sets the upper bound of the total time spent processing a request.
	// For streaming calls, this applies to each Send/Recv call individually, not the whole streaming session.
	// This does not take into account the time it takes to abort the request upon timeout.
	Timeout time.Duration
}

// IOCfg specifies the configuration for IO operations.
type IOCfg struct {
	// OpenFilesLimit sets the upper bound for the number of files being simultanuously processed.
	// Must be > 0.
	OpenFilesLimit int

	// OpenLargeFilesLimit sets the upper bound for the number of large files being simultanuously processed.
	// Must be > 0.
	// Open large files count towards OpenFilesLimit. I.e. thef following inequality is always effectively true:
	// OpenFilesLimit >= OpenLargeFilesLimit
	OpenLargeFilesLimit int

	// SmallFileSizeThreshold sets the upper bound (inclusive) for the file size to be considered a small file.
	// Such files are buffered entirely in memory.
	// Must be >= 0.
	SmallFileSizeThreshold int64

	// LargeFileSizeThreshold sets the lower bound (inclusive) for the file size to be considered a large file.
	// Such files are uploaded in chunks using the file streaming API.
	// Must be >= 0.
	LargeFileSizeThreshold int64

	// CompressionSizeThreshold sets the lower bound for the chunk size before it is subject to compression.
	// A value of 0 enables compression for any chunk size. To disable compression, use math.MaxInt64.
	// Must >= 0.
	CompressionSizeThreshold int64

	// BufferSize sets the buffer size for IO read/write operations.
	// Must be > 0.
	BufferSize int64

	// OptimizeForDiskLocality enables sorting files by path before they are written to disk to optimize for disk locality.
	// Assuming files under the same directory are located close to each other on disk, the such files are batched together.
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

	// LogicalBytesMoved is the amount of BytesRequsted that was processed.
	// It cannot be larger than BytesRequested, but may be smaller in case of a partial response.
	LogicalBytesMoved int64

	// BytesMoved is the total number of bytes moved over the wire.
	// It may be larger than (retries) or smaller than BytesRequested (compression, cache hits or partial response).
	BytesMoved int64

	// BytesAttempted is the total number of bytes moved over the wire, excluding retries.
	// It may be higher than BytesRequested (compression headers), but never higher than BytesMoved.
	BytesAttempted int64

	// BytesCached is the total number of bytes not moved over the wire due to caching (either remotely or locally).
	BytesCached int64

	// BytesStreamed is the total number of logical bytes moved by the streaming API.
	// It may be larger than (retries) or smaller than (cache hits or partial response) than the requested size.
	BytesStreamed int64

	// BytesBatched is the total number of logical bytes moved by the batching API.
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

	// BatchedCount is the number of batched files.
	BatchedCount int64

	// StreamedCount is the number of streamed files.
	// For methods that accept bytes, the value is 1 upon success, 0 otherwise.
	StreamedCount int64
}
