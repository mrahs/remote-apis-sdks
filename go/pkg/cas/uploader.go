package cas

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
	"errors"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/pborman/uuid"
	"golang.org/x/sync/semaphore"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrNegativeLimit indicates an invalid value that is < 0.
	ErrNegativeLimit = errors.New("cas: limit value must be >= 0")

	// ErrZeroOrNegativeLimit indicates an invalid value that is <= 0.
	ErrZeroOrNegativeLimit = errors.New("cas: limit value must be > 0")

	// ErrNilClient indicates an invalid nil argument.
	ErrNilClient = errors.New("cas: client cannot be nil")

	// ErrCompression indicates an error in the compression routine.
	ErrCompression = errors.New("cas: compression error")

	// ErrIO indicates an error in an IO routine.
	ErrIO = errors.New("cas: io error")

	// ErrGRPC indicates an error in a gRPC routine.
	ErrGRPC = errors.New("cas: grpc error")

	// ErrOversizedItem indicates an item that is too large to fit into the set byte limit for the corresponding gRPC call.
	ErrOversizedItem = errors.New("cas: oversized item")

	// EOR indicates the end of a stream of responses for a particular subscriber.
	//
	// It is used to signal to the subscriber that no further responses are to be expected for the request in context.
	// This is useful for upload requests that generate multiple responses for each single request so the subscriber can tell
	// when to unsubscribe.
	EOR = errors.New("end of response")
)

// MakeWriteResourceName returns a valid resource name for writing an uncompressed blob.
func MakeWriteResourceName(instanceName, hash string, size int64) string {
	return fmt.Sprintf("%s/uploads/%s/blobs/%s/%d", instanceName, uuid.New(), hash, size)
}

// MakeCompressedWriteResourceName returns a valid resource name for writing a compressed blob.
func MakeCompressedWriteResourceName(instanceName, hash string, size int64) string {
	return fmt.Sprintf("%s/uploads/%s/compressed-blobs/zstd/%s/%d", instanceName, uuid.New(), hash, size)
}

// BatchingUploader provides a simple imperative API to upload to the CAS.
type BatchingUploader interface {
	// MissingBlobs queries the CAS for the specified digests and returns a slice of the missing ones.
	//
	// The digests are batched based on the set gRPC limits (count and size).
	// Errors from a batch do not affect other batches, but all digests from such bad batches will be reported as missing by this call.
	// In other words, if an error is returned, any digest that is not in the returned slice is not missing.
	// If no error is returned, the returned slice contains all the missing digests.
	// The returned error wraps a number of errors proportional to the length of the specified slice.
	MissingBlobs(context.Context, []digest.Digest) ([]digest.Digest, error)

	// Upload processes the specified blobs for upload. Blobs that already exist in the CAS are not uploaded.
	// Any path or file that matches the specified filter is excluded.
	// Additionally, any path that is not a symlink, a directory or a regular file is skipped (e.g. sockets and pipes).
	//
	// Requests are unified across a window of time defined by the BundleTimeout value of the gRPC configuration.
	// The unification is affected by the order of the requests, bundle limits (length, size, timeout) and the upload speed.
	// With infinite speed and limits, every blob will uploaded exactly once. On the other extreme, every blob is uploaded
	// alone and no unification takes place.
	// In the average case, blobs that make it into the same bundle will be deduplicated.
	//
	// Returns a slice of the digests of the blobs that were uploaded (excluding the ones that already exist in the CAS).
	// If the returned error is nil, any digest that is not in the returned slice was already in the CAS.
	// If the returned error is not nil, the returned slice may be incomplete (fatal error) and every digest
	// in it may or may not have been successfully uploaded (individual errors).
	// The returned error wraps a number of errors proportional to the length of the specified slice.
	Upload(context.Context, []impath.Absolute, slo.Options, *walker.Filter) ([]digest.Digest, *Stats, error)

	// WriteBytes uploads all the bytes (until EOF) of the specified reader directly to the specified resource name starting remotely at the specified offset.
	//
	// The specified size is used to toggle compression as well as report some stats. It must be reflect the actual number of bytes the specified reader has to give.
	// The server is notified to finalize the resource name and further writes may not succeed.
	// The errors returned are either from the context, ErrGRPC, ErrIO, or ErrCompression. More errors may be wrapped inside.
	// In case of error while the returned stats indicate that all the bytes were sent, it is still not a guarantee all the bytes
	// were received by the server since an acknlowedgement was not observed.
	WriteBytes(ctx context.Context, name string, r io.Reader, size int64, offset int64) (*Stats, error)

	// WriteBytesPartial is the same as WriteBytes, but does not notify the server to finalize the resource name.
	WriteBytesPartial(ctx context.Context, name string, r io.Reader, size int64, offset int64) (*Stats, error)

	// Wait blocks until all resources held by the uploader are released.
	// It must be called after at least one of the methods has been called to avoid races with wait groups.
	Wait()
}

// StreamingUploader provides a concurrency-friendly API to upload to the CAS.
type StreamingUploader interface {
	// MissingBlobs is a non-blocking call that queries the CAS for incoming digests.
	//
	// The caller must close the specified input channel as a termination signal.
	// The consumption speed is subject to the concurrency and timeout configurations of the gRPC call.
	// All received requests will have corresponding responses sent on the returned channel.
	//
	// The returned channel is unbuffered and will be closed after the input channel is closed and no more responses are available for this call.
	// This could indicate completion or cancellation (in case the context was canceled).
	// Slow consumption speed on this channel affects the consumption speed on the input channel.
	MissingBlobs(context.Context, <-chan digest.Digest) <-chan MissingBlobsResponse

	// Upload is a non-blocking call that uploads incoming files to the CAS if necessary.
	//
	// Requests are unified across a window of time defined by the BundleTimeout value of the gRPC configuration.
	// The unification is affected by the order of the requests, bundle limits (length, size, timeout) and the upload speed.
	// With infinite speed and limits, every blob will uploaded exactly once. On the other extreme, every blob is uploaded
	// alone and no unification takes place.
	// In the average case, blobs that make it into the same bundle will be grouped by digest. Once a digest is processed, each requester of that
	// digest receives a copy of the coorresponding UploadResponse.
	Upload(context.Context, <-chan impath.Absolute, slo.Options, *walker.Filter) <-chan UploadResponse

	// Wait blocks until all resources held by the uploader are released.
	// It must be called after at least one of the methods has been called to avoid races with wait groups.
	Wait()
}

// batchingUplodaer implements the corresponding interface.
type batchingUploader struct {
	*uploaderv2
}

// streamingUploader implements the corresponding interface.
type streamingUploader struct {
	*uploaderv2
}

// uploader represents the state of an uploader implementation.
type uploaderv2 struct {
	cas          repb.ContentAddressableStorageClient
	byteStream   bspb.ByteStreamClient
	instanceName string

	queryRpcConfig  GRPCConfig
	uploadRpcConfig GRPCConfig
	streamRpcConfig GRPCConfig

	// gRPC throttling controls.
	querySem  *semaphore.Weighted
	uploadSem *semaphore.Weighted
	streamSem *semaphore.Weighted

	// IO controls.
	ioCfg                 IOConfig
	buffers               sync.Pool
	zstdEncoders          sync.Pool
	walkSem               *semaphore.Weighted
	walkWg                sync.WaitGroup
	ioSem                 *semaphore.Weighted
	ioLargeSem            *semaphore.Weighted
	dirChildren           sliceCache
	queryRequestBaseSize  int
	uploadRequestBaseSize int

	// Concurrency controls.
	processorWg sync.WaitGroup // Main event loops.
	workerWg    sync.WaitGroup // Messaging goroutines downstream of event loops.
	// queryChan is the fan-in channel for queries.
	// All senders must also listen on the context to avoid deadlocks.
	queryChan chan missingBlobRequest
	// uploadChan is the fan-in channel for uploads.
	// All senders must also listen on the context to avoid deadlocks.
	uploadChan         chan UploadRequest
	uploadBatcherChan  chan blob
	uploadStreamerChan chan blob
	// grpcWg is used to wait for in-flight gRPC calls upon graceful termination.
	grpcWg sync.WaitGroup

	// queryPubSub routes responses to callers.
	queryPubSub *pubsub
	// uploadCallerPubSub routes responses to callers.
	uploadCallerPubSub *pubsub
	// uploadReqPubSub routes responses internally.
	uploadReqPubSub *pubsub
}

func (u *uploaderv2) Wait() {
	// Wait for filesystem walks first, since they may still trigger requests.
	u.walkWg.Wait()
	// Wait for query subscribers to stop triggering requests and drain their responses.
	u.queryPubSub.wait()
	// Wait for gRPC methods, since they may still issue responses.
	u.grpcWg.Wait()
	// Wait for upload requests to collect their responses.
	u.uploadReqPubSub.wait()
	// Wait for upload subscribers to drain their responses.
	u.uploadCallerPubSub.wait()
	// Wait for workers to ensure all senders are done.
	u.workerWg.Wait()
	// Close channels to let processors terminate.
	close(u.queryChan)
	close(u.uploadChan)
	close(u.uploadBatcherChan)
	close(u.uploadStreamerChan)
	u.processorWg.Wait()
}

func (u *uploaderv2) withRetry(ctx context.Context, retryPolicy retry.BackoffPolicy, fn func() error) error {
	return retry.WithPolicy(ctx, retry.TransientOnly, retryPolicy, fn)
}

func (u *uploaderv2) withTimeout(timeout time.Duration, cancelFn context.CancelFunc, fn func() error) error {
	// Success signal.
	done := make(chan struct{})
	defer close(done)
	// Timeout signal.
	timer := time.NewTimer(timeout)
	go func() {
		select {
		case <-done:
			timer.Stop()
		case <-timer.C:
			cancelFn()
		}
	}()
	return fn()
}

func newUploaderv2(
	ctx context.Context, cas repb.ContentAddressableStorageClient, byteStream bspb.ByteStreamClient, instanceName string,
	queryCfg, uploadCfg, streamCfg GRPCConfig, ioCfg IOConfig,
) (*uploaderv2, error) {
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

	u := &uploaderv2{
		cas:          cas,
		byteStream:   byteStream,
		instanceName: instanceName,

		queryRpcConfig:  queryCfg,
		uploadRpcConfig: uploadCfg,
		streamRpcConfig: streamCfg,

		querySem:  semaphore.NewWeighted(int64(queryCfg.ConcurrentCallsLimit)),
		uploadSem: semaphore.NewWeighted(int64(uploadCfg.ConcurrentCallsLimit)),
		streamSem: semaphore.NewWeighted(int64(streamCfg.ConcurrentCallsLimit)),

		ioCfg: ioCfg,
		buffers: sync.Pool{
			New: func() any {
				// Since the buffers are never resized, treating the slice as a pointer-like
				// type for this pool is safe.
				buf := make([]byte, ioCfg.BufferSize)
				return buf
			},
		},
		zstdEncoders: sync.Pool{
			New: func() any {
				// Providing a nil writer implies that the encoder needs to be
				// (re)initilaized with a writer using enc.Reset(w) before using it.
				enc, _ := zstd.NewWriter(nil)
				return enc
			},
		},
		walkSem:     semaphore.NewWeighted(int64(ioCfg.ConcurrentWalksLimit)),
		ioSem:       semaphore.NewWeighted(int64(ioCfg.OpenFilesLimit)),
		ioLargeSem:  semaphore.NewWeighted(int64(ioCfg.OpenLargeFilesLimit)),
		dirChildren: initSliceCache(),

		queryChan:          make(chan missingBlobRequest),
		queryPubSub:        newPubSub(),
		uploadChan:         make(chan UploadRequest),
		uploadBatcherChan:  make(chan blob),
		uploadStreamerChan: make(chan blob),
		uploadCallerPubSub: newPubSub(),
		uploadReqPubSub:    newPubSub(),

		queryRequestBaseSize:  proto.Size(&repb.FindMissingBlobsRequest{InstanceName: instanceName, BlobDigests: []*repb.Digest{}}),
		uploadRequestBaseSize: proto.Size(&repb.BatchUpdateBlobsRequest{InstanceName: instanceName, Requests: []*repb.BatchUpdateBlobsRequest_Request{}}),
	}

	u.processorWg.Add(1)
	go u.queryProcessor(ctx)
	return u, nil
}

// NewBatchingUploader creates a new instance of the batching uploader interface.
// The specified configs must be compatible with the capabilities of the server
// which the specified clients are connected to.
func NewBatchingUploader(
	ctx context.Context, cas repb.ContentAddressableStorageClient, byteStream bspb.ByteStreamClient, instanceName string,
	queryCfg, uploadCfg, streamCfg GRPCConfig, ioCfg IOConfig,
) (BatchingUploader, error) {
	uploader, err := newUploaderv2(ctx, cas, byteStream, instanceName, queryCfg, uploadCfg, streamCfg, ioCfg)
	if err != nil {
		return nil, err
	}
	return &batchingUploader{uploaderv2: uploader}, nil
}

// NewStreamingUploader creates a new instance of the streaming uploader interface.
// The specified configs must be compatible with the capabilities of the server
// which the specified clients are connected to.
func NewStreamingUploader(
	ctx context.Context, cas repb.ContentAddressableStorageClient, byteStream bspb.ByteStreamClient, instanceName string,
	queryCfg, uploadCfg, streamCfg GRPCConfig, ioCfg IOConfig,
) (StreamingUploader, error) {
	uploader, err := newUploaderv2(ctx, cas, byteStream, instanceName, queryCfg, uploadCfg, streamCfg, ioCfg)
	if err != nil {
		return nil, err
	}
	return &streamingUploader{uploaderv2: uploader}, nil
}
