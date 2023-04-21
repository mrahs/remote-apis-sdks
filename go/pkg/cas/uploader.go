package cas

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/pborman/uuid"
	"golang.org/x/sync/semaphore"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrNegativeLimit indicates an invalid value that is < 0.
	ErrNegativeLimit = errors.New("limit value must be >= 0")

	// ErrZeroOrNegativeLimit indicates an invalid value that is <= 0.
	ErrZeroOrNegativeLimit = errors.New("limit value must be > 0")

	// ErrNilClient indicates an invalid nil argument.
	ErrNilClient = errors.New("client cannot be nil")

	// ErrCompression indicates an error in the compression routine.
	ErrCompression = errors.New("compression error")

	// ErrIO indicates an error in an IO routine.
	ErrIO = errors.New("io error")

	// ErrGRPC indicates an error in a gRPC routine.
	ErrGRPC = errors.New("grpc error")

	// ErrOversizedItem indicates an item that is too large to fit into the set byte limit for the corresponding gRPC call.
	ErrOversizedItem = errors.New("oversized item")

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

// batchingUplodaer implements the corresponding interface.
type BatchingUploader struct {
	*uploaderv2
}

// streamingUploader implements the corresponding interface.
type StreamingUploader struct {
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
	ioSem                 *semaphore.Weighted
	ioLargeSem            *semaphore.Weighted
	dirChildren           sliceCache
	queryRequestBaseSize  int
	uploadRequestBaseSize int

	// Concurrency controls.
	senderWg    sync.WaitGroup // Long-lived top-level producers.
	processorWg sync.WaitGroup // Long-lived brokers.
	receiverWg  sync.WaitGroup // Long-lived consumers.
	workerWg    sync.WaitGroup // Short-lived workers.
	// queryChan is the fan-in channel for queries.
	// All senders must also listen on the context to avoid deadlocks.
	queryChan chan missingBlobRequest
	// uploadChan is the fan-in channel for uploads.
	// All senders must also listen on the context to avoid deadlocks.
	uploadChan         chan UploadRequest
	uploadBatcherChan  chan blob
	uploadStreamerChan chan blob
	// grpcWg is used to wait for in-flight gRPC calls upon graceful termination.

	// queryPubSub routes responses to callers.
	queryPubSub *pubsub
	// uploadCallerPubSub routes responses to callers.
	uploadCallerPubSub *pubsub
	// uploadReqPubSub routes responses internally.
	uploadReqPubSub *pubsub
}

// Wait blocks until all resources held by the uploader are released.
// This method must be called after all other methods have returned to avoid race conditions.
func (u *uploaderv2) Wait() {
	// 1st, senders must stop sending.
	// This call must happen after all other query/upload methods have returned to ensure the wait group does not grow while waiting.
	u.senderWg.Wait()
	// 2nd, brokers must stop sending.
	u.processorWg.Wait()
	// 3rd, intermediate brokers must stop sending.
	u.queryPubSub.wait()
	u.uploadReqPubSub.wait()
	u.uploadCallerPubSub.wait()
	// 4th, receivers must drain their channels, which could involve spawning more workers.
	u.receiverWg.Wait()
	// 5th, ensure all workers have terminated.
	u.workerWg.Wait()
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

	// Start processors. Each one will launch a goroutine for background processing.
	// This way allows ensuring that all waiting groups are set once this function returns.
	u.queryProcessor(ctx)
	u.uploadDispatcher(ctx)
	u.uploadBatcher(ctx)
	u.uploadStreamer(ctx)
	return u, nil
}

// NewBatchingUploader creates a new instance of the batching uploader.
//
// The specified configs must be compatible with the capabilities of the server that the specified clients are connected to.
// ctx must be cancelled to properly shutdown the uploader.
func NewBatchingUploader(
	ctx context.Context, cas repb.ContentAddressableStorageClient, byteStream bspb.ByteStreamClient, instanceName string,
	queryCfg, uploadCfg, streamCfg GRPCConfig, ioCfg IOConfig,
) (*BatchingUploader, error) {
	uploader, err := newUploaderv2(ctx, cas, byteStream, instanceName, queryCfg, uploadCfg, streamCfg, ioCfg)
	if err != nil {
		return nil, err
	}
	return &BatchingUploader{uploaderv2: uploader}, nil
}

// NewStreamingUploader creates a new instance of the streaming uploader.
//
// The specified configs must be compatible with the capabilities of the server which the specified clients are connected to.
// ctx must be cancelled to properly shutdown the uploader.
func NewStreamingUploader(
	ctx context.Context, cas repb.ContentAddressableStorageClient, byteStream bspb.ByteStreamClient, instanceName string,
	queryCfg, uploadCfg, streamCfg GRPCConfig, ioCfg IOConfig,
) (*StreamingUploader, error) {
	uploader, err := newUploaderv2(ctx, cas, byteStream, instanceName, queryCfg, uploadCfg, streamCfg, ioCfg)
	if err != nil {
		return nil, err
	}
	return &StreamingUploader{uploaderv2: uploader}, nil
}
