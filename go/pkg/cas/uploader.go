package cas

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/glog"
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

	// ErrBadCacheValueType indicates an unexpected type for a cache value.
	ErrBadCacheValueType = errors.New("cache value type not expected")

	// ErrTerminatedUploader indicates an attempt to use a terminated uploader.
	ErrTerminatedUploader = errors.New("cannot use a terminated uploader")
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
	querySem  *semaphore.Weighted // Controls concurrent calls to the query API.
	uploadSem *semaphore.Weighted // Controls concurrent calls to the batch API.
	streamSem *semaphore.Weighted // Controls concurrent calls to the byte streaming API.

	// IO controls.
	ioCfg        IOConfig
	buffers      sync.Pool
	zstdEncoders sync.Pool
	walkSem      *semaphore.Weighted // Controls concurrent file system walks.
	ioSem        *semaphore.Weighted // Controls total number of open files.
	ioLargeSem   *semaphore.Weighted // Controls total number of open large files.
	// digestCache allows digesting each path only once.
	// Concurrent walkers claim a path by storing a nil value, which allows other walkers to defer
	// digesting that path until the first walker stores the digest once it's computed.
	digestCache sync.Map
	// dirChildren is shared between all callers. However, since a directory is owned by a single
	// walker at a time, there is no concurrent read/write to this map, but there might be concurrent reads.
	dirChildren           map[string][]proto.Message
	queryRequestBaseSize  int
	uploadRequestBaseSize int

	// Concurrency controls.
	clientSenderWg    sync.WaitGroup          // First-level producers.
	querySenderWg     sync.WaitGroup          // Second-level producers.
	uploadSenderWg    sync.WaitGroup          // Second-level producers.
	processorWg       sync.WaitGroup          // Itermediate routers.
	receiverWg        sync.WaitGroup          // Consumers.
	workerWg          sync.WaitGroup          // Short-lived intermediate producers/consumers.
	callerWalkWg      map[tag]*sync.WaitGroup // Tracks file system walks per caller.
	walkerWg          sync.WaitGroup          // Tracks all walkers.
	queryCh           chan missingBlobRequest // Fan-in channel for query requests.
	uploadCh          chan UploadRequest      // Fan-in channel for upload requests.
	uploadDispatchCh  chan blob               // Fan-in channel for dispatched blobs.
	uploadQueryPipeCh chan blob               // A pipe channel for presence checking before uploading.
	uploadResCh       chan UploadResponse     // Fan-in channel for responses.
	uploadBatchCh     chan blob               // Fan-in channel for unified requests to the batching API.
	uploadStreamCh    chan blob               // Fan-in channel for unified requests to the byte streaming API.
	queryPubSub       *pubsub                 // Fan-out broker for query responses.
	uploadPubSub      *pubsub                 // Fan-out broker for upload responses.

	// The reference is used internally to terminate request workers or prevent them from running on a terminated uploader.
	ctx context.Context
}

// Wait blocks until all resources held by the uploader are released.
func (u *uploaderv2) Wait() {
	<-u.ctx.Done()

	glog.V(1).Infof("uploader: waiting for client senders")
	u.clientSenderWg.Wait()

	glog.V(1).Infof("uploader: waiting for upload senders")
	u.uploadSenderWg.Wait()
	close(u.uploadCh)

	glog.V(1).Infof("uploader: waiting for query senders")
	u.querySenderWg.Wait()
	close(u.queryCh)

	glog.V(1).Infof("uploader: waiting for processors")
	u.processorWg.Wait()

	glog.V(1).Infof("uploader: waiting for brokers")
	u.queryPubSub.wait()
	u.uploadPubSub.wait()

	glog.V(1).Infof("uploader: waiting for receivers")
	u.receiverWg.Wait()

	glog.V(1).Infof("uploader: waiting for workers")
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
		ctx: ctx,

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
		dirChildren: make(map[string][]proto.Message),

		callerWalkWg:      make(map[tag]*sync.WaitGroup),
		queryCh:           make(chan missingBlobRequest),
		queryPubSub:       newPubSub(),
		uploadCh:          make(chan UploadRequest),
		uploadDispatchCh:  make(chan blob),
		uploadQueryPipeCh: make(chan blob),
		uploadResCh:       make(chan UploadResponse),
		uploadBatchCh:     make(chan blob),
		uploadStreamCh:    make(chan blob),
		uploadPubSub:      newPubSub(),

		queryRequestBaseSize:  proto.Size(&repb.FindMissingBlobsRequest{InstanceName: instanceName, BlobDigests: []*repb.Digest{}}),
		uploadRequestBaseSize: proto.Size(&repb.BatchUpdateBlobsRequest{InstanceName: instanceName, Requests: []*repb.BatchUpdateBlobsRequest_Request{}}),
	}

	u.processorWg.Add(1)
	go func() {
		u.queryProcessor()
		u.processorWg.Done()
	}()

	u.processorWg.Add(1)
	go func() {
		u.uploadProcessor()
		u.processorWg.Done()
	}()

	u.processorWg.Add(1)
	go func() {
		u.uploadDispatcher()
		u.processorWg.Done()
	}()

	// Initializing the query streamer here to ensure wait groups are initialized before returning from this constructor.
	queryCh := make(chan digest.Digest)
	queryResCh := u.missingBlobsStreamer(queryCh)
	u.processorWg.Add(1)
	go func() {
		u.uploadQueryPipe(queryCh, queryResCh)
		u.processorWg.Done()
	}()

	u.processorWg.Add(1)
	go func() {
		u.uploadBatchProcessor()
		u.processorWg.Done()
	}()

	u.processorWg.Add(1)
	go func() {
		u.uploadStreamProcessor()
		u.processorWg.Done()
	}()

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
