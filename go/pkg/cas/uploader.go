package cas

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	ep "github.com/bazelbuild/remote-apis-sdks/go/pkg/io/exppath"
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
	io.Closer
	// MissingBlobs queries the CAS for the specified digests and returns a slice of the missing ones.
	MissingBlobs(context.Context, []digest.Digest) ([]digest.Digest, error)

	// Upload digests the specified paths and upload to the CAS what is not already there.
	// Returns a slice of the digests of the files that had to be uploaded.
	Upload(context.Context, []ep.Abs, slo.Options, ep.Filter) ([]digest.Digest, *Stats, error)

	// WriteBytes uploads all the bytes of the specified reader directly to the specified resource name starting remotely at the specified offset.
	// If finish is true, the server is notified to finalize the resource name and no further writes are allowed.
	WriteBytes(ctx context.Context, name string, r io.Reader, size int64, offset int64, finish bool) (*Stats, error)
}

// StreamingUploader provides a concurrency friendly API to upload to the CAS.
type StreamingUploader interface {
	io.Closer
	// MissingBlobs is a non-blocking call that queries the CAS for incoming digests.
	MissingBlobs(context.Context, <-chan digest.Digest) <-chan MissingBlobsResponse

	// Upload is a non-blocking call that uploads incoming files to the CAS if necessary.
	Upload(context.Context, <-chan ep.Abs, slo.Options, ep.Filter) <-chan UploadResponse
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

	// Throttling controls.
	querySem    *semaphore.Weighted
	uploadSem   *semaphore.Weighted
	streamSem   *semaphore.Weighted

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
	processorWg sync.WaitGroup
	// queryChan is the fan-in channel for queries.
	// All senders must also listen on the context to avoid deadlocks.
	queryChan chan missingBlobRequest
	// uploadChan is the fan-in channel for uploads.
	// All senders must also listen on the context to avoid deadlocks.
	uploadChan         chan UploadRequest
	uploadBundlerChan  chan blob
	uploadStreamerChan chan blob
	// grpcWg is used to wait for in-flight gRPC calls upon graceful termination.
	grpcWg sync.WaitGroup

	// queryCaller is set of active query callers and their associated channels.
	queryCaller map[string]queryCaller
	// queryCallerMutex is necessary since the map holds references which can survive the atomicity of sync.Map.
	queryCallerMutex sync.Mutex
	// queryCallerWg is used to wait for in-flight receivers upon graceful termination.
	queryCallerWg sync.WaitGroup

	// uploadCaller is set of active upload callers and their associated channels.
	uploadCaller map[string]uploadCaller
	// uploadCallerMutex is necessary since the map holds references which can survive the atomicity of sync.Map.
	uploadCallerMutex sync.Mutex
	// uploadCallerWg is used to wait for in-flight receivers upon graceful termination.
	uploadCallerWg sync.WaitGroup
}

// Close blocks until all resources have been cleaned up.
// It always returns nil.
// It must be called after at least one of the methods has been called to avoid races with wait groups.
func (u *uploaderv2) Close() error {
	// Wait for filesystem walks first, since they may still trigger requests.
	u.walkWg.Wait()
	// Wait for query callers to stop triggering requests and drain their responses.
	u.queryCallerWg.Wait()
	// Wait for gRPC methods, since they may still issue reponses.
	u.grpcWg.Wait()
	// Wait for upload callers to drain their responses.
	u.uploadCallerWg.Wait()
	close(u.queryChan)
	close(u.uploadChan)
	close(u.uploadBundlerChan)
	close(u.uploadStreamerChan)
	u.processorWg.Wait()
	return nil
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
	queryCfg, uploadCfg, streamCfg GRPCConfig, ioCfg IOConfig) (*uploaderv2, error) {

	if cas == nil || byteStream == nil {
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
			New: func() interface{} {
				// Since the buffers are never resized, treating the slice as a pointer-like
				// type for this pool is safe.
				buf := make([]byte, ioCfg.BufferSize)
				return buf
			},
		},
		zstdEncoders: sync.Pool{
			New: func() interface{} {
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
		uploadChan:         make(chan UploadRequest),
		uploadBundlerChan:  make(chan blob),
		uploadStreamerChan: make(chan blob),
		queryCaller:        make(map[string]queryCaller),
		uploadCaller:       make(map[string]uploadCaller),

		queryRequestBaseSize:  proto.Size(&repb.FindMissingBlobsRequest{InstanceName: instanceName, BlobDigests: []*repb.Digest{}}),
		uploadRequestBaseSize: proto.Size(&repb.BatchUpdateBlobsRequest{InstanceName: instanceName, Requests: []*repb.BatchUpdateBlobsRequest_Request{}}),
	}

	u.processorWg.Add(1)
	go u.queryProcessor(ctx)
	return u, nil
}

// NewBatchingUploader creates a new instance of the batching uploader interface.
// The specified configs must be compatbile with the capabilities of the server
// which the specified clients are connected to.
func NewBatchingUploader(
	ctx context.Context, cas repb.ContentAddressableStorageClient, byteStream bspb.ByteStreamClient, instanceName string,
	queryCfg, uploadCfg, streamCfg GRPCConfig, ioCfg IOConfig) (BatchingUploader, error) {
	uploader, err := newUploaderv2(ctx, cas, byteStream, instanceName, queryCfg, uploadCfg, streamCfg, ioCfg)
	if err != nil {
		return nil, err
	}
	return &batchingUploader{uploaderv2: uploader}, nil
}

// NewStreamingUploader creates a new instance of the streaming uploader interface.
// The specified configs must be compatbile with the capabilities of the server
// which the specified clients are connected to.
func NewStreamingUploader(
	ctx context.Context, cas repb.ContentAddressableStorageClient, byteStream bspb.ByteStreamClient, instanceName string,
	queryCfg, uploadCfg, streamCfg GRPCConfig, ioCfg IOConfig) (StreamingUploader, error) {
	uploader, err := newUploaderv2(ctx, cas, byteStream, instanceName, queryCfg, uploadCfg, streamCfg, ioCfg)
	if err != nil {
		return nil, err
	}
	return &streamingUploader{uploaderv2: uploader}, nil
}
