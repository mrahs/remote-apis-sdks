// Package casng provides a CAS client implementation with the following incomplete list of features:
//   - Streaming interface to upload files during the digestion process rather than after.
//   - Unified uploads and downloads.
//   - Simplified public API.
package casng

// This file includes the implementation for uploading blobs to the CAS.
//
// The following diagram illustrates the overview of the design implemented in this package.
// The request follows a linear path through the system: request -> digest -> query -> upload -> response.
// Each box represents a processor with its own state to manage concurrent requests and proper messaging with other processors.
/*


                               Dispatcher
                    ┌─────────────────────────┐
                    │                         │
     ┌───────────┐  │ ┌─────┐       ┌──────┐  │ Digest
     │           │  │ │     │ Digest│ Pipe ├──┼───────┐
     │ Digester  ├──┼─► Req ├───────► Req  │  │       │
     │           │  │ └─────┘       └──────┘  │  ┌────▼─────┐
     └─────▲─────┘  │                         │  │          │
   Upload  │        │                         │  │  Query   │
   Request │        │                         │  │ Processor│
           │        │                         │  │          │
      ┌────┴───┐    │ ┌─────┐ Cache ┌──────┐  │  └────┬─────┘
      │        ◄────┼─┤ Res │  Hit  │ Pipe │  │       │
      │  User  │    │ │     ◄───────┤ Res  ◄──┼───────┘
      │        │    │ └▲──▲─┘       └┬────┬┘  │  Query
      └────────┘    │  │  │     Small│    │   │ Response
                    │  │  │     Blob │    │   │
                    └──┼──┼──────────┼────┼───┘
                       │  │          │    │
                       │  │ ┌────────▼─┐  │Large
                       │  │ │  Batcher │  │Blob
                       │  └─┤   gRPC   │  │
                       │    └──────────┘  │
                       │                  │
                       │    ┌──────────┐  │
                       │    │ Streamer │  │
                       └────┤   gRPC   ◄──┘
                            └──────────┘
*/
// The overall streaming flow is as follows:
//   digester        -> dispatcher/req
//   dispatcher/req  -> dispatcher/pipe
//   dispatcher/pipe -> query processor
//   query processor -> dispatcher/pipe
//   dispatcher/pipe -> dispatcher/res (cache hit)
//   dispatcher/res  -> requester (cache hit)
//   dispatcher/pipe -> batcher (small file)
//   dispatcher/pipe -> streamer (medium and large file)
//   batcher         -> dispatcher/res
//   streamer        -> dispatcher/res
//   dispatcher/res  -> requester
//
// The termination sequence is as follows:
//   user cancels the batching or the streaming context, not the uploader's context, and closes input streaming channels.
//       cancelling the context triggers aborting in-flight requests.
//   user cancels uploader's context: cancels pending digestions and gRPC processors blocked on throttlers.
//   client senders (top level) terminate.
//   the digester channel is closed, and a termination signal is sent to the dispatcher.
//   the dispatcher terminates its sender and propagates the signal to its piper.
//   the dispatcher's piper propagtes the signal to the intermediate query streamer.
//   the intermediate query streamer terminates and propagates the signal to the query processor and dispatcher's piper.
//   the query processor terminates.
//   the dispatcher's piper terminates.
//   the dispatcher's counter terminates (after observing all the remaining blobs) and propagates the signal to the receiver.
//   the dispatcher's receiver terminates.
//   the dispatcher terminates and propagates the signal to the batcher and the streamer.
//   the batcher and the streamer terminate.
//   user waits for the termination signal: return from batching uploader or response channel closed from streaming uploader.
//       this ensures the whole pipeline is drained properly.
//
// Logging:
//  Level 4 turns on debug logs and tracing. Tracing only logs errors, but also prints a summary of metrics.
//  Level 5 turns on merkle tree comparison with the client pkg.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"

	// Redundant imports are required for the google3 mirror. Aliases should not be changed.
	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"github.com/klauspost/compress/zstd"
	"github.com/pborman/uuid"

	// Alias should not be changed because it's used as is for the google3 mirror.
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/protobuf/proto"
)

var (
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

// IsCompressedWriteResourceName returns true if the name was generated using MakeCompressedWriteResourceName.
func IsCompressedWriteResourceName(name string) bool {
	return strings.Contains(name, "compressed-blobs/zstd")
}

// BatchingUploader provides a blocking interface to query and upload to the CAS.
type BatchingUploader struct {
	*uploader
}

// StreamingUploader provides an non-blocking interface to query and upload to the CAS
type StreamingUploader struct {
	*uploader
}

// uploader represents the state of an uploader implementation.
type uploader struct {
	// gRPC Services.
	cas        regrpc.ContentAddressableStorageClient
	byteStream bsgrpc.ByteStreamClient

	// gRPC configs.
	instanceName string
	queryRPCCfg  GRPCConfig
	batchRPCCfg  GRPCConfig
	streamRPCCfg GRPCConfig

	// gRPC throttling controls.
	queryThrottler  *throttler // Controls concurrent calls to the query API.
	uploadThrottler *throttler // Controls concurrent calls to the batch API.
	streamThrottler *throttler // Controls concurrent calls to the byte streaming API.

	// IO controls.
	ioCfg            IOConfig
	buffers          sync.Pool
	zstdEncoders     sync.Pool
	ioThrottler      *throttler // Controls total number of open files.
	ioLargeThrottler *throttler // Controls total number of open large files (subset of open files).

	// Caches.
	// nodeCache synchronizes digesting a path.
	// Concurrent walkers claim a path by storing a sync.WaitGroup reference, which allows other walkers to defer
	// digesting that path until the first successful walker stores the corresponding digest.
	// The keys are unique per walk, which means two walkers with different filters may cache
	// the same path twice, but each copy with a different node associated with it.
	nodeCache sync.Map

	// fileNodeCache synchronizes digesting regular files.
	// The keys are real paths without filters, which ensures that regular files are only digested once, even
	// across walks with different exclusion filters.
	// It also ensures that nodeCache does not have duplicate nodes for identical files.
	// In other words, nodeCache might hold different views of the same directory node, but fileNodeCache
	// will always hold the canonical file node for the corresponding real path.
	// Since nodes are pointer-like references, the shared memory cost between the two caches is limited to keys and addresses.
	fileNodeCache sync.Map

	// dirChildren is shared between all walkers. However, since a directory is owned by a single
	// walker at a time, there is no concurrent read/write to this map, but there might be concurrent reads.
	dirChildren nodeSliceMap

	// casPresenceCache helps short-circuit querying the CAS. It maps a digest to a boolean.
	// If the digest was previously seen in the CAS, the boolean will be true.
	casPresenceCache sync.Map

	// batchCache synchronizes uploading files via the bather.
	batchCache sync.Map

	// streamCache synchronizes uploading files via the streamer.
	streamCache sync.Map

	// Size padding values are used to improve the accuracy of estimating size limits for gRPC services.
	uploadBatchRequestItemBytesLimit int64
	uploadBatchRequestItemBaseSize   int
	uploadBatchRequestBaseSize       int

	// Concurrency controls.
	requestWorkerWg sync.WaitGroup
	digestWorkerWg  sync.WaitGroup
	queryWorkerWg   sync.WaitGroup
	uploadWorkerWg  sync.WaitGroup
	batchWorkerWg   sync.WaitGroup
	streamWorkerWg  sync.WaitGroup
	workerWg        sync.WaitGroup
	cleanupWg       sync.WaitGroup

	logBeatCloseCh chan struct{}
	closeCh        chan struct{}
}

// Node looks up a node from the node cache which is populated during digestion.
// The node is either an repb.FileNode, repb.DirectoryNode, or repb.SymlinkNode.
//
// Returns nil if no node corresponds to req.
func (u *uploader) Node(req UploadRequest) proto.Message {
	key := nodeCacheKey(req.Path, req.Exclude)
	n, ok := u.nodeCache.Load(key)
	if !ok {
		return nil
	}
	node, ok := n.(proto.Message)
	if !ok {
		return nil
	}
	return node
}

// NewBatchingUploader creates a new instance of the batching uploader.
// WIP: While this is intended to replace the uploader in the client and cas packages, it is not yet ready for production envionrments.
//
// The specified configs must be compatible with the capabilities of the server that the specified clients are connected to.
// ctx is used to make unified calls and terminate saturated throttlers and in-flight workers.
// ctx must be cancelled after all batching calls have returned to properly shutdown the uploader. It is only used for cancellation (not used with remote calls).
// gRPC timeouts are multiplied by retries. Batched RPCs are retried per batch. Streaming PRCs are retried per chunk.
func NewBatchingUploader(
	ctx context.Context, cas regrpc.ContentAddressableStorageClient, byteStream bsgrpc.ByteStreamClient, instanceName string,
	queryCfg, batchCfg, streamCfg GRPCConfig, ioCfg IOConfig,
) (*BatchingUploader, error) {
	uploader, err := newUploader(ctx, cas, byteStream, instanceName, queryCfg, batchCfg, streamCfg, ioCfg)
	if err != nil {
		return nil, err
	}
	return &BatchingUploader{uploader: uploader}, nil
}

// NewStreamingUploader creates a new instance of the streaming uploader.
// WIP: While this is intended to replace the uploader in the client and cas packages, it is not yet ready for production envionrments.
//
// The specified configs must be compatible with the capabilities of the server which the specified clients are connected to.
// ctx is used to make unified calls and terminate saturated throttlers and in-flight workers.
// ctx must be cancelled after all response channels have been closed to properly shutdown the uploader. It is only used for cancellation (not used with remote calls).
// gRPC timeouts are multiplied by retries. Batched RPCs are retried per batch. Streaming PRCs are retried per chunk.
func NewStreamingUploader(
	ctx context.Context, cas regrpc.ContentAddressableStorageClient, byteStream bsgrpc.ByteStreamClient, instanceName string,
	queryCfg, batchCfg, streamCfg GRPCConfig, ioCfg IOConfig,
) (*StreamingUploader, error) {
	uploader, err := newUploader(ctx, cas, byteStream, instanceName, queryCfg, batchCfg, streamCfg, ioCfg)
	if err != nil {
		return nil, err
	}
	return &StreamingUploader{uploader: uploader}, nil
}

// TODO: support uploading repb.Tree.
// TODO: support node properties as in https://github.com/bazelbuild/remote-apis-sdks/pull/475
// TODO: review ctx used in semaphores: should it be req.ctx to stop waiting if the request is cancelled?
// TODO: track unifications in processors (and MissingBlobs) to see how effective it is.
func newUploader(
	ctx context.Context, cas regrpc.ContentAddressableStorageClient, byteStream bsgrpc.ByteStreamClient, instanceName string,
	queryCfg, uploadCfg, streamCfg GRPCConfig, ioCfg IOConfig,
) (*uploader, error) {
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

	verbose = bool(log.V(4))

	queryRequestBaseSize := proto.Size(&repb.FindMissingBlobsRequest{
		InstanceName: instanceName,
		BlobDigests:  []*repb.Digest{},
	})
	dgProtoSample := digest.NewFromBlob([]byte("casng")).ToProto()
	dgProtoSize := proto.Size(dgProtoSample)
	queryRequestMaxSize := dgProtoSize*queryCfg.ItemsLimit + queryRequestBaseSize
	if queryRequestMaxSize > queryCfg.BytesLimit {
		return nil, fmt.Errorf("%w: insufficient bytes limit %d for items limit %d with estimated size of %d",
			ErrInvalidGRPCConfig, queryCfg.BytesLimit, queryCfg.ItemsLimit, queryRequestMaxSize)
	}

	uploadBatchRequestBaseSize := proto.Size(&repb.BatchUpdateBlobsRequest{
		InstanceName: instanceName,
		Requests:     []*repb.BatchUpdateBlobsRequest_Request{},
	})
	uploadBatchRequestItemBaseSize := proto.Size(&repb.BatchUpdateBlobsRequest_Request{Digest: dgProtoSample, Data: []byte{}})
	uploadBatchRequestItemBytesLimit := int64(uploadCfg.BytesLimit - uploadBatchRequestBaseSize - uploadBatchRequestItemBaseSize)

	u := &uploader{
		cas:          cas,
		byteStream:   byteStream,
		instanceName: instanceName,

		queryRPCCfg:  queryCfg,
		batchRPCCfg:  uploadCfg,
		streamRPCCfg: streamCfg,

		queryThrottler:  newThrottler(int64(queryCfg.ConcurrentCallsLimit)),
		uploadThrottler: newThrottler(int64(uploadCfg.ConcurrentCallsLimit)),
		streamThrottler: newThrottler(int64(streamCfg.ConcurrentCallsLimit)),

		ioCfg: ioCfg,
		buffers: sync.Pool{
			New: func() any {
				buf := make([]byte, ioCfg.BufferSize)
				return &buf
			},
		},
		zstdEncoders: sync.Pool{
			New: func() any {
				// Providing a nil writer implies that the encoder needs to be (re)initilaized with a writer using enc.Reset(w) before using it.
				enc, _ := zstd.NewWriter(nil)
				return enc
			},
		},
		ioThrottler:      newThrottler(int64(ioCfg.OpenFilesLimit)),
		ioLargeThrottler: newThrottler(int64(ioCfg.OpenLargeFilesLimit)),
		dirChildren:      nodeSliceMap{store: make(map[string][]proto.Message)},

		uploadBatchRequestBaseSize:       uploadBatchRequestBaseSize,
		uploadBatchRequestItemBaseSize:   uploadBatchRequestItemBaseSize,
		uploadBatchRequestItemBytesLimit: uploadBatchRequestItemBytesLimit,

		logBeatCloseCh: make(chan struct{}),
		closeCh:        make(chan struct{}),
	}
	log.V(1).Infof("new; cfg_query=%+v, cfg_batch=%+v, cfg_stream=%+v, cfg_io=%+v", queryCfg, uploadCfg, streamCfg, ioCfg)

	go u.close(ctx)

	u.cleanupWg.Add(1)
	go func() {
		u.logBeat()
		u.cleanupWg.Done()
	}()

	u.cleanupWg.Add(1)
	go func() {
		runMetricsCollector()
		u.cleanupWg.Done()
	}()

	go func() {
		for x := range traceCountCh {
			traceCount += x
		}
	}()
	return u, nil
}

func (u *uploader) close(ctx context.Context) {
	// The context must be cancelled first.
	<-ctx.Done()

	log.Info("waiting for request workers")
	u.requestWorkerWg.Wait()

	log.Info("waiting for digest workers")
	u.digestWorkerWg.Wait()

	log.Info("waiting for query workers")
	u.queryWorkerWg.Wait()

	log.Info("waiting for upload workers")
	u.uploadWorkerWg.Wait()

	log.Info("waiting for batch workers")
	u.batchWorkerWg.Wait()

	log.Info("waiting for stream workers")
	u.streamWorkerWg.Wait()

	log.Info("waiting for other workers")
	u.workerWg.Wait()

	log.Info("waiting for trace collectors")
	traceWg.Wait()
	close(traceCountCh)

	log.Info("waiting for cleanup")
	close(metricsCh)
	close(u.logBeatCloseCh)
	u.cleanupWg.Wait()

	log.Info("casng uploader done")
	close(u.closeCh)
}

// Done returns a channel that is closed when the the uploader is done.
func (u *uploader) Done() chan struct{} {
	return u.closeCh
}

func (u *uploader) logBeat() {
	interval := time.Minute
	if bool(log.V(3)) {
		interval = time.Second
	} else if log.V(2) {
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	i := 0
	kvs := []string{
		"#", "0",
		"open_files_tokens_util", "0",
		"large_open_files_tokens_util", "0",
		"query_tokens_util", "0",
		"batch_tokens_util", "0",
		"stream_tokens_util", "0",
		"traces", "0",
	}
	for {
		select {
		case <-u.logBeatCloseCh:
			return
		case <-ticker.C:
		}

		i++

		kvs[1] = strconv.Itoa(i)
		kvs[3] = fmtRate(u.ioThrottler.len(), u.ioThrottler.cap())
		kvs[5] = fmtRate(u.ioLargeThrottler.len(), u.ioLargeThrottler.cap())
		kvs[7] = fmtRate(u.queryThrottler.len(), u.queryThrottler.cap())
		kvs[9] = fmtRate(u.uploadThrottler.len(), u.uploadThrottler.cap())
		kvs[11] = fmtRate(u.streamThrottler.len(), u.streamThrottler.cap())
		kvs[13] = strconv.Itoa(traceCount)
		log.Infof("beat; %s", strings.Join(kvs, ", "))
	}
}

func fmtRate(x, y int) string {
	return fmt.Sprintf("%.2f", (float64(x)/float64(y))*100)
}

func isExec(mode fs.FileMode) bool {
	return mode&0100 != 0
}

func nodeCacheKey(path impath.Absolute, filter walker.Filter) string {
	return path.String() + filter.String()
}
