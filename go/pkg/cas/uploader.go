package cas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/blob"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	ep "github.com/bazelbuild/remote-apis-sdks/go/pkg/io/exppath"
	iow "github.com/bazelbuild/remote-apis-sdks/go/pkg/io/wrappers"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/pborman/uuid"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	bspb "google.golang.org/genproto/googleapis/bytestream"
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

	// ErrOversizedBlob indicates a blob that is too large to fit into the set byte limit.
	ErrOversizedBlob = errors.New("cas: oversized blob")
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

	// Upload deduplicates the specified blobs (unified uploads) and only uploads the ones that are not already in the CAS.
	// Large files are streamed while others are batched together.
	// Returns a slice of the digests of the uploaded blobs, excluding the ones that already exist in the CAS.
	Upload(context.Context, []blob.Blob, symlinkopts.Opts, ep.Predicate) ([]digest.Digest, Stats, error)

	// WriteBytes uploads all of the specified bytes directly to the specified resource name starting remotely at the specified offset.
	// If finish is true, the server is notified to finalize the resource name and no further writes are allowed.
	WriteBytes(ctx context.Context, name string, bytes []byte, offset int64, finish bool) (Stats, error)
}

// StreamingUploader provides a concurrency friendly API to upload to the CAS.
type StreamingUploader interface {
	io.Closer
	// MissingBlobs is a non-blocking call that queries the CAS for incoming digests.
	MissingBlobs(context.Context, <-chan digest.Digest) <-chan MissingBlobsResponse

	// Upload is a blocking call that uploads incoming blobs to the CAS.
	// It returns when the context is done, the input channel is closed, or a fatal error occurs.
	// The returned error is either from the context or an error that necessitates terminating the call.
	// Errors related to particular items are reported as part of their result.
	// During the lifetime of the call, digests are cached to avoid duplicate uploads (unified uploads).
	Upload(context.Context, <-chan blob.Blob, symlinkopts.Opts, ep.Predicate) (<-chan UploadResponse, error)

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

// missingBlobRequest associates a digest with tag the identifies the requester.
type missingBlobRequest struct {
	digest digest.Digest
	tag    string
}

// missingBlobRequestBundle is a set of digests, each is associated with multiple tags (queriers).
// It is used for unified requests when multiple concurrent requesters share seats in the same bundle.
type missingBlobRequestBundle = map[digest.Digest][]string

// querier associates a context (used as a control signal) with a channel
// the querier can receive responses on.
// When the context is done, the processor aborts all associated requests.
type querier struct {
	ctx context.Context
	ch  chan MissingBlobsResponse
}

// uploader represents the state of an uploader implementation.
type uploaderv2 struct {
	cas          repb.ContentAddressableStorageClient
	byteStream   bspb.ByteStreamClient
	instanceName string

	queryRpcConfig  RPCCfg
	uploadRpcConfig RPCCfg
	streamRpcConfig RPCCfg

	// Throttling controls.
	querySem    *semaphore.Weighted
	uploadSem   *semaphore.Weighted
	streamSem   *semaphore.Weighted
	retryPolicy retry.BackoffPolicy

	// IO controls.
	ioCfg        IOCfg
	buffers      sync.Pool
	zstdEncoders sync.Pool

	// Concurrency controls.

	// queryReqChan is the fan-in channel for queries.
	// All senders must also listen on the context to avoid deadlocks.
	queryReqChan chan missingBlobRequest
	// grpcWaitGroup is used to wait for in-flight gRPC calls upon graceful termination.
	grpcWaitGroup sync.WaitGroup
	// querier is set of active queriers and their associated channels.
	querier sync.Map
	// querierWaitGroup is used to wait for in-flight receivers upon graceful termination.
	querierWaitGroup sync.WaitGroup
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
//
// The digests are batched based on the set gRPC limits (count and size).
// Errors from a batch do not affect other batches, but all digests from such bad batches will be reported as missing by this call.
// In other words, if an error is returned, any digest that is not in the returned slice is not missing.
// If no error is returned, the returned slice contains all the missing digests.
func (u *batchingUploader) MissingBlobs(ctx context.Context, digests []digest.Digest) ([]digest.Digest, error) {
	if len(digests) < 1 {
		return nil, nil
	}

	ctxQuerier, ctxQuerierCancel := context.WithCancel(ctx)
	defer ctxQuerierCancel()

	tag, resChan := u.registerQuerier(ctxQuerier)
	go func() {
		for _, d := range digests {
			select {
			case <-ctx.Done():
				return
			case u.queryReqChan <- missingBlobRequest{digest: d, tag: tag}:
			}
		}
	}()

	var missing []digest.Digest
	var err error
	var total = len(digests)
	var i = 0
	for r := range resChan {
		switch {
		case r.Err != nil:
			missing = append(missing, r.Digest)
			// Don't join the same error from a batch more than once.
			if !errors.Is(err, r.Err) {
				err = errors.Join(r.Err, err)
			}
		case r.Missing:
			missing = append(missing, r.Digest)
		}
		i += 1
		if i >= total {
			ctxQuerierCancel()
			break
		}
	}

	// Request aborted, possibly midflight. Reporting a hit as a miss is safer than otherwise.
	if ctx.Err() != nil {
		return digests, ctx.Err()
	}

	// Ideally, this should never be true at this point. Otherwise, it's a fatal error.
	if i < total {
		panic(fmt.Sprintf("channel closed unexpectedly: got %d msgs, want %d", i, total))
	}

	return missing, err
}

// Upload deduplicates the specified blobs (unified uploads) and only uploads the ones that are not already in the CAS.
// Large files are streamed while others are batched together.
// Returns a slice of the digests of the uploaded blobs, excluding the ones that already exist in the CAS.
func (u *batchingUploader) Upload(ctx context.Context, blobs []blob.Blob, symlinkOpts symlinkopts.Opts, predicate ep.Predicate) ([]digest.Digest, Stats, error) {
	panic("not yet implemented")
}

// WriteBytes uploads all of the specified bytes directly to the specified resource name starting remotely at the specified offset.
// If finish is true, the server is notified to finalize the resource name and further writes may not succeed.
// The errors returned are either from the context, ErrGRPC, ErrIO, or ErrCompression. More errors may be wrapped inside.
// In the case were the returned stats indicate that all the bytes were sent, it is still not a guarantee all the bytes
// were received by the server since an acknlowedgement was not observed.
func (u *batchingUploader) WriteBytes(ctx context.Context, name string, b []byte, offset int64, finish bool) (Stats, error) {
	stats := Stats{}

	if err := u.streamSem.Acquire(ctx, 1); err != nil {
		// err is always ctx.Err(), so abort immediately.
		return stats, err
	}
	defer u.streamSem.Release(1)

	rawBytesReader := iow.NewBytesReadCloser(bytes.NewReader(b))
	var src io.ReadCloser = rawBytesReader
	eg, ctx := errgroup.WithContext(ctx)

	// If compression is enabled, plug in the encoder via a pipe.
	if len(b) >= int(u.ioCfg.CompressionSizeThreshold) {
		pr, pw := io.Pipe()
		// Closing pr always returns a nil error, but also sends ErrClosedPipe to pw.
		defer pr.Close()
		src = pr

		enc := zstdEncoders.Get().(*zstd.Encoder)
		defer zstdEncoders.Put(enc)
		// (Re)initialize the encoder with this writer.
		enc.Reset(pw)
		// Get it going.
		eg.Go(func() (errCompr error) {
			// Closing pw always returns a nil error, but also sends an EOF to pr.
			defer pw.Close()

			// Closing the encoder is necessary to flush remaining bytes.
			defer func() {
				if errClose := enc.Close(); errClose != nil {
					errCompr = errors.Join(ErrCompression, errClose, errCompr)
				}
			}()

			// The encoder will theoretically read continuously. However, pw will block it
			// while pr is not reading from the other side.
			// In other words, the chunk size of the encoder's output is controlled by the reader.
			switch _, errEnc := enc.ReadFrom(rawBytesReader); {
			case errEnc == io.ErrClosedPipe:
				// pr was closed first, which means the actual error is on that end.
				return nil
			case errEnc != nil:
				return errors.Join(ErrCompression, errEnc)
			}

			return nil
		})
	}

	ctx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()

	stream, errStream := u.byteStream.Write(ctx)
	if errStream != nil {
		return stats, errors.Join(ErrGRPC, errStream)
	}

	req := &bspb.WriteRequest{
		ResourceName: name,
		FinishWrite:  finish,
		WriteOffset:  0,
	}

	buf := u.buffers.Get().([]byte)
	defer u.buffers.Put(buf)

	cacheHit := false
	var err error
	for {
		n, errRead := src.Read(buf)
		if errRead != nil && errRead != io.EOF {
			err = errors.Join(ErrIO, errRead, err)
			break
		}

		req.Data = buf[:n]
		stats.BytesAttempted += int64(n)
		errStream := u.withTimeout(u.streamRpcConfig.Timeout, ctxCancel, func() error {
			return u.withRetry(ctx, func() error {
				stats.BytesMoved += int64(n)
				return stream.Send(req)
			})
		})
		if errStream != nil && errStream != io.EOF {
			err = errors.Join(ErrGRPC, errStream, err)
			break
		}

		// The server thinks it already has the content for the specified resource.
		if errStream == io.EOF {
			cacheHit = true
			break
		}

		req.WriteOffset += int64(n)

		// The reader is done (all bytes processed or interrupted).
		if errRead == io.EOF {
			break
		}
	}

	// Close the reader to signal to the encoder's goroutine to terminate.
	if errClose := src.Close(); errClose != nil {
		err = errors.Join(ErrIO, errClose, err)
	}

	// Check if the encoder sent EOF due to an error.
	// This theoratically will block until the encoder's goroutine returns.
	// However, closing the reader eventually terminates that goroutine.
	if errEnc := eg.Wait(); errEnc != nil {
		err = errors.Join(ErrCompression, errEnc, err)
	}

	// Capture stats before processing errors.
	stats.BytesRequesetd = int64(len(b))
	stats.LogicalBytesMoved = int64(len(b) - rawBytesReader.Len())
	if cacheHit {
		stats.BytesCached = stats.BytesRequesetd
	}
	stats.BytesStreamed = stats.LogicalBytesMoved
	stats.BytesBatched = 0
	stats.InputFileCount = 0
	stats.InputDirCount = 0
	stats.InputSymlinkCount = 0
	if cacheHit {
		stats.CacheHitCount = 1
	} else {
		stats.CacheMissCount = 1
	}
	stats.DigestCount = 0
	stats.BatchedCount = 0
	if err == nil {
		stats.StreamedCount = 1
	}

	res, errClose := stream.CloseAndRecv()
	if errClose != nil {
		return stats, errors.Join(ErrGRPC, errClose, err)
	}

	if !cacheHit && res.CommittedSize != stats.BytesAttempted {
		err = errors.Join(ErrGRPC, fmt.Errorf("committed size mismatch: got %d, want %d", res.CommittedSize, stats.BytesAttempted), err)
	}

	return stats, err
}

// MissingBlobs is a non-blocking call that queries the CAS for incoming digests.
// It starts concurrent two goruotines, one for reading from the input channel, and one for sending on the returned channel.
// The receiver goroutine terminates when the context is done.
// The sender goroutine terminates when the processor has completed all the related requests.
// The returned channel is closed when the sender goroutine terminates.
func (u *streamingUploader) MissingBlobs(ctx context.Context, in <-chan digest.Digest) <-chan MissingBlobsResponse {
	ch := make(chan MissingBlobsResponse)
	ctxQuerier, ctxQuerierCancel := context.WithCancel(ctx)
	tag, resChan := u.registerQuerier(ctxQuerier)

	go func() {
		for r := range resChan {
			ch <- r
		}
		close(ch)
	}()

	go func() {
		defer ctxQuerierCancel()
		for d := range in {
			select {
			case <-ctx.Done():
				return
			case u.queryReqChan <- missingBlobRequest{digest: d, tag: tag}:
			}
		}
		// TODO: closing the input channel shouldn't apport in-flight requests.
	}()

	return ch
}

// Upload is a blocking call that uploads incoming blobs to the CAS.
// It returns when the context is done, the input channel is closed, or a fatal error occurs.
// The returned error is either from the context or an error that necessitates terminating the call.
// Errors related to particular items are reported as part of their result.
// During the lifetime of the call, digests are cached to avoid duplicate uploads (unified uploads).
func (u *streamingUploader) Upload(context.Context, <-chan blob.Blob, symlinkopts.Opts, ep.Predicate) (<-chan UploadResponse, error) {
	panic("not yet implemented")
}

// WriteBytes is a blocking call that uploads incoming bytes to the CAS at the specified resource name starting remotely at the specified offset.
// It returns when the context is done, the input channel is closed, or a fatal error occurs.
// Each outgoing integer value corresponds with an incoming item and represents the total bytes moved over the wire while streaming the bytes. This may be higher (retries) or lower (interrupted) than the number of incoming bytes.
// If finish is true, the server is notified to finalize the resource name and no further writes are allowed.
func (u *streamingUploader) WriteBytes(ctx context.Context, name string, bytesChan <-chan []byte, offset int64, finish bool) (<-chan int64, error) {
	panic("not yet implemented")
}

// Close blocks until all resources have been cleaned up.
// It always returns nil.
func (u *uploaderv2) Close() error {
	u.querierWaitGroup.Wait()
	u.grpcWaitGroup.Wait()
	return nil
}

func (u *uploaderv2) withRetry(ctx context.Context, fn func() error) error {
	return retry.WithPolicy(ctx, retry.TransientOnly, u.retryPolicy, fn)
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

// registerQuerier creates a new channel to send responses on.
// Only requests associated with the returned tag are sent on the returned channel.
func (u *uploaderv2) registerQuerier(ctx context.Context) (string, <-chan MissingBlobsResponse) {
	tag := uuid.New()
	q := &querier{ctx: ctx, ch: make(chan MissingBlobsResponse)}
	u.querier.Store(tag, q)

	u.querierWaitGroup.Add(1)
	go func() {
		<-ctx.Done()
		u.querier.Delete(tag)
		// Close after deleting the querier to ensure exclusive access to channel.
		close(q.ch)
		u.querierWaitGroup.Done()
	}()

	return tag, q.ch
}

// callMissingBlobs calls the gRPC endpoint and notifies queriers of the results.
// It assumes ownership of the reqs argument.
func (u *uploaderv2) callMissingBlobs(ctx context.Context, reqs missingBlobRequestBundle) {
	u.grpcWaitGroup.Add(1)
	defer u.grpcWaitGroup.Done()

	digests := make([]*repb.Digest, 0, len(reqs))
	var oversized []digest.Digest
	for d := range reqs {
		if d.Size > int64(u.queryRpcConfig.BytesLimit) {
			oversized = append(oversized, d)
			continue
		}
		digests = append(digests, d.ToProto())
	}

	// Report oversized blobs.
	for _, d := range oversized {
		u.notifyQueriers(MissingBlobsResponse{
			Digest: d,
			Err:    ErrOversizedBlob,
		}, reqs[d]...)
	}

	if len(digests) < 1 {
		return
	}

	req := &repb.FindMissingBlobsRequest{
		InstanceName: u.instanceName,
		BlobDigests:  digests,
	}

	var res *repb.FindMissingBlobsResponse
	var err error
	ctx, ctxCancel := context.WithCancel(ctx)
	err = u.withTimeout(u.queryRpcConfig.Timeout, ctxCancel, func() error {
		return u.withRetry(ctx, func() error {
			res, err = u.cas.FindMissingBlobs(ctx, req)
			return err
		})
	})
	ctxCancel()

	var missing []*repb.Digest
	if err != nil {
		err = errors.Join(ErrGRPC, err)
		missing = append(missing, digests...)
	} else {
		missing = append(missing, res.MissingBlobDigests...)
	}

	// Reprot missing.
	for _, dpb := range missing {
		d := digest.NewFromProtoUnvalidated(dpb)
		u.notifyQueriers(MissingBlobsResponse{
			Digest:  d,
			Missing: true,
			Err:     err,
		}, reqs[d]...)
		delete(reqs, d)
	}

	// Report non-missing.
	for d := range reqs {
		u.notifyQueriers(MissingBlobsResponse{
			Digest:  d,
			Missing: false,
			// This should always be nil at this point.
			Err: err,
		}, reqs[d]...)
	}
}

func (u *uploaderv2) queryProcessor(ctx context.Context) {
	bundle := make(missingBlobRequestBundle)
	var bundleSize int64

	handle := func() {
		if len(bundle) < 1 {
			return
		}
		// Block the entire processor if the concurrency limit is reached.
		if err := u.querySem.Acquire(ctx, 1); err != nil {
			// err is always ctx.Err(), so abort immediately.
			return
		}
		defer u.querySem.Release(1)

		go u.callMissingBlobs(ctx, bundle)

		bundle = make(missingBlobRequestBundle)
		bundleSize = 0
	}

	bundleTicker := time.NewTicker(u.queryRpcConfig.BundleTimeout)
	defer bundleTicker.Stop()

	for {
		select {
		case req := <-u.queryReqChan:
			// Check size threshold. Oversized items are handled downstream.
			if bundleSize+req.digest.Size >= int64(u.queryRpcConfig.BytesLimit) {
				handle()
			}

			bundle[req.digest] = append(bundle[req.digest], req.tag)
			bundleSize += req.digest.Size

			// Check length threshold.
			if len(bundle) >= u.queryRpcConfig.ItemsLimit {
				handle()
				continue
			}
		case <-bundleTicker.C:
			handle()
		case <-ctx.Done():
			// Nothing to wait for since all the senders and receivers should have terminated as well.
			// The only things that might still be in-flight are the gRPC calls, which will eventually terminate since
			// there are no active queriers.
			return
		}
	}
}

func (u *uploaderv2) notifyQueriers(r MissingBlobsResponse, tags ...string) {
	for _, tag := range tags {
		rawQ, ok := u.querier.Load(tag)
		if !ok {
			continue
		}
		if q, ok := rawQ.(*querier); ok {
			// TODO: channel may be closed.
			q.ch <- r
		}
	}
}

func isQuerierActive(q *querier) bool {
	if q == nil {
		return false
	}
	select {
	case <-q.ctx.Done():
		return false
	default:
	}
	return true
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

func digestsFromProtos(dprotots ...*repb.Digest) []digest.Digest {
	ds := make([]digest.Digest, len(dprotots))
	for i, dp := range dprotots {
		ds[i] = digest.NewFromProtoUnvalidated(dp)
	}
	return ds
}

func digestsToProtos(digests ...digest.Digest) []*repb.Digest {
	dp := make([]*repb.Digest, len(digests))
	for i, d := range digests {
		dp[i] = d.ToProto()
	}
	return dp
}

func newUploaderv2(
	ctx context.Context,
	cas repb.ContentAddressableStorageClient,
	byteStream bspb.ByteStreamClient,
	instanceName string,
	queryCfg, uploadCfg, streamCfg RPCCfg, ioCfg IOCfg,
	retryPolicy retry.BackoffPolicy) (*uploaderv2, error) {

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

		querySem:    semaphore.NewWeighted(int64(queryCfg.ConcurrentCallsLimit)),
		uploadSem:   semaphore.NewWeighted(int64(uploadCfg.ConcurrentCallsLimit)),
		streamSem:   semaphore.NewWeighted(int64(streamCfg.ConcurrentCallsLimit)),
		retryPolicy: retryPolicy,

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

		queryReqChan:     make(chan missingBlobRequest),
		grpcWaitGroup:    sync.WaitGroup{},
		querier:          sync.Map{},
		querierWaitGroup: sync.WaitGroup{},
	}

	go u.queryProcessor(ctx)
	return u, nil
}

// NewBatchingUploader creates a new instance of the batching uploader interface.
// The specified configs must be compatbile with the capabilities of the server
// which the specified clients are connected to.
func NewBatchingUploader(
	ctx context.Context,
	cas repb.ContentAddressableStorageClient,
	byteStream bspb.ByteStreamClient,
	instanceName string,
	queryCfg, uploadCfg, streamCfg RPCCfg, ioCfg IOCfg,
	retryPolicy retry.BackoffPolicy) (BatchingUploader, error) {
	uploader, err := newUploaderv2(ctx, cas, byteStream, instanceName, queryCfg, uploadCfg, streamCfg, ioCfg, retryPolicy)
	if err != nil {
		return nil, err
	}
	return &batchingUploader{uploaderv2: uploader}, nil
}

// NewStreamingUploader creates a new instance of the streaming uploader interface.
// The specified configs must be compatbile with the capabilities of the server
// which the specified clients are connected to.
func NewStreamingUploader(
	ctx context.Context,
	cas repb.ContentAddressableStorageClient,
	byteStream bspb.ByteStreamClient,
	instanceName string,
	queryCfg, uploadCfg, streamCfg RPCCfg, ioCfg IOCfg,
	retryPolicy retry.BackoffPolicy) (StreamingUploader, error) {
	uploader, err := newUploaderv2(ctx, cas, byteStream, instanceName, queryCfg, uploadCfg, streamCfg, ioCfg, retryPolicy)
	if err != nil {
		return nil, err
	}
	return &streamingUploader{uploaderv2: uploader}, nil
}
