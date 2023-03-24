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
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/pborman/uuid"
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

	// Upload digests the specified paths and upload to the CAS what is not already there.
	// Returns a slice of the digests of the files that had to be uploaded.
	Upload(context.Context, []ep.Abs, symlinkopts.Opts, ep.Predicate) ([]digest.Digest, Stats, error)

	// WriteBytes uploads all the bytes of the specified reader directly to the specified resource name starting remotely at the specified offset.
	// If finish is true, the server is notified to finalize the resource name and no further writes are allowed.
	WriteBytes(ctx context.Context, name string, r io.Reader, size int64, offset int64, finish bool) (Stats, error)
}

// StreamingUploader provides a concurrency friendly API to upload to the CAS.
type StreamingUploader interface {
	io.Closer
	// MissingBlobs is a non-blocking call that queries the CAS for incoming digests.
	MissingBlobs(context.Context, <-chan digest.Digest) <-chan MissingBlobsResponse

	// Upload is a non-blocking call that uploads incoming files to the CAS if necessary.
	Upload(context.Context, <-chan ep.Abs, symlinkopts.Opts, ep.Predicate) <-chan UploadResponse
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

// missingBlobRequest associates a digest with a tag the identifies the requester.
type missingBlobRequest struct {
	digest digest.Digest
	tag    string
}

// missingBlobRequestBundle is a set of digests, each is associated with multiple tags (query callers).
// It is used for unified requests when multiple concurrent requesters share seats in the same bundle.
type missingBlobRequestBundle = map[digest.Digest][]string

// queryCaller is channel that represents an active query caller who is listening
// on it waiting for responses.
// The channel is owned by the processor and closed when the query caller signals to the processor
// that it is no longer interested in responses.
// That signal is sent via a context that is captured in a closure so no need to capture it again here.
// See registerQueryCaller for details.
type queryCaller = chan MissingBlobsResponse

type uploadRequest struct {
	path ep.Abs
	tag  string
}

type uploadRequestBundle = map[digest.Digest][]string

type uploadCaller = chan UploadResponse

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

	// queryChan is the fan-in channel for queries.
	// This channel is deliberately not closed to avoid send-on-closed-channel errors.
	// All senders must also listen on the context to avoid deadlocks.
	queryChan chan missingBlobRequest
	// uploadChan is the fan-in channel for uploads.
	// This channel is deliberately not closed to avoid send-on-closed-channel errors.
	// All senders must also listen on the context to avoid deadlocks.
	uploadChan chan uploadRequest
	// grpcWg is used to wait for in-flight gRPC calls upon graceful termination.
	grpcWg sync.WaitGroup

	// queryCaller is set of active query callers and their associated channels.
	queryCaller map[string]queryCaller
	// queryCallerWg is used to wait for in-flight receivers upon graceful termination.
	queryCallerWg    sync.WaitGroup
	queryCallerMutex sync.Mutex

	// uploadCaller is set of active upload callers and their associated channels.
	uploadCaller map[string]uploadCaller
	// uploadCallerWg is used to wait for in-flight receivers upon graceful termination.
	uploadCallerWg    sync.WaitGroup
	uploadCallerMutex sync.Mutex
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

	// This implementation converts the underlying nonblocking implementation into a blocking one.
	// A separate goroutine is used to push the requests into the processor.
	// The receiving code blocks the goroutine of the call until all responses are received or the context is canceled.

	ctxQueryCaller, ctxQueryCallerCancel := context.WithCancel(ctx)
	defer ctxQueryCallerCancel()

	tag, resChan := u.registerQueryCaller(ctxQueryCaller)
	go func() {
		for _, d := range digests {
			select {
			case <-ctx.Done():
				return
			case u.queryChan <- missingBlobRequest{digest: d, tag: tag}:
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
			// This may not prevent similar errors from multiple batches sine errors.Is does not necessarily match by content.
			if !errors.Is(err, r.Err) {
				err = errors.Join(r.Err, err)
			}
		case r.Missing:
			missing = append(missing, r.Digest)
		}
		i += 1
		if i >= total {
			ctxQueryCallerCancel()
			// It's tempting to break here, but the channel must be drained until the processor closes it.
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

// Upload processes the specified blobs for upload. Blobs that already exist in the CAS are not uploaded.
// The specified predicate takes precedence over any blob-specific predicate.
//
// Returns a slice of the digests of the blobs that were uploaded (excluding the ones that already exist in the CAS).
// If the returned error is nil, any digest that is not in the returned slice was already in the CAS, and any digest
// that is in the slice may have been successfully uploaded or not.
// If the returned error is not nil, the returned slice may be incomplete.
func (u *batchingUploader) Upload(ctx context.Context, paths []ep.Abs, symlinkOpts symlinkopts.Opts, predicate ep.Predicate) ([]digest.Digest, Stats, error) {
	var stats Stats

	if len(paths) < 1 {
		return nil, stats, nil
	}

	// This implementation converts the underlying nonblocking implementation into a blocking one.
	// A separate goroutine is used to push the requests into the processor.
	// The receiving code blocks the goroutine of the call until all responses are received or the context is canceled.

	ctxUploadCaller, ctxUploaderCallerCancel := context.WithCancel(ctx)
	defer ctxUploaderCallerCancel()

	tag, resChan := u.registerUploadCaller(ctxUploadCaller)
	go func() {
		for _, p := range paths {
			select {
			case <-ctx.Done():
				return
			case u.uploadChan <- uploadRequest{path: p, tag: tag}:
			}
		}
	}()

	var missing []digest.Digest
	var err error
	var total = len(paths)
	var i = 0
	for r := range resChan {
		switch {
		case r.Err != nil:
			missing = append(missing, r.Digest)
			// Don't join the same error from a batch more than once.
			// This may not prevent similar errors from multiple batches sine errors.Is does not necessarily match by content.
			if !errors.Is(err, r.Err) {
				err = errors.Join(r.Err, err)
			}
		}
		stats.Add(r.Stats)
		i += 1
		if i >= total {
			ctxUploaderCallerCancel()
			// It's tempting to break here, but the channel must be drained until the processor closes it.
		}
	}

	// Request aborted, possibly midflight. Reporting a hit as a miss is safer than otherwise.
	if ctx.Err() != nil {
		return missing, stats, ctx.Err()
	}

	// Ideally, this should never be true at this point. Otherwise, it's a fatal error.
	if i < total {
		panic(fmt.Sprintf("channel closed unexpectedly: got %d msgs, want %d", i, total))
	}

	return missing, stats, err
}

// WriteBytes uploads all the bytes (until EOF) of the specified reader directly to the specified resource name starting remotely at the specified offset.
// The specified size is used to toggle compression as well as report some stats. It must be reflect the actual number of bytes the specified reader has to give.
// If finish is true, the server is notified to finalize the resource name and further writes may not succeed.
// The errors returned are either from the context, ErrGRPC, ErrIO, or ErrCompression. More errors may be wrapped inside.
// In case of error while the returned stats indicate that all the bytes were sent, it is still not a guarantee all the bytes
// were received by the server since an acknlowedgement was not observed.
func (u *batchingUploader) WriteBytes(ctx context.Context, name string, r io.Reader, size int64, offset int64, finish bool) (Stats, error) {
	stats := Stats{}

	if err := u.streamSem.Acquire(ctx, 1); err != nil {
		// err is always ctx.Err(), so abort immediately.
		return stats, err
	}
	defer u.streamSem.Release(1)

	var src = r

	// If compression is enabled, plug in the encoder via a pipe.
	var errCompr error
	var nRawBytes int64
	var encWg sync.WaitGroup
	if size >= u.ioCfg.CompressionSizeThreshold {
		pr, pw := io.Pipe()
		// Closing pr always returns a nil error, but also sends ErrClosedPipe to pw.
		defer pr.Close()
		src = pr

		enc := zstdEncoders.Get().(*zstd.Encoder)
		defer zstdEncoders.Put(enc)
		// (Re)initialize the encoder with this writer.
		enc.Reset(pw)
		// Get it going.
		encWg.Add(1)
		go func() {
			defer encWg.Done()
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
			var errEnc error
			switch nRawBytes, errEnc = enc.ReadFrom(r); {
			case errEnc == io.ErrClosedPipe:
				// pr was closed first, which means the actual error is on that end.
				return
			case errEnc != nil:
				errCompr = errors.Join(ErrCompression, errEnc)
				return
			}
		}()
	}

	ctx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()

	stream, errStream := u.byteStream.Write(ctx)
	if errStream != nil {
		return stats, errors.Join(ErrGRPC, errStream)
	}

	buf := u.buffers.Get().([]byte)
	defer u.buffers.Put(buf)

	cacheHit := false
	var err error
	req := &bspb.WriteRequest{
		ResourceName: name,
		WriteOffset:  offset,
	}
	for {
		n, errRead := src.Read(buf)
		if errRead != nil && errRead != io.EOF {
			err = errors.Join(ErrIO, errRead, err)
			break
		}

		n64 := int64(n)
		stats.LogicalBytesMoved += n64 // This may be adjusted later to exclude compression. See below.
		stats.EffectiveBytesMoved += n64

		req.Data = buf[:n]
		req.FinishWrite = finish && errRead == io.EOF
		errStream := u.withTimeout(u.streamRpcConfig.Timeout, ctxCancel, func() error {
			return u.withRetry(ctx, func() error {
				stats.TotalBytesMoved += n64
				return stream.Send(req)
			})
		})
		if errStream != nil && errStream != io.EOF {
			err = errors.Join(ErrGRPC, errStream, err)
			break
		}

		// The server says the content for the specified resource already exists.
		if errStream == io.EOF {
			cacheHit = true
			break
		}

		req.WriteOffset += n64

		// The reader is done (all bytes processed or interrupted).
		if errRead == io.EOF {
			break
		}
	}

	// Close the reader to signal to the encoder's goroutine to terminate.
	if srcCloser, ok := src.(io.Closer); ok {
		if errClose := srcCloser.Close(); errClose != nil {
			err = errors.Join(ErrIO, errClose, err)
		}
	}

	// This theoratically will block until the encoder's goroutine returns.
	// However, closing the reader eventually terminates that goroutine.
	// This is necessary because the encoder's goroutine currently owns errCompr and nRawBytes.
	encWg.Wait()
	if errCompr != nil {
		err = errors.Join(ErrCompression, errCompr, err)
	}

	// Capture stats before processing errors.
	stats.BytesRequested = size
	if nRawBytes > 0 {
		// Compression was turned on.
		// nRawBytes may be smaller than compressed bytes (additional headers without effective compression).
		stats.LogicalBytesMoved = nRawBytes
	}
	if cacheHit {
		stats.LogicalBytesCached = size
	}
	stats.LogicalBytesStreamed = stats.LogicalBytesMoved
	stats.LogicalBytesBatched = 0
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

	if !cacheHit && res.CommittedSize != stats.EffectiveBytesMoved {
		err = errors.Join(ErrGRPC, fmt.Errorf("committed size mismatch: got %d, want %d", res.CommittedSize, stats.EffectiveBytesMoved), err)
	}

	return stats, err
}

// MissingBlobs is a non-blocking call that queries the CAS for incoming digests.
//
// The caller must close the specified input channel as a termination signal.
// The returned channel is closed when no more responses are available for this call. This could indicate
// completion or abortion (in case the context was canceled).
func (u *streamingUploader) MissingBlobs(ctx context.Context, in <-chan digest.Digest) <-chan MissingBlobsResponse {
	// The implementation here acts like a pipe with a count-based coordinator.
	// Closing the input channel should not close the pipe until all the requests are piped through to the responses channel.
	// At the same time, all goroutines must abort when the context is done.
	// To ahcieve this, a third goroutine (in addition to a sender and a receiver) is used to maintain a count of pending requests.
	// Once the count is reduced to zero after the sender is done, the receiver is closed and the processor is notified to unregister this query caller.

	ch := make(chan MissingBlobsResponse)
	ctxQueryCaller, ctxQueryCallerCancel := context.WithCancel(ctx)
	tag, resChan := u.registerQueryCaller(ctxQueryCaller)

	// Counter.
	pending := 0
	pendingChan := make(chan int)
	u.queryCallerWg.Add(1)
	go func() {
		defer u.queryCallerWg.Done()
		defer ctxQueryCallerCancel()
		done := false
		for {
			select {
			case <-ctx.Done():
				return
			case x := <-pendingChan: // The channel is never closed so no need to capture the closing signal.
				if x == 0 {
					done = true
				}
				pending += x
				if pending == 0 && done {
					return
				}
			}
		}
	}()

	// Receiver.
	u.queryCallerWg.Add(1)
	go func() {
		defer u.queryCallerWg.Done()
		// Continue to drain until the processor closes the channel to avoid deadlocks.
		for r := range resChan {
			ch <- r
			pendingChan <- -1
		}
		close(ch)
	}()

	// Sender.
	u.queryCallerWg.Add(1)
	go func() {
		defer u.queryCallerWg.Done()
		for {
			select {
			case <-ctx.Done():
				ctxQueryCallerCancel()
				return
			case d, ok := <-in:
				if !ok {
					pendingChan <- 0
					return
				}
				u.queryChan <- missingBlobRequest{digest: d, tag: tag}
				pendingChan <- 1
			}
		}
	}()

	return ch
}

// Upload is a non-blocking call that uploads incoming files to the CAS if necessary.
func (u *streamingUploader) Upload(context.Context, <-chan ep.Abs, symlinkopts.Opts, ep.Predicate) <-chan UploadResponse {
	panic("not yet implemented")
}

// Close blocks until all resources have been cleaned up.
// It always returns nil.
func (u *uploaderv2) Close() error {
	u.queryCallerWg.Wait()
	u.grpcWg.Wait()
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

// registerQueryCaller returns a new channel to the caller to read responses from.
//
// Only requests associated with the returned tag are sent on the returned channel.
//
// The returned channel is closed when the specified context is done. The caller should
// ensure the context is canceled at the right time to avoid send-on-closed-channel errors
// and avoid deadlocks.
//
// The caller must continue to drain the returned channel until it is closed to avoid deadlocks.
func (u *uploaderv2) registerQueryCaller(ctx context.Context) (string, <-chan MissingBlobsResponse) {
	tag := uuid.New()

	// Serialize this block to avoid concurrent map-read-write errors.
	u.queryCallerMutex.Lock()
	qc, ok := u.queryCaller[tag]
	if !ok {
		qc = make(chan MissingBlobsResponse)
		u.queryCaller[tag] = qc
	}
	u.queryCallerMutex.Unlock()

	u.queryCallerWg.Add(1)
	go func() {
		<-ctx.Done()
		// Serialize this block to avoid concurrent map-read-write and send-on-closed-channel errors.
		u.queryCallerMutex.Lock()
		delete(u.queryCaller, tag)
		u.queryCallerMutex.Unlock()

		close(qc)
		u.queryCallerWg.Done()
	}()

	return tag, qc
}

func (u *uploaderv2) notifyQueryCallers(r MissingBlobsResponse, tags ...string) {
	// Serialize this block to avoid concurrent map-read-write and send-on-closed-channel errors.
	u.queryCallerMutex.Lock()
	defer u.queryCallerMutex.Unlock()
	for _, tag := range tags {
		qc, ok := u.queryCaller[tag]
		if ok {
			// Possible deadlock if the receiver had abandoned the channel.
			qc <- r
		}
	}
}

// callMissingBlobs calls the gRPC endpoint and notifies query callers of the results.
// It assumes ownership of the reqs argument.
func (u *uploaderv2) callMissingBlobs(ctx context.Context, bundle missingBlobRequestBundle) {
	u.grpcWg.Add(1)
	defer u.grpcWg.Done()

	digests := make([]*repb.Digest, 0, len(bundle))
	var oversized []digest.Digest
	for d := range bundle {
		if d.Size > int64(u.queryRpcConfig.BytesLimit) {
			oversized = append(oversized, d)
			continue
		}
		digests = append(digests, d.ToProto())
	}

	// Report oversized blobs.
	for _, d := range oversized {
		u.notifyQueryCallers(MissingBlobsResponse{
			Digest: d,
			Err:    ErrOversizedBlob,
		}, bundle[d]...)
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
		u.notifyQueryCallers(MissingBlobsResponse{
			Digest:  d,
			Missing: true,
			Err:     err,
		}, bundle[d]...)
		delete(bundle, d)
	}

	// Report non-missing.
	for d := range bundle {
		u.notifyQueryCallers(MissingBlobsResponse{
			Digest:  d,
			Missing: false,
			// This should always be nil at this point.
			Err: err,
		}, bundle[d]...)
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
		case req, ok := <-u.queryChan:
			if !ok {
				// This should never happen since this channel is never closed.
				return
			}

			// Check size threshold. Oversized items are handled downstream.
			if bundleSize+req.digest.Size >= int64(u.queryRpcConfig.BytesLimit) {
				handle()
			}

			// Duplicate tags are allowed to ensure the query caller can match the number of responses to the number of requests.
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
			// there are no active query callers.
			return
		}
	}
}

// registerUploadCaller returns a new channel to the caller to read responses from.
//
// Only requests associated with the returned tag are sent on the returned channel.
//
// The returned channel is closed when the specified context is done. The caller should
// ensure the context is canceled at the right time to avoid send-on-closed-channel errors
// and avoid deadlocks.
//
// The caller must continue to drain the returned channel until it is closed to avoid deadlocks.
func (u *uploaderv2) registerUploadCaller(ctx context.Context) (string, <-chan UploadResponse) {
	tag := uuid.New()

	// Serialize this block to avoid concurrent map-read-write errors.
	u.uploadCallerMutex.Lock()
	uc, ok := u.uploadCaller[tag]
	if !ok {
		uc = make(chan UploadResponse)
		u.uploadCaller[tag] = uc
	}
	u.uploadCallerMutex.Unlock()

	u.uploadCallerWg.Add(1)
	go func() {
		<-ctx.Done()
		// Serialize this block to avoid concurrent map-read-write and send-on-closed-channel errors.
		u.uploadCallerMutex.Lock()
		delete(u.uploadCaller, tag)
		u.uploadCallerMutex.Unlock()

		close(uc)
		u.uploadCallerWg.Done()
	}()

	return tag, uc
}

func (u *uploaderv2) notifyUploadCallers(r UploadResponse, tags ...string) {
	// Serialize this block to avoid concurrent map-read-write and send-on-closed-channel errors.
	u.uploadCallerMutex.Lock()
	defer u.uploadCallerMutex.Unlock()
	for _, tag := range tags {
		uc, ok := u.uploadCaller[tag]
		if ok {
			// Possible deadlock if the receiver had abandoned the channel.
			uc <- r
		}
	}
}

func (u *uploaderv2) uploadDispatcher(ctx context.Context) {
	// receive blob upload requests
	// generate a tag for it
	// traverse its tree and
	// if small, glob it and dispatch to bundler
	// if medium, dispatch to bundler
	// if large, dispatch to streamer
	// each time a file is dispatched, increment a counter
	// each time a file is processed, decrement the counter
	// once the counter is zero, notify the caller

	// for {
	// 	select {
	// 	case req, ok := <-u.uploadChan:
	// 		if !ok {
	// 			// This should never happen since this channel is never closed.
	// 			return
	// 		}
	//
	// 		walker.DepthFirst()
	// 	case <-ctx.Done():
	// 		return
	// 	}
	// }
}

func (u *uploaderv2) uploadBundler(ctx context.Context) {
	// bundle := make(uploadRequestBundle)
	// var bundleSize int64
	//
	// handle := func() {
	// 	if len(bundle) < 1 {
	// 		return
	// 	}
	// 	// Block the entire processor if the concurrency limit is reached.
	// 	if err := u.uploadSem.Acquire(ctx, 1); err != nil {
	// 		// err is always ctx.Err(), so abort immediately.
	// 		return
	// 	}
	// 	defer u.uploadSem.Release(1)
	//
	// 	// go u.callMissingBlobs(ctx, bundle)
	//
	// 	bundle = make(uploadRequestBundle)
	// 	bundleSize = 0
	// }
	//
	// bundleTicker := time.NewTicker(u.uploadRpcConfig.BundleTimeout)
	// defer bundleTicker.Stop()
	//
	// for {
	// 	select {
	// 	case req, ok := <-u.uploadChan:
	// 		if !ok {
	// 			// This should never happen since this channel is never closed.
	// 			return
	// 		}
	//
	// 		// TODO: stream oversized items.
	// 		if bundleSize+d.Size >= int64(u.uploadRpcConfig.BytesLimit) {
	// 			handle()
	// 		}
	//
	// 		// Duplicate tags are allowed to ensure the query caller can match the number of responses to the number of requests.
	// 		bundle[d] = append(bundle[d], req.tag)
	// 		bundleSize += d.Size
	//
	// 		// Check length threshold.
	// 		if len(bundle) >= u.uploadRpcConfig.ItemsLimit {
	// 			handle()
	// 			continue
	// 		}
	// 	case <-bundleTicker.C:
	// 		handle()
	// 	case <-ctx.Done():
	// 		// Nothing to wait for since all the senders and receivers should have terminated as well.
	// 		// The only things that might still be in-flight are the gRPC calls, which will eventually terminate since
	// 		// there are no active query callers.
	// 		return
	// 	}
	// }
}

func (u *uploaderv2) uploadStreamer(ctx context.Context) {

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

		queryChan:    make(chan missingBlobRequest),
		uploadChan:   make(chan uploadRequest),
		queryCaller:  make(map[string]queryCaller),
		uploadCaller: make(map[string]uploadCaller),
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
