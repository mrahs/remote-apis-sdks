// This file includes the streaming implementation.
// The overall streaming flow is as follows:
//   digester        -> dispatcher/blob
//   dispatcher/blob -> dispatcher/pipe
//   dispatcher/pipe -> dispatcher/res
//   dispatcher/res  -> requester (cache hit)
//   dispatcher/pipe -> batcher (small)
//   dispatcher/pipe -> streamer (medium and large)
//   batcher         -> dispatcher/res
//   streamer        -> dispatcher/res
//   dispatcher/res  -> requester
//
// The termination sequence is as follows:
//   user closes input channels (streaming uploader).
//   user waits for termination signal (return from batching uploader or channel closed from streaming uploader).
//   user cancels uploader's context: cancel in-flight digestions and gRPC processors blocked on semaphores.
//   client senders (top level) terminate.
//   the digester channel is closed, and a termination signal is sent to the dispatcher.
//   the dispatcher terminates its sender and propagates the signal to its piper.
//   the dispatcher's piper propagtes the signal to the intermediate query streamer.
//   the intermediate query streamer terimnates and propagates the signal to the query processor and dispatcher's piper.
//   the query processor terminates.
//   the dispatcher's piper terminates.
//   the dispatcher's counter termiantes (after observing all the remaining blobs) and propagates the signal to the receiver.
//   the dispatcher's receiver terminates.
//   the dispatcher terminates and propagates the signal to the batcher and the streamer.
//   the batcher and the streamer terminate.

package casng

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"google.golang.org/grpc/status"
)

// UploadRequest represents a path to start uploading from.
//
// If the path is a directory, its entire tree is traversed and only files that are not excluded by the filter are uploaded.
// Symlinks are handled according to the SymlinkOptions field.
type UploadRequest struct {
	// Digest is for pre-digested requests.
	// If set, it implies that this request is a single blob. I.e. either Bytes is set or Path is a regular file and
	// both SymlinkOptions and Exclude are ignored.
	Digest digest.Digest

	// Bytes takes precedence over Path. It is meant for small blobs. Using a large slice of bytes might slow things down.
	Bytes []byte

	// Path is used to access and read files. It is ignored if Bytes is set.
	Path impath.Absolute

	// PathRemote is used when uploading files. It must be set to be identical to Path to avoid using the zero value unintentionally. It is ignored if Path is ignored.
	PathRemote impath.Absolute

	// SymlinkOptions is used when Path is set. It is ignored if Path is ignored.
	SymlinkOptions slo.Options

	// Exclude is used when Path is set. It is ignored if Path is ignored.
	Exclude walker.Filter

	// ctx is used to unify metadata in the streaming uploader when making remote calls.
	ctx context.Context
	// tag is used internally to identify the client of the request in the streaming uploader.
	tag tag
	// done is used internally to signal to the processor that the client will not be sending any further requests.
	// This allows the processor to notify the client once all buffered requests are processed.
	// Once a tag is associated with done=true, sending subsequent requests for that tag might cause races.
	done bool
}

// UploadResponse represents an upload result for a single blob (which may represent a tree of files).
type UploadResponse struct {
	// Digest identifies the blob associated with this response.
	// May be empty (created from an empty byte slice or from a composite literal), in which case Err is set.
	Digest digest.Digest

	// Stats may be zero if this response has not been updated yet. It should be ignored if Err is set.
	// If this response has been processed, then either CacheHitCount or CacheHitMiss is not zero.
	Stats Stats

	// Err indicates the error encountered while processing the request associated with Digest.
	// If set, Stats should be ignored.
	Err error

	// tags is used internally to identify the clients that are interested in this response.
	tags []tag
	// done is used internally to signal that this is the last response for the associated clients.
	done bool
}

// uploadRequestBundleItem is a tuple of an upload request and a list of clients interested in the response.
type uploadRequestBundleItem struct {
	req  *repb.BatchUpdateBlobsRequest_Request
	tags []tag
}

// uploadRequestBundle is used to aggregate (unify) requests by digest.
type uploadRequestBundle = map[digest.Digest]uploadRequestBundleItem

// blob is a tuple of (digest, content, client_id, client_ctx, done_signal, queried_flag).
// The digest is the blob's unique identifier.
// The content must be one of reader, path, or bytes, in that order. Depending on which field is set, resources are acquired and released.
type blob struct {
	digest digest.Digest
	bytes  []byte
	path   string
	reader io.ReadSeekCloser
	// ctx is client's context.
	ctx context.Context
	// tag identifies the client of this blob.
	tag tag
	// done is used internally to signal to the dispatcher that no further blobs are expected for the associated tag.
	done bool
}

// tagCount is a tuple used by the dispatcher to track the number of in-flight blobs for each client.
// A blob is in-flight if it has been dispatched, but no corresponding response has been received for it yet.
type tagCount struct {
	t tag
	c int
}

// Upload is a non-blocking call that uploads incoming files to the CAS if necessary.
//
// The caller must close in as a termination signal. Cancelling ctx or the uploader's context is not enough.
// The uploader's context is used to make remote calls. It will carry any metadata present in ctx.
// Metadata unification assumes all requests share the same correlated invocation ID.
//
// The consumption speed is subject to the concurrency and timeout configurations of the gRPC call.
// All received requests will have corresponding responses sent on the returned channel.
//
// Requests are unified across a window of time defined by the BundleTimeout value of the gRPC configuration.
// The unification is affected by the order of the requests, bundle limits (length, size, timeout) and the upload speed.
// With infinite speed and limits, every blob will be uploaded exactly once. On the other extreme, every blob is uploaded
// alone and no unification takes place.
// In the average case, blobs that make it into the same bundle will be grouped by digest. Once a digest is processed, each requester of that
// digest receives a copy of the coorresponding UploadResponse.
//
// This method must not be called after cancelling the uploader's context.
func (u *StreamingUploader) Upload(ctx context.Context, in <-chan UploadRequest) <-chan UploadResponse {
	return u.streamPipe(ctx, in)
}

func (u *uploader) streamPipe(ctx context.Context, in <-chan UploadRequest) <-chan UploadResponse {
	ch := make(chan UploadResponse)

	// If this was called after the the uploader was terminated, short the circuit and return.
	select {
	case <-u.ctx.Done():
		go func() {
			defer close(ch)
			r := UploadResponse{Err: ErrTerminatedUploader}
			for range in {
				ch <- r
			}
		}()
		return ch
	default:
	}

	// Register a new requester with the internal processor.
	// This borker should not remove the subscription until the sender tells it to, hence, the background context.
	// The broker uses the context for cancellation only. It's not propagated further.
	ctxSub, ctxSubCancel := context.WithCancel(context.Background())
	tag, resChan := u.uploadPubSub.sub(ctxSub)

	// Forward the requests to the internal processor.
	u.uploadSenderWg.Add(1)
	go func() {
		log.V(1).Info("[casng] upload.stream_pipe.sender.start")
		defer log.V(1).Info("[casng] upload.stream_pipe.sender.stop")
		defer u.uploadSenderWg.Done()
		for r := range in {
			r.tag = tag
			r.ctx = ctx
			u.digesterCh <- r
		}
		// Let the processor know that no further requests are expected.
		u.digesterCh <- UploadRequest{tag: tag, done: true}
	}()

	// Receive responses from the internal processor.
	// Once the sender above sends a done-tagged request, the processor will send a done-tagged response.
	u.receiverWg.Add(1)
	go func() {
		log.V(1).Info("[casng] upload.stream_pipe.receiver.start")
		defer log.V(1).Info("[casng] upload.stream_pipe.receiver.stop")
		defer u.receiverWg.Done()
		defer close(ch)
		for rawR := range resChan {
			r := rawR.(UploadResponse)
			if r.done {
				ctxSubCancel() // let the broker terminate the subscription.
				continue
			}
			ch <- r
		}
	}()

	return ch
}

// uploadBatcher handles files below the small threshold which are buffered in-memory.
func (u *uploader) batcher() {
	log.V(1).Info("[casng] upload.batch.start")
	defer log.V(1).Info("[casng] upload.batch.stop")

	bundle := make(uploadRequestBundle)
	bundleSize := u.uploadRequestBaseSize
	ctx := u.ctx // context with unified metadata.

	handle := func() {
		if len(bundle) < 1 {
			return
		}
		// Block the bundler if the concurrency limit is reached.
		startTime := time.Now()
		if err := u.uploadSem.Acquire(u.ctx, 1); err != nil {
			// err is always ctx.Err(), so abort immediately.
			return
		}
		log.V(2).Infof("[casng] upload.batch.sem: duration=%v", time.Since(startTime))
		defer u.uploadSem.Release(1)

		u.workerWg.Add(1)
		go func(ctx context.Context, b uploadRequestBundle) {
			defer u.workerWg.Done()
			u.callBatchUpload(ctx, b)
		}(ctx, bundle)

		bundle = make(uploadRequestBundle)
		bundleSize = u.uploadRequestBaseSize
		ctx = u.ctx
	}

	bundleTicker := time.NewTicker(u.batchRpcCfg.BundleTimeout)
	defer bundleTicker.Stop()
	for {
		select {
		// The dispatcher guarantees that the dispatched blob is not oversized.
		case b, ok := <-u.batcherCh:
			if !ok {
				return
			}
			log.V(2).Infof("[casng] upload.batch.req: digest=%s, tag=%s", b.digest, b.tag)

			// Unify.
			item, ok := bundle[b.digest]
			if ok {
				// Duplicate tags are allowed to ensure the requester can match the number of responses to the number of requests.
				item.tags = append(item.tags, b.tag)
				bundle[b.digest] = item
				log.V(2).Infof("[casng] upload.batch.unified: digest=%s, len=%d", b.digest, len(item.tags))
				continue
			}

			// It's possible for files to be considered medium and large, but still fit into a batch request.
			// Load the bytes without blocking the batcher by deferring the blob.
			if len(b.bytes) == 0 {
				log.V(2).Infof("[casng] upload.batch.file: digest=%s, path=%s", b.digest, b.path)
				u.workerWg.Add(1)
				go func(b blob) (err error) {
					defer u.workerWg.Done()
					defer func() {
						if err != nil {
							u.dispatcherResCh <- UploadResponse{
								Digest: b.digest,
								Err:    err,
								tags:   []tag{b.tag},
							}
						}
						// If this blob was from a large file, ensure IO holds are released.
						if b.reader != nil {
							u.ioSem.Release(1)
							u.ioLargeSem.Release(1)
						}
					}()
					r := b.reader
					if r == nil {
						startTime := time.Now()
						if err := u.ioSem.Acquire(b.ctx, 1); err != nil {
							return err
						}
						log.V(2).Infof("[casng] upload.batch.file.io_sem: duration=%v", time.Since(startTime))
						defer u.ioSem.Release(1)
						f, err := os.Open(b.path)
						if err != nil {
							return errors.Join(ErrIO, err)
						}
						r = f
					}
					defer func() {
						if errClose := r.Close(); err != nil {
							err = errors.Join(ErrIO, errClose, err)
						}
					}()
					bytes, err := io.ReadAll(r)
					if err != nil {
						return errors.Join(ErrIO, err)
					}
					b.bytes = bytes
					u.batcherCh <- b
					return nil
				}(b)
				continue
			}

			// If the blob doesn't fit in the current bundle, cycle it.
			rSize := u.uploadRequestItemBaseSize + len(b.bytes)
			if bundleSize+rSize >= u.batchRpcCfg.BytesLimit {
				handle()
			}

			r := &repb.BatchUpdateBlobsRequest_Request{
				Digest: b.digest.ToProto(),
				Data:   b.bytes, // TODO: add compression support as in https://github.com/bazelbuild/remote-apis-sdks/pull/443/files
			}
			item.tags = append(item.tags, b.tag)
			item.req = r
			bundle[b.digest] = item
			bundleSize += rSize
			ctx, _ = contextmd.FromContexts(ctx, b.ctx) // ignore non-essential error.

			// If the bundle is full, cycle it.
			if len(bundle) >= u.batchRpcCfg.ItemsLimit {
				handle()
				continue
			}

		case <-bundleTicker.C:
			handle()
		}
	}
}

func (u *uploader) callBatchUpload(ctx context.Context, bundle uploadRequestBundle) {
	log.V(2).Infof("[casng] upload.batch.call: len=%d", len(bundle))

	req := &repb.BatchUpdateBlobsRequest{InstanceName: u.instanceName}
	req.Requests = make([]*repb.BatchUpdateBlobsRequest_Request, 0, len(bundle))
	for _, item := range bundle {
		req.Requests = append(req.Requests, item.req)
	}

	var uploaded []digest.Digest
	failed := make(map[digest.Digest]error)
	digestRetryCount := make(map[digest.Digest]int64)
	ctxGrpc, ctxGrpcCancel := context.WithCancel(ctx)
	err := u.withTimeout(u.queryRpcCfg.Timeout, ctxGrpcCancel, func() error {
		return u.withRetry(ctxGrpc, u.batchRpcCfg.RetryPredicate, u.batchRpcCfg.RetryPolicy, func() error {
			// This call can have partial failures. Only retry retryable failed requests.
			res, errCall := u.cas.BatchUpdateBlobs(ctxGrpc, req)
			reqErr := errCall // return this error if nothing is retryable.
			req.Requests = nil
			for _, r := range res.Responses {
				if errItem := status.FromProto(r.Status).Err(); errItem != nil {
					if retry.TransientOnly(errItem) {
						d := digest.NewFromProtoUnvalidated(r.Digest)
						req.Requests = append(req.Requests, bundle[d].req)
						digestRetryCount[d]++
						reqErr = errItem // return any retryable error if there is one.
						continue
					}
					// Permanent error.
					failed[digest.NewFromProtoUnvalidated(r.Digest)] = errItem
					continue
				}
				uploaded = append(uploaded, digest.NewFromProtoUnvalidated(r.Digest))
			}
			if l := len(req.Requests); l > 0 {
				log.V(2).Infof("[casng] upload.batch.call.retry: len=%d", l)
			}
			return reqErr
		})
	})
	log.V(2).Infof("[casng] upload.batch.call.done: uploaded=%d, failed=%d, req_failed=%d", len(uploaded), len(failed), len(bundle)-len(uploaded)-len(failed))

	// Report uploaded.
	for _, d := range uploaded {
		s := Stats{
			BytesRequested:      d.Size,
			LogicalBytesMoved:   d.Size,
			TotalBytesMoved:     d.Size,
			EffectiveBytesMoved: d.Size,
			LogicalBytesBatched: d.Size,
			CacheMissCount:      1,
			BatchedCount:        1,
		}
		if r := digestRetryCount[d]; r > 0 {
			s.TotalBytesMoved = d.Size * (r + 1)
		}
		u.dispatcherResCh <- UploadResponse{
			Digest: d,
			Stats:  s,
			tags:   bundle[d].tags,
		}
		delete(bundle, d)
	}

	// Report individually failed requests.
	for d, dErr := range failed {
		s := Stats{
			BytesRequested:    d.Size,
			LogicalBytesMoved: d.Size,
			TotalBytesMoved:   d.Size,
			CacheMissCount:    1,
			BatchedCount:      1,
		}
		if r := digestRetryCount[d]; r > 0 {
			s.TotalBytesMoved = d.Size * (r + 1)
		}
		u.dispatcherResCh <- UploadResponse{
			Digest: d,
			Stats:  s,
			Err:    errors.Join(ErrGRPC, dErr),
			tags:   bundle[d].tags,
		}
		delete(bundle, d)
	}

	if len(bundle) == 0 {
		return
	}

	if err == nil {
		err = fmt.Errorf("server did not return a response for %d requests", len(bundle))
	}
	err = errors.Join(ErrGRPC, err)

	// Report failed requests due to call failure.
	for d, item := range bundle {
		tags := item.tags
		s := Stats{
			BytesRequested:  d.Size,
			TotalBytesMoved: d.Size,
			CacheMissCount:  1,
			BatchedCount:    1,
		}
		if r := digestRetryCount[d]; r > 0 {
			s.TotalBytesMoved = d.Size * (r + 1)
		}
		u.dispatcherResCh <- UploadResponse{
			Digest: d,
			Stats:  s,
			Err:    err,
			tags:   tags,
		}
	}
}

// uploadStreamer handles files above the small threshold.
// Unlike the batched call, querying the CAS is not required because the API handles this automatically.
// See https://github.com/bazelbuild/remote-apis/blob/0cd22f7b466ced15d7803e8845d08d3e8d2c51bc/build/bazel/remote/execution/v2/remote_execution.proto#L250-L254
// For files above the large threshold, this method assumes the io and large io holds are already acquired and will release them accordingly.
// For other files, only an io hold is acquired and released in this method.
func (u *uploader) streamer() {
	log.V(1).Info("[casng] upload.stream.start")
	defer log.V(1).Info("[casng] upload.stream.stop")

	// Unify duplicate requests.
	digestTags := make(map[digest.Digest][]tag)
	streamResCh := make(chan UploadResponse)
	pending := 0
	for {
		select {
		// The dispatcher closes this channel when it's done dispatching, which happens after the streamer
		// had sent all pending responses.
		case b, ok := <-u.streamerCh:
			if !ok {
				return
			}
			log.V(2).Infof("[casng] upload.stream.req: digest=%s, tag=%s", b.digest, b.tag)

			isLargeFile := b.reader != nil

			tags := digestTags[b.digest]
			tags = append(tags, b.tag)
			digestTags[b.digest] = tags
			if len(tags) > 1 {
				// Already in-flight. Release duplicate resources if it's a large file.
				log.V(2).Infof("[casng] upload.stream.unified: digest=%s, tag=%s", b.digest, b.tag)
				if isLargeFile {
					u.ioSem.Release(1)
					u.ioLargeSem.Release(1)
				}
				continue
			}

			// Block the streamer if the gRPC call is being throttled.
			startTime := time.Now()
			if err := u.streamSem.Acquire(u.ctx, 1); err != nil {
				// err is always ctx.Err()
				if isLargeFile {
					u.ioSem.Release(1)
					u.ioLargeSem.Release(1)
				}
				return
			}
			log.V(2).Infof("[casng] upload.stream.sem: duration=%v", time.Since(startTime))

			var name string
			if b.digest.Size >= u.ioCfg.CompressionSizeThreshold {
				log.V(2).Infof("[casng] upload.stream.compress: digest=%s, tag=%s", b.digest, b.tag)
				name = MakeCompressedWriteResourceName(u.instanceName, b.digest.Hash, b.digest.Size)
			} else {
				name = MakeWriteResourceName(u.instanceName, b.digest.Hash, b.digest.Size)
			}

			pending += 1
			u.workerWg.Add(1)
			go func() {
				defer u.workerWg.Done()
				defer u.streamSem.Release(1)
				s, err := u.callStream(b.ctx, name, b)
				streamResCh <- UploadResponse{Digest: b.digest, Stats: s, Err: err}
			}()
			log.V(2).Infof("[casng] upload.stream.req: pending=%d", pending)

		case r := <-streamResCh:
			r.tags = digestTags[r.Digest]
			delete(digestTags, r.Digest)
			u.dispatcherResCh <- r
			pending -= 1
			log.V(2).Infof("[casng] upload.stream.res: pending=%d", pending)
		}
	}
}

func (u *uploader) callStream(ctx context.Context, name string, b blob) (stats Stats, err error) {
	log.V(2).Infof("[casng] upload.stream.call: digest=%s, tag=%s", b.digest, b.tag)
	defer log.V(2).Infof("[casng] upload.stream.call.done: digest=%s, tag=%s, err=%v", b.digest, b.tag, err)

	var reader io.Reader

	// In the off chance that the blob is mis-constructed (more than one content field is set), start
	// with b.reader to ensure any held locks are released.
	switch {
	// Large file.
	case b.reader != nil:
		reader = b.reader
		defer func() {
			// IO holds were acquired during digestion for large files and are expected to be released here.
			u.ioSem.Release(1)
			u.ioLargeSem.Release(1)
			if errClose := b.reader.Close(); err != nil {
				err = errors.Join(ErrIO, errClose, err)
			}
		}()

	// Medium file.
	case len(b.path) > 0:
		startTime := time.Now()
		if errSem := u.ioSem.Acquire(ctx, 1); errSem != nil {
			return
		}
		log.V(2).Infof("[casng] upload.stream.io_sem: duration=%v", time.Since(startTime))
		defer u.ioSem.Release(1)

		f, errOpen := os.Open(b.path)
		if errOpen != nil {
			return Stats{BytesRequested: b.digest.Size}, errors.Join(ErrIO, errOpen)
		}
		defer func() {
			if errClose := f.Close(); errClose != nil {
				err = errors.Join(ErrIO, errClose, err)
			}
		}()
		reader = f

	// Small file, a proto message (node), or an empty file.
	default:
		reader = bytes.NewReader(b.bytes)
	}

	return u.writeBytes(ctx, name, reader, b.digest.Size, 0, true)
}
