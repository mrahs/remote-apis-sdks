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
//   user cancels the batching or the streaming context, not the uploader's context, and closes input streaming channels.
//       cancelling the context triggers aborting in-flight requests.
//   user cancels uploader's context: cancels pending digestions and gRPC processors blocked on throttlers.
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
//   user waits for the termination signal: return from batching uploader or response channel closed from streaming uploader.
//       this ensures the whole pipeline is drained properly.

package casng

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	"google.golang.org/grpc/status"

	// "github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	// "google.golang.org/grpc/status"
)

// UploadRequest represents a path to start uploading from.
//
// If the path is a directory, its entire tree is traversed and only files that are not excluded by the filter are uploaded.
// Symlinks are handled according to the SymlinkOptions field.
type UploadRequest struct {
	// Digest is for pre-digested requests. This digest is trusted to be the one for the associated Bytes or Path.
	//
	// If not set, it will be calculated.
	// If set, it implies that this request is a single blob. I.e. either Bytes is set or Path is a regular file and both SymlinkOptions and Exclude are ignored.
	Digest digest.Digest

	// Bytes is meant for small blobs. Using a large slice of bytes might cause memory thrashing.
	//
	// If Bytes is nil, BytesFileMode is ignored and Path is used for traversal.
	// If Bytes is not nil (may be empty), Path is used to refer to the bytes content and is not used for traversal.
	Bytes []byte

	// BytesFileMode describes the bytes content. It is ignored if Bytes is not set.
	BytesFileMode fs.FileMode

	// Path is used to access and read files if Bytes is nil. Otherwise, Bytes is assumed to be the paths content (even if empty).
	//
	// This must not be equal to impath.Root since this is considered a zero value (Path not set).
	// If Bytes is not nil and Path is not set, a node cannot be constructed and therefore no node is cached.
	Path impath.Absolute

	// SymlinkOptions are used to handle symlinks when Path is set and Bytes is not.
	SymlinkOptions slo.Options

	// Exclude is used to exclude paths during traversal when Path is set and Bytes is not.
	//
	// The filter ID is used in the keys of the node cache, even when Bytes is set.
	// Using the same ID for effectively different filters will cause erroneous cache hits.
	// Using a different ID for effectively identical filters will reduce cache hit rates and increase digestion compute cost.
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

// blob is a tuple of bytes, their owner, and signaling flags.
// The digest is the blob's unique identifier.
type blob struct {
	// The zero value (not to be confused with Digest.IsEmpty) is used a signal between the digster and the dispatcher.
	digest digest.Digest
	// One of bytes, path or reader may be set. If more than one is set, the precedence is reader, path, bytes.
	bytes  []byte
	path   string
	reader io.ReadSeekCloser
	// ctx is client's context which is used to extract metadata from and abort in-flight tasks for this blob.
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
// To properly stop this call, close in and cancel ctx, then wait for the returned channel to close.
// The channel in must be closed as a termination signal. Cancelling ctx is not enough.
// The uploader's context is used to make remote calls using metadata from ctx.
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
		contextmd.Infof(ctx, log.Level(1), "[casng] upload.stream_pipe.sender.start: tag=%s", tag)
		defer contextmd.Infof(ctx, log.Level(1), "[casng] upload.stream_pipe.sender.stop: tag=%s", tag)
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
		contextmd.Infof(ctx, log.Level(1), "[casng] upload.stream_pipe.receiver.start: tag=%s", tag)
		defer contextmd.Infof(ctx, log.Level(1), "[casng] upload.stream_pipe.receiver.stop: tag=%s", tag)
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
	log.V(1).Info("[casng] upload.batcher.start")
	defer log.V(1).Info("[casng] uploader.batcher.stop")

	bundle := make(uploadRequestBundle)
	bundleSize := u.uploadRequestBaseSize
	ctx := u.ctx // context with unified metadata.

	handle := func() {
		if len(bundle) < 1 {
			return
		}
		// Block the bundler if the concurrency limit is reached.
		startTime := time.Now()
		if !u.uploadThrottler.acquire(u.ctx) {
			// Ensure responses are dispatched before aborting.
			for d, item := range bundle {
				tags := item.tags
				u.dispatcherResCh <- UploadResponse{
					Digest: d,
					Stats:  Stats{BytesRequested: d.Size},
					Err:    context.Canceled,
					tags:   tags,
				}
			}
			return
		}
		log.V(3).Infof("[casng] upload.batcher.throttle.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())

		u.workerWg.Add(1)
		go func(ctx context.Context, b uploadRequestBundle) {
			defer u.uploadThrottler.release()
			defer u.workerWg.Done()
			u.callBatchUpload(ctx, b)
		}(ctx, bundle)

		bundle = make(uploadRequestBundle)
		bundleSize = u.uploadRequestBaseSize
		ctx = u.ctx
	}

	bundleTicker := time.NewTicker(u.batchRPCCfg.BundleTimeout)
	defer bundleTicker.Stop()
	for {
		select {
		// The dispatcher guarantees that the dispatched blob is not oversized.
		case b, ok := <-u.batcherCh:
			if !ok {
				return
			}
			log.V(3).Infof("[casng] upload.batcher.req: digest=%s, tag=%s", b.digest, b.tag)

			// Unify.
			item, ok := bundle[b.digest]
			if ok {
				// Duplicate tags are allowed to ensure the requester can match the number of responses to the number of requests.
				item.tags = append(item.tags, b.tag)
				bundle[b.digest] = item
				log.V(3).Infof("[casng] upload.batcher.unified: digest=%s, bundle=%d", b.digest, len(item.tags))
				continue
			}

			// It's possible for files to be considered medium and large, but still fit into a batch request.
			// Load the bytes without blocking the batcher by deferring the blob.
			if len(b.bytes) == 0 {
				log.V(3).Infof("[casng] upload.batcher.file: digest=%s, path=%s, tag=%s", b.digest, b.path, b.tag)
				u.workerWg.Add(1)
				go func(b blob) (err error) {
					defer u.workerWg.Done()
					defer func() {
						// If this blob was from a large file, ensure IO holds are released.
						if b.reader != nil {
							u.ioThrottler.release()
							u.ioLargeThrottler.release()
						}
						if err != nil {
							u.dispatcherResCh <- UploadResponse{
								Digest: b.digest,
								Err:    err,
								tags:   []tag{b.tag},
							}
							return
						}
						// Send after releasing resources.
						u.batcherCh <- b
					}()
					r := b.reader
					if r == nil {
						startTime := time.Now()
						if !u.ioThrottler.acquire(b.ctx) {
							return b.ctx.Err()
						}
						log.V(3).Infof("[casng] upload.batcher.io_throttle.duration: start=%d, end=%d, tag=%s", startTime.UnixNano(), time.Now().UnixNano(), b.tag)
						defer u.ioThrottler.release()
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
					return nil
				}(b)
				continue
			}

			// If the blob doesn't fit in the current bundle, cycle it.
			rSize := u.uploadRequestItemBaseSize + len(b.bytes)
			if bundleSize+rSize >= u.batchRPCCfg.BytesLimit {
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
			if len(bundle) >= u.batchRPCCfg.ItemsLimit {
				handle()
			}
		case <-bundleTicker.C:
			handle()
		}
	}
}

func (u *uploader) callBatchUpload(ctx context.Context, bundle uploadRequestBundle) {
	req := &repb.BatchUpdateBlobsRequest{InstanceName: u.instanceName}
	req.Requests = make([]*repb.BatchUpdateBlobsRequest_Request, 0, len(bundle))
	for _, item := range bundle {
		req.Requests = append(req.Requests, item.req)
	}

	var uploaded []digest.Digest
	failed := make(map[digest.Digest]error)
	digestRetryCount := make(map[digest.Digest]int64)

	startTime := time.Now()
	err := u.withRetry(ctx, u.batchRPCCfg.RetryPredicate, u.batchRPCCfg.RetryPolicy, func() error {
		// This call can have partial failures. Only retry retryable failed requests.
		ctx, ctxCancel := context.WithTimeout(ctx, u.batchRPCCfg.Timeout)
		defer ctxCancel()
		res, errCall := u.cas.BatchUpdateBlobs(ctx, req)
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
			log.V(3).Infof("[casng] upload.batcher.call.retry: len=%d", l)
		}
		return reqErr
	})
	log.V(3).Infof("[casng] upload.batcher.grpc.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
	log.V(3).Infof("[casng] upload.batcher.call.result: uploaded=%d, failed=%d, req_failed=%d", len(uploaded), len(failed), len(bundle)-len(uploaded)-len(failed))

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
		log.V(3).Infof("[casng] upload.batcher.pub.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
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
	log.V(3).Infof("[casng] upload.batcher.pub.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
}

// uploadStreamer handles files above the small threshold.
// Unlike the batched call, querying the CAS is not required because the API handles this automatically.
// See https://github.com/bazelbuild/remote-apis/blob/0cd22f7b466ced15d7803e8845d08d3e8d2c51bc/build/bazel/remote/execution/v2/remote_execution.proto#L250-L254
// For files above the large threshold, this method assumes the io and large io holds are already acquired and will release them accordingly.
// For other files, only an io hold is acquired and released in this method.
func (u *uploader) streamer() {
	log.V(1).Info("[casng] upload.streamer.start")
	defer log.V(1).Info("[casng] upload.streamer.stop")

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
			log.V(3).Infof("[casng] upload.streamer.req: digest=%s, tag=%s", b.digest, b.tag)

			isLargeFile := b.reader != nil

			tags := digestTags[b.digest]
			tags = append(tags, b.tag)
			digestTags[b.digest] = tags
			if len(tags) > 1 {
				// Already in-flight. Release duplicate resources if it's a large file.
				log.V(3).Infof("[casng] upload.streamer.unified: digest=%s, tag=%s, bundle=%d", b.digest, b.tag, len(tags))
				if isLargeFile {
					u.ioThrottler.release()
					u.ioLargeThrottler.release()
				}
				continue
			}

			// Block the streamer if the gRPC call is being throttled.
			startTime := time.Now()
			if !u.streamThrottle.acquire(u.ctx) {
				if isLargeFile {
					u.ioThrottler.release()
					u.ioLargeThrottler.release()
				}
				// Ensure the response is dispatched before aborting.
				u.workerWg.Add(1)
				go func(b blob) {
					defer u.workerWg.Done()
					streamResCh <- UploadResponse{Digest: b.digest, Stats: Stats{BytesRequested: b.digest.Size}, Err: context.Canceled, tags: []tag{b.tag}}
				}(b)
				continue
			}
			log.V(3).Infof("[casng] upload.streamer.throttle.duration: start=%d, end=%d, tag=%s", startTime.UnixNano(), time.Now().UnixNano(), b.tag)

			var name string
			if b.digest.Size >= u.ioCfg.CompressionSizeThreshold {
				log.V(3).Infof("[casng] upload.streamer.compress: digest=%s, tag=%s", b.digest, b.tag)
				name = MakeCompressedWriteResourceName(u.instanceName, b.digest.Hash, b.digest.Size)
			} else {
				name = MakeWriteResourceName(u.instanceName, b.digest.Hash, b.digest.Size)
			}

			pending += 1
			u.workerWg.Add(1)
			go func(b blob) {
				defer u.workerWg.Done()
				defer u.streamThrottle.release()
				s, err := u.callStream(b.ctx, name, b)
				streamResCh <- UploadResponse{Digest: b.digest, Stats: s, Err: err, tags: []tag{b.tag}}
			}(b)
			log.V(3).Infof("[casng] upload.streamer.req: pending=%d", pending)
		case r := <-streamResCh:
			startTime := time.Now()
			r.tags = digestTags[r.Digest]
			delete(digestTags, r.Digest)
			u.dispatcherResCh <- r
			pending -= 1
			log.V(3).Infof("[casng] upload.streamer.res: pending=%d", pending)
			// Covers waiting on the dispatcher.
			log.V(3).Infof("[casng] upload.streamer.pub.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
		}
	}
}

func (u *uploader) callStream(ctx context.Context, name string, b blob) (stats Stats, err error) {
	var reader io.Reader

	// In the off chance that the blob is mis-constructed (more than one content field is set), start
	// with b.reader to ensure any held locks are released.
	switch {
	// Large file.
	case b.reader != nil:
		reader = b.reader
		defer func() {
			// IO holds were acquired during digestion for large files and are expected to be released here.
			u.ioThrottler.release()
			u.ioLargeThrottler.release()
			if errClose := b.reader.Close(); err != nil {
				err = errors.Join(ErrIO, errClose, err)
			}
		}()

	// Medium file.
	case len(b.path) > 0:
		startTime := time.Now()
		if !u.ioThrottler.acquire(ctx) {
			return
		}
		log.V(3).Infof("[casng] upload.streamer.io_throttle.duration: start=%d, end=%d, tag=%s", startTime.UnixNano(), time.Now().UnixNano(), b.tag)
		defer u.ioThrottler.release()

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
