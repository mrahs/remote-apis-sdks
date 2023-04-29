// This file includes the implementation for uploading blobs to the CAS.
// A note about logging:
//
//	Level 1 is used for top-level functions, typically called once during the lifetime of the process or initiated by the user.
//	Level 2 is used for internal functions that may be called per request.
//	Level 3 is used for messages that contain large arguments.
package cas

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/glog"
	"github.com/klauspost/compress/zstd"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	errEarly = errors.New("early cancellation")
)

// UploadRequest represents a path to start uploading from.
//
// If the path is a directory, its entire tree is traversed and only files that are not excluded
// by the filter are uploaded.
// Symlinks are handled according to the specified options.
type UploadRequest struct {
	Path           impath.Absolute
	SymlinkOptions slo.Options
	Exclude        walker.Filter
	// tag is used internally to identify the client of the request.
	tag tag
	// done is used to signal to the processor that the client will not be sending any further requests.
	// This allows the processor to notify the client once all buffered requests are processed.
	// Sending more requests after sending one with done=true will cause races.
	done bool
}

// UploadResponse represents an upload result for a single blob (which may represent a tree of files).
type UploadResponse struct {
	Digest digest.Digest
	Stats  Stats
	Err    error // Also used internally for control signals
	// tags is used internally to identify the clients that are interested in this response.
	tags []tag
	// done is used internally to signal to the consumer that the producer has completed all buffered requests.
	done bool
}

// uploadRequestBundleItem is used to unify requests by grouping them by digest while aggregating the clients.
type uploadRequestBundleItem struct {
	req  *repb.BatchUpdateBlobsRequest_Request
	tags []tag
}

// uploadRequestBundle associates an upload request with list of clients that are interested in uploading it.
type uploadRequestBundle = map[digest.Digest]uploadRequestBundleItem

// blob associates a digest with its original bytes.
// Only one of reader, path, or bytes is used, in that order.
// See uploadStreamer implementation below.
type blob struct {
	digest digest.Digest
	bytes  []byte
	path   string
	reader io.ReadSeekCloser
	// tag identifies the client of this blob.
	tag tag
	// done is used internally to signal to the dispatcher that no further blobs are expected for the associated tag.
	done bool
}

// WriteBytes uploads all the bytes (until EOF) of the specified reader directly to the specified resource name starting remotely at the specified offset.
//
// The specified context is used to make the remote calls.
// This method does not use the uploader's context which means it is safe to call after calling Wait.
//
// The specified size is used to toggle compression as well as report some stats. It must be reflect the actual number of bytes the specified reader has to give.
// The server is notified to finalize the resource name and further writes may not succeed.
// The errors returned are either from the context, ErrGRPC, ErrIO, or ErrCompression. More errors may be wrapped inside.
// In case of error while the returned stats indicate that all the bytes were sent, it is still not a guarantee all the bytes
// were received by the server since an acknlowedgement was not observed.
//
// This method must not be called after calling Wait.
func (u *BatchingUploader) WriteBytes(ctx context.Context, name string, r io.Reader, size int64, offset int64) (*Stats, error) {
	return u.writeBytes(ctx, name, r, size, offset, true)
}

// WriteBytesPartial is the same as WriteBytes, but does not notify the server to finalize the resource name.
// This method must not be called after calling Wait.
func (u *BatchingUploader) WriteBytesPartial(ctx context.Context, name string, r io.Reader, size int64, offset int64) (*Stats, error) {
	return u.writeBytes(ctx, name, r, size, offset, false)
}

func (u *uploaderv2) writeBytes(ctx context.Context, name string, r io.Reader, size int64, offset int64, finish bool) (*Stats, error) {
	glog.V(2).Infof("upload.write_bytes.start: name=%s, size=%d, offset=%d, finish=%t", name, size, offset, finish)
	defer glog.V(2).Infof("upload.write_bytes.done: name=%s, size=%d, offset=%d, finish=%t", name, size, offset, finish)

	if err := u.streamSem.Acquire(ctx, 1); err != nil {
		// err is always ctx.Err(), so abort immediately.
		return nil, err
	}
	defer u.streamSem.Release(1)

	// Read raw bytes if compression is disabled.
	src := r

	// If compression is enabled, plug in the encoder via a pipe.
	var errEnc error
	var nRawBytes int64
	var encWg sync.WaitGroup
	var withCompression bool
	if size >= u.ioCfg.CompressionSizeThreshold {
		glog.V(2).Infof("upload.write_bytes.compression: name=%s, size=%d", name, size)
		withCompression = true
		pr, pw := io.Pipe()
		// Closing pr always returns a nil error, but also sends ErrClosedPipe to pw.
		defer pr.Close()
		src = pr // Read compressed bytes instead of raw bytes.

		enc := u.zstdEncoders.Get().(*zstd.Encoder)
		defer u.zstdEncoders.Put(enc)
		// (Re)initialize the encoder with this writer.
		enc.Reset(pw)
		// Get it going.
		encWg.Add(1)
		go func() {
			defer encWg.Done()
			// Closing pw always returns a nil error, but also sends an EOF to pr.
			defer pw.Close()

			// The encoder will theoretically read continuously. However, pw will block it
			// while pr is not reading from the other side.
			// In other words, the chunk size of the encoder's output is controlled by the reader.
			nRawBytes, errEnc = enc.ReadFrom(r)
			// Closing the encoder is necessary to flush remaining bytes.
			errEnc = errors.Join(enc.Close(), errEnc)
			if errors.Is(errEnc, io.ErrClosedPipe) {
				// pr was closed first, which means the actual error is on that end.
				errEnc = nil
			}
		}()
	}

	ctx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()

	stream, errStream := u.byteStream.Write(ctx)
	if errStream != nil {
		return nil, errors.Join(ErrGRPC, errStream)
	}

	buf := u.buffers.Get().([]byte)
	defer u.buffers.Put(buf)

	stats := &Stats{}
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
			return u.withRetry(ctx, u.streamRpcConfig.RetryPolicy, func() error {
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

	// In case of a cache hit or an error, the pipe must be closed to terminate the encoder's goroutine
	// which would have otherwise terminated after draining the reader.
	if srcCloser, ok := src.(io.Closer); ok && withCompression {
		if errClose := srcCloser.Close(); errClose != nil {
			err = errors.Join(ErrIO, errClose, err)
		}
	}

	// This theoretically will block until the encoder's goroutine returns.
	// However, closing the reader eventually terminates that goroutine.
	// This is necessary because the encoder's goroutine currently owns errEnc and nRawBytes.
	encWg.Wait()
	if errEnc != nil {
		err = errors.Join(ErrCompression, errEnc, err)
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

	// CommittedSize is based on the uncompressed size of the blob.
	if !cacheHit && res.CommittedSize != size {
		err = errors.Join(ErrGRPC, fmt.Errorf("committed size mismatch: got %d, want %d", res.CommittedSize, size), err)
	}

	return stats, err
}

// Upload processes the specified blobs for upload. Blobs that already exist in the CAS are not uploaded.
// Any path or file that matches the specified filter is excluded.
// Additionally, any path that is not a symlink, a directory or a regular file is skipped (e.g. sockets and pipes).
//
// This method does not accept a context because the requests are unified across concurrent calls of it.
// The context that was used to initialize the uploader is used to make remote calls.
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
//
// This method must not be called after calling Wait.
func (u *BatchingUploader) Upload(reqs ...UploadRequest) ([]digest.Digest, *Stats, error) {
	glog.V(1).Infof("upload: %d requests", len(reqs))
	defer glog.V(1).Infof("upload.done")

	if len(reqs) == 0 {
		return nil, nil, nil
	}

	ch := make(chan UploadRequest)
	u.senderWg.Add(1)
	go func() {
		glog.V(1).Info("upload.sender.start")
		defer glog.V(1).Info("upload.sender.stop")
		defer close(ch) // ensure the streamer closes its response channel
		defer u.senderWg.Done()
		for _, r := range reqs {
			ch <- r
		}
	}()

	var uploaded []digest.Digest
	var err error
	stats := &Stats{}
	resCh := u.uploadStreamer(ch)
	for r := range resCh {
		if r.Err != nil {
			err = errors.Join(r.Err, err)
		}
		stats.Add(r.Stats)
		if r.Stats.CacheMissCount > 0 {
			uploaded = append(uploaded, r.Digest)
		}
	}

	return uploaded, stats, err
}

// Upload is a non-blocking call that uploads incoming files to the CAS if necessary.
//
// The caller must close the specified input channel as a termination signal.
// This method does not accept a context because the requests are unified across concurrent calls of it.
// The context that was used to initialize the uploader is used to make remote calls.
//
// Requests are unified across a window of time defined by the BundleTimeout value of the gRPC configuration.
// The unification is affected by the order of the requests, bundle limits (length, size, timeout) and the upload speed.
// With infinite speed and limits, every blob will be uploaded exactly once. On the other extreme, every blob is uploaded
// alone and no unification takes place.
// In the average case, blobs that make it into the same bundle will be grouped by digest. Once a digest is processed, each requester of that
// digest receives a copy of the coorresponding UploadResponse.
//
// This method must not be called after calling Wait.
func (u *StreamingUploader) Upload(in <-chan UploadRequest) <-chan UploadResponse {
	return u.uploadStreamer(in)
}

func (u *uploaderv2) uploadStreamer(in<-chan UploadRequest) <-chan UploadResponse {
	ch := make(chan UploadResponse)

	// If this was called after the the uploader was terminated, short the circuit and return.
	select {
	case <-u.ctx.Done():
		go func(){
			defer close(ch)
			r := UploadResponse{Err: ErrTerminatedUploader}
			for range in {
				ch <- r
			}
		}()
		return ch
	default:
	}

	// Register a new caller with the internal processor.
	// This borker should not remove the subscription until the sender tells it to, hence, the background context.
	// The broker uses the context for cancellation only. It's not propagated further.
	ctxSub, ctxSubCancel := context.WithCancel(context.Background())
	tag, resChan := u.uploadPubSub.sub(ctxSub)

	// Forward the requests to the internal processor.
	u.senderWg.Add(1)
	go func() {
		glog.V(1).Info("upload.streamer.sender.start")
		defer glog.V(1).Info("upload.streamer.sender.stop")
		defer u.senderWg.Done()
		for r := range in {
			r.tag = tag
			select {
			case u.uploadCh <- r:
			case <-u.ctx.Done():
				// Continue draining the channel.
				continue
			}
		}
		// Let the processor know that no further requests are expected.
		select {
		case u.uploadCh <- UploadRequest{tag: tag, done: true}:
		case <-u.ctx.Done():
			// The uploader terminated which means the response cannot be delivered which means the receiver cannot terminate.
			// Let the broker terminate the subscription.
			ctxSubCancel()
			return
		}
	}()

	u.receiverWg.Add(1)
	go func() {
		glog.V(1).Info("upload.streamer.receiver.start")
		defer glog.V(1).Info("upload.streamer.receiver.stop")
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

// uploadProcessor reveives upload requests from multiple concurrent callers.
// For each request, a file system walk is started concurrently to digest and dispatch files for uploading.
// The number of concurrent requests is limited to the number of concurrent file system walks.
func (u *uploaderv2) uploadProcessor() {
	// The processor forwards requests to other workers which makes it a top-level broker, but not a top-level sender.
	u.processorWg.Add(1)
	go func() {
		glog.V(1).Info("upload.processor.start")
		defer glog.V(1).Info("upload.processor.stop")
		defer u.processorWg.Done()

		for {
			select {
			case req, ok := <-u.uploadCh:
				if !ok {
					return
				}

				// If the caller will not be sending any further requests, wait for in-flight walks from previous requests
				// then tell the dispatcher to forward the signal once all dispatched blobs are done.
				if req.done {
					glog.V(2).Infof("upload.processor.req.done: tag=%s", req.tag)
					wg := u.callerWalkWg[req.tag]
					if wg == nil {
						glog.Errorf("upload.processor.req: received a done signal but no previous requests for %s", req.tag)
						// Special case: this an early cancellation since the caller did not send any requests yet.
						// The response must be delivered after the blob, which is the case because the dispatcher listens on both
						// channels simlutanuously and the blobs channel is unbufferred.
						u.uploadDispatchCh <- blob{tag: req.tag, done: true}
						u.uploadResCh <- UploadResponse{tags: []tag{req.tag}, done: true}
						continue
					}
					u.workerWg.Add(1)
					go func() {
						glog.V(2).Infof("upload.processor.walk.wait.start", req.tag)
						defer glog.V(2).Infof("upload.processor.walk.wait.done", req.tag)
						defer u.workerWg.Done()
						wg.Wait()
						u.uploadDispatchCh <- blob{tag: req.tag, done: true}
					}()
					continue
				}

				glog.V(2).Infof("upload.processor.req: path=%s, slo=%s, filter=%s, tag=%s", req.Path, req.SymlinkOptions, req.Exclude, req.tag)
				// Wait if too many walks are in-flight.
				if err := u.walkSem.Acquire(u.ctx, 1); err != nil {
					// err is always ctx.Err()
					return
				}
				wg := u.callerWalkWg[req.tag]
				if wg == nil {
					wg = &sync.WaitGroup{}
					u.callerWalkWg[req.tag] = wg
				}
				wg.Add(1)
				u.workerWg.Add(1)
				go func() {
					defer u.workerWg.Done()
					defer wg.Done()
					defer u.walkSem.Release(1)
					u.digestAndDispatch(req)
				}()

			case <-u.ctx.Done():
				return
			}
		}
	}()
}

// digestAndDispatch initiates a file system walk to digest files and dispatch them for uploading.
func (u *uploaderv2) digestAndDispatch(req UploadRequest) {
	glog.V(2).Info("upload.digest.start: root=%s, tag=%s", req.Path, req.tag)
	defer glog.V(2).Info("upload.digest.done: root=%s, tag=%s", req.Path, req.tag)

	dispatch := func(msg any) {
		switch m := msg.(type) {
		// Cache hit or error.
		case UploadResponse:
			m.tags = []tag{req.tag}
			select {
			case u.uploadResCh <- m:
			case <-u.ctx.Done():
			}
		// Cache miss.
		case blob:
			m.tag = req.tag
			select {
			case u.uploadDispatchCh <- m:
			case <-u.ctx.Done():
			}
		}
	}

	stats := Stats{}
	var err error
	errWalk := walker.DepthFirst(req.Path, req.Exclude, func(realPath impath.Absolute, desiredPath impath.Absolute, info fs.FileInfo, errVisit error) (nextStep walker.NextStep) {
		glog.V(2).Infof("upload.digest.visit: first=%t, realPath=%s, desiredPath=%s, err=%v", info == nil, realPath, desiredPath, errVisit)
		select {
		case <-u.ctx.Done():
			glog.V(2).Info("upload.digest.cancel")
			return walker.Cancel
		default:
		}

		if err != nil {
			err = errors.Join(errVisit, err)
			return walker.Cancel
		}

		key := desiredPath.String() + req.Exclude.String()
		parentKey := desiredPath.Dir().String() + req.Exclude.String()

		// Pre-access.
		if info == nil {
			// A cache hit here indicates a cyclic symlink or multiple callers attempting to upload the exact same path with an identical filter.
			// In both cases, deferring is the right call. Once the upload is processed, all uploaders will revisit the path to get the processing result.
			// If the path was not cached before, claim it by makring it as in-flight.
			if rawNode, ok := u.digestCache.LoadOrStore(key, nil); ok {
				// Defer if in-flight.
				if rawNode == nil {
					glog.V(2).Infof("upload.digest.visit.defer: realPath=%s, desiredPath=%s", realPath, desiredPath)
					return walker.Defer
				}

				node, ok := rawNode.(proto.Message)
				if !ok {
					err = errors.Join(ErrBadCacheValueType, fmt.Errorf("expected proto.Message, but got %T", rawNode), err)
					return walker.Cancel
				}
				glog.V(2).Infof("upload.digest.visit.cached: realPath=%s, desiredPath=%s", realPath, desiredPath)
				// Dispatch it to correctly account for a cache hit or upload if the original blob is blocked elsewhere.
				switch m := node.(type) {
				case *repb.FileNode:
					// Dispatching with a file path will deliver the blob to the streaming API.
					// If the original blob is queued in the batching API, this blob could potentially get uploaded twice.
					// However, that would mean the file is small.
					dispatch(blob{digest: digest.NewFromProtoUnvalidated(m.Digest), path: realPath.String()})
				case *repb.DirectoryNode:
					// The blob of the directory node is its proto representation.
					// Generate and dispatch it. If it was uploaded before, it'll be reported as a cache hit.
					// Otherwise, it means the previous attempt to upload it failed and it is going to be retried.
					node, b, errDigest := digestDirectory(realPath, u.dirChildren[key])
					if errDigest != nil {
						err = errors.Join(errDigest, err)
						return walker.Cancel
					}
					dispatch(blob{digest: digest.NewFromProtoUnvalidated(node.Digest), bytes: b})
				case *repb.SymlinkNode:
					// It was already appended as a child to its parent. Nothing to dispatch.
				default:
					err = errors.Join(ErrBadCacheValueType, fmt.Errorf("expected *repb.FileNode, *repb.DirectoryNode, or *repb.SymlinkNode, but got %T", node), err)
					return walker.Cancel
				}
				return walker.Skip
			}

			// Access it.
			return walker.Continue
		}

		defer func() {
			if nextStep == walker.Cancel {
				u.digestCache.Delete(key)
			}
		}()
		stats.DigestCount += 1
		switch {
		case info.Mode()&fs.ModeSymlink == fs.ModeSymlink:
			glog.V(2).Infof("upload.digest.visit.symlink: realPath=%s, desiredPath=%s", realPath, desiredPath)
			stats.InputSymlinkCount += 1
			node, nextStep, errDigest := digestSymlink(req.Path, realPath, req.SymlinkOptions)
			if errDigest != nil {
				err = errors.Join(errDigest, err)
				return walker.Cancel
			}
			if node != nil {
				u.dirChildren[parentKey] = append(u.dirChildren[parentKey], node)
				u.digestCache.Store(key, digest.Digest{})
			}
			return nextStep

		case info.Mode().IsDir():
			stats.InputDirCount += 1
			// All the descendants have already been visited (DFS).
			node, b, errDigest := digestDirectory(realPath, u.dirChildren[key])
			if errDigest != nil {
				err = errors.Join(errDigest, err)
				return walker.Cancel
			}
			u.dirChildren[parentKey] = append(u.dirChildren[parentKey], node)
			dispatch(blob{digest: digest.NewFromProtoUnvalidated(node.Digest), bytes: b})
			u.digestCache.Store(key, digest.NewFromProtoUnvalidated(node.Digest))
			glog.V(2).Infof("upload.digest.visit.dir: realPath=%s, desiredPath=%s, digset=%v", realPath, desiredPath, node.Digest)
			glog.V(3).Infof("upload.digest.visit.dir: realPath=%s, desiredPath=%s, digset=%v, node=%v", realPath, desiredPath, node.Digest, node)
			return walker.Continue

		case info.Mode().IsRegular():
			stats.InputFileCount += 1
			node, blb, errDigest := digestFile(u.ctx, realPath, info, u.ioSem, u.ioLargeSem, u.ioCfg.SmallFileSizeThreshold, u.ioCfg.LargeFileSizeThreshold)
			if errDigest != nil {
				err = errors.Join(errDigest, err)
				return walker.Cancel
			}
			u.dirChildren[parentKey] = append(u.dirChildren[parentKey], node)
			dispatch(blb)
			u.digestCache.Store(key, digest.NewFromProtoUnvalidated(node.Digest))
			glog.V(2).Infof("upload.digest.visit.file: realPath=%s, desiredPath=%s, digest=%v", realPath, desiredPath, node.Digest)
			return walker.Continue

		default:
			// Skip everything else (e.g. sockets and pipes).
			glog.V(2).Infof("upload.digest.visit.other: realPath=%s, desiredPath=%s", realPath, desiredPath)
			return walker.Skip
		}
	})

	// errWalk is always walker.ErrBadNextStep, which means there is a bug in the code above.
	if errWalk != nil {
		panic(fmt.Errorf("internal fatal error: %v", errWalk))
	}
	// err includes any IO errors that happened during the walk.
	if err != nil {
		// Special case: this response didn't have a corresponding request. The dispatcher should not decrement its counter.
		dispatch(UploadResponse{Err: err})
	}
}

// uploadDispatcher receives digested blobs and forwards them to the uploader or back to the caller in case of a cache hit or error.
// The dispatcher handles counting in-flight requests per caller and notifying callers when all of their requests are completed.
func (u *uploaderv2) uploadDispatcher() {
	// Maintain a count of in-flight uploads per caller.
	tagReqCount := make(map[tag]int)
	tagDone := make(map[tag]bool)

	// Digestion could still dispatch blobs after the user has sent a done signal.
	// This makes the dispatcher a top-level sender.
	u.senderWg.Add(1)
	go func() {
		glog.V(1).Info("upload.dispatcher.start")
		defer glog.V(1).Info("upload.dispatcher.stop")
		defer u.senderWg.Done()
		for {
			select {
			case b := <-u.uploadDispatchCh:
				// The caller will not be sending any further requests.
				if b.done {
					tagDone[b.tag] = true
					glog.V(2).Infof("upload.dispatcher.blob.done: tag=%s", b.tag)
					continue
				}

				tagReqCount[b.tag]++
				glog.V(2).Infof("upload.dispatcher.blob: digest=%s, tag=%s, count=%d", b.digest, b.tag, tagReqCount[b.tag])

				switch {
				// Forward in-memory blobs to the query pipe which might forward them to the upload batcher or return a cache hit or error to the dispatcher.
				case len(b.bytes) > 0:
					u.uploadQueryPipeCh <- b

				// Forward open-files to the upload streamer which has built-in presence checking.
				default:
					u.uploadStreamCh <- b
				}

			case r := <-u.uploadResCh:
				glog.V(2).Infof("upload.dispatcher.res: digest=%s, err=%v", r.Digest, r.Err)
				// If multiple callers are interested in this response, ensure stats are not double-counted.
				if len(r.tags) == 1 {
					u.uploadPubSub.pub(r, r.tags[0])
				} else {
					rCached := r
					rCached.Stats = rCached.Stats.ToCacheHit()
					u.uploadPubSub.mpub(r, rCached, r.tags...)
				}

				for _, tag := range r.tags {
					// Special case: do not decrement if the response was from a digestion error or from an early cancellation.
					if r.Digest.Hash != "" {
						tagReqCount[tag]--
					}
					glog.V(2).Infof("upload.dispatcher.res: tag=%s, count=%d", tag, tagReqCount[tag])
					if tagReqCount[tag] == 0 && tagDone[tag] {
						delete(tagDone, tag)
						delete(tagReqCount, tag)
						// Signal to the caller that all of its requests are done.
						glog.V(2).Infof("upload.dispatcher.done: digest=%s", r.Digest)
						u.uploadPubSub.pub(UploadResponse{done: true}, tag)
					}
				}

			case <-u.ctx.Done():
				return
			}
		}
	}()
}

// uploadQueryPipe pipes the digest of a blob to the internal query processor to determine if it needs uploading.
// Cache hits and errors are piped back to the dispatcher while cache misses are piped to the uploader.
func (u *uploaderv2) uploadQueryPipe() {
	queryCh := make(chan digest.Digest)
	queryResCh := u.missingBlobsStreamer(queryCh)

	// The pipe forwards requests internally which makes it a top-level broker.
	u.processorWg.Add(1)
	go func() {
		glog.V(1).Info("upload.pipe.start")
		defer glog.V(1).Info("upload.pipe.stop")
		defer u.processorWg.Done()
		defer close(queryCh) // terminate the query streamer.

		// Keep track of the associated blob and tags since the query API accepts a digest only.
		// Identical digests have identical blobs.
		digestBlob := make(map[digest.Digest]blob)
		// Multiple callers might ask for the same digest.
		digestTags := make(map[digest.Digest][]tag)
		for {
			select {
			// Pipe dispatched blobs to the query processor.
			case b := <-u.uploadQueryPipeCh:
				digestBlob[b.digest] = b
				digestTags[b.digest] = append(digestTags[b.digest], b.tag)
				queryCh <- b.digest

			// Pipe to the uploader or back to the dispatcher.
			// The query streamer closes this channel when the context is done.
			case r, ok := <-queryResCh:
				if !ok {
					return
				}
				glog.V(2).Infof("upload.pipe.res: digest=%s, missing=%t, err=%v", r.Digest, r.Missing, r.Err)

				tags := digestTags[r.Digest]
				delete(digestTags, r.Digest)

				// Queryig failed. Pipe back to the dispatcher.
				if r.Err != nil {
					u.uploadResCh <- UploadResponse{Digest: r.Digest, Err: r.Err, tags: tags}
				}

				// Pipe to the uploader.
				if r.Missing {
					// Let the upload processors handle unification.
					b := digestBlob[r.Digest]
					for _, t := range tags {
						b.tag = t
						u.uploadBatchCh <- b
					}
					continue
				}

				// Cache hit. Pipe back to the dispatcher.
				u.uploadResCh <- UploadResponse{
					Digest: r.Digest,
					Stats: Stats{
						BytesRequested:     r.Digest.Size,
						LogicalBytesCached: r.Digest.Size,
						CacheHitCount:      1,
					},
					tags: tags,
				}
			}
		}
	}()
}

// uploadBatcher handles files below the small threshold which are buffered in-memory.
func (u *uploaderv2) uploadBatchProcessor() {
	bundle := make(uploadRequestBundle)
	bundleSize := u.uploadRequestBaseSize

	handle := func() {
		if len(bundle) < 1 {
			return
		}
		// Block the bundler if the concurrency limit is reached.
		if err := u.uploadSem.Acquire(u.ctx, 1); err != nil {
			// err is always ctx.Err(), so abort immediately.
			return
		}
		defer u.uploadSem.Release(1)

		u.workerWg.Add(1)
		go func(b uploadRequestBundle) {
			defer u.workerWg.Done()
			u.callBatchUpload(b)
		}(bundle)

		bundle = make(uploadRequestBundle)
		bundleSize = u.uploadRequestBaseSize
	}

	// This does internal routing, which makes it a top-level broker.
	u.processorWg.Add(1)
	go func() {
		glog.V(1).Info("upload.batch.start")
		defer glog.V(1).Info("upload.batch.stop")
		defer u.processorWg.Done()

		bundleTicker := time.NewTicker(u.uploadRpcConfig.BundleTimeout)
		defer bundleTicker.Stop()
		for {
			select {
			case b, ok := <-u.uploadBatchCh:
				glog.V(2).Infof("upload.batch.req: digest=%s, tag=%s", b.digest, b.tag)
				if !ok {
					return
				}

				r := &repb.BatchUpdateBlobsRequest_Request{
					Digest: b.digest.ToProto(),
					Data:   b.bytes, // TODO: add compression support as in https://github.com/bazelbuild/remote-apis-sdks/pull/443/files
				}
				rSize := proto.Size(r)

				// Reroute oversized blobs to the streamer.
				if rSize >= (u.uploadRpcConfig.BytesLimit - u.uploadRequestBaseSize) {
					u.uploadStreamCh <- b
					continue
				}

				item, ok := bundle[b.digest]
				if ok {
					// Duplicate tags are allowed to ensure the caller can match the number of responses to the number of requests.
					item.tags = append(item.tags, b.tag)
					glog.V(2).Infof("upload.batch.unified: digest=%s, len=%d", b.digest, len(item.tags))
					continue
				}

				if bundleSize+rSize >= u.uploadRpcConfig.BytesLimit {
					handle()
				}

				item.tags = append(item.tags, b.tag)
				item.req = r
				bundle[b.digest] = item
				bundleSize += rSize

				// Check length threshold.
				if len(bundle) >= u.uploadRpcConfig.ItemsLimit {
					handle()
					continue
				}
			case <-bundleTicker.C:
				handle()
			case <-u.ctx.Done():
				return
			}
		}
	}()
}

func (u *uploaderv2) callBatchUpload(bundle uploadRequestBundle) {
	glog.V(2).Infof("upload.batch.call: len=%d", len(bundle))

	req := &repb.BatchUpdateBlobsRequest{InstanceName: u.instanceName}
	req.Requests = make([]*repb.BatchUpdateBlobsRequest_Request, 0, len(bundle))
	for _, item := range bundle {
		req.Requests = append(req.Requests, item.req)
	}

	var uploaded []digest.Digest
	failed := make(map[digest.Digest]error)
	digestRetryCount := make(map[digest.Digest]int64)
	ctx, ctxCancel := context.WithCancel(u.ctx)
	defer ctxCancel()
	err := u.withTimeout(u.queryRpcConfig.Timeout, ctxCancel, func() error {
		return u.withRetry(ctx, u.uploadRpcConfig.RetryPolicy, func() error {
			// This call can have partial failures. Only retry retryable failed requests.
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
				glog.V(2).Infof("upload.batch.call.retry: len=%d", l)
			}
			return reqErr
		})
	})
	glog.V(2).Infof("upload.batch.call.done: uploaded=%d, failed=%d, req_failed=%d", len(uploaded), len(failed), len(bundle)-len(uploaded)-len(failed))

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
		select {
		case u.uploadResCh <- UploadResponse{
			Digest: d,
			Stats:  s,
			tags:   bundle[d].tags,
		}:
		case <-ctx.Done():
			return
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
		select {
		case u.uploadResCh <- UploadResponse{
			Digest: d,
			Stats:  s,
			Err:    errors.Join(ErrGRPC, dErr),
			tags:   bundle[d].tags,
		}:
		case <-ctx.Done():
			return
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
		select {
		case u.uploadResCh <- UploadResponse{
			Digest: d,
			Stats:  s,
			Err:    err,
			tags:   tags,
		}:
		case <-ctx.Done():
			return
		}
	}
}

// uploadStreamer handles files above the small threshold.
// Unlike the batched call, presence check is not required for streaming files because the API
// handles this automatically: https://github.com/bazelbuild/remote-apis/blob/0cd22f7b466ced15d7803e8845d08d3e8d2c51bc/build/bazel/remote/execution/v2/remote_execution.proto#L250-L254
// For files above the large threshold, this method assumes the io and large io holds are
// already acquired and will release them accordingly.
// For other files, only an io hold is acquired and released in this method.
func (u *uploaderv2) uploadStreamProcessor() {
	// This does internal routing, which makes it a top-level broker.
	u.processorWg.Add(1)
	go func() {
		glog.V(1).Info("upload.stream.start")
		defer glog.V(1).Info("upload.stream.stop")
		defer u.processorWg.Done()
		digestTags := initSliceCache()
		for {
			select {
			case b, ok := <-u.uploadStreamCh:
				glog.V(2).Infof("upload.stream.req: digest=%s, tag=%s", b.digest, b.tag)
				if !ok {
					return
				}

				isLargeFile := b.reader != nil

				if l := digestTags.Append(b.digest, b.tag); l > 1 {
					// Already in-flight. Release duplicate resources if it's a large file.
					glog.V(2).Infof("upload.stream.unified: digest=%s, tag=%s", b.digest, b.tag)
					if isLargeFile {
						u.ioSem.Release(1)
						u.ioLargeSem.Release(1)
					}
					continue
				}

				// Block the streamer if the gRPC call is being throttled.
				if err := u.streamSem.Acquire(u.ctx, 1); err != nil {
					// err is always ctx.Err()
					if isLargeFile {
						u.ioSem.Release(1)
						u.ioLargeSem.Release(1)
					}
					return
				}

				var name string
				if b.digest.Size >= u.ioCfg.CompressionSizeThreshold {
					glog.V(2).Infof("upload.stream.compress: digest=%s, tag=%s", b.digest, b.tag)
					name = MakeCompressedWriteResourceName(u.instanceName, b.digest.Hash, b.digest.Size)
				} else {
					name = MakeWriteResourceName(u.instanceName, b.digest.Hash, b.digest.Size)
				}

				u.workerWg.Add(1)
				go func() (stats Stats, err error) {
					glog.V(2).Infof("upload.stream.call: digest=%s, tag=%s", b.digest, b.tag)
					defer u.workerWg.Done()
					defer u.streamSem.Release(1)
					defer func() {
						glog.V(2).Infof("upload.stream.call.done: digest=%s, tag=%s, err=%v", b.digest, b.tag, err)
						tagsRaw := digestTags.LoadAndDelete(b.digest)
						tags := make([]tag, 0, len(tagsRaw))
						for _, t := range tagsRaw {
							tags = append(tags, t.(tag))
						}
						select {
						case u.uploadResCh <- UploadResponse{
							Digest: b.digest,
							Stats:  stats,
							Err:    err,
							tags:   tags,
						}:
						case <-u.ctx.Done():
						}
					}()

					var reader io.Reader

					// In the off chance that the blob is mis-constructed (more than one content field is set), start
					// with b.reader to ensure any held locks are released.
					switch {
					// Large file.
					case b.reader != nil:
						reader = b.reader
						defer func() {
							if errClose := b.reader.Close(); err != nil {
								err = errors.Join(ErrIO, errClose, err)
							}
						}()
						// IO holds were acquired during digestion for large files and are expected to be released here.
						defer u.ioSem.Release(1)
						defer u.ioLargeSem.Release(1)

					// Medium file.
					case len(b.path) > 0:
						if errSem := u.ioSem.Acquire(u.ctx, 1); errSem != nil {
							return
						}
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

					s, err := u.writeBytes(u.ctx, name, reader, b.digest.Size, 0, true)
					return *s, err
				}()

			case <-u.ctx.Done():
				return
			}
		}
	}()
}
