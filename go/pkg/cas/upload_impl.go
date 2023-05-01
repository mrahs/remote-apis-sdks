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

var errEarly = errors.New("early cancellation")

// UploadRequest represents a path to start uploading from.
//
// If the path is a directory, its entire tree is traversed and only files that are not excluded
// by the filter are uploaded.
// Symlinks are handled according to the SymlinkOptions field.
type UploadRequest struct {
	Path           impath.Absolute
	SymlinkOptions slo.Options
	Exclude        walker.Filter
	// tag is used internally to identify the client of the request.
	tag tag
	// done is used internally to signal to the processor that the client will not be sending any further requests.
	// This allows the processor to notify the client once all buffered requests are processed.
	// Once a tag is associated with done=true, sending subsequent requests for that tag might cause races.
	done bool
}

// UploadResponse represents an upload result for a single blob (which may represent a tree of files).
type UploadResponse struct {
	Digest digest.Digest
	Stats  Stats
	Err    error
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

// blob is a tuple of (digest, content, client_id, done_signal).
// The digest is the blob's unique identifier.
// The content must be one of reader, path, or bytes, in that order.
// Depending on which field is set, resources are acquired and released.
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

// tagCount is a tuple used by the dispatcher to track the number of in-flight blobs for each client.
// A blob is in-flight if it has been dispatched, but no corresponding response has been received for it.
type tagCount struct {
	t tag
	c int
}

// WriteBytes uploads all the bytes of r directly to the resource name starting remotely at offset.
//
// r must return io.EOF to terminate the call.
//
// ctx is used to make the remote calls.
// This method does not use the uploader's context which means it is safe to call even after that context is cancelled.
//
// size is used to toggle compression as well as report some stats. It must be reflect the actual number of bytes the specified reader has to give.
// The server is notified to finalize the resource name and subsequent writes may not succeed.
// The errors returned are either from the context, ErrGRPC, ErrIO, or ErrCompression. More errors may be wrapped inside.
// If an error was returned, the returned stats may indicate that all the bytes were sent, but that does not guarantee that the server committed all of them.
func (u *BatchingUploader) WriteBytes(ctx context.Context, name string, r io.Reader, size int64, offset int64) (Stats, error) {
	return u.writeBytes(ctx, name, r, size, offset, true)
}

// WriteBytesPartial is the same as WriteBytes, but does not notify the server to finalize the resource name.
func (u *BatchingUploader) WriteBytesPartial(ctx context.Context, name string, r io.Reader, size int64, offset int64) (Stats, error) {
	return u.writeBytes(ctx, name, r, size, offset, false)
}

func (u *uploaderv2) writeBytes(ctx context.Context, name string, r io.Reader, size int64, offset int64, finish bool) (Stats, error) {
	glog.V(2).Infof("upload.write_bytes.start: name=%s, size=%d, offset=%d, finish=%t", name, size, offset, finish)
	defer glog.V(2).Infof("upload.write_bytes.done: name=%s, size=%d, offset=%d, finish=%t", name, size, offset, finish)

	var stats Stats
	if err := u.streamSem.Acquire(ctx, 1); err != nil {
		// err is always ctx.Err()
		return stats, err
	}
	defer u.streamSem.Release(1)

	// Read raw bytes if compression is disabled.
	src := r

	// If compression is enabled, plug in the encoder via a pipe.
	var errEnc error
	var nRawBytes int64 // Track the actual number of the consumed raw bytes.
	var encWg sync.WaitGroup
	var withCompression bool // Used later to ensure the pipe is closed.
	if size >= u.ioCfg.CompressionSizeThreshold {
		glog.V(2).Infof("upload.write_bytes.compressing: name=%s, size=%d", name, size)
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
		return stats, errors.Join(ErrGRPC, errStream)
	}

	buf := u.buffers.Get().([]byte)
	defer u.buffers.Put(buf) // buf slice is never resliced which makes it safe to use a pointer-like type.

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

		// The reader is done (interrupted or completed).
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

	// This theoretically will block until the encoder's goroutine has returned, which is the happy path.
	// If the reader failed without the encoder's knowledge, closing the pipe will trigger the encoder to terminate, which is done above.
	// In any case, waiting here is necessary because the encoder's goroutine currently owns errEnc and nRawBytes.
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
// This method does not accept a context because the requests are unified across concurrent calls.
// The context that was used to initialize the uploader is used to make remote calls.
//
// Requests are unified across a window of time defined by the BundleTimeout value of the gRPC configuration.
// The unification is affected by the order of the requests, bundle limits (length, size, timeout) and the upload speed.
// With infinite speed and limits, every blob will be uploaded exactly once. On the other extreme, every blob is uploaded
// alone and no unification takes place.
// In the average case, blobs that make it into the same bundle will be unified (deduplicated).
//
// Returns a slice of the digests of the blobs that were uploaded (did not exist in the CAS).
// If the returned error is nil, any digest that is not in the returned slice was already in the CAS.
// If the returned error is not nil, the returned slice may be incomplete (fatal error) and every digest
// in it may or may not have been successfully uploaded (individual errors).
// The returned error wraps a number of errors proportional to the length of the specified slice.
//
// This method must not be called after cancelling the uploader's context.
func (u *BatchingUploader) Upload(reqs ...UploadRequest) ([]digest.Digest, *Stats, error) {
	glog.V(1).Infof("upload: %d requests", len(reqs))
	defer glog.V(1).Infof("upload.done")

	if len(reqs) == 0 {
		return nil, nil, nil
	}

	ch := make(chan UploadRequest)
	resCh := u.uploadStreamer(ch)

	u.clientSenderWg.Add(1)
	go func() {
		glog.V(1).Info("upload.sender.start")
		defer glog.V(1).Info("upload.sender.stop")
		defer close(ch) // ensure the streamer closes its response channel.
		defer u.clientSenderWg.Done()
		for _, r := range reqs {
			ch <- r
		}
	}()

	var uploaded []digest.Digest
	var err error
	stats := &Stats{}
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
// The caller must close in as a termination signal.
// This method does not accept a context because the requests are unified across concurrent calls.
// The context that was used to initialize the uploader is used to make remote calls.
//
// Requests are unified across a window of time defined by the BundleTimeout value of the gRPC configuration.
// The unification is affected by the order of the requests, bundle limits (length, size, timeout) and the upload speed.
// With infinite speed and limits, every blob will be uploaded exactly once. On the other extreme, every blob is uploaded
// alone and no unification takes place.
// In the average case, blobs that make it into the same bundle will be grouped by digest. Once a digest is processed, each requester of that
// digest receives a copy of the coorresponding UploadResponse.
//
// This method must not be called after cancelling the uploader's context.
func (u *StreamingUploader) Upload(in <-chan UploadRequest) <-chan UploadResponse {
	return u.uploadStreamer(in)
}

func (u *uploaderv2) uploadStreamer(in <-chan UploadRequest) <-chan UploadResponse {
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

	// Register a new caller with the internal processor.
	// This borker should not remove the subscription until the sender tells it to, hence, the background context.
	// The broker uses the context for cancellation only. It's not propagated further.
	ctxSub, ctxSubCancel := context.WithCancel(context.Background())
	tag, resChan := u.uploadPubSub.sub(ctxSub)

	// Forward the requests to the internal processor.
	u.uploadSenderWg.Add(1)
	go func() {
		glog.V(1).Info("upload.streamer.sender.start")
		defer glog.V(1).Info("upload.streamer.sender.stop")
		defer u.uploadSenderWg.Done()
		for r := range in {
			r.tag = tag
			u.uploadCh <- r
		}
		// Let the processor know that no further requests are expected.
		u.uploadCh <- UploadRequest{tag: tag, done: true}
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
	glog.V(1).Info("upload.processor.start")
	defer glog.V(1).Info("upload.processor.stop")

	defer func() {
		u.walkerWg.Wait()
		// Tell the dispatcher to terminate once it forwarded all remaining responses.
		u.uploadDispatchCh <- blob{done: true}
	}()

	for req := range u.uploadCh {
		// If the caller will not be sending any further requests, wait for in-flight walks from previous requests
		// then tell the dispatcher to forward the signal once all dispatched blobs are done.
		if req.done {
			glog.V(2).Infof("upload.processor.req.done: tag=%s", req.tag)
			wg := u.callerWalkWg[req.tag]
			if wg == nil {
				glog.Errorf("upload.processor.req: received a done signal but no previous requests for tag=%s", req.tag)
				continue
			}
			// Remove the wg to ensure a new one is used if the caller decides to send more requests.
			// Otherwise, races on the wg might happen.
			u.callerWalkWg[req.tag] = nil
			u.workerWg.Add(1)
			// Wait for the walkers to finish dispatching blobs then tell the dispatcher that no further blobs are expected from this caller.
			go func() {
				glog.V(2).Infof("upload.processor.walk.wait.start: tag=%s", req.tag)
				defer glog.V(2).Infof("upload.processor.walk.wait.done: tag=%s", req.tag)
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
		u.walkerWg.Add(1)
		go func(r UploadRequest) {
			defer u.walkerWg.Done()
			defer wg.Done()
			defer u.walkSem.Release(1)
			u.digestAndDispatch(r)
		}(req)
	}
}

// digestAndDispatch initiates a file system walk to digest files and dispatch them for uploading.
func (u *uploaderv2) digestAndDispatch(req UploadRequest) {
	glog.V(2).Infof("upload.digest.start: root=%s, tag=%s", req.Path, req.tag)
	defer glog.V(2).Infof("upload.digest.done: root=%s, tag=%s", req.Path, req.tag)

	stats := Stats{}
	var err error
	walker.DepthFirst(req.Path, req.Exclude, walker.Callback{
		Err: func(path impath.Absolute, realPath impath.Absolute, errVisit error) bool {
			glog.V(2).Infof("upload.digest.visit.err: realPath=%s, desiredPath=%s, err=%v", realPath, path, errVisit)
			err = errors.Join(errVisit, err)
			return false
		},
		Pre: func(path impath.Absolute, realPath impath.Absolute) (walker.PreAction, bool) {
			glog.V(2).Infof("upload.digest.visit.pre: realPath=%s, desiredPath=%s", realPath, path)
			select {
			case <-u.ctx.Done():
				glog.V(2).Info("upload.digest.cancel")
				return walker.SkipPath, false
			default:
			}

			key := path.String() + req.Exclude.String()

			// A cache hit here indicates a cyclic symlink with the same caller or multiple callers attempting to upload the exact same path with an identical filter.
			// In both cases, deferring is the right call. Once the upload is processed, all callers will revisit the path to get the digestion result.
			// If the path was not cached before, claim it by makring it as in-flight using a nil value.
			m, ok := u.digestCache.LoadOrStore(key, nil)
			if !ok {
				// Claimed. Access it.
				return walker.Access, true
			}

			// Defer if in-flight.
			if m == nil {
				glog.V(2).Infof("upload.digest.visit.defer: realPath=%s, desiredPath=%s", realPath, path)
				return walker.Defer, true
			}

			node, _ := m.(proto.Message) // Guaranteed assertion because the cache is an internal field.
			glog.V(2).Infof("upload.digest.visit.cached: realPath=%s, desiredPath=%s", realPath, path)

			// Dispatch it to correctly account for a cache hit or upload if the original blob is blocked elsewhere.
			switch node := node.(type) {
			case *repb.FileNode:
				// Dispatching with a file path will deliver the blob to the streaming API.
				// If the original blob is queued in the batching API, this blob could potentially get uploaded twice.
				// However, that would mean the file is small.
				u.uploadDispatchCh <- blob{digest: digest.NewFromProtoUnvalidated(node.Digest), path: realPath.String(), tag: req.tag}
			case *repb.DirectoryNode:
				// The blob of the directory node is its proto representation.
				// Generate and dispatch it. If it was uploaded before, it'll be reported as a cache hit.
				// Otherwise, it means the previous attempt to upload it failed and it is going to be retried.
				node, b, errDigest := digestDirectory(realPath, u.dirChildren[key])
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					return walker.SkipPath, false
				}
				u.uploadDispatchCh <- blob{digest: digest.NewFromProtoUnvalidated(node.Digest), bytes: b, tag: req.tag}
			case *repb.SymlinkNode:
				// It was already appended as a child to its parent. Nothing to dispatch.
			}

			return walker.SkipPath, true
		},
		Post: func(path impath.Absolute, realPath impath.Absolute, info fs.FileInfo) (ok bool) {
			glog.V(2).Infof("upload.digest.visit.post: realPath=%s, desiredPath=%s", realPath, path)
			select {
			case <-u.ctx.Done():
				glog.V(2).Info("upload.digest.cancel")
				return false
			default:
			}

			key := path.String() + req.Exclude.String()
			parentKey := path.Dir().String() + req.Exclude.String()

			// If there was a digestion error, unclaim the path.
			defer func() {
				if !ok {
					u.digestCache.Delete(key)
				}
			}()

			switch {
			case info.Mode().IsDir():
				stats.DigestCount += 1
				stats.InputDirCount += 1
				// All the descendants have already been visited (DFS).
				node, b, errDigest := digestDirectory(realPath, u.dirChildren[key])
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					return false
				}
				u.dirChildren[parentKey] = append(u.dirChildren[parentKey], node)
				u.uploadDispatchCh <- blob{digest: digest.NewFromProtoUnvalidated(node.Digest), bytes: b, tag: req.tag}
				u.digestCache.Store(key, digest.NewFromProtoUnvalidated(node.Digest))
				glog.V(2).Infof("upload.digest.visit.dir: realPath=%s, desiredPath=%s, digset=%v", realPath, path, node.Digest)
				glog.V(3).Infof("upload.digest.visit.dir: realPath=%s, desiredPath=%s, digset=%v, node=%v", realPath, path, node.Digest, node)
				return true

			case info.Mode().IsRegular():
				stats.DigestCount += 1
				stats.InputFileCount += 1
				node, blb, errDigest := digestFile(u.ctx, realPath, info, u.ioSem, u.ioLargeSem, u.ioCfg.SmallFileSizeThreshold, u.ioCfg.LargeFileSizeThreshold)
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					return false
				}
				u.dirChildren[parentKey] = append(u.dirChildren[parentKey], node)
				blb.tag = req.tag
				u.uploadDispatchCh <- blb
				u.digestCache.Store(key, digest.NewFromProtoUnvalidated(node.Digest))
				glog.V(2).Infof("upload.digest.visit.file: realPath=%s, desiredPath=%s, digest=%v", realPath, path, node.Digest)
				return true

			default:
				// Ignore everything else (e.g. sockets and pipes).
				glog.V(2).Infof("upload.digest.visit.other: realPath=%s, desiredPath=%s", realPath, path)
			}
			return true
		},
		Symlink: func(path impath.Absolute, realPath impath.Absolute, _ fs.FileInfo) (action walker.SymlinkAction, ok bool) {
			glog.V(2).Infof("upload.digest.visit.symlink: realPath=%s, desiredPath=%s", realPath, path)
			select {
			case <-u.ctx.Done():
				glog.V(2).Info("upload.digest.cancel")
				return walker.SkipSymlink, false
			default:
			}

			key := path.String() + req.Exclude.String()
			parentKey := path.Dir().String() + req.Exclude.String()

			// If there was a digestion error, unclaim the path.
			defer func() {
				if !ok {
					u.digestCache.Delete(key)
				}
			}()

			stats.DigestCount += 1
			glog.V(2).Infof("upload.digest.visit.symlink: realPath=%s, desiredPath=%s", realPath, path)
			stats.InputSymlinkCount += 1
			node, nextStep, errDigest := digestSymlink(req.Path, realPath, req.SymlinkOptions)
			if errDigest != nil {
				err = errors.Join(errDigest, err)
				return walker.SkipSymlink, false
			}
			if node != nil {
				u.dirChildren[parentKey] = append(u.dirChildren[parentKey], node)
				u.digestCache.Store(key, digest.Digest{})
			}
			return nextStep, true
		},
	})

	// err includes any IO errors that happened during the walk.
	if err != nil {
		// Special case: this response didn't have a corresponding blob. The dispatcher should not decrement its counter.
		u.uploadResCh <- UploadResponse{tags: []tag{req.tag}, Err: err}
	}
}

// uploadDispatcher receives digested blobs and forwards them to the uploader or back to the caller in case of a cache hit or error.
// The dispatcher handles counting in-flight requests per caller and notifying callers when all of their requests are completed.
func (u *uploaderv2) uploadDispatcher() {
	glog.V(1).Info("upload.dispatcher.start")
	defer glog.V(1).Info("upload.dispatcher.stop")

	defer func() {
		// Tell the pipe processor to terminate once it forwarded all remaining responses.
		u.uploadQueryPipeCh <- blob{done: true}
	}()

	// Maintain a count of in-flight uploads per caller.
	pendingCh := make(chan tagCount)
	// Wait until all requests have been fully dispatched before terminating.
	wg := sync.WaitGroup{}

	// This counter keeps track of in-flight blobs and notifies callers when all their blobs have been fully dispatched.
	// It terminates after the sender below, but before the receiver.
	wg.Add(1)
	go func() {
		defer wg.Done()
		glog.V(1).Info("upload.dispatcher.counter.start")
		defer glog.V(1).Info("upload.dispatcher.counter.stop")
		defer func() { close(u.uploadResCh) }()

		tagReqCount := make(map[tag]int)
		tagDone := make(map[tag]bool)
		allDone := false
		for tc := range pendingCh {
			if tc.c == 0 { // There will be no more blobs for this tag.
				if tc.t == "" { // In fact, no more blobs for any tag.
					if len(tagReqCount) == 0 {
						return
					}
					allDone = true
					continue
				}
				tagDone[tc.t] = true
				// If nothing was previously dispatched for this tag, let the caller know this is the last response.
				// This happens when all digestions failed or there was nothing to digest (everything was excluded).
				if _, ok := tagReqCount[tc.t]; !ok {
					u.uploadPubSub.pub(UploadResponse{done: true}, tc.t)
				}
				glog.V(2).Infof("upload.dispatcher.blob.done: tag=%s", tc.t)
				continue
			}
			tagReqCount[tc.t] += tc.c
			glog.V(2).Infof("upload.dispatcher.count: tag=%s, count=%d", tc.t, tagReqCount[tc.t])
			if tagReqCount[tc.t] <= 0 && tagDone[tc.t] {
				delete(tagDone, tc.t)
				delete(tagReqCount, tc.t)
				// Signal to the caller that all of its requests are done.
				glog.V(2).Infof("upload.dispatcher.done: tag=%s", tc.t)
				u.uploadPubSub.pub(UploadResponse{done: true}, tc.t)
			}
			if len(tagReqCount) == 0 && allDone {
				return
			}
		}
	}()

	// This sender dispatches digested blobs to the query processor or to the streaming processor.
	// It is the first to terminate among the three in this block.
	wg.Add(1)
	go func() {
		defer wg.Done()
		glog.V(1).Info("upload.dispatcher.sender.start")
		defer glog.V(1).Info("upload.dispatcher.sender.stop")

		for b := range u.uploadDispatchCh {
			if b.done { // The caller will not be sending any further requests.
				pendingCh <- tagCount{b.tag, 0}
				if b.tag == "" { // In fact, all callers have terminated.
					return
				}
				continue
			}
			if b.digest.Hash == "" {
				glog.Errorf("upload.dispatcher: received a blob with an empty digest for tag=%s; ignoring", b.tag)
				continue
			}
			glog.V(2).Infof("upload.dispatcher.blob: digest=%s, tag=%s", b.digest, b.tag)
			switch {
			// Forward in-memory blobs to the query pipe which might forward them to the upload batcher or return a cache hit or error to the dispatcher.
			case len(b.bytes) > 0:
				u.uploadQueryPipeCh <- b

			// Forward open-files to the upload streamer which has built-in presence checking.
			default:
				u.uploadStreamCh <- b
			}
			pendingCh <- tagCount{b.tag, 1}
		}
	}()

	// This receiver forwards upload responses to callers.
	// It is the last to terminate among the three in this block.
	wg.Add(1)
	go func() {
		defer wg.Done()
		glog.V(1).Info("upload.dispatcher.receiver.start")
		defer glog.V(1).Info("upload.dispatcher.receiver.stop")

		// Messages delivered here are either went through the sender above (dispatched for upload), or bypassed (digestion error).
		// In either case, the client should get the response.
		// In the last case, the client should get the response with the error, but the counter should not decrement.
		for r := range u.uploadResCh {
			glog.V(2).Infof("upload.dispatcher.res: digest=%s, err=%v", r.Digest, r.Err)
			// If multiple callers are interested in this response, ensure stats are not double-counted.
			if len(r.tags) == 1 {
				u.uploadPubSub.pub(r, r.tags[0])
			} else {
				rCached := r
				rCached.Stats = r.Stats.ToCacheHit()
				u.uploadPubSub.mpub(r, rCached, r.tags...)
			}

			for _, tag := range r.tags {
				// Special case: do not decrement if the response was from a digestion error.
				if r.Digest.Hash != "" {
					pendingCh <- tagCount{tag, -1}
				}
			}
		}
	}()

	wg.Wait()
}

// uploadQueryPipe pipes the digest of a blob to the internal query processor to determine if it needs uploading.
// Cache hits and errors are piped back to the dispatcher while cache misses are piped to the uploader.
func (u *uploaderv2) uploadQueryPipe(queryCh chan digest.Digest, queryResCh <-chan MissingBlobsResponse) {
	glog.V(1).Info("upload.pipe.start")
	defer glog.V(1).Info("upload.pipe.stop")

	defer func() {
		// Tell the batch processor to terminate.
		close(u.uploadBatchCh)
	}()

	// Keep track of the associated blob and tags since the query API accepts a digest only.
	// Identical digests have identical blobs.
	digestBlob := make(map[digest.Digest]blob)
	// Multiple callers might ask for the same digest.
	digestTags := make(map[digest.Digest][]tag)
	done := false
	for {
		select {
		// Pipe dispatched blobs to the query processor.
		case b := <-u.uploadQueryPipeCh:
			// In the off chance that a request is received after a done signal, ignore it to avoid sending on a closed channel.
			if done {
				glog.Errorf("upload.pipe: received a request after a done signal from tag=%s; ignoring", b.tag)
				continue
			}
			// If the dispatcher has terminated, tell the streamer we're done and continue draining the response channel.
			if b.done {
				done = true
				close(queryCh)
				glog.V(2).Info("upload.pipe.done")
				continue
			}

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
}

// uploadBatcher handles files below the small threshold which are buffered in-memory.
func (u *uploaderv2) uploadBatchProcessor() {
	glog.V(1).Info("upload.batch.start")
	defer glog.V(1).Info("upload.batch.stop")

	defer func() {
		// Tell the stream processor to terminate once it finished all remaining requests.
		u.uploadStreamCh <- blob{done: true}
	}()

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

	bundleTicker := time.NewTicker(u.uploadRpcConfig.BundleTimeout)
	defer bundleTicker.Stop()
	for {
		select {
		case b, ok := <-u.uploadBatchCh:
			if !ok {
				return
			}
			glog.V(2).Infof("upload.batch.req: digest=%s, tag=%s", b.digest, b.tag)

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
				bundle[b.digest] = item
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
		}
	}
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
	ctxGrpc, ctxGrpcCancel := context.WithCancel(u.ctx)
	defer ctxGrpcCancel()
	err := u.withTimeout(u.queryRpcConfig.Timeout, ctxGrpcCancel, func() error {
		return u.withRetry(ctxGrpc, u.uploadRpcConfig.RetryPolicy, func() error {
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
		u.uploadResCh <- UploadResponse{
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
		u.uploadResCh <- UploadResponse{
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
		u.uploadResCh <- UploadResponse{
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
func (u *uploaderv2) uploadStreamProcessor() {
	glog.V(1).Info("upload.stream.start")
	defer glog.V(1).Info("upload.stream.stop")

	// Unify duplicate requests.
	digestTags := make(map[digest.Digest][]tag)
	streamResCh := make(chan UploadResponse)
	done := false
	pending := 0
	for {
		select {
		case b := <-u.uploadStreamCh:
			// In the off chance that a request is received after a done signal, ignore it to avoid sending on a closed channel.
			if done {
				glog.Errorf("upload.stream: received a request after a done signal from tag=%s; ignoring", b.tag)
				continue
			}
			// If the dispatcher has terminated, continue draining the response channel then terminate.
			// The dispatcher and the batch processor are the only senders.
			// This signal comes from the batch processor, which terminates after the dispatcher.
			if b.done {
				glog.V(2).Info("upload.stream.done: pendint=%d", pending)
				if pending == 0 {
					return
				}
				done = true
				continue
			}
			glog.V(2).Infof("upload.stream.req: digest=%s, tag=%s", b.digest, b.tag)

			isLargeFile := b.reader != nil

			tags := digestTags[b.digest]
			tags = append(tags, b.tag)
			digestTags[b.digest] = tags
			if len(tags) > 1 {
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

			pending += 1
			u.workerWg.Add(1)
			go func() {
				defer u.workerWg.Done()
				defer u.streamSem.Release(1)
				s, err := u.callStream(name, b)
				streamResCh <- UploadResponse{Digest: b.digest, Stats: s, Err: err}
			}()
			glog.V(2).Infof("upload.stream.req: pending=%d", pending)

		case r := <-streamResCh:
			r.tags = digestTags[r.Digest]
			delete(digestTags, r.Digest)
			u.uploadResCh <- r
			pending -= 1
			glog.V(2).Infof("upload.stream.res: pending=%d, done=%t", pending, done)
			if pending == 0 && done {
				return
			}
		}
	}
}

func (u *uploaderv2) callStream(name string, b blob) (stats Stats, err error) {
	glog.V(2).Infof("upload.stream.call: digest=%s, tag=%s", b.digest, b.tag)
	defer glog.V(2).Infof("upload.stream.call.done: digest=%s, tag=%s, err=%v", b.digest, b.tag, err)

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

	return u.writeBytes(u.ctx, name, reader, b.digest.Size, 0, true)
}
