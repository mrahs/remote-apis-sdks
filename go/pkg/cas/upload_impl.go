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

// UploadRequest represents a path to start uploading from.
// If the path is a directory, its entire tree is traversed and only files that are not excluded
// by the filter are uploaded.
// Symlinks are handled according to the specified options.
type UploadRequest struct {
	Path           impath.Absolute
	SymlinkOptions slo.Options
	ShouldSkip     *walker.Filter
	// For internal use.
	tag tag
}

// UploadResponse represents an upload result for a single blob (which may represent a tree of files).
type UploadResponse struct {
	Digest digest.Digest
	Stats  Stats
	Err    error
}

type uploadRequestBundleItem struct {
	req  *repb.BatchUpdateBlobsRequest_Request
	tags []tag
}

type uploadRequestBundle = map[digest.Digest]uploadRequestBundleItem

// blob associates a digest with its original bytes.
// Only one of reader, path, or bytes is used, in that order.
// See uploadStreamer implementation below.
type blob struct {
	digest *repb.Digest
	bytes  []byte
	path   string
	reader io.ReadSeekCloser
	tag    tag
	err    error
}

// WriteBytes uploads all the bytes (until EOF) of the specified reader directly to the specified resource name starting remotely at the specified offset.
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
	if err := u.streamSem.Acquire(ctx, 1); err != nil {
		// err is always ctx.Err(), so abort immediately.
		return nil, err
	}
	defer u.streamSem.Release(1)

	src := r

	// If compression is enabled, plug in the encoder via a pipe.
	var errCompr error
	var nRawBytes int64
	var encWg sync.WaitGroup
	var withCompression bool
	if size >= u.ioCfg.CompressionSizeThreshold {
		withCompression = true
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

	// Close the reader to signal to the encoder's goroutine to terminate.
	// However, do not close the reader if it is the given argument; hence the boolean guard.
	if srcCloser, ok := src.(io.Closer); ok && withCompression {
		if errClose := srcCloser.Close(); errClose != nil {
			err = errors.Join(ErrIO, errClose, err)
		}
	}

	// This theoretically will block until the encoder's goroutine returns.
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
func (u *BatchingUploader) Upload(ctx context.Context, paths []impath.Absolute, slo slo.Options, shouldSkip *walker.Filter) ([]digest.Digest, *Stats, error) {
	glog.V(3).Infof("upload: paths=%v, slo=%s, filter=%s", paths, slo, shouldSkip)
	if len(paths) < 1 {
		return nil, nil, nil
	}

	// This implementation converts the underlying nonblocking implementation into a blocking one.
	// A separate goroutine is used to push the requests into the processor.
	// The receiving code blocks the goroutine of the call until all responses are received or the context is canceled.

	ctxUploadCaller, ctxUploaderCallerCancel := context.WithCancel(ctx)
	defer ctxUploaderCallerCancel()

	tag, resChan := u.uploadCallerPubSub.sub(ctxUploadCaller)
	u.senderWg.Add(1)
	go func() {
		defer u.senderWg.Done()
		for _, p := range paths {
			glog.V(3).Infof("upload.send: path=%s, tag=%s", p, tag)
			select {
			case u.uploadChan <- UploadRequest{Path: p, SymlinkOptions: slo, ShouldSkip: shouldSkip, tag: tag}:
				continue
			case <-ctx.Done():
				glog.V(3).Infof("upload.send.cancel")
				return
			}
		}
	}()

	stats := &Stats{}
	var uploaded []digest.Digest
	var err error
	// The channel will be closed if the context is cancelled so no need to watch the context here.
	for rawR := range resChan {
		r := rawR.(UploadResponse)
		glog.V(3).Infof("upload.receive: digest=%s, stats=%v, err=%v", r.Digest, r.Stats, r.Err)
		switch {
		case errors.Is(r.Err, EOR):
			ctxUploaderCallerCancel()
			// It's tempting to break here, but the channel must be drained until the publisher closes it.

		case r.Err == nil:
			uploaded = append(uploaded, r.Digest)

		default:
			err = errors.Join(r.Err, err)
		}
		stats.Add(r.Stats)
	}

	return uploaded, stats, err
}

// Upload is a non-blocking call that uploads incoming files to the CAS if necessary.
//
// Requests are unified across a window of time defined by the BundleTimeout value of the gRPC configuration.
// The unification is affected by the order of the requests, bundle limits (length, size, timeout) and the upload speed.
// With infinite speed and limits, every blob will uploaded exactly once. On the other extreme, every blob is uploaded
// alone and no unification takes place.
// In the average case, blobs that make it into the same bundle will be grouped by digest. Once a digest is processed, each requester of that
// digest receives a copy of the coorresponding UploadResponse.
//
// This method must not be called after calling Wait.
func (u *StreamingUploader) Upload(context.Context, <-chan impath.Absolute, slo.Options, *walker.Filter) <-chan UploadResponse {
	panic("not yet implemented")
}

// uploadDispatcher is the entry point for upload requests.
// It starts by computing a merkle tree from the file system view specified by the request and its filter.
// Files and blobs are uploaded during the digestion to minimize IO induced latency.
//
// This effectively optimizes for frequent uploads of never-seen-before files.
// Using a depth-first style file traversal suits this use-case.
//
// To optimize for frequent uploads of the same files, consider computing the merkle tree separately
// then construct a list of blobs that are missing from the CAS and upload that list.
func (u *uploaderv2) uploadDispatcher(ctx context.Context) {
	glog.V(3).Info("upload.dispatcher")

	// Set up a pipe for querying the cas before uploading.
	dispatch := make(chan any)
	queryBlobCh := make(chan blob)
	queryCh := make(chan digest.Digest)
	queryResCh := u.missingBlobsStreamer(ctx, queryCh)

	go func() {
		for x := range dispatch {
			switch v := x.(type) {
			case blob:
				glog.V(3).Infof("upload.dispatcher.dispatch: digest=%s, tag=%s, err=%v", v.digest, v.tag, v.err)

				switch {

				// Digestion failed.
				case v.err != nil:
					// The error contains information about the file. Notify callers.
					u.uploadCallerPubSub.pub(UploadResponse{
						Err: v.err,
					}, v.tag)

				// Forward in-memory blobs to the query pipe which might forward them to the upload batcher.
				case len(v.bytes) > 0:
					queryBlobCh <- v // TODO: close

				// Forward open-files to the upload streamer which has built-in presence checking.
				default:
					u.uploadStreamerChan <- v
				}
			case UploadResponse:

			default:
				panic("invalid value type for dispatch")
			}
		}
	}()

	// TODO: wg
	go func() {
		digestBlob := make(map[digest.Digest]blob)
		digestTags := make(map[digest.Digest][]tag)
		for {
			select {
			// Forward dispatched blobs to the query processor.
			case b, ok := <-queryBlobCh:
				if !ok {
					close(queryCh)
					// The query processor will close queryResCh.
					// Until then, continue draining it.
					continue
				}

				d := digest.NewFromProtoUnvalidated(b.digest)
				digestBlob[d] = b
				digestTags[d] = append(digestTags[d], b.tag)
				queryCh <- d

			// Forward missing blobs to the uploader.
			case r, ok := <-queryResCh:
				if !ok {
					return
				}

				glog.V(3).Infof("upload.dispatcher.queryRes: digest=%s, missing=%t, err=%v", r.Digest, r.Missing, r.Err)

				tags := digestTags[r.Digest]
				delete(digestTags, r.Digest)
				// Queryig failed. Notify callers.
				if r.Err != nil {
					for _, t := range tags {
						dispatch <- UploadResponse{
							Digest: r.Digest,
							Err: r.Err,
						}
					}
					continue
				}

				// Forward to the uploader.
				if r.Missing {
					// Let the upload processors handle unification.
					b := digestBlob[r.Digest]
					for _, t := range tags {
						b.tag = t
						u.uploadBatcherChan <- b
					}
					continue
				}

				// Cache hit. Notify callers.
				dispatch(digestBlob[r.Digest], tags...)
				u.uploadCallerPubSub.pub(UploadResponse{
					Digest: r.Digest,
					Stats: Stats{
						BytesRequested:     r.Digest.Size,
						LogicalBytesCached: r.Digest.Size,
						CacheHitCount:      1,
					},
				}, digestTags[r.Digest]...)
			}
		}
	}()

	u.processorWg.Add(1)
	go func() {
		defer u.processorWg.Done()

		// Digestion and dispatching loop.
		for {
			select {
			case req, ok := <-u.uploadChan:
				glog.V(3).Infof("upload.dispatcher.digest: path=%s, slo=%s, filter=%s, tag=%s", req.Path, req.SymlinkOptions, req.ShouldSkip, req.tag)
				if !ok {
					return
				}

				// Wait if too many walks are in-flight.
				if err := u.walkSem.Acquire(ctx, 1); err != nil {
					// err is always ctx.Err()
					return
				}
				// walkSem is released downstream.
				u.digestAndDispatch(ctx, req.Path, req.ShouldSkip, req.SymlinkOptions, req.tag, dispatch)

			case <-ctx.Done():
				glog.V(3).Info("upload.dispatcher.cancel")
				return
			}
		}
	}()
}

// digestAndDispatch is a non-blocking call that initiates a file system walk to digest and dispatch the files for upload.
func (u *uploaderv2) digestAndDispatch(ctx context.Context, root impath.Absolute, filter *walker.Filter, slo slo.Options, callerTag tag, dispatch func(blob)) {
	// callerTag may be associated with multiple requests.
	// Create a subscriber to receive upload responses for this particular request.
	ctxReq, ctxReqCancel := context.WithCancel(ctx)
	reqTag, ch := u.uploadReqPubSub.sub(ctxReq)

	// Redirect the response to the caller.
	u.receiverWg.Add(1)
	go func() {
		defer u.receiverWg.Done()
		for r := range ch {
			glog.V(3).Infof("upload.digest.response: tag=%s", callerTag)
			u.uploadCallerPubSub.pub(r, callerTag)
		}
	}()

	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()
		defer u.walkSem.Release(1)
		defer ctxReqCancel()

		errCh := make(chan error)
		errWg := sync.WaitGroup{}
		var err error
		errWg.Add(1)
		go func() {
			defer errWg.Done()
			for e := range errCh {
				glog.V(3).Infof("upload.digest.err: %s", err)
				err = errors.Join(e, err)
			}
		}()

		glog.V(3).Infof("upload.digest.walker.start: root=%s, tag=%s", root, callerTag)
		stats := Stats{}
		errWalk := walker.DepthFirst(root, filter, func(realPath impath.Absolute, desiredPath impath.Absolute, info fs.FileInfo, err error) walker.NextStep {
			glog.V(3).Infof("upload.digest.walker.visit: first=%t, realPath=%s, desiredPath=%s, err=%v", info == nil, realPath, desiredPath, err)
			select {
			case <-ctx.Done():
				glog.V(3).Info("upload.digest.walker.cancel")
				return walker.Cancel
			default:
			}

			if err != nil {
				errCh <- err
				return walker.Cancel
			}

			key := desiredPath.String() + filter.String()
			parentKey := desiredPath.Dir().String() + filter.String()

			// Pre-access.
			if info == nil {
				// A cache hit here indicates a cyclic symlink or multiple callers attempting to upload the exact same path with an identical filter.
				// In both cases, deferring is the right call. Once the upload is processed, all uploaders will revisit the path to get the processing result.
				if rawR, ok := u.ioCfg.UploadCache.Load(key); ok {
					// Defer if in-flight.
					if rawR == nil {
						glog.V(3).Infof("upload.digest.walker.visit.defer: realPath=%s, desiredPath=%s", realPath, desiredPath)
						return walker.Defer
					}
					// Update the stats to reflect a cache hit before publishing.
					r := rawR.(UploadResponse)
					glog.V(3).Infof("upload.digest.walker.visit.cached: realPath=%s, desiredPath=%s", realPath, desiredPath)
					r.Stats = r.Stats.ToCacheHit()
					u.uploadCallerPubSub.pub(r, callerTag)
					return walker.Skip
				}

				// Access it.
				return walker.Continue
			}

			// Mark the file as being in-flight.
			u.ioCfg.UploadCache.Store(key, nil)
			stats.DigestCount += 1
			switch {
			case info.Mode()&fs.ModeSymlink == fs.ModeSymlink:
				glog.V(3).Infof("upload.digest.walker.visit.symlink: realPath=%s, desiredPath=%s", realPath, desiredPath)
				stats.InputSymlinkCount += 1
				node, nextStep, err := digestSymlink(root, realPath, slo)
				if err != nil {
					dispatch(blob{err: err, tag: reqTag})
					return walker.Cancel
				}
				if node != nil {
					u.dirChildren.Append(parentKey, node)
				}
				return nextStep

			case info.Mode().IsDir():
				glog.V(3).Infof("upload.digest.walker.visit.dir: realPath=%s, desiredPath=%s", realPath, desiredPath)
				stats.InputDirCount += 1
				// All the descendants have already been visited (DFS).
				node, b, nextStep, err := digestDirectory(realPath, u.dirChildren.Load(key))
				if err != nil {
					dispatch(blob{err: err, tag: reqTag})
					return walker.Cancel
				}
				if node != nil {
					u.dirChildren.Append(parentKey, node)
				}
				if b != nil {
					dispatch(blob{digest: node.Digest, bytes: b, tag: reqTag})
				}
				return nextStep

			case info.Mode().IsRegular():
				glog.V(3).Infof("upload.digest.walker.visit.file: realPath=%s, desiredPath=%s", realPath, desiredPath)
				stats.InputFileCount += 1
				node, blb, nextStep, err := digestFile(ctx, realPath, info, u.ioSem, u.ioLargeSem, u.ioCfg.SmallFileSizeThreshold, u.ioCfg.LargeFileSizeThreshold)
				if err != nil {
					dispatch(blob{err: err, tag: reqTag})
					return walker.Cancel
				}
				if node != nil {
					u.dirChildren.Append(parentKey, node)
				}
				if blb != nil {
					blb.tag = reqTag
					dispatch(*blb)
				}
				return nextStep

			default:
				// Skip everything else (e.g. sockets and pipes).
				glog.V(3).Infof("upload.digest.walker.visit.other: realPath=%s, desiredPath=%s", realPath, desiredPath)
				return walker.Skip
			}
		})
		// errWalk is always walker.ErrBadNextStep, which means there is a bug in the code above.
		if errWalk != nil {
			panic(fmt.Errorf("internal fatal error: %v", errWalk))
		}
		close(errCh)
		errWg.Wait()
		glog.V(3).Infof("upload.digest.walker.done: root=%s, tag=%s, err=%v", root, callerTag, err)
		// err includes any IO errors that happened during the walk.
		if err != nil {
			dispatch(blob{err: err, tag: reqTag})
		}
	}()
}

// uploadBatcher handles files below the small threshold which are buffered in-memory.
func (u *uploaderv2) uploadBatcher(ctx context.Context) {
	glog.V(3).Info("upload.batcher")
	bundle := make(uploadRequestBundle)
	bundleSize := u.uploadRequestBaseSize

	handle := func() {
		if len(bundle) < 1 {
			return
		}
		// Block the bundler if the concurrency limit is reached.
		if err := u.uploadSem.Acquire(ctx, 1); err != nil {
			// err is always ctx.Err(), so abort immediately.
			return
		}
		defer u.uploadSem.Release(1)

		go u.callBatchUpload(ctx, bundle)

		bundle = make(uploadRequestBundle)
		bundleSize = u.uploadRequestBaseSize
	}

	u.processorWg.Add(1)
	go func() {
		defer u.processorWg.Done()

		bundleTicker := time.NewTicker(u.uploadRpcConfig.BundleTimeout)
		defer bundleTicker.Stop()
		for {
			select {
			case b, ok := <-u.uploadBatcherChan:
				glog.V(3).Infof("upload.batcher.req: digest=%s, tag=%s", b.digest, b.tag)
				if !ok {
					return
				}

				r := &repb.BatchUpdateBlobsRequest_Request{
					Digest: b.digest,
					Data:   b.bytes,
				}
				rSize := proto.Size(r)

				// Reroute oversized blobs to the streamer.
				if rSize >= (u.uploadRpcConfig.BytesLimit - u.uploadRequestBaseSize) {
					u.uploadStreamerChan <- b
					continue
				}

				d := digest.NewFromProtoUnvalidated(b.digest)
				item, ok := bundle[d]
				if ok {
					// Duplicate tags are allowed to ensure the caller can match the number of responses to the number of requests.
					item.tags = append(item.tags, b.tag)
					continue
				}

				if bundleSize+rSize >= u.uploadRpcConfig.BytesLimit {
					handle()
				}

				item.tags = append(item.tags, b.tag)
				item.req = r
				bundle[d] = item
				bundleSize += rSize

				// Check length threshold.
				if len(bundle) >= u.uploadRpcConfig.ItemsLimit {
					handle()
					continue
				}
			case <-bundleTicker.C:
				handle()
			case <-ctx.Done():
				glog.V(3).Info("upload.batcher.cancel")
				return
			}
		}
	}()
}

func (u *uploaderv2) callBatchUpload(ctx context.Context, bundle uploadRequestBundle) {
	glog.V(3).Infof("upload.batcher.call: length=%d", len(bundle))
	u.workerWg.Add(1)
	defer u.workerWg.Done()

	req := &repb.BatchUpdateBlobsRequest{InstanceName: u.instanceName}
	req.Requests = make([]*repb.BatchUpdateBlobsRequest_Request, 0, len(bundle))
	for _, item := range bundle {
		req.Requests = append(req.Requests, item.req)
	}

	var uploaded []digest.Digest
	failed := make(map[digest.Digest]error)
	digestRetryCount := make(map[digest.Digest]int64)
	var res *repb.BatchUpdateBlobsResponse
	var err error
	ctx, ctxCancel := context.WithCancel(ctx)
	err = u.withTimeout(u.queryRpcConfig.Timeout, ctxCancel, func() error {
		return u.withRetry(ctx, u.uploadRpcConfig.RetryPolicy, func() error {
			// This call can have partial failures. Only retry retryable failed requests.
			res, err = u.cas.BatchUpdateBlobs(ctx, req)
			reqErr := err
			req.Requests = nil
			for _, r := range res.Responses {
				if err := status.FromProto(r.Status).Err(); err != nil {
					if retry.TransientOnly(err) {
						d := digest.NewFromProtoUnvalidated(r.Digest)
						req.Requests = append(req.Requests, bundle[d].req)
						digestRetryCount[d]++
						reqErr = err
						continue
					}
					failed[digest.NewFromProtoUnvalidated(r.Digest)] = err
					continue
				}
				uploaded = append(uploaded, digest.NewFromProtoUnvalidated(r.Digest))
			}
			return reqErr
		})
	})
	ctxCancel()

	// Report uploaded.
	for _, d := range uploaded {
		s := Stats{
			BytesRequested:      d.Size,
			TotalBytesMoved:     d.Size * digestRetryCount[d],
			EffectiveBytesMoved: d.Size,
			LogicalBytesBatched: d.Size,
			CacheMissCount:      1,
			BatchedCount:        1,
		}
		sCached := s.ToCacheHit()

		tags := bundle[d].tags
		u.uploadReqPubSub.mpub(
			UploadResponse{
				Digest: d,
				Stats:  s,
			},
			UploadResponse{
				Digest: d,
				Stats:  sCached,
			},
			tags...,
		)
		delete(bundle, d)
	}

	// Report individually failed requests.
	for d, dErr := range failed {
		if dErr != nil {
			dErr = err
		}
		if dErr != nil {
			dErr = errors.Join(ErrGRPC, dErr)
		}
		tags := bundle[d].tags
		s := Stats{
			BytesRequested:  d.Size,
			TotalBytesMoved: d.Size * digestRetryCount[d],
			CacheMissCount:  1,
			BatchedCount:    1,
		}
		sCached := s.ToCacheHit()
		u.uploadReqPubSub.mpub(
			UploadResponse{
				Digest: d,
				Stats:  s,
				Err:    dErr,
			}, UploadResponse{
				Digest: d,
				Stats:  sCached,
				Err:    dErr,
			},
			tags...,
		)
		delete(bundle, d)
	}

	if err == nil && len(bundle) == 0 {
		return
	}

	if err != nil {
		err = errors.Join(ErrGRPC, err)
	}

	// Report failed requests due to call failure.
	for d, item := range bundle {
		tags := item.tags
		s := Stats{
			BytesRequested:  d.Size,
			TotalBytesMoved: d.Size * digestRetryCount[d],
			CacheMissCount:  1,
			BatchedCount:    1,
		}
		sCached := s.ToCacheHit()
		u.uploadReqPubSub.mpub(
			UploadResponse{
				Digest: d,
				Stats:  s,
				Err:    err,
			},
			UploadResponse{
				Digest: d,
				Stats:  sCached,
				Err:    err,
			},
			tags...,
		)
	}
}

// uploadStreamer handles files above the small threshold.
// Unlike the batched call, presence check is not required for streaming files because the API
// handles this automatically: https://github.com/bazelbuild/remote-apis/blob/0cd22f7b466ced15d7803e8845d08d3e8d2c51bc/build/bazel/remote/execution/v2/remote_execution.proto#L250-L254
// For files above the large threshold, this method assumes the io and large io holds are
// already acquired and will release them accordingly.
// For other files, only an io hold is acquired and released in this method.
func (u *uploaderv2) uploadStreamer(ctx context.Context) {
	glog.V(3).Info("upload.streamer")
	u.processorWg.Add(1)
	go func() {
		defer u.processorWg.Done()
		digestTags := initSliceCache()
		for {
			select {
			case b, ok := <-u.uploadStreamerChan:
				glog.V(3).Infof("upload.streamer.req: digest=%s, tag=%s", b.digest, b.tag)
				if !ok {
					return
				}

				isLargeFile := b.reader != nil

				d := digest.NewFromProtoUnvalidated(b.digest)
				if l := digestTags.Append(d, b.tag); l > 1 {
					// Already in-flight. Release duplicate resources if it's a large file.
					glog.V(3).Infof("upload.streamer.req.unified: digest=%s, tag=%s", b.digest, b.tag)
					if isLargeFile {
						u.ioSem.Release(1)
						u.ioLargeSem.Release(1)
					}
					continue
				}

				// Block the streamer if the gRPC call is being throttled.
				if err := u.streamSem.Acquire(ctx, 1); err != nil {
					// err is always ctx.Err()
					if isLargeFile {
						u.ioSem.Release(1)
						u.ioLargeSem.Release(1)
					}
					return
				}

				var name string
				if b.digest.SizeBytes >= u.ioCfg.CompressionSizeThreshold {
					glog.V(3).Infof("upload.streamer.req.compress: digest=%s, tag=%s", b.digest, b.tag)
					name = MakeCompressedWriteResourceName(u.instanceName, b.digest.Hash, b.digest.SizeBytes)
				} else {
					name = MakeWriteResourceName(u.instanceName, b.digest.Hash, b.digest.SizeBytes)
				}

				u.workerWg.Add(1)
				go func() (stats Stats, err error) {
					defer u.workerWg.Done()
					defer u.streamSem.Release(1)
					defer func() {
						glog.V(3).Infof("upload.streamer.call.done: digest=%s, tag=%s", b.digest, b.tag)
						tagsRaw := digestTags.LoadAndDelete(d)
						tags := make([]tag, 0, len(tagsRaw))
						for _, t := range tagsRaw {
							tags = append(tags, t.(tag))
						}
						sCached := stats.ToCacheHit()
						u.uploadReqPubSub.mpub(
							UploadResponse{
								Digest: d,
								Stats:  stats,
								Err:    err,
							},
							UploadResponse{
								Digest: d,
								Stats:  sCached,
								Err:    err,
							},
							tags...,
						)
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
						if errSem := u.ioSem.Acquire(ctx, 1); errSem != nil {
							return
						}
						defer u.ioSem.Release(1)

						f, errOpen := os.Open(b.path)
						if errOpen != nil {
							return Stats{BytesRequested: b.digest.SizeBytes}, errors.Join(ErrIO, errOpen)
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

					glog.V(3).Infof("upload.streamer.call: digest=%s, tag=%s", b.digest, b.tag)
					s, err := u.writeBytes(ctx, name, reader, b.digest.SizeBytes, 0, true)
					return *s, err
				}()

			case <-ctx.Done():
				glog.V(3).Info("upload.streamer.cancel")
				return
			}
		}
	}()
}
