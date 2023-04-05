package cas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	ep "github.com/bazelbuild/remote-apis-sdks/go/pkg/io/exppath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
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
	Path           ep.Abs
	SymlinkOptions slo.Options
	ShouldSkip     ep.Filter
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
}

func (u *batchingUploader) WriteBytes(ctx context.Context, name string, r io.Reader, size int64, offset int64) (*Stats, error) {
	return u.writeBytes(ctx, name, r, size, offset, true)
}

func (u *batchingUploader) WriteBytesPartial(ctx context.Context, name string, r io.Reader, size int64, offset int64) (*Stats, error) {
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

func (u *batchingUploader) Upload(ctx context.Context, paths []ep.Abs, slo slo.Options, shouldSkip ep.Filter) ([]digest.Digest, *Stats, error) {
	if len(paths) < 1 {
		return nil, nil, nil
	}

	// This implementation converts the underlying nonblocking implementation into a blocking one.
	// A separate goroutine is used to push the requests into the processor.
	// The receiving code blocks the goroutine of the call until all responses are received or the context is canceled.

	ctxUploadCaller, ctxUploaderCallerCancel := context.WithCancel(ctx)
	defer ctxUploaderCallerCancel()

	tag, resChan := u.uploadCallerPubSub.sub(ctxUploadCaller)
	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()
		for _, p := range paths {
			select {
			case <-ctx.Done():
				return
			case u.uploadChan <- UploadRequest{Path: p, SymlinkOptions: slo, ShouldSkip: shouldSkip, tag: tag}:
			}
		}
	}()

	stats := &Stats{}
	var uploaded []digest.Digest
	var err error
	// The channel will be closed if the context is cancelled so no need to watch the context here.
	for rawR := range resChan {
		r := rawR.(UploadResponse)
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

func (u *streamingUploader) Upload(context.Context, <-chan ep.Abs, slo.Options, ep.Filter) <-chan UploadResponse {
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
	// Set up a pipe for presence checking.
	queryChan := make(chan digest.Digest)
	queryResChan := u.missingBlobsStreamer(ctx, queryChan)
	digestBlobs := initSliceCache()

	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()
		// The channel is closed when the context is cancelled so no need to watch the context here.
		for queryRes := range queryResChan {
			blobs := digestBlobs.LoadAndDelete(queryRes.Digest)

			if queryRes.Err != nil {
				tags := make([]tag, 0, len(blobs))
				for _, b := range blobs {
					tags = append(tags, b.(blob).tag)
				}
				u.uploadCallerPubSub.pub(UploadResponse{
					Digest: queryRes.Digest,
					Stats:  Stats{BytesRequested: queryRes.Digest.Size},
					Err:    queryRes.Err,
				}, tags...)
				continue
			}

			if queryRes.Missing {
				// Let the upload processors handle unification.
				for _, b := range blobs {
					u.uploadBatcherChan <- b.(blob)
				}
				continue
			}

			// Notify callers of a cache hit.
			tags := make([]tag, 0, len(blobs))
			for _, b := range blobs {
				tags = append(tags, b.(blob).tag)
			}
			u.uploadCallerPubSub.pub(UploadResponse{
				Digest: queryRes.Digest,
				Stats: Stats{
					BytesRequested:     queryRes.Digest.Size,
					LogicalBytesCached: queryRes.Digest.Size,
					CacheHitCount:      1,
				},
			}, tags...)
		}
	}()

	// A helper for dispatching from appropriate call sites.
	dispatch := func(b blob) {
		switch {
		case len(b.bytes) > 0:
			d := digest.NewFromProtoUnvalidated(b.digest)
			digestBlobs.Append(d, b)
			queryChan <- d
		default:
			u.uploadStreamerChan <- b
		}
	}

	// Digestion and dispatching loop.
	for {
		select {
		case req, ok := <-u.uploadChan:
			if !ok {
				return
			}

			if err := u.walkSem.Acquire(ctx, 1); err != nil {
				// err is always ctx.Err()
				return
			}
			// walkSem is released downstream.
			u.digestAndUploadTree(ctx, req.Path, req.ShouldSkip, req.SymlinkOptions, req.tag, dispatch)

		case <-ctx.Done():
			return
		}
	}
}

// digestAndUploadTree is a non-blocking call that initiates a file system walk to digest and dispatch the files for upload.
func (u *uploaderv2) digestAndUploadTree(ctx context.Context, root ep.Abs, filter ep.Filter, slo slo.Options, callerTag tag, dispatch func(blob)) {
	// Create a subscriber to receive upload responses for this particular request.
	ctxReq, ctxReqCancel := context.WithCancel(ctx)
	defer ctxReqCancel()
	reqTag, ch := u.uploadReqPubSub.sub(ctxReq)

	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()
		for r := range ch {
			// Redirect the response to the caller.
			u.uploadCallerPubSub.pub(r, callerTag)
		}
	}()

	u.walkWg.Add(1)
	go func() {
		defer u.walkWg.Done()
		defer u.walkSem.Release(1)

		// TODO: implement walker.
		stats := Stats{}
		walker.DepthFirst(root, u.ioCfg.ConcurrentWalkerVisits, func(path ep.Abs, info fs.FileInfo, err error) (walker.NextStep, error) {
			select {
			case <-ctx.Done():
				return walker.Cancel, nil
			default:
			}

			if err != nil {
				return walker.Cancel, nil
			}

			key := path.String() + filter.String()
			parentKey := path.Dir().String() + filter.String()

			// Pre-access.
			if info == nil {
				// Excluded.
				if filter.Path(path.String()) {
					return walker.Skip, nil
				}

				// A cache hit here indicates a cyclic symlink or multiple callers attempting to upload the exact same path with an identical filter.
				// In both cases, deferring is the right call. Once the upload is processed, all uploaders will revisit the path to get the processing result.
				if rawR, ok := u.ioCfg.UploadCache.Load(key); ok {
					// Defer if in-flight.
					if rawR == nil {
						return walker.Defer, nil
					}
					// Update the stats to reflect a cache hit before publishing.
					r := rawR.(UploadResponse)
					r.Stats = r.Stats.ToCacheHit()
					u.uploadCallerPubSub.pub(r, callerTag)
					return walker.Skip, nil
				}

				// Access it.
				return walker.Continue, nil
			}

			// Excluded.
			if filter.File(path.String(), info.Mode()) {
				return walker.Skip, nil
			}

			// Mark the file as being in-flight.
			u.ioCfg.UploadCache.Store(key, nil)
			stats.DigestCount += 1
			switch {
			case info.Mode()&fs.ModeSymlink == fs.ModeSymlink:
				stats.InputSymlinkCount += 1
				node, nextStep, err := u.digestSymlink(root, path, slo)
				if node != nil {
					u.dirChildren.Append(parentKey, node)
				}
				return nextStep, err

			case info.Mode().IsDir():
				stats.InputDirCount += 1
				// All the descendants have already been visited (DFS).
				node, b, nextStep, err := u.digestDirectory(path, u.dirChildren.Load(key))
				if node != nil {
					u.dirChildren.Append(parentKey, node)
				}
				if b != nil {
					dispatch(blob{digest: node.Digest, bytes: b, tag: reqTag})
				}
				return nextStep, err

			case info.Mode().IsRegular():
				stats.InputFileCount += 1
				node, blb, nextStep, err := u.digestFile(ctx, path, info)
				if node != nil {
					u.dirChildren.Append(parentKey, node)
				}
				if blb != nil {
					blb.tag = reqTag
					dispatch(*blb)
				}
				return nextStep, err

			default:
				// Skip everything else (e.g. sockets and pipes).
				return walker.Skip, nil
			}
		})
	}()
}

// digestSymlink might need to follow target and/or construct a symlink node.
func (u *uploaderv2) digestSymlink(root ep.Abs, path ep.Abs, slo slo.Options) (*repb.SymlinkNode, walker.NextStep, error) {
	// Replace symlink with target.
	if slo.Resolve() {
		return nil, walker.Replace, nil
	}
	if slo.ResolveExternal() {
		if _, err := ep.Descendant(root, path); err != nil {
			return nil, walker.Replace, nil
		}
	}

	target, err := os.Readlink(path.String())
	if err != nil {
		return nil, walker.Cancel, err
	}

	// Cannot access the target since it might be relative to the symlink directory, not the cwd of the process.
	var targetRelative string
	if filepath.IsAbs(target) {
		targetRelative, err = filepath.Rel(path.Dir().String(), target)
		if err != nil {
			return nil, walker.Cancel, err
		}
	} else {
		targetRelative = target
		target = filepath.Join(path.Dir().String(), targetRelative)
	}

	if slo.NoDangling() {
		_, err := os.Lstat(target)
		if err != nil {
			return nil, walker.Cancel, err
		}
	}

	var node *repb.SymlinkNode
	if slo.Preserve() {
		if err != nil {
			return nil, walker.Cancel, err
		}
		node = &repb.SymlinkNode{
			Name:   path.Base().String(),
			Target: targetRelative,
		}
	}

	if slo.IncludeTarget() {
		return node, walker.Continue, nil
	}
	return node, walker.Skip, nil
}

// digestDirectory constructs a hash-deterministic directory node and returns it along with the corresponding bytes of the directory proto.
func (u *uploaderv2) digestDirectory(path ep.Abs, children []any) (*repb.DirectoryNode, []byte, walker.NextStep, error) {
	dir := &repb.Directory{}
	node := &repb.DirectoryNode{
		Name: path.Base().String(),
	}
	for _, child := range children {
		switch n := child.(type) {
		case *repb.FileNode:
			dir.Files = append(dir.Files, n)
		case *repb.DirectoryNode:
			dir.Directories = append(dir.Directories, n)
		case *repb.SymlinkNode:
			dir.Symlinks = append(dir.Symlinks, n)
		}
	}
	// Sort children to get a deterministic hash.
	sort.Slice(dir.Files, func(i, j int) bool {
		return dir.Files[i].Name < dir.Files[j].Name
	})
	sort.Slice(dir.Directories, func(i, j int) bool {
		return dir.Directories[i].Name < dir.Directories[j].Name
	})
	sort.Slice(dir.Symlinks, func(i, j int) bool {
		return dir.Symlinks[i].Name < dir.Symlinks[j].Name
	})
	b, err := proto.Marshal(dir)
	if err != nil {
		return nil, nil, walker.Cancel, err
	}
	d := digest.NewFromBlob(b)
	node.Digest = d.ToProto()
	return node, b, walker.Continue, nil
}

// digestFile constructs a file node and returns it along with the blob to be dispatched.
// If the file size exceeds the large threshold, both IO and large IO holds are retained upon returning and it's
// the responsibility of the streamer to release them.
// This allows the walker to collect the digest and proceed without having to wait for the streamer.
func (u *uploaderv2) digestFile(ctx context.Context, path ep.Abs, info fs.FileInfo) (node *repb.FileNode, blb *blob, nextStep walker.NextStep, err error) {
	if err := u.ioSem.Acquire(ctx, 1); err != nil {
		return nil, nil, walker.Cancel, nil
	}
	defer func() {
		// Keep the IO hold if the streamer is going to assume ownership of it.
		if blb.reader != nil {
			return
		}
		u.ioSem.Release(1)
	}()

	node = &repb.FileNode{
		Name:         path.Base().String(),
		IsExecutable: info.Mode()&0100 != 0,
	}
	if info.Size() <= u.ioCfg.SmallFileSizeThreshold {
		f, err := os.Open(path.String())
		if err != nil {
			return nil, nil, walker.Cancel, err
		}
		defer func() {
			if errClose := f.Close(); errClose != nil {
				err = errors.Join(errClose, err)
			}
		}()

		b, err := io.ReadAll(f)
		if err != nil {
			return nil, nil, walker.Cancel, err
		}
		node.Digest = digest.NewFromBlob(b).ToProto()
		return node, &blob{digest: node.Digest, bytes: b}, walker.Continue, nil
	}

	if info.Size() < u.ioCfg.LargeFileSizeThreshold {
		d, err := digest.NewFromFile(path.String())
		if err != nil {
			return nil, nil, walker.Cancel, err
		}
		node.Digest = d.ToProto()
		return node, &blob{digest: node.Digest, path: path.String()}, walker.Continue, nil
	}

	if err := u.ioLargeSem.Acquire(ctx, 1); err != nil {
		return nil, nil, walker.Cancel, nil
	}
	defer func() {
		if err != nil {
			u.ioLargeSem.Release(1)
		}
	}()

	f, err := os.Open(path.String())
	if err != nil {
		return nil, nil, walker.Cancel, err
	}
	defer func() {
		if err != nil {
			errClose := f.Close()
			if errClose != nil {
				err = errors.Join(errClose, err)
			}
		}
	}()

	d, err := digest.NewFromReader(f)
	if err != nil {
		return nil, nil, walker.Cancel, err
	}

	// Reset the offset for the streamer.
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, nil, walker.Cancel, err
	}

	node.Digest = d.ToProto()
	// The streamer is responsible for closing the file and releasing both ioSem and ioLargeSem.
	return node, &blob{digest: node.Digest, reader: f}, walker.Continue, nil
}

// uploadBatcher handles files below the small threshold which are buffered in-memory.
func (u *uploaderv2) uploadBatcher(ctx context.Context) {
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

	bundleTicker := time.NewTicker(u.uploadRpcConfig.BundleTimeout)
	defer bundleTicker.Stop()

	for {
		select {
		case b, ok := <-u.uploadBatcherChan:
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
			return
		}
	}
}

func (u *uploaderv2) callBatchUpload(ctx context.Context, bundle uploadRequestBundle) {
	u.grpcWg.Add(1)
	defer u.grpcWg.Done()

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
		t := u.uploadReqPubSub.pubOnce(UploadResponse{
			Digest: d,
			Stats:  s,
		}, tags...)
		u.uploadReqPubSub.pub(UploadResponse{
			Digest: d,
			Stats:  sCached,
		}, excludeTag(tags, t)...)
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
		t := u.uploadReqPubSub.pubOnce(UploadResponse{
			Digest: d,
			Stats:  s,
			Err:    dErr,
		}, tags...)
		u.uploadReqPubSub.pub(UploadResponse{
			Digest: d,
			Stats:  sCached,
			Err:    dErr,
		}, excludeTag(tags, t)...)
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
		t := u.uploadReqPubSub.pubOnce(UploadResponse{
			Digest: d,
			Stats:  s,
			Err:    err,
		}, tags...)
		u.uploadReqPubSub.pub(UploadResponse{
			Digest: d,
			Stats:  sCached,
			Err:    err,
		}, excludeTag(tags, t)...)
	}
}

// uploadStreamer handles files above the small threshold.
// Unlike the batched call, presence check is not required for streaming files because the API
// handles this automatically: https://github.com/bazelbuild/remote-apis/blob/0cd22f7b466ced15d7803e8845d08d3e8d2c51bc/build/bazel/remote/execution/v2/remote_execution.proto#L250-L254
// For files above the large threshold, this method assumes the io and large io holds are
// already acquired and will release them accordingly.
// For other files, only an io hold is acquired and released in this method.
func (u *uploaderv2) uploadStreamer(ctx context.Context) {
	digestTags := initSliceCache()
	for {
		select {
		case b, ok := <-u.uploadStreamerChan:
			if !ok {
				return
			}

			d := digest.NewFromProtoUnvalidated(b.digest)
			if l := digestTags.Append(d, b.tag); l > 1 {
				// Already in-flight.
				continue
			}

			// Block the streamer if the gRPC call is being throttled.
			if err := u.streamSem.Acquire(ctx, 1); err != nil {
				// err is always ctx.Err()
				return
			}

			var name string
			if b.digest.SizeBytes >= u.ioCfg.CompressionSizeThreshold {
				name = MakeCompressedWriteResourceName(u.instanceName, b.digest.Hash, b.digest.SizeBytes)
			} else {
				name = MakeWriteResourceName(u.instanceName, b.digest.Hash, b.digest.SizeBytes)
			}

			go func() (stats Stats, err error) {
				defer u.streamSem.Release(1)
				defer func() {
					tagsRaw := digestTags.LoadAndDelete(d)
					tags := make([]tag, 0, len(tagsRaw))
					for _, t := range tagsRaw {
						tags = append(tags, t.(tag))
					}
					sCached := stats.ToCacheHit()
					t := u.uploadReqPubSub.pubOnce(UploadResponse{
						Digest: d,
						Stats:  stats,
						Err:    err,
					}, tags...)
					u.uploadReqPubSub.pubOnce(UploadResponse{
						Digest: d,
						Stats:  sCached,
						Err:    err,
					}, excludeTag(tags, t)...)
				}()

				var reader io.Reader

				// In the off chance that the blob is mis-constructed (more than one content field is set), start
				// with b.reader to ensure any held locks are released.
				switch {
				// Large file.
				case b.reader != nil:
					reader = b.reader
					// IO holds were acquired during digestion for large files and are expected to be released here.
					defer u.ioSem.Release(1)
					defer u.ioLargeSem.Release(1)

				// Medium file.
				case len(b.path) > 0:
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

				// Small file or a proto message (node).
				case len(b.bytes) > 0:
					reader = bytes.NewReader(b.bytes)
				}

				s, err := u.writeBytes(ctx, name, reader, b.digest.SizeBytes, 0, true)
				return *s, err
			}()

		case <-ctx.Done():
		}
	}
}
