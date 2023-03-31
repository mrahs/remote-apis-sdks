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
// Only one of bytes, path or reader is used, in that order.
// If the bytes field is set, the blob is either batched or streamed based on gRPC size limits.
// If the path or reader field is set, the blob is streamed.
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

	tag, resChan := u.uploadPubSub.subscribe(ctxUploadCaller)
	go func() {
		for _, p := range paths {
			select {
			case <-ctx.Done():
				return
			case u.uploadChan <- UploadRequest{Path: p, SymlinkOptions: slo, ShouldSkip: shouldSkip, tag: tag}:
			}
		}
	}()

	stats := &Stats{}
	var missing []digest.Digest
	var err error
	total := len(paths)
	i := 0
	for rawR := range resChan {
		r := rawR.(UploadResponse)
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
	digestBlob := sync.Map{}

	u.processorWg.Add(1)
	go func() {
		defer u.processorWg.Done()
		// Context handling is done upstream in the missing blobs call.
		for queryRes := range queryResChan {
			if queryRes.Err != nil {
				// TODO: notify
			}
			if queryRes.Missing {
				if b, ok := digestBlob.Load(queryRes.Digest); ok {
					u.uploadBatcherChan <- b.(blob)
				}
			}
			// TODO: notify cache hit
		}
	}()

	// A helper for dispatching from appropriate sites.
	dispatch := func(b blob) {
		switch {
		case len(b.bytes) == 0 && b.reader == nil && b.path == "":
			// TODO: notify cache hit
		case len(b.bytes) > 0:
			// TODO: bundle
			d := digest.NewFromProtoUnvalidated(b.digest)
			if _, ok := digestBlob.LoadOrStore(d, b); !ok {
				queryChan <- d
			}
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

			// Create a tag for this request and associate it with the caller.
			// TODO: req pubsub
			// Start the walk.
			u.walkWg.Add(1)
			go func() {
				defer u.walkWg.Done()
				defer u.walkSem.Release(1)

				// TODO: implement walker.
				stats := Stats{}
				walker.DepthFirst(req.Path, u.ioCfg.ConcurrentWalkerVisits, func(path ep.Abs, info fs.FileInfo, err error) (walker.NextStep, error) {
					select {
					case <-ctx.Done():
						return walker.Cancel, nil
					default:
					}

					if err != nil {
						return walker.Cancel, nil
					}

					key := path.String() + req.ShouldSkip.String()
					parentKey := path.Dir().String() + req.ShouldSkip.String()

					// Pre-access.
					if info == nil {
						// Excluded.
						if req.ShouldSkip.Path(path.String()) {
							return walker.Skip, nil
						}

						// A cache hit here indicates a cyclic symlink or two callers attempting to upload the exact same path.
						// In both cases, deferring is the right call. Once the upload is processed, both uploaders, or the same one in the first case, will
						// revisit the path to get the processing result.
						// WARN: this can still cause the same file to be digested twice at the same time.
						// Consider caching a notifier that other goroutines can block on while waiting for the value to be computed.
						if _, ok := u.ioCfg.Cache.Load(key); ok {
							return walker.Defer, nil
						}

						// Access it.
						return walker.Continue, nil
					}

					// Excluded.
					if req.ShouldSkip.File(path.String(), info.Mode()) {
						return walker.Skip, nil
					}

					stats.DigestCount += 1
					switch {
					case info.Mode()&fs.ModeSymlink == fs.ModeSymlink:
						stats.InputSymlinkCount += 1
						node, nextStep, err := u.digesetSymlink(req.Path, path, req.SymlinkOptions)
						if node != nil {
							u.ioCfg.Cache.Store(key, node)
							u.dirChildren.Append(parentKey, node)
						}
						return nextStep, err

					case info.Mode().IsDir():
						stats.InputDirCount += 1
						// All the descendants have already been visited since it's a DFS traversal.
						node, b, nextStep, err := u.digestDirectory(path, u.dirChildren.Load(key))
						if node != nil {
							u.ioCfg.Cache.Store(key, node)
							u.dirChildren.Append(parentKey, node)
						}
						if b != nil {
							dispatch(blob{digest: node.Digest, bytes: b, tag: req.tag})
						}
						return nextStep, err

					case info.Mode().IsRegular():
						stats.InputFileCount += 1
						node, blb, nextStep, err := u.digestFile(ctx, path, info)
						if node != nil {
							u.ioCfg.Cache.Store(key, node)
							u.dirChildren.Append(parentKey, node)
						}
						if blb != nil {
							blb.tag = req.tag
							dispatch(*blb)
						}
						return nextStep, err

					default:
						// Skip everything else (e.g. sockets and pipes).
						return walker.Skip, nil
					}
				},
				)
			}()
		case <-ctx.Done():
			return
		}
	}
}

// digestSymlink might need to follow target and/or construct a symlink node.
func (u *uploaderv2) digesetSymlink(root ep.Abs, path ep.Abs, slo slo.Options) (*repb.SymlinkNode, walker.NextStep, error) {
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
func (u *uploaderv2) digestDirectory(path ep.Abs, children []interface{}) (*repb.DirectoryNode, []byte, walker.NextStep, error) {
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

	// TODO: should collate responses by UploadRequest

	// Report uploaded.
	for _, d := range uploaded {
		u.uploadPubSub.publish(UploadResponse{
			Digest: d,
			Stats: Stats{
				BytesRequested:      d.Size,
				TotalBytesMoved:     d.Size + digestRetryCount[d],
				EffectiveBytesMoved: d.Size,
				LogicalBytesBatched: d.Size,
				CacheMissCount:      1,
				BatchedCount:        1,
			},
		}, bundle[d].tags...)
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
		u.uploadPubSub.publish(UploadResponse{
			Digest: d,
			Stats: Stats{
				BytesRequested:  d.Size,
				TotalBytesMoved: d.Size + digestRetryCount[d],
				CacheMissCount:  1,
				BatchedCount:    1,
			},
			Err: dErr,
		}, bundle[d].tags...)
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
		u.uploadPubSub.publish(UploadResponse{
			Digest: d,
			Stats: Stats{
				BytesRequested:  d.Size,
				TotalBytesMoved: d.Size + digestRetryCount[d],
				CacheMissCount:  1,
				BatchedCount:    1,
			},
			Err: err,
		}, item.tags...)
	}
}

// uploadStreamer handles files above the small threshold.
// Unlike the batched call, presence check is not required for streaming files because the API
// handles this automatically: https://github.com/bazelbuild/remote-apis/blob/0cd22f7b466ced15d7803e8845d08d3e8d2c51bc/build/bazel/remote/execution/v2/remote_execution.proto#L250-L254
// For files above the large threshold, this method assumes the io and large io holds are
// already acquired and will release them approperiately.
// For other files, only an io hold is acquired and released in this method.
func (u *uploaderv2) uploadStreamer(ctx context.Context) {
	// TODO: bundle
	for {
		select {
		case b, ok := <-u.uploadStreamerChan:
			if !ok {
				return
			}

			// Block the streamer if the gRPC call is being throttled.
			if err := u.streamSem.Acquire(ctx, 1); err != nil {
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
					u.uploadPubSub.publish(UploadResponse{
						Digest: digest.NewFromProtoUnvalidated(b.digest),
						Stats:  stats,
						Err:    err,
					}, b.tag)
				}()

				var reader io.Reader

				if len(b.bytes) > 0 {
					reader = bytes.NewReader(b.bytes)
				}

				if len(b.path) > 0 {
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
				}

				if b.reader != nil {
					reader = b.reader
					// IO holds were acquired during digestion for large files and are expected to be released here.
					defer u.ioSem.Release(1)
					defer u.ioLargeSem.Release(1)
				}

				s, err := u.writeBytes(ctx, name, reader, b.digest.SizeBytes, 0, true)
				return *s, err
			}()

		case <-ctx.Done():
		}
	}
}
