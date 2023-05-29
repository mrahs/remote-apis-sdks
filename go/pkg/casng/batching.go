package casng

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"github.com/klauspost/compress/zstd"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/protobuf/proto"
)

// MissingBlobs queries the CAS for digests and returns a slice of the missing ones.
//
// This method is useful when a large number of digests is already known. For other use cases, consider the streaming uploader.
// This method does not use internal processors and does not use the uploader's context. It is safe to use even if the uploader's context is cancelled.
//
// Cancelling the context will cancel retries, but not a pending request which will be cancelled upon timeout.
// The digests are batched based on ItemLimits of the gRPC config. BytesLimit and BundleTimeout are not used in this method.
// Errors from a batch do not affect other batches, but all digests from such bad batches will be reported as missing by this call.
// In other words, if an error is returned, any digest that is not in the returned slice is not missing.
// If no error is returned, the returned slice contains all the missing digests.
func (u *BatchingUploader) MissingBlobs(ctx context.Context, digests []digest.Digest) ([]digest.Digest, error) {
	log.V(1).Infof("[casng] batch.query: len=%d", len(digests))
	if len(digests) == 0 {
		return nil, nil
	}

	// Deduplicate and split into batches.
	var batches [][]*repb.Digest
	var batch []*repb.Digest
	dgSet := make(map[digest.Digest]struct{})
	for _, d := range digests {
		if _, ok := dgSet[d]; ok {
			continue
		}
		dgSet[d] = struct{}{}
		batch = append(batch, d.ToProto())
		if len(batch) >= u.queryRPCCfg.ItemsLimit {
			batches = append(batches, batch)
			batch = nil
		}
	}
	if len(batch) > 0 {
		batches = append(batches, batch)
	}
	if len(batches) == 0 {
		return nil, nil
	}
	log.V(1).Infof("[casng] batch.query.deduped: len=%d", len(dgSet))

	// Call remote.
	missing := make([]digest.Digest, 0, len(dgSet))
	var err error
	var res *repb.FindMissingBlobsResponse
	var errRes error
	req := &repb.FindMissingBlobsRequest{InstanceName: u.instanceName}
	for _, batch := range batches {
		req.BlobDigests = batch
		ctx, ctxCancel := context.WithCancel(ctx)
		errRes = u.withTimeout(u.queryRPCCfg.Timeout, ctxCancel, func() error {
			return u.withRetry(ctx, u.queryRPCCfg.RetryPredicate, u.queryRPCCfg.RetryPolicy, func() error {
				res, errRes = u.cas.FindMissingBlobs(ctx, req)
				return errRes
			})
		})
		if res == nil {
			res = &repb.FindMissingBlobsResponse{}
		}
		if errRes != nil {
			err = errors.Join(errRes, err)
			res.MissingBlobDigests = batch
		}
		for _, d := range res.MissingBlobDigests {
			missing = append(missing, digest.NewFromProtoUnvalidated(d))
		}
	}
	log.V(1).Infof("[casng] batch.query.done: missing=%d", len(missing))

	if err != nil {
		err = errors.Join(ErrGRPC, err)
	}
	return missing, err
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
	log.V(1).Infof("[casng] upload.write_bytes: name=%s, size=%d, offset=%d, finish=%t", name, size, offset)
	return u.writeBytes(ctx, name, r, size, offset, true)
}

// WriteBytesPartial is the same as WriteBytes, but does not notify the server to finalize the resource name.
func (u *BatchingUploader) WriteBytesPartial(ctx context.Context, name string, r io.Reader, size int64, offset int64) (Stats, error) {
	log.V(1).Infof("[casng] upload.write_bytes_partial: name=%s, size=%d, offset=%d, finish=%t", name, size, offset)
	return u.writeBytes(ctx, name, r, size, offset, false)
}

func (u *uploader) writeBytes(ctx context.Context, name string, r io.Reader, size int64, offset int64, finish bool) (Stats, error) {
	log.V(2).Infof("[casng] upload.write_bytes.start: name=%s, size=%d, offset=%d, finish=%t", name, size, offset, finish)
	startTime := time.Now()
	defer func() {
		log.V(2).Infof("[casng] upload.write_bytes.done: duration=%v, name=%s, size=%d, offset=%d, finish=%t", time.Since(startTime), name, size, offset, finish)
	}()

	var stats Stats
	if !u.streamThrottle.acquire(ctx) {
		return stats, ctx.Err()
	}
	defer u.streamThrottle.release()

	// Read raw bytes if compression is disabled.
	src := r

	// If compression is enabled, plug in the encoder via a pipe.
	var errEnc error
	var nRawBytes int64 // Track the actual number of the consumed raw bytes.
	var encWg sync.WaitGroup
	var withCompression bool // Used later to ensure the pipe is closed.
	if size >= u.ioCfg.CompressionSizeThreshold {
		log.V(2).Infof("[casng] upload.write_bytes.compressing: name=%s, size=%d", name, size)
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
		errStream := u.withTimeout(u.streamRPCCfg.Timeout, ctxCancel, func() error {
			return u.withRetry(ctx, u.streamRPCCfg.RetryPredicate, u.streamRPCCfg.RetryPolicy, func() error {
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

// Upload processes reqs for upload. Blobs that already exist in the CAS are not uploaded.
// Additionally, any path that is not a regular file, a directory or a symlink file is skipped (e.g. sockets and pipes).
// For requests with non-empty Bytes fields, only the Content field is used. In that case, the Path field is ignored.
//
// Cancelling ctx gracefully aborts the upload process.
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
func (u *BatchingUploader) Upload(ctx context.Context, reqs ...UploadRequest) ([]digest.Digest, Stats, error) {
	log.V(1).Infof("[casng] upload: %d requests", len(reqs))
	defer log.V(1).Infof("[casng] upload.done")

	var stats Stats

	if len(reqs) == 0 {
		return nil, stats, nil
	}

	var undigested []UploadRequest
	digested := make(map[digest.Digest]UploadRequest)
	var digests []digest.Digest
	for _, r := range reqs {
		if r.Digest.Hash == "" {
			undigested = append(undigested, r)
			continue
		}
		digested[r.Digest] = r
		digests = append(digests, r.Digest)
	}
	missing, err := u.MissingBlobs(ctx, digests)
	if err != nil {
		return nil, stats, err
	}
	log.V(1).Infof("[casng] upload: missing=%d, undigested=%d", len(missing), len(undigested))

	reqs = undigested
	for _, d := range missing {
		reqs = append(reqs, digested[d])
		delete(digested, d)
	}
	for d := range digested {
		stats.BytesRequested += d.Size
		stats.LogicalBytesCached += d.Size
		stats.CacheHitCount += 1
		stats.DigestCount += 1
	}
	if len(reqs) == 0 {
		log.V(1).Info("[casng] upload: nothing is missing")
		return nil, stats, nil
	}

	log.V(1).Infof("[casng] upload: uploading %d blobs", len(reqs))
	ch := make(chan UploadRequest)
	resCh := u.streamPipe(ctx, ch)

	u.clientSenderWg.Add(1)
	go func() {
		defer close(ch) // let the streamer terminate.
		defer u.clientSenderWg.Done()

		log.V(1).Info("[casng] upload.sender.start")
		defer log.V(1).Info("[casng] upload.sender.stop")

		for _, r := range reqs {
			r.ctx = ctx
			select {
			case ch <- r:
			case <-u.ctx.Done():
				return
			}
		}
	}()

	var uploaded []digest.Digest
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

// UploadTree assumes reqs share localPrefix and appends to them any intermediate directories up to and excluding execRoot.
// Requests must not have digests to ensure proper construction of the tree through the digsetion process.
// Requests must share the local prefix and the filter.
//
// remotePrefix replaces localPrefix when handling paths, which means the merkle tree will include all the directories between execRoot and remotePrerix.
func (u *BatchingUploader) UploadTree(ctx context.Context, execRoot, localPrefix, remotePrefix impath.Absolute, reqs ...UploadRequest) (rootDigest digest.Digest, uploaded []digest.Digest, stats Stats, err error) {
	log.V(2).Infof("[casng] upload.tree: reqs=%d", len(reqs))

	if len(reqs) == 0 {
		return
	}

	filterID := reqs[0].Exclude.String()
	for _, r := range reqs {
		if r.Exclude.String() != filterID {
			err = fmt.Errorf("cannot create a tree from requests with different exclusion filters: %q and %q", filterID, r.Exclude.String())
			return
		}
	}

	// Upload the requests first to digest the files and cache the nodes.
	uploaded, stats, err = u.Upload(ctx, reqs...)
	if err != nil {
		return
	}

	// Upload the shared ancestor nodes.

	// This block creates a flattened tree of the paths in reqs rooted at execRoot.
	// Each key is an absolute path to a node in the tree and its value is a list of absolute paths that any of them can be a key as well.
	// Example: /a: [/a/b /a/c], /a/b: [/a/b/foo.go], /a/c: [/a/c/bar.go]
	reqPaths := make([]string, 0, len(reqs)) // TODO: logs
	// dirChildren associates a remote directory with a list of its children.
	dirChildren := make(map[impath.Absolute]map[impath.Absolute]proto.Message)
	for _, req := range reqs {
		// Each request in reqs must correspond to a cached node.
		node := u.Node(req)
		if node == nil {
			err = fmt.Errorf("cannot construct the merkle tree with a missing node for path %q", req.Path)
			return
		}

		// Every path must be relative execRoot (which means the remote working directory is included in the merkle tree).
		remotePath, errIm := req.Path.ReplacePrefix(localPrefix, remotePrefix)
		if errIm != nil {
			err = errIm
			return
		}
		reqPaths = append(reqPaths, req.Path.String()) // TODO: logs
		// Add this leaf node to its ancestors.
		parent := remotePath
		for {
			remotePath = parent
			parent = parent.Dir()
			children := dirChildren[parent]
			if children == nil {
				children = make(map[impath.Absolute]proto.Message)
				dirChildren[parent] = children
			}
			// If the parent already has this child, then no need to traverse up the tree again.
			_, ancestrySeen := children[remotePath]
			// This will nil a previously set node which is required to regenerate directory nodes that get new children in this loop, which
			// would happen if there are virtual inputs or multiple reqs with shared ancestors in the reqs set.
			// For example, if /a/b was digested during the first upload above, but /a/b/c was a virtual directory, the cached node for /a/b would not have /a/b/c as a child.
			// Another example: if /a/b and /a/b/c/foo.go were in the reqs set, but the filter would exclude /a/b/c except for foo.go because it's in a separate request, then /a/b would be missing /a/b/c/foo.go from its descendants.
			children[remotePath] = node
			// Make sure every parent has a nil node to ensure it gets regenerated later.
			node = nil

			// Do not go beyond the root. Also stop if ancestors are already processed.
			if parent.String() == execRoot.String() || ancestrySeen {
				break
			}
		}
	}

	// TODO: logs
	dirs := make([]string, 0, len(dirChildren))
	for p := range dirChildren {
		dirs = append(dirs, p.String())
	}
	sort.Strings(dirs)
	sort.Strings(reqPaths)
	log.V(4).Infof("[casng] upload.tree.phase1: paths:\n%s\n extra_dirs=\n%s", strings.Join(reqPaths, "\n"), strings.Join(dirs, "\n"))

	// This block generates directory nodes for shared ancestors starting from leaf nodes (DFS-style).
	var dirReqs []UploadRequest
	stack := make([]impath.Absolute, 0, len(dirChildren))
	stack = append(stack, execRoot)
	pathDigest := make(map[string]digest.Digest) // TODO: logs
	for len(stack) > 0 {
		// Peek.
		dir := stack[len(stack)-1]

		// Depth first.
		children := dirChildren[dir]
		pending := false
		for child, node := range children {
			if node == nil {
				pending = true
				stack = append(stack, child)
			}
		}
		if pending {
			continue
		}

		// Pop.
		stack = stack[:len(stack)-1]
		// Include cached children in addition to the ones added above.
		path, _ := dir.ReplacePrefix(remotePrefix, localPrefix) // This will not error out because it's the reverse operation of the one done above.
		key := path.String() + filterID
		cachedChildren := u.dirChildren.load(key)

		// A real directory (not virtual) that is the direct parent of a path in the reqs set could have duplicate new and cached nodes.
		// Examples: if it was /root/remote and the requests were
		//   /root/remote/a/foo.go,/root/remote/a/bar.go then all new nodes for a are duplicates of cached ones.
		//   /root/remote/a/foo.go,/root/remote/a/b/bar.go then one node for a would be duplicate (b is new and non-duplicate).
		//   /root/remote/a/b/foo.go,/root/remote/a/b/bar.go then all new nodes for a are non-duplicates (no cached nodes).
		// The children must deduplicated based on name, not digest, because the digest may have changed.
		type named interface{ GetName() string }
		seenChild := make(map[string]bool, len(children)+len(cachedChildren))
		childrenNodes := make([]proto.Message, 0, len(children)+len(cachedChildren))
		// New children must override cached ones to ensure the tree is up to date.
		for _, n := range children {
			seenChild[n.(named).GetName()] = true
			childrenNodes = append(childrenNodes, n)
		}
		for _, n := range cachedChildren {
			if seenChild[n.(named).GetName()] {
				continue
			}
			childrenNodes = append(childrenNodes, n)
		}
		log.V(3).Infof("[casng] upload.tree.phase2: dir=%s, key=%s, children=%d, cached=%d, new=%d", dir, key, len(childrenNodes), len(cachedChildren), len(children))

		node, b, errDigest := digestDirectory(dir, childrenNodes)
		if errDigest != nil {
			err = errDigest
			return
		}
		dirReqs = append(dirReqs, UploadRequest{Bytes: b, Digest: digest.NewFromProtoUnvalidated(node.Digest)})
		pathDigest[dir.String()] = digest.NewFromProtoUnvalidated(node.Digest) // TODO: logs
		if dir.String() == execRoot.String() {
			rootDigest = digest.NewFromProtoUnvalidated(node.Digest)
			break
		}
		// Attach the node to its parent if it's not the exec root.
		dirChildren[dir.Dir()][dir] = node
	}

	// Upload the blobs the shared ancestors.
	moreUploaded, moreStats, moreErr := u.Upload(ctx, dirReqs...)
	if moreErr != nil {
		err = moreErr
	}
	stats.Add(moreStats)
	uploaded = append(uploaded, moreUploaded...)

	// TODO: logs
	paths := make([]string, 0, len(reqs))
	for _, r := range reqs {
		paths = append(paths, r.Path.String())
	}
	extraDirs := make([]impath.Absolute, 0, len(dirChildren))
	for p := range dirChildren {
		extraDirs = append(extraDirs, p)
	}
	sort.Slice(extraDirs, func(i, j int) bool { return extraDirs[i].String() < extraDirs[j].String() })
	sort.Strings(paths)
	strBuilder := strings.Builder{}
	for _, p := range extraDirs {
		strBuilder.WriteString(fmt.Sprintf("  %s: %v\n", p, pathDigest[p.String()]))
		for c := range dirChildren[p] {
			strBuilder.WriteString(fmt.Sprintf("    %s\n", c))
		}
	}
	log.V(4).Infof("[casng] upload.tree.result: \n  root=%v\n  paths=%d\n%v\n  extra_dirs=%d\n%s", rootDigest, len(paths), strings.Join(paths, "\n"), len(extraDirs), strBuilder.String())

	return
}
