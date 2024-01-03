package casng

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	log "github.com/golang/glog"
)

type uploadStreamBundleItem struct {
	wg     *sync.WaitGroup
	copies int
}

type uploadStreamBundle map[digest.Digest]uploadStreamBundleItem

// streamer handles files that do not fit into a batching request.
// For files above the large threshold, this call assumes the io and large io holds are already acquired and will release them accordingly.
// For other files, only an io hold is acquired and released in this call.
// cancelling ctx does not cancel the processor. in must be closed to terminate this processor.
func (u *uploader) streamProcessor(ctx context.Context, in <-chan UploadRequest, out chan<- UploadResponse) {
	u.streamWorkerWg.Add(1)
	defer u.streamWorkerWg.Done()

	ctx = traceStart(ctx, "stream_processor")
	defer traceEnd(ctx)

	// Ensure all in-flight responses are sent before returning.
	callWg := sync.WaitGroup{}
	defer func() { callWg.Wait() }()

	// Facilitates deferring requests.
	pipe := make(chan any)

	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()

		for req := range in {
			pipe <- req
		}
		pipe <- true
	}()

	// Unify duplicate requests.
	bundle := make(uploadStreamBundle)
	streamResCh := make(chan UploadResponse)
	pending := 0
	deferred := 0
	done := false
	for {
		select {
		// pipe is never closed because it has multiple senders.
		case pipedVal := <-pipe:
			var req UploadRequest
			switch r := pipedVal.(type) {
			case bool:
				// The sender is done.
				done = true
				if deferred == 0 && pending == 0 {
					return
				}
				continue
			case int:
				// A deferred request was sent back.
				deferred--
				if done && deferred == 0 && pending == 0 {
					return
				}
				continue
			case UploadRequest:
				req = r
			default:
				errorf(ctx, fmt.Sprintf("unexpected message type: %T", r))
				continue
			}
			ctx = traceStart(ctx, "bundle.append", "digest", req.Digest, "start_count", len(bundle))
			shouldReleaseIOTokens := req.reader != nil

			item, ok := bundle[req.Digest]
			if ok {
				item.copies++
				bundle[req.Digest] = item
				// Already in-flight. Release duplicate resources if it's a large file.
				if shouldReleaseIOTokens {
					u.ioThrottler.release(ctx)
					u.ioLargeThrottler.release(ctx)
				}
				ctx = traceEnd(ctx, "dst", "unified", "copies", item.copies+1)
				continue
			}

			// Claim the digest.
			cachedWg := &sync.WaitGroup{}
			cachedWg.Add(1)
			cached, ok := u.streamCache.LoadOrStore(req.Digest, cachedWg)
			if ok {
				// Already claimed.
				if _, ok := cached.(bool); ok {
					// Already uploaded.
					out <- UploadResponse{
						Digest: req.Digest,
						Stats: Stats{
							BytesRequested:     req.Digest.Size,
							LogicalBytesCached: req.Digest.Size,
							CacheHitCount:      1,
						},
					}
					ctx = traceEnd(ctx, "dst", "cached")
					continue
				}
				// Defer
				cachedWg, ok := cached.(*sync.WaitGroup)
				if !ok {
					log.Errorf("unexpected item type in streamCache: %T", cached)
					ctx = traceEnd(ctx, "err", "unexpected message type")
					continue
				}
				deferred++
				u.workerWg.Add(1)
				go func() {
					defer u.workerWg.Done()
					cachedWg.Wait()
					pipe <- req
					pipe <- -1
				}()
				ctx = traceEnd(ctx, "dst", "deferred")
				continue
			}

			item.wg = cachedWg
			bundle[req.Digest] = item

			var name string
			if req.Digest.Size >= u.ioCfg.CompressionSizeThreshold {
				debugf(ctx, "compressed", "digest", req.Digest)
				name = MakeCompressedWriteResourceName(u.instanceName, req.Digest.Hash, req.Digest.Size)
			} else {
				name = MakeWriteResourceName(u.instanceName, req.Digest.Hash, req.Digest.Size)
			}

			// Block the streamer if the gRPC call is being throttled.
			ctx = traceStart(ctx, "sem.stream")
			if !u.streamThrottle.acquire(ctx) {
				if shouldReleaseIOTokens {
					u.ioThrottler.release(ctx)
					u.ioLargeThrottler.release(ctx)
				}
				// Ensure the response is dispatched before aborting.
				out <- UploadResponse{
					Digest: req.Digest,
					Stats:  Stats{BytesRequested: req.Digest.Size},
					Err:    ctx.Err(),
				}
				ctx = traceEnd(ctx, "err", ctx.Err())
				continue
			}
			ctx = traceEnd(ctx)

			pending++
			u.workerWg.Add(1)
			go func(req UploadRequest) {
				defer u.workerWg.Done()
				s, err := u.callStream(ctx, name, req)
				ctx = traceStart(ctx, "stream.grpc->stream.res")
				// Release before sending on the channel to avoid blocking without actually using the gRPC resources.
				u.streamThrottle.release(ctx)
				streamResCh <- UploadResponse{Digest: req.Digest, Stats: s, Err: err}
				ctx = traceEnd(ctx)
			}(req)
		case r := <-streamResCh:
			ctx = traceStart(ctx, "stream.res->out")
			out <- r
			if r.Err == nil {
				u.streamCache.Store(r.Digest, true)
			} else {
				u.streamCache.Delete(r.Digest)
			}
			item := bundle[r.Digest]
			item.wg.Done()
			c := int64(item.copies)
			if c > 0 {
				sCached := r.Stats.ToCacheHit()
				sCached.CacheHitCount = c
				sCached.LogicalBytesCached *= c
				out <- UploadResponse{
					Digest: r.Digest,
					Stats:  sCached,
				}
			}
			delete(bundle, r.Digest)
			pending--
			ctx = traceEnd(ctx, "digest", r.Digest, "pending", pending)
			if done && pending == 0 {
				return
			}
		}
	}
}

func (u *uploader) callStream(ctx context.Context, name string, req UploadRequest) (stats Stats, err error) {
	var reader io.Reader

	// In the off chance that the blob is mis-constructed (more than one content field is set), start
	// with b.reader to ensure any held locks are released.
	switch {
	// Large file.
	case req.reader != nil:
		reader = req.reader
		defer func() {
			if errClose := req.reader.Close(); errClose != nil {
				err = errors.Join(ErrIO, errClose, err)
			}
			// IO holds were acquired during digestion for large files and are expected to be released here.
			u.ioThrottler.release(ctx)
			u.ioLargeThrottler.release(ctx)
		}()

	// Small file, a proto message (node), or an empty file.
	case len(req.Bytes) > 0:
		reader = bytes.NewReader(req.Bytes)

	// Medium file.
	default:
		ctx = traceStart(ctx, "sem.io")
		if !u.ioThrottler.acquire(ctx) {
			ctx = traceEnd(ctx, "err", ctx.Err())
			return Stats{BytesRequested: req.Digest.Size}, context.Canceled
		}
		ctx = traceEnd(ctx)
		defer u.ioThrottler.release(ctx)

		f, errOpen := os.Open(req.Path.String())
		if errOpen != nil {
			return Stats{BytesRequested: req.Digest.Size}, errors.Join(ErrIO, errOpen)
		}
		defer func() {
			if errClose := f.Close(); errClose != nil {
				err = errors.Join(ErrIO, errClose, err)
			}
		}()
		reader = f
	}

	return u.writeBytes(ctx, name, reader, req.Digest.Size, 0, true)
}
