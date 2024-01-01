package casng

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"time"

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
func (u *uploader) streamProcessor(ctx context.Context, in <-chan UploadRequest, out chan<- UploadResponse) {
	u.streamWorkerWg.Add(1)
	defer u.streamWorkerWg.Done()

	ctx = ctxWithValues(ctx, ctxKeyModule, "stream_processor")
	infof(ctx, 4, "start")
	defer infof(ctx, 4, "stop")

	// Ensure all in-flight responses are sent before returning.
	callWg := sync.WaitGroup{}
	defer func() { callWg.Wait() }()

	// Facilitates deferring requests.
	pipe := make(chan UploadRequest)
	deferredWg := sync.WaitGroup{}

	// Add 1 for the sender.
	deferredWg.Add(1)

	// Launch this waiter first to ensure Wait is called before any other Done.
	go func(){
		// Adding one above allows calling wait here even though more may be added later.
		deferredWg.Wait()
		close(pipe)
	}()

	go func() {
		defer deferredWg.Done()

		infof(ctx, 4, "sender.start")
		defer infof(ctx, 4, "sender.stop")

		for req := range in {
			pipe <- req
		}
	}()

	// Unify duplicate requests.
	bundle := make(uploadStreamBundle)
	streamResCh := make(chan UploadResponse)
	pending := 0
	for {
		select {
		// The dispatcher closes this channel when it's done dispatching, which happens after the streamer
		// had sent all pending responses.
		case req, ok := <-pipe:
			if !ok {
				return
			}
			shouldReleaseIOTokens := req.reader != nil
			infof(ctx, 4, "req", "digest", req.Digest, "large", shouldReleaseIOTokens, "pending", pending)

			item, ok := bundle[req.Digest]
			if ok {
				item.copies++
				// Already in-flight. Release duplicate resources if it's a large file.
				if shouldReleaseIOTokens {
					u.ioThrottler.release(ctx)
					u.ioLargeThrottler.release(ctx)
				}
				infof(ctx, 4, "unified", "digest", req.Digest, "count", item.copies+1)
				continue
			}

			// Claim the digest.
			wg := &sync.WaitGroup{}
			wg.Add(1)
			cached, ok := u.streamCache.LoadOrStore(req.Digest, wg)
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
					continue
				}
				// Defer
				wg, ok := cached.(*sync.WaitGroup)
				if !ok {
					log.Errorf("unexpected item type in streamCache: %T", cached)
					continue
				}
				u.workerWg.Add(1)
				go func() {
					defer u.workerWg.Done()
					wg.Wait()
					pipe <- req
				}()
				continue
			}

			item.wg = wg
			bundle[req.Digest] = item

			var name string
			if req.Digest.Size >= u.ioCfg.CompressionSizeThreshold {
				infof(ctx, 4, "compressed", "digest", req.Digest)
				name = MakeCompressedWriteResourceName(u.instanceName, req.Digest.Hash, req.Digest.Size)
			} else {
				name = MakeWriteResourceName(u.instanceName, req.Digest.Hash, req.Digest.Size)
			}

			pending++
			// Block the streamer if the gRPC call is being throttled.
			startTime := time.Now()
			if !u.streamThrottle.acquire(ctx) { // TODO: should also cancel if req.ctx is cancelled.
				if shouldReleaseIOTokens {
					u.ioThrottler.release(ctx)
					u.ioLargeThrottler.release(ctx)
				}
				// Ensure the response is dispatched before aborting.
				deferredWg.Add(1)
				go func(req UploadRequest) {
					defer deferredWg.Done()
					startTime := time.Now()
					streamResCh <- UploadResponse{Digest: req.Digest, Stats: Stats{BytesRequested: req.Digest.Size}, Err: ctx.Err()}
					durationf(ctx, startTime, "stream.req->stream.res", "digest", req.Digest)
				}(req)
				continue
			}
			durationf(ctx, startTime, "sem.stream")
			u.workerWg.Add(1)
			go func(req UploadRequest) {
				defer u.workerWg.Done()
				s, err := u.callStream(ctx, name, req)
				startTime := time.Now()
				// Release before sending on the channel to avoid blocking without actually using the gRPC resources.
				u.streamThrottle.release(ctx)
				streamResCh <- UploadResponse{Digest: req.Digest, Stats: s, Err: err}
				durationf(ctx, startTime, "stream.grpc->stream.res", "digest", req.Digest)
			}(req)
		case r := <-streamResCh:
			startTime := time.Now()
			out <- r
			c := int64(bundle[r.Digest].copies)
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
			durationf(ctx, startTime, "stream.res->out", "digest", r.Digest, "pending", pending)
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
		startTime := time.Now()
		if !u.ioThrottler.acquire(ctx) {
			return Stats{BytesRequested: req.Digest.Size}, context.Canceled
		}
		durationf(ctx, startTime, "sem.io")
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
