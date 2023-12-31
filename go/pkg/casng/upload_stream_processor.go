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
	m := "upload.streamer"
	ctx = ctxWithValues(ctx, ctxKeyModule, m)
	infof(ctx, 4, "start")
	defer infof(ctx, 4, "stop")

	// Ensure all in-flight responses are sent before returning.
	callWg := sync.WaitGroup{}
	defer func() { callWg.Wait() }()

	// Facilitates deferring requests.
	pipe := make(chan UploadRequest)
	// Coordinates closing the pipe channel which has two senders.
	counterCh := make(chan int)

	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()
		for req := range in {
			// Count first to ensure proper sequencing.
			counterCh <- 1
			pipe <- req
		}
		// Send a done signal.
		counterCh <- 0
	}()

	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()

		done := false
		count := 0
		for c := range counterCh {
			count += c
			if c == 0 {
				done = true
			}
			if done && count == 0 {
				close(pipe)
				return
			}
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
			rctx := ctxWithValues(req.ctx, ctxKeyModule, m, ctxKeySqID, req.id, ctxKeyRtID, req.route)
			infof(rctx, 4, "req", "digest", req.Digest, "large", shouldReleaseIOTokens, "pending", pending)

			item, ok := bundle[req.Digest]
			if ok {
				item.copies++
				// Already in-flight. Release duplicate resources if it's a large file.
				if shouldReleaseIOTokens {
					u.ioThrottler.release(ctx)
					u.ioLargeThrottler.release(ctx)
				}
				infof(rctx, 4, "unified", "digest", req.Digest, "count", item.copies+1)
				counterCh <- -1
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
					counterCh <- -1
					continue
				}
				// Defer
				wg, ok := cached.(*sync.WaitGroup)
				if !ok {
					log.Errorf("unexpected item type in streamCache: %T", cached)
					counterCh <- -1
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
				infof(rctx, 4, "compress", "digest", req.Digest)
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
				u.workerWg.Add(1)
				go func(req UploadRequest) {
					defer u.workerWg.Done()
					startTime := time.Now()
					streamResCh <- UploadResponse{Digest: req.Digest, Stats: Stats{BytesRequested: req.Digest.Size}, Err: ctx.Err()}
					durationf(rctx, startTime, "streamer.req->streamer.res", "digest", req.Digest)
				}(req)
				continue
			}
			durationf(rctx, startTime, "sem.stream")
			u.workerWg.Add(1)
			go func(req UploadRequest) {
				defer u.workerWg.Done()
				s, err := u.callStream(rctx, name, req)
				startTime := time.Now()
				// Release before sending on the channel to avoid blocking without actually using the gRPC resources.
				u.streamThrottle.release(ctx)
				streamResCh <- UploadResponse{Digest: req.Digest, Stats: s, Err: err}
				durationf(rctx, startTime, "streamer->streamer.res", "digest", req.Digest)
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
			counterCh <- -1
			if log.V(3) {
				fctx := ctxWithValues(ctx, ctxKeySqID, r.reqs, ctxKeyRtID, r.routes)
				durationf(fctx, startTime, "streamer.res->dispatcher.res", "digest", r.Digest, "pending", pending)
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
