package casng

import (
	"context"
	"sync"
	"time"

	log "github.com/golang/glog"
)

// routeCount is a tuple used by the dispatcher to track the number of in-flight requests for each requester.
// A request is in-flight if it has been dispatched, but no corresponding response has been received for it yet.
type routeCount struct {
	t string
	c int
}

// dispatcher receives digested blobs and forwards them to the uploader or back to the requester in case of a cache hit or error.
// A single upload request may generate multiple upload requests (file tree).
// The dispatcher handles counting in-flight (sub-)requests per requester and notifying requesters when all of their requests are completed.
func (u *uploader) dispatcher(ctx context.Context) {
	ctx = ctxWithValues(ctx, ctxKeyModule, "upload.dispatcher")
	infof(ctx, 1, "start")
	defer infof(ctx, 1, "stop")
	defer close(u.batcherCh)
	defer close(u.streamerCh)

	// Maintain a count of in-flight uploads per requester.
	counterCh := make(chan routeCount)
	// Wait until all requests have been fully dispatched before terminating.
	wg := sync.WaitGroup{}

	// This sender dispatches digested blobs to the query processor.
	// It is the first to terminate among the goroutines in this block.
	wg.Add(1)
	go func() {
		defer wg.Done()
		infof(ctx, 1, "sender.start")
		defer infof(ctx, 1, "sender.stop")
		defer close(queryCh)
		// Let the counter know we're done incrementing.
		defer func(){ counterCh <- routeCount{} }()

		for req := range u.dispatcherReqCh {
			fctx := ctxWithValues(ctx, ctxKeyRtID, req.route, ctxKeySqID, req.id)
			if req.done { // The digester will not be sending any further blobs.
				infof(fctx, 4, "req.done")
				startTime := time.Now()
				counterCh <- routeCount{req.route, 0}
				durationf(fctx, startTime, "dispatcher.req->dispatcher.counter")
				continue
			}
			if req.Digest.Hash == "" {
				log.Errorf("ignoring a request without a digest; %s", fmtCtx(fctx))
				continue
			}
			infof(fctx, 4, "req", "digest", req.Digest, "bytes", len(req.Bytes))
			startTime := time.Now()
			// Count before sending the request to avoid an edge case where the response makes it to the counter before the increment here.
			counterCh <- routeCount{req.route, 1}
			durationf(fctx, startTime, "dispatcher.req->dispatcher.counter")
			if req.digestOnly {
				startTime := time.Now()
				u.dispatcherResCh <- UploadResponse{Digest: req.Digest, Stats: Stats{}, routes: []string{req.route}, reqs: []string{req.id}}
				durationf(fctx, startTime, "dispatcher.req->dispatcher.res")
				continue
			}
			startTime = time.Now()
			queryCh <- missingBlobRequest{digest: req.Digest, meta: missingBlobRequestMeta{ctx: req.ctx, id: req.id, ref: req}}
			durationf(fctx, startTime, "dispatcher.req->query")
		}
	}()

	// The pipe receiver forwards blobs from the sender to the query processor.
	// Cache hits are forwarded to the receiver.
	// Cache misses are dispatched to the batcher or the streamer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		infof(ctx, 1, "pipe.receive.start")
		defer infof(ctx, 1, "pipe.receive.stop")

		batchItemSizeLimit := int64(u.batchRPCCfg.BytesLimit - u.uploadRequestBaseSize - u.uploadRequestItemBaseSize)
		// This channel is closed by the processor when queryCh is closed, which happens when the sender
		// sends a done signal. This ensures all responses are forwarded to the dispatcher.
		for r := range queryResCh {
			startTime := time.Now()
			fctx := ctxWithValues(ctx, ctxKeyRtID, r.meta.route, ctxKeySqID, r.meta.id)
			infof(ctx, 4, "pipe.res", "digest", r.Digest, "missing", r.Missing, "err", r.Err)

			req := r.meta.ref.(UploadRequest)
			res := UploadResponse{Digest: r.Digest, Err: r.Err, routes: []string{req.route}, reqs: []string{req.id}}

			if !r.Missing {
				res.Stats = Stats{
					BytesRequested:     r.Digest.Size,
					LogicalBytesCached: r.Digest.Size,
					CacheHitCount:      1,
				}
			}

			if r.Err != nil || !r.Missing {
				// Release associated IO holds before dispatching the result.
				if req.reader != nil {
					u.ioThrottler.release(ctx)
					u.ioLargeThrottler.release(ctx)
				}

				u.dispatcherResCh <- res
				durationf(fctx, startTime, "dispatcher.res.q->dispatcher.res")
				continue
			}

			if req.Digest.Size <= batchItemSizeLimit {
				u.batcherCh <- req
				durationf(fctx, startTime, "dispatcher.res.q->batcher.req")
				continue
			}
			u.streamerCh <- req
			durationf(fctx, startTime, "dispatcher.res.q->streamer.req")
		}
	}()

	// This receiver forwards upload responses to requesters.
	// It is the last to terminate among the three in this block.
	wg.Add(1)
	go func() {
		defer wg.Done()
		infof(ctx, 1, "receiver.start")
		defer infof(ctx, 1, "receiver.stop")

		// Messages delivered here are either went through the sender above (dispatched for upload), bypassed (digestion error), or piped back from the querier.
		for r := range u.dispatcherResCh {
			startTime := time.Now()
			fctx := ctxWithValues(ctx, ctxKeyRtID, r.routes, ctxKeySqID, r.reqs)
			infof(fctx, 4, "res", "digest", r.Digest, "routes", len(r.routes), "cache_hit", r.Stats.CacheHitCount, "end_of_walk", r.endOfWalk, "err", r.Err)
			// If multiple requesters are interested in this response, ensure stats are not double-counted.
			if len(r.routes) == 1 {
				u.uploadPubSub.pub(ctx, r, r.routes[0])
			} else {
				rCached := r
				rCached.Stats = r.Stats.ToCacheHit()
				u.uploadPubSub.mpub(ctx, r, rCached, r.routes...)
			}
			durationf(fctx, startTime, "dispatcher.res->pub", "count", len(r.routes))

			// Special case: do not decrement if it's an end of walk response.
			if !r.endOfWalk {
				startTime := time.Now()
				for _, t := range r.routes {
					counterCh <- routeCount{t, -1}
				}
				durationf(fctx, startTime, "dispatcher.res->dispatcher.counter", "count", len(r.routes))
			}
		}
	}()

	// This counter keeps track of in-flight blobs and notifies requesters when they have no more responses.
	// It terminates after the sender, but before the receiver.
	wg.Add(1)
	go func() {
		defer wg.Done()
		infof(ctx, 1, "counter.start")
		defer infof(ctx, 1, "counter.stop")
		defer close(u.dispatcherResCh)

		routeReqCount := make(map[string]int)
		routeDone := make(map[string]bool)
		allDone := false
		for rc := range counterCh {
			fctx := ctxWithValues(ctx, ctxKeyRtID, rc.t)
			if rc.c == 0 { // There will be no more blobs from this route.
				infof(fctx, 4, "counter.done.in")
				if rc.t == "" { // In fact, no more blobs for any route.
					if len(routeReqCount) == 0 { // All counting is done.
						return
					}
					// Remember to terminate once all counting is done.
					allDone = true
					continue
				}
				routeDone[rc.t] = true
			}
			routeReqCount[rc.t] += rc.c
			if routeReqCount[rc.t] < 0 {
				log.Errorf("counter.negative; %s", fmtCtx(fctx, "inc", rc.c, "count", routeReqCount[rc.t], "done", routeDone[rc.t]))
			}
			infof(ctx, 4, "counter.count", "inc", rc.c, "count", routeReqCount[rc.t], "done", routeDone[rc.t], "pending_routes", len(routeReqCount))
			if routeReqCount[rc.t] <= 0 && routeDone[rc.t] {
				infof(ctx, 2, "counter.done.to")
				delete(routeDone, rc.t)
				delete(routeReqCount, rc.t)
				startTime := time.Now()
				// Signal to the requester that all of its requests are done.
				u.uploadPubSub.pub(ctx, UploadResponse{done: true}, rc.t)
				durationf(fctx, startTime, "dispatcher.counter->pub")
			}
			if len(routeReqCount) == 0 && allDone {
				return
			}
		}
	}()

	wg.Wait()
}
