package casng

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	log "github.com/golang/glog"
)

// tagCount is a tuple used by the dispatcher to track the number of in-flight requests for each requester.
// A request is in-flight if it has been dispatched, but no corresponding response has been received for it yet.
type tagCount struct {
	t string
	c int
}

// dispatcher receives digested blobs and forwards them to the uploader or back to the requester in case of a cache hit or error.
// The dispatcher handles counting in-flight requests per requester and notifying requesters when all of their requests are completed.
func (u *uploader) dispatcher(ctx context.Context, queryCh chan<- missingBlobRequest, queryResCh <-chan MissingBlobsResponse) {
	ctx = ctxWithValues(ctx, ctxKeyModule, "upload.dispatcher")
	log.V(1).Infof("start; %s", fmtCtx(ctx))
	defer log.V(1).Infof("stop; %s", fmtCtx(ctx))

	defer func() {
		// Let the batcher and the streamer know we're done dispatching blobs.
		close(u.batcherCh)
		close(u.streamerCh)
	}()

	// Maintain a count of in-flight uploads per requester.
	counterCh := make(chan tagCount)
	// Wait until all requests have been fully dispatched before terminating.
	wg := sync.WaitGroup{}

	// This sender dispatches digested blobs to the query processor.
	// It is the first to terminate among the goroutines in this block.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			// Let the piper know that the sender will not be sending any more blobs.
			u.dispatcherPipeCh <- UploadRequest{done: true}
		}()
		infof(ctx, 1, "sender.start")
		defer infof(ctx, 1, "sender.stop")

		for req := range u.dispatcherReqCh {
			fctx := ctxWithValues(ctx, ctxKeyRtID, req.tag, ctxKeySqID, req.id)
			if req.done { // The digester will not be sending any further blobs.
				log.V(3).Infof("req.done; %s", fmtCtx(fctx))
				startTime := time.Now()
				counterCh <- tagCount{req.tag, 0}
				logDuration(fctx, startTime, "req->counter.done")
				if req.tag == "" { // In fact, the digester (and all requesters) have terminated.
					return
				}
				continue
			}
			if req.Digest.Hash == "" {
				log.Errorf("ignoring a request without a digest; %s", fmtCtx(fctx))
				continue
			}
			log.V(3).Infof("req; %s", fmtCtx(fctx, "digest", req.Digest, "bytes", len(req.Bytes)))
			startTime := time.Now()
			// Count before sending the request to avoid an edge case where the response makes it to the counter before the increment here.
			counterCh <- tagCount{req.tag, 1}
			logDuration(fctx, startTime, "req->counter.inc")
			if req.digestOnly {
				startTime := time.Now()
				u.dispatcherResCh <- UploadResponse{Digest: req.Digest, Stats: Stats{}, tags: []string{req.tag}, reqs: []string{req.id}}
				logDuration(fctx, startTime, "req->res")
				continue
			}
			startTime = time.Now()
			u.dispatcherPipeCh <- req
			logDuration(fctx, startTime, "req->pipe")
		}
	}()

	// The piper forwards blobs from the sender to the query processor.
	// Cache hits are forwarded to the receiver.
	// Cache misses are dispatched to the batcher or the streamer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.V(1).Infof("pipe.start; %s", fmtCtx(ctx))
		defer log.V(1).Infof("pipe.stop; %s", fmtCtx(ctx))

		done := false
		batchItemSizeLimit := int64(u.batchRPCCfg.BytesLimit - u.uploadRequestBaseSize - u.uploadRequestItemBaseSize)
		// Keep track of blobs that are associated with a digest since the query API only accepts digests.
		// Each blob may have a different tag and context so all must be dispathced.
		digestReqs := make(map[digest.Digest][]UploadRequest)

		// BUG: this loop forms a circular path
		// The path is: digester -> dispatcherReqCh -> dispatcherPipeCh -> queryCh -> pubsub -> queryResCh -> dispatcherResCh
		// The loop may block sending on queryCh, but at the same time pubsub may be blocked sending on queryResCh.
		for {
			select {
			// The dispatcher sends blobs on this channel, but never closes it.
			case req := <-u.dispatcherPipeCh:
				startTime := time.Now()
				fctx := ctxWithValues(ctx, ctxKeyRtID, req.tag, ctxKeySqID, req.id)
				// In the off chance that a request is received after a done signal, ignore it to avoid sending on a closed channel.
				if done {
					log.Errorf("ignoring a request after a done signal; %s", fmtCtx(fctx))
					continue
				}
				// If the dispatcher has terminated, tell the streamer we're done and continue draining the response channel.
				if req.done {
					log.V(2).Info("pipe.done; %s", fmtCtx(fctx))
					done = true
					close(queryCh)
					continue
				}

				log.V(3).Infof("pipe.req; %s", fmtCtx(fctx, "digest", req.Digest))
				reqs := digestReqs[req.Digest]
				reqs = append(reqs, req)
				digestReqs[req.Digest] = reqs
				if len(reqs) > 1 {
					continue
				}
				// TODO: experimental fix for the deadlock. Not yet sure about the upper bound of the number of
				// spawned goroutines here.
				u.workerWg.Add(1)
				go func(ctx context.Context, reqID string, d digest.Digest){
					defer u.workerWg.Done()
					queryCh <- missingBlobRequest{digest: d, ctx: ctx, id: reqID}
				}(req.ctx, req.id, req.Digest)
				logDuration(fctx, startTime, "pipe->query")

			// This channel is closed by the query pipe when queryCh is closed, which happens when the sender
			// sends a done signal. This ensures all responses are forwarded to the dispatcher.
			case r, ok := <-queryResCh:
				if !ok {
					return
				}
				startTime := time.Now()
				infof(ctx, 3, "pipe.res", "digest", r.Digest, "missing", r.Missing, "err", r.Err)
				reqs := digestReqs[r.Digest]
				delete(digestReqs, r.Digest)
				res := UploadResponse{Digest: r.Digest, Err: r.Err}

				if !r.Missing {
					res.Stats = Stats{
						BytesRequested:     r.Digest.Size,
						LogicalBytesCached: r.Digest.Size,
						CacheHitCount:      1,
					}
				}

				fctx := ctxWithValues(ctx, ctxKeyRtID, res.tags, ctxKeySqID, res.reqs)

				if r.Err != nil || !r.Missing {
					res.tags = make([]string, len(reqs))
					res.reqs = make([]string, len(reqs))
					for i, req := range reqs {
						res.tags[i] = req.tag
						res.reqs[i] = req.id
						if req.reader != nil {
							u.releaseIOTokens()
						}
					}
					infof(fctx, 3, "pipe.res.hit", "digest", r.Digest)
					u.dispatcherResCh <- res
					logDuration(fctx, startTime, "query->res")
					continue
				}

				infof(fctx, 3, "pipe.res.miss", "digest", r.Digest)
				for _, req := range reqs {
					if req.Digest.Size <= batchItemSizeLimit {
						u.batcherCh <- req
						continue
					}
					u.streamerCh <- req
				}
				// Covers waiting on the batcher and streamer.
				logDuration(fctx, startTime, "query->upload")
			}
		}
	}()

	// This receiver forwards upload responses to requesters.
	// It is the last to terminate among the three in this block.
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.V(1).Info("receiver.start; %s", fmtCtx(ctx))
		defer log.V(1).Info("receiver.stop; %s", fmtCtx(ctx))

		// Messages delivered here are either went through the sender above (dispatched for upload), bypassed (digestion error), or piped back from the querier.
		for r := range u.dispatcherResCh {
			startTime := time.Now()
			fctx := ctxWithValues(ctx, ctxKeyRtID, strings.Join(r.tags, "|"), ctxKeySqID, strings.Join(r.reqs, "|"))
			infof(fctx, 3, "res", "digest", r.Digest, "tags", len(r.tags), "cache_hit", r.Stats.CacheHitCount, "end_of_walk", r.endOfWalk, "err", r.Err)
			// If multiple requesters are interested in this response, ensure stats are not double-counted.
			if len(r.tags) == 1 {
				u.uploadPubSub.pub(ctx, r, r.tags[0])
			} else {
				rCached := r
				rCached.Stats = r.Stats.ToCacheHit()
				u.uploadPubSub.mpub(ctx, r, rCached, r.tags...)
			}
			logDuration(fctx, startTime, "res->pub", "count", len(r.tags))

			// Special case: do not decrement if it's an end of walk response.
			if !r.endOfWalk {
				startTime := time.Now()
				for _, t := range r.tags {
					counterCh <- tagCount{t, -1}
				}
				logDuration(fctx, startTime, "res->counter.dec", "count", len(r.tags))
			}
		}
	}()

	// This counter keeps track of in-flight blobs and notifies requesters when they have no more responses.
	// It terminates after the sender, but before the receiver.
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.V(1).Info("counter.start; %s", fmtCtx(ctx))
		defer log.V(1).Info("counter.stop; %s", fmtCtx(ctx))
		defer close(u.dispatcherResCh) // Let the receiver know we're done.

		tagReqCount := make(map[string]int)
		tagDone := make(map[string]bool)
		allDone := false
		for tc := range counterCh {
			fctx := ctxWithValues(ctx, ctxKeyRtID, tc.t)
			if tc.c == 0 { // There will be no more blobs from this requester.
				log.V(3).Infof("counter.done.in; %s", fmtCtx(fctx))
				if tc.t == "" { // In fact, no more blobs for any requester.
					if len(tagReqCount) == 0 { // All counting is done.
						return
					}
					// Remember to return once all counting is done.
					allDone = true
					continue
				}
				tagDone[tc.t] = true
			}
			tagReqCount[tc.t] += tc.c
			if tagReqCount[tc.t] < 0 {
				log.Errorf("counter.negative; %s", fmtCtx(fctx, "inc", tc.c, "count", tagReqCount[tc.t], "done", tagDone[tc.t]))
			}
			log.V(3).Infof("counter.count; %s", fmtCtx(fctx, "inc", tc.c, "count", tagReqCount[tc.t], "done", tagDone[tc.t], "pending_tags", len(tagReqCount)))
			if tagReqCount[tc.t] <= 0 && tagDone[tc.t] {
				log.V(2).Infof("counter.done.to; %s", fmtCtx(fctx))
				delete(tagDone, tc.t)
				delete(tagReqCount, tc.t)
				startTime := time.Now()
				// Signal to the requester that all of its requests are done.
				u.uploadPubSub.pub(ctx, UploadResponse{done: true}, tc.t)
				logDuration(fctx, startTime, "coutner->pub")
			}
			if len(tagReqCount) == 0 && allDone {
				return
			}
		}
	}()

	wg.Wait()
}
