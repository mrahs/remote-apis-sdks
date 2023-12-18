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
	log.V(1).Info("start; m=upload.dispatcher")
	defer log.V(1).Info("stop; m=upload.dispatcher")

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
		log.V(1).Info("sender.start; m=upload.dispatcher")
		defer log.V(1).Info("sender.stop; m=upload.dispatcher")

		for req := range u.dispatcherReqCh {
			startTime := time.Now()
			if req.done { // The digester will not be sending any further blobs.
				log.V(3).Infof("req.done; m=upload.dispatcher, tag=%s", req.tag)
				counterCh <- tagCount{req.tag, 0}
				// Covers waiting on the counter.
				log.V(3).Infof("duration.req; m=upload.dispatcher, start=%d, end=%d, req=%s, tag=%s, tid=%s", startTime.UnixNano(), time.Now().UnixNano(), req.id, req.tag)
				if req.tag == "" { // In fact, the digester (and all requesters) have terminated.
					return
				}
				continue
			}
			if req.Digest.Hash == "" {
				log.Errorf("ignoring a request without a digest; m=upload.dispatcher, req=%s, tag=%s", req.id, req.tag)
				continue
			}
			log.V(3).Infof("req; m=upload.dispatcher, digest=%s, bytes=%d, req=%s, tag=%s, tid=%s", req.Digest, len(req.Bytes), req.id, req.tag)
			// Count before sending the request to avoid an edge case where the response makes it to the counter before the increment here.
			counterCh <- tagCount{req.tag, 1}
			if req.digestOnly {
				u.dispatcherResCh <- UploadResponse{Digest: req.Digest, Stats: Stats{}, tags: []string{req.tag}, reqs: []string{req.id}}
				continue
			}
			u.dispatcherPipeCh <- req
			// Covers waiting on the counter and the dispatcher.
			log.V(3).Infof("duration.req; m=upload.dispatcher, start=%d, end=%d, req=%s, tag=%s, tid=%s", startTime.UnixNano(), time.Now().UnixNano(), req.id, req.tag)
		}
	}()

	// The piper forwards blobs from the sender to the query processor.
	// Cache hits are forwarded to the receiver.
	// Cache misses are dispatched to the batcher or the streamer.
	wg.Add(1)
	go func() {
		log.V(1).Info("pipe.start; m=upload.dispatcher")
		defer log.V(1).Info("pipe.stop; m=upload.dispatcher")

		done := false
		batchItemSizeLimit := int64(u.batchRPCCfg.BytesLimit - u.uploadRequestBaseSize - u.uploadRequestItemBaseSize)
		// Keep track of blobs that are associated with a digest since the query API only accepts digests.
		// Each blob may have a different tag and context so all must be dispathced.
		digestReqs := make(map[digest.Digest][]UploadRequest)

		for {
			select {
			// The dispatcher sends blobs on this channel, but never closes it.
			case req := <-u.dispatcherPipeCh:
				startTime := time.Now()
				// In the off chance that a request is received after a done signal, ignore it to avoid sending on a closed channel.
				if done {
					log.Errorf("ignoring a request after a done signal; m=upload.dispatcher, req=%s, tag=%s", req.id, req.tag)
					continue
				}
				// If the dispatcher has terminated, tell the streamer we're done and continue draining the response channel.
				if req.done {
					log.V(2).Info("pipe.done; m=upload.dispatcher")
					done = true
					close(queryCh)
					continue
				}

				log.V(3).Infof("pipe.req; m=upload.dispatcher, digest=%s, req=%s, tag=%s", req.Digest, req.id, req.tag)
				reqs := digestReqs[req.Digest]
				reqs = append(reqs, req)
				digestReqs[req.Digest] = reqs
				if len(reqs) > 1 {
					continue
				}
				queryCh <- missingBlobRequest{digest: req.Digest, ctx: req.ctx, id: req.id}
				// Covers waiting on the query processor.
				log.V(3).Infof("duration.pipe.send, m=upload.dispatcher, start=%d, end=%d, req=%s, tag=%s", startTime.UnixNano(), time.Now().UnixNano(), req.id, req.tag)

			// This channel is closed by the query pipe when queryCh is closed, which happens when the sender
			// sends a done signal. This ensures all responses are forwarded to the dispatcher.
			case r, ok := <-queryResCh:
				if !ok {
					return
				}
				startTime := time.Now()
				log.V(3).Infof("pipe.res; m=upload.dispatcher, digest=%s, missing=%t, err=%v", r.Digest, r.Missing, r.Err)
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
					if log.V(3) {
						log.Infof("pipe.res.hit; m=upload.dispatcher, digest=%s, req=%s, tag=%s", r.Digest, strings.Join(res.reqs, "|"), strings.Join(res.tags, "|"))
					}
					u.dispatcherResCh <- res
					// Covers waiting on the dispatcher.
					if log.V(3) {
						log.Infof("duration.pipe.pub; m=upload.dispatcher, start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
					}
					continue
				}

				if log.V(3) {
					log.Infof("pipe.res.miss; m=upload.dispatcher, digest=%s, req=%s, tag=%s", r.Digest, strings.Join(res.reqs, "|"), strings.Join(res.tags, "|"))
				}
				for _, req := range reqs {
					if req.Digest.Size <= batchItemSizeLimit {
						u.batcherCh <- req
						continue
					}
					u.streamerCh <- req
				}
				// Covers waiting on the batcher and streamer.
				log.V(3).Infof("duration.pipe.upload; m=upload.dispatcher, start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
			}
		}
	}()

	// This receiver forwards upload responses to requesters.
	// It is the last to terminate among the three in this block.
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.V(1).Info("receiver.start; m=upload.dispatcher")
		defer log.V(1).Info("receiver.stop; m=upload.dispatcher")

		// Messages delivered here are either went through the sender above (dispatched for upload), bypassed (digestion error), or piped back from the querier.
		for r := range u.dispatcherResCh {
			startTime := time.Now()
			if log.V(3) {
				log.Infof("res; m=upload.dispatcher, digest=%s, cache_hit=%d, end_of_walk=%t, err=%v, req=%s, tag=%s", r.Digest, r.Stats.CacheHitCount, r.endOfWalk, r.Err, strings.Join(r.reqs, "|"), strings.Join(r.tags, "|"))
			}
			// If multiple requesters are interested in this response, ensure stats are not double-counted.
			if len(r.tags) == 1 {
				u.uploadPubSub.pub(r, r.tags[0])
			} else {
				rCached := r
				rCached.Stats = r.Stats.ToCacheHit()
				u.uploadPubSub.mpub(r, rCached, r.tags...)
			}

			// Special case: do not decrement if it's an end of walk response.
			if !r.endOfWalk {
				for _, t := range r.tags {
					counterCh <- tagCount{t, -1}
				}
			}
			// Covers waiting on the counter and subscribers.
			log.V(3).Infof("duration.pub; m=upload.dispatcher, start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
		}
	}()

	// This counter keeps track of in-flight blobs and notifies requesters when they have no more responses.
	// It terminates after the sender, but before the receiver.
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.V(1).Info("counter.start; m=upload.dispatcher")
		defer log.V(1).Info("counter.stop; m=upload.dispatcher")
		defer close(u.dispatcherResCh) // Let the receiver know we're done.

		tagReqCount := make(map[string]int)
		tagDone := make(map[string]bool)
		allDone := false
		for tc := range counterCh {
			startTime := time.Now()
			if tc.c == 0 { // There will be no more blobs from this requester.
				log.V(3).Infof("counter.done.from; m=upload.dispatcher, tag=%s", tc.t)
				if tc.t == "" { // In fact, no more blobs for any requester.
					if len(tagReqCount) == 0 {
						return
					}
					allDone = true
					continue
				}
				tagDone[tc.t] = true
			}
			tagReqCount[tc.t] += tc.c
			if tagReqCount[tc.t] < 0 {
				log.Errorf("counter.negative; m=upload.dispatcher, tag=%s, inc=%d, count=%d, done=%t", tc.t, tc.c, tagReqCount[tc.t], tagDone[tc.t])
			}
			log.V(3).Infof("counter.count; m=upload.dispatcher, tag=%s, inc=%d, count=%d, done=%t, pending_tags=%d", tc.t, tc.c, tagReqCount[tc.t], tagDone[tc.t], len(tagReqCount))
			if tagReqCount[tc.t] <= 0 && tagDone[tc.t] {
				log.V(2).Infof("counter.done.to; m=upload.dispatcher, tag=%s", tc.t)
				delete(tagDone, tc.t)
				delete(tagReqCount, tc.t)
				// Signal to the requester that all of its requests are done.
				u.uploadPubSub.pub(UploadResponse{done: true}, tc.t)
			}
			// Covers waiting on subscribers.
			log.V(3).Infof("duration.counter.pub; m=upload.dispatcher, start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
			if len(tagReqCount) == 0 && allDone {
				return
			}
		}
	}()

	wg.Wait()
}
