package casng

import (
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	log "github.com/golang/glog"
)

// dispatcher receives digested blobs and forwards them to the uploader or back to the requester in case of a cache hit or error.
// The dispatcher handles counting in-flight requests per requester and notifying requesters when all of their requests are completed.
func (u *uploader) dispatcher(queryCh chan<- missingBlobRequest, queryResCh <-chan MissingBlobsResponse) {
	log.V(1).Info("[casng] upload.dispatcher.start")
	defer log.V(1).Info("[casng] upload.dispatcher.stop")

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
			u.dispatcherPipeCh <- blob{done: true}
		}()
		log.V(1).Info("[casng] upload.dispatcher.sender.start")
		defer log.V(1).Info("[casng] upload.dispatcher.sender.stop")

		for b := range u.dispatcherBlobCh {
			startTime := time.Now()
			if b.done { // The digester will not be sending any further blobs.
				log.V(3).Infof("[casng] upload.dispatcher.blob.done: tag=%s", b.tag)
				counterCh <- tagCount{b.tag, 0}
				// Covers waiting on the counter.
				log.V(3).Infof("[casng] upload.dispatcher.duration: start=%d, end=%d, tag=%s", startTime.UnixNano(), time.Now().UnixNano(), b.tag)
				if b.tag == "" { // In fact, the digester (and all requesters) have terminated.
					return
				}
				continue
			}
			if b.digest.Hash == "" {
				log.Errorf("[casng] upload.dispatcher: received a blob without a digest for tag=%s; ignoring", b.tag)
				continue
			}
			log.V(3).Infof("[casng] upload.dispatcher.blob: digest=%s, bytes=%d, tag=%s", b.digest, len(b.bytes), b.tag)
			u.dispatcherPipeCh <- b
			counterCh <- tagCount{b.tag, 1}
			// Covers waiting on the counter and the dispatcher.
			log.V(3).Infof("[casng] upload.dispatcher.duration: start=%d, end=%d, tag=%s", startTime.UnixNano(), time.Now().UnixNano(), b.tag)
		}
	}()

	// The piper forwards blobs from the sender to the query processor.
	// Cache hits are forwarded to the receiver.
	// Cache misses are dispatched to the batcher or the streamer.
	wg.Add(1)
	go func() {
		log.V(1).Info("[casng] upload.dispatcher.pipe.start")
		defer log.V(1).Info("[casng] upload.dispatcher.pipe.stop")

		done := false
		batchItemSizeLimit := int64(u.batchRPCCfg.BytesLimit - u.uploadRequestBaseSize - u.uploadRequestItemBaseSize)
		// Keep track of blobs that are associated with a digest since the query API only accepts digests.
		// Each blob may have a different tag and context so all must be dispathced.
		digestBlobs := make(map[digest.Digest][]blob)

		for {
			select {
			// The dispatcher sends blobs on this channel, but never closes it.
			case b := <-u.dispatcherPipeCh:
				startTime := time.Now()
				// In the off chance that a request is received after a done signal, ignore it to avoid sending on a closed channel.
				if done {
					log.Errorf("[casng] upload.dispatcher.pipe: received a request after a done signal from tag=%s; ignoring", b.tag)
					continue
				}
				// If the dispatcher has terminated, tell the streamer we're done and continue draining the response channel.
				if b.done {
					log.V(2).Info("upload.dispatcher.pipe.done")
					done = true
					close(queryCh)
					continue
				}

				log.V(3).Infof("[casng] upload.dispatcher.pipe.blob: digest=%s, tag=%s", b.digest, b.tag)
				digestBlobs[b.digest] = append(digestBlobs[b.digest], b)
				if len(digestBlobs[b.digest]) > 1 {
					continue
				}
				queryCh <- missingBlobRequest{digest: b.digest, ctx: b.ctx}
				// Covers waiting on the query processor.
				log.V(3).Infof("[casng] upload.dispatcher.pipe.send.duration: start=%d, end=%d, tag=%s", startTime.UnixNano(), time.Now().UnixNano(), b.tag)

			// This channel is closed by the query pipe when queryCh is closed, which happens when the sender
			// sends a done signal. This ensures all responses are forwarded to the dispatcher.
			case r, ok := <-queryResCh:
				if !ok {
					return
				}
				startTime := time.Now()
				log.V(3).Infof("[casng] upload.dispatcher.pipe.res: digest=%s, missing=%t, err=%v", r.Digest, r.Missing, r.Err)
				blobs := digestBlobs[r.Digest]
				delete(digestBlobs, r.Digest)
				res := UploadResponse{Digest: r.Digest, Err: r.Err}

				if !r.Missing {
					res.Stats = Stats{
						BytesRequested:     r.Digest.Size,
						LogicalBytesCached: r.Digest.Size,
						CacheHitCount:      1,
					}
				}

				if r.Err != nil || !r.Missing {
					res.tags = make([]tag, len(blobs))
					for i, b := range blobs {
						res.tags[i] = b.tag
						if b.reader != nil {
							u.ioThrottler.release()
							u.ioLargeThrottler.release()
						}
					}
					log.V(3).Infof("[casng] upload.dispatcher.pipe.res.hit: digest=%s, tags=%d", r.Digest, len(res.tags))
					u.dispatcherResCh <- res
					// Covers waiting on the dispatcher.
					log.V(3).Infof("[casng] upload.dispatcher.pipe.pub.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
					continue
				}

				for _, b := range blobs {
					if b.digest.Size <= batchItemSizeLimit {
						u.batcherCh <- b
						continue
					}
					u.streamerCh <- b
				}
				// Covers waiting on the batcher and streamer.
				log.V(3).Infof("[casng] upload.dispatcher.pipe.upload.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
			}
		}
	}()

	// This receiver forwards upload responses to requesters.
	// It is the last to terminate among the three in this block.
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.V(1).Info("[casng] upload.dispatcher.receiver.start")
		defer log.V(1).Info("[casng] upload.dispatcher.receiver.stop")

		// Messages delivered here are either went through the sender above (dispatched for upload), bypassed (digestion error), or piped back from the querier.
		for r := range u.dispatcherResCh {
			startTime := time.Now()
			log.V(3).Infof("[casng] upload.dispatcher.res: digest=%s, cache_hit=%d, cache_miss=%d, tags=%d, err=%v", r.Digest, r.Stats.CacheHitCount, r.Stats.CacheMissCount, len(r.tags), r.Err)
			// If multiple requesters are interested in this response, ensure stats are not double-counted.
			if len(r.tags) == 1 {
				u.uploadPubSub.pub(r, r.tags[0])
			} else {
				rCached := r
				rCached.Stats = r.Stats.ToCacheHit()
				u.uploadPubSub.mpub(r, rCached, r.tags...)
			}

			for _, t := range r.tags {
				// Special case: do not decrement if the response was from a digestion error.
				if r.Digest.Hash != "" {
					counterCh <- tagCount{t, -1}
				}
			}
			// Covers waiting on the counter and subscribers.
			log.V(3).Infof("[casng] upload.dispatcher.pub.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
		}
	}()

	// This counter keeps track of in-flight blobs and notifies requesters when they have no more responses.
	// It terminates after the sender, but before the receiver.
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.V(1).Info("[casng] upload.dispatcher.counter.start")
		defer log.V(1).Info("[casng] upload.dispatcher.counter.stop")
		defer close(u.dispatcherResCh) // Let the receiver know we're done.

		tagReqCount := make(map[tag]int)
		tagDone := make(map[tag]bool)
		allDone := false
		for tc := range counterCh {
			startTime := time.Now()
			if tc.c == 0 { // There will be no more blobs from this requester.
				log.V(3).Infof("[casng] upload.dispatcher.counter.done.from: tag=%s", tc.t)
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
			log.V(3).Infof("[casng] upload.dispatcher.counter.count: tag=%s, count=%d, pending_tags=%d", tc.t, tagReqCount[tc.t], len(tagReqCount))
			if tagReqCount[tc.t] <= 0 && tagDone[tc.t] {
				log.V(2).Infof("[casng] upload.dispatcher.counter.done.to: tag=%s", tc.t)
				delete(tagDone, tc.t)
				delete(tagReqCount, tc.t)
				// Signal to the requester that all of its requests are done.
				u.uploadPubSub.pub(UploadResponse{done: true}, tc.t)
			}
			// Covers waiting on subscribers.
			log.Infof("[casng] upload.dispatcher.counter.pub.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
			if len(tagReqCount) == 0 && allDone {
				return
			}
		}
	}()

	wg.Wait()
}
