package casng

import (
	"sync"

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
	pendingCh := make(chan tagCount)
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
			if b.done { // The requester will not be sending any further requests.
				pendingCh <- tagCount{b.tag, 0}
				if b.tag == "" { // In fact, the digester (and all requesters) have terminated.
					return
				}
				continue
			}
			if b.digest.IsEmpty() || b.digest.Hash == "" {
				log.Errorf("[casng] upload.dispatcher: received a blob with an empty digest for tag=%s; ignoring", b.tag)
				continue
			}
			log.V(2).Infof("[casng] upload.dispatcher.blob: digest=%s, bytes=%d, tag=%s", b.digest, len(b.bytes), b.tag)
			u.dispatcherPipeCh <- b
			pendingCh <- tagCount{b.tag, 1}
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
				// In the off chance that a request is received after a done signal, ignore it to avoid sending on a closed channel.
				if done {
					log.Errorf("[casng] upload.pipe: received a request after a done signal from tag=%s; ignoring", b.tag)
					continue
				}
				// If the dispatcher has terminated, tell the streamer we're done and continue draining the response channel.
				if b.done {
					done = true
					close(queryCh)
					log.V(2).Info("upload.dispatcher.pipe.done")
					continue
				}

				digestBlobs[b.digest] = append(digestBlobs[b.digest], b)
				queryCh <- missingBlobRequest{digest: b.digest, ctx: b.ctx}

			// This channel is closed by the query pipe when queryCh is closed, which happens when the sender
			// sends a done signal. This ensures all responses are forwarded to the dispatcher.
			case r, ok := <-queryResCh:
				if !ok {
					return
				}
				log.V(2).Infof("[casng] upload.dispatcher.pipe.res: digest=%s, missing=%t, err=%v", r.Digest, r.Missing, r.Err)

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
					}
					u.dispatcherResCh <- res
					continue
				}

				for _, b := range blobs {
					if b.digest.Size <= batchItemSizeLimit {
						u.batcherCh <- b
						continue
					}
					u.streamerCh <- b
				}
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
			log.V(2).Infof("[casng] upload.dispatcher.res: digest=%s, cache_hit=%d, cache_miss=%d, err=%v", r.Digest, r.Stats.CacheHitCount, r.Stats.CacheMissCount, r.Err)
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
				if !r.Digest.IsEmpty() && r.Digest.Hash != "" {
					pendingCh <- tagCount{t, -1}
				}
			}
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
		for tc := range pendingCh {
			if tc.c == 0 { // There will be no more blobs from this requester.
				if tc.t == "" { // In fact, no more blobs for any requester.
					if len(tagReqCount) == 0 {
						return
					}
					allDone = true
					continue
				}
				tagDone[tc.t] = true
				log.V(2).Infof("[casng] upload.dispatcher.blob.done: tag=%s", tc.t)
			}
			tagReqCount[tc.t] += tc.c
			log.V(2).Infof("[casng] upload.dispatcher.count: tag=%s, count=%d", tc.t, tagReqCount[tc.t])
			if tagReqCount[tc.t] <= 0 && tagDone[tc.t] {
				delete(tagDone, tc.t)
				delete(tagReqCount, tc.t)
				// Signal to the requester that all of its requests are done.
				log.V(2).Infof("[casng] upload.dispatcher.done: tag=%s", tc.t)
				u.uploadPubSub.pub(UploadResponse{done: true}, tc.t)
			}
			if len(tagReqCount) == 0 && allDone {
				return
			}
		}
	}()

	wg.Wait()
}
