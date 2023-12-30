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
	defer func(){ callWg.Wait() }()

	// Unify duplicate requests.
	digestRoutes := make(map[digest.Digest][]string)
	digestReqs := make(map[digest.Digest][]string)
	streamResCh := make(chan UploadResponse)
	pending := 0
	for {
		select {
		// The dispatcher closes this channel when it's done dispatching, which happens after the streamer
		// had sent all pending responses.
		// TODO: when in is closed, this is going to be a busy loop.
		case req, ok := <-in:
			if !ok {
				return
			}
			shouldReleaseIOTokens := req.reader != nil
			rctx := ctxWithValues(req.ctx, ctxKeyModule, m, ctxKeySqID, req.id, ctxKeyRtID, req.route)
			infof(rctx, 4, "req", "digest", req.Digest, "large", shouldReleaseIOTokens, "pending", pending)

			digestReqs[req.Digest] = append(digestReqs[req.Digest], req.id)
			routes := digestRoutes[req.Digest]
			routes = append(routes, req.route)
			digestRoutes[req.Digest] = routes
			if len(routes) > 1 {
				// Already in-flight. Release duplicate resources if it's a large file.
				if shouldReleaseIOTokens {
					u.ioThrottler.release(ctx)
					u.ioLargeThrottler.release(ctx)
				}
				infof(rctx, 4, "unified", "digest", req.Digest, "bundle_count", len(routes))
				continue
			}

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
			r.routes = digestRoutes[r.Digest]
			r.reqs = digestReqs[r.Digest]
			delete(digestRoutes, r.Digest)
			delete(digestReqs, r.Digest)
			out <- r
			pending--
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
