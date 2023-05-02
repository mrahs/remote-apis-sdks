// Using a different package name to strictly exclude types defined here from the original package.
package cas_test

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/cas"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/glog"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
)

func TestMissingBlobs_Batching(t *testing.T) {
	tests := []struct {
		name        string
		digests     []digest.Digest
		cas         *fakeCAS
		wantErr     error
		wantDigests []digest.Digest
	}{
		{"empty_request", nil, &fakeCAS{}, nil, nil},
		{"bad_batch", []digest.Digest{largeDigest}, &fakeCAS{}, cas.ErrOversizedItem, []digest.Digest{largeDigest}},
		{
			"no_missing",
			[]digest.Digest{{Hash: "a"}, {Hash: "b"}},
			&fakeCAS{findMissingBlobs: func(_ context.Context, _ *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
				return &repb.FindMissingBlobsResponse{}, nil
			}},
			nil,
			nil,
		},
		{
			"all_missing",
			[]digest.Digest{{Hash: "a"}, {Hash: "b"}},
			&fakeCAS{findMissingBlobs: func(_ context.Context, req *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
				return &repb.FindMissingBlobsResponse{MissingBlobDigests: req.BlobDigests}, nil
			}},
			nil,
			[]digest.Digest{{Hash: "a"}, {Hash: "b"}},
		},
		{
			"some_missing",
			[]digest.Digest{{Hash: "a"}, {Hash: "b"}},
			&fakeCAS{findMissingBlobs: func(_ context.Context, req *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
				return &repb.FindMissingBlobsResponse{MissingBlobDigests: []*repb.Digest{{Hash: "a"}}}, nil
			}},
			nil,
			[]digest.Digest{{Hash: "a"}},
		},
		{
			"error_call",
			[]digest.Digest{{Hash: "a"}, {Hash: "b"}},
			&fakeCAS{findMissingBlobs: func(_ context.Context, req *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
				return &repb.FindMissingBlobsResponse{}, errSend
			}},
			errSend,
			[]digest.Digest{{Hash: "a"}, {Hash: "b"}},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			glog.Infof("test: %s", test.name)
			ctx, ctxCancel := context.WithCancel(context.Background())
			u, err := cas.NewBatchingUploader(ctx, test.cas, &fakeByteStreamClient{}, "", rpcCfg, rpcCfg, rpcCfg, ioCfg)
			if err != nil {
				t.Fatalf("error creating batching uploader: %v", err)
			}
			missing, err := u.MissingBlobs(test.digests)
			if test.wantErr == nil && err != nil {
				t.Errorf("MissingBlobs failed: %v", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Errorf("error mismatch: want %v, got %v", test.wantErr, err)
			}
			sort.Sort(byHash(test.wantDigests))
			sort.Sort(byHash(missing))
			if diff := cmp.Diff(test.wantDigests, missing); diff != "" {
				t.Errorf("missing mismatch, (-want +got): %s", diff)
			}
			ctxCancel()
			u.Wait()
		})
	}
	glog.Flush()
}

func TestMissingBlobs_Concurrent(t *testing.T) {
	fCas := &fakeCAS{findMissingBlobs: func(_ context.Context, _ *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
		return &repb.FindMissingBlobsResponse{}, nil
	}}
	ctx, ctxCancel := context.WithCancel(context.Background())
	u, err := cas.NewBatchingUploader(ctx, fCas, &fakeByteStreamClient{}, "", rpcCfg, rpcCfg, rpcCfg, ioCfg)
	if err != nil {
		t.Fatalf("error creating batching uploader: %v", err)
	}
	digests := []digest.Digest{{Hash: "a"}, {Hash: "b"}, {Hash: "c"}}
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			missing, err := u.MissingBlobs(digests)
			if err != nil {
				t.Errorf("MissingBlobs failed: %v", err)
			}
			if len(missing) > 0 {
				t.Errorf("missing found: %d", len(missing))
			}
		}()
	}
	wg.Wait()
	ctxCancel()
	u.Wait()
}

func TestMissingBlobs_Abort(t *testing.T) {
	fCas := &fakeCAS{findMissingBlobs: func(_ context.Context, _ *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
		return &repb.FindMissingBlobsResponse{}, nil
	}}
	ctx, ctxCancel := context.WithCancel(context.Background())
	u, err := cas.NewBatchingUploader(ctx, fCas, &fakeByteStreamClient{}, "", rpcCfg, rpcCfg, rpcCfg, ioCfg)
	if err != nil {
		t.Fatalf("error creating batching uploader: %v", err)
	}
	ctxCancel()
	digests := []digest.Digest{{Hash: "a"}, {Hash: "b"}, {Hash: "c"}}
	missing, err := u.MissingBlobs(digests)
	if !errors.Is(err, cas.ErrTerminatedUploader) {
		t.Errorf("error mismatch: want %v, got %v", cas.ErrTerminatedUploader, err)
	}
	// No need to sort since the input as is should be returned.
	if diff := cmp.Diff(digests, missing); diff != "" {
		t.Errorf("missing mismatch, (-want +got): %s", diff)
	}
	u.Wait()
}

func TestMissingBlobs_Streaming(t *testing.T) {
	fCas := &fakeCAS{findMissingBlobs: func(_ context.Context, _ *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
		return &repb.FindMissingBlobsResponse{}, nil
	}}
	ctx, ctxCancel := context.WithCancel(context.Background())
	u, err := cas.NewStreamingUploader(ctx, fCas, &fakeByteStreamClient{}, "", rpcCfg, rpcCfg, rpcCfg, ioCfg)
	if err != nil {
		t.Fatalf("error creating batching uploader: %v", err)
	}
	reqChan := make(chan digest.Digest)
	ch := u.MissingBlobs(reqChan)

	go func() {
		for i := 0; i < 1000; i++ {
			reqChan <- digest.Digest{Hash: "a"}
		}
		close(reqChan)
	}()

	// It's not necessary to receive in a separate goroutine, but this allows
	// for testing the Wait() as well, which should block until all concurrent code is properly terminated.
	go func() {
		defer ctxCancel()
		for r := range ch {
			if r.Err != nil {
				t.Errorf("unexpected error: %v", r.Err)
			}
			if r.Missing {
				t.Errorf("unexpected missing: %s", r.Digest.Hash)
			}
		}
	}()
	u.Wait()
}
