package cas_test

import (
	"context"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/cas"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc"
)

func TestMissingBlobs_StreamingAbort(t *testing.T) {
	fCas := &fakeCAS{findMissingBlobs: func(_ context.Context, _ *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
		return &repb.FindMissingBlobsResponse{}, nil
	}}
	ctx, ctxCancel := context.WithCancel(context.Background())
	ctxCancel()
	u, err := cas.NewStreamingUploader(ctx, fCas, &fakeByteStreamClient{}, "", defaultRpcCfg, defaultRpcCfg, defaultRpcCfg, defaultIoCfg)
	if err != nil {
		t.Fatalf("error creating batching uploader: %v", err)
	}
	u.Wait()
}

func TestMissingBlobs_Streaming(t *testing.T) {
	fCas := &fakeCAS{findMissingBlobs: func(_ context.Context, _ *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
		return &repb.FindMissingBlobsResponse{}, nil
	}}
	ctx, ctxCancel := context.WithCancel(context.Background())
	u, err := cas.NewStreamingUploader(ctx, fCas, &fakeByteStreamClient{}, "", defaultRpcCfg, defaultRpcCfg, defaultRpcCfg, defaultIoCfg)
	if err != nil {
		t.Fatalf("error creating batching uploader: %v", err)
	}
	reqChan := make(chan digest.Digest)
	ch := u.MissingBlobs(ctx, reqChan)

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
