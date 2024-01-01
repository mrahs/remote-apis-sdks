package casng_test

import (
	"context"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/casng"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc"
)

func TestStreaming_MissingBlobsAbort(t *testing.T) {
	fCas := &fakeCAS{findMissingBlobs: func(_ context.Context, _ *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
		return &repb.FindMissingBlobsResponse{}, nil
	}}
	ctx, ctxCancel := context.WithCancel(context.Background())
	ctxCancel()
	u, err := casng.NewStreamingUploader(ctx, fCas, &fakeByteStreamClient{}, "", defaultRPCCfg, defaultRPCCfg, defaultRPCCfg, defaultIOCfg)
	if err != nil {
		t.Fatalf("error creating batching uploader: %v", err)
	}
	reqChan := make(chan digest.Digest)
	ch := u.MissingBlobs(ctx, reqChan)

	go func() {
		reqChan <- digest.Digest{Hash: "a"}
		close(reqChan)
	}()

	for r := range ch {
		if !errors.Is(r.Err, ctx.Err()) {
			t.Errorf("expected ctx error, but got: %v", r.Err)
		}
	}
}

func TestStreaming_MissingBlobs(t *testing.T) {
	fCas := &fakeCAS{findMissingBlobs: func(_ context.Context, _ *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
		return &repb.FindMissingBlobsResponse{}, nil
	}}
	ctx, ctxCancel := context.WithCancel(context.Background())
	u, err := casng.NewStreamingUploader(ctx, fCas, &fakeByteStreamClient{}, "", defaultRPCCfg, defaultRPCCfg, defaultRPCCfg, defaultIOCfg)
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

	defer ctxCancel()
	for r := range ch {
		if r.Err != nil {
			t.Errorf("unexpected error: %v", r.Err)
		}
		if r.Missing {
			t.Errorf("unexpected missing: %s", r.Digest.Hash)
		}
	}
}
