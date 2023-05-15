// Using a different package name to strictly exclude types defined here from the original package.
package casng_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/casng"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

var (
	retryNever    = retry.Immediately(retry.Attempts(1))
	retryTwice    = retry.ExponentialBackoff(time.Microsecond, time.Microsecond, retry.Attempts(2))
	retryAll      = func(error) bool { return true }
	defaultRpcCfg = casng.GRPCConfig{
		ConcurrentCallsLimit: 5,
		ItemsLimit:           2,
		BytesLimit:           1024,
		Timeout:              time.Second,
		BundleTimeout:        time.Millisecond,
		RetryPolicy:          retryNever,
		RetryPredicate:       retryAll,
	}
	defaultIoCfg = casng.IOConfig{
		ConcurrentWalksLimit:     1,
		OpenFilesLimit:           1,
		OpenLargeFilesLimit:      1,
		SmallFileSizeThreshold:   1,
		LargeFileSizeThreshold:   1,
		CompressionSizeThreshold: 10,
		BufferSize:               2,
	}
)

// makeFs creates a temp dir, populates it with files, and returns the path of the temp dir.
func makeFs(t *testing.T, paths map[string][]byte) string {
	t.Helper()

	if len(paths) == 0 {
		t.Fatalf("paths cannot be empty")
	}

	tmp := t.TempDir()

	for p, b := range paths {
		// Check for suffix before joining since filepath.Join removes trailing slashes.
		d := p
		if !strings.HasSuffix(p, "/") {
			d = filepath.Dir(p)
		}
		if err := os.MkdirAll(filepath.Join(tmp, d), 0766); err != nil {
			t.Fatalf("io error: %v", err)
		}
		if p == d {
			continue
		}
		if err := os.WriteFile(filepath.Join(tmp, p), b, 0666); err != nil {
			t.Fatalf("io error: %v", err)
		}
	}

	return tmp
}

type fakeByteStreamClient struct {
	bspb.ByteStreamClient
	write func(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error)
}

type fakeByteStream_WriteClient struct {
	bspb.ByteStream_WriteClient
	send         func(*bspb.WriteRequest) error
	closeAndRecv func() (*bspb.WriteResponse, error)
}

func (s *fakeByteStreamClient) Write(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
	if s.write != nil {
		return s.write(ctx, opts...)
	}
	return &fakeByteStream_WriteClient{}, nil
}

func (s *fakeByteStream_WriteClient) Send(wr *bspb.WriteRequest) error {
	if s.send != nil {
		return s.send(wr)
	}
	return nil
}

func (s *fakeByteStream_WriteClient) CloseAndRecv() (*bspb.WriteResponse, error) {
	if s.closeAndRecv != nil {
		return s.closeAndRecv()
	}
	return &bspb.WriteResponse{}, nil
}

type fakeCAS struct {
	repb.ContentAddressableStorageClient
	findMissingBlobs func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error)
	batchUpdateBlobs func(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error)
}

func (c *fakeCAS) FindMissingBlobs(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
	return c.findMissingBlobs(ctx, in, opts...)
}

func (c *fakeCAS) BatchUpdateBlobs(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
	return c.batchUpdateBlobs(ctx, in, opts...)
}

type byHash []digest.Digest

func (a byHash) Len() int {
	return len(a)
}

func (a byHash) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a byHash) Less(i, j int) bool {
	return a[i].Hash < a[j].Hash
}