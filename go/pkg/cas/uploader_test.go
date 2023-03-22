package cas

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	// repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/google/go-cmp/cmp"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	rpcCfg = RPCCfg{
		ConcurrentCallsLimit: 5,
		ItemsLimit:           2,
		BytesLimit:           1024,
		Timeout:              time.Second,
		BundleTimeout:        time.Millisecond,
	}
	ioCfg = IOCfg{
		OpenFilesLimit:           1,
		OpenLargeFilesLimit:      1,
		SmallFileSizeThreshold:   1,
		LargeFileSizeThreshold:   1,
		CompressionSizeThreshold: 10,
		BufferSize:               2,
	}
	largeDigest = digest.Digest{Size: 2048}
	errWrite    = fmt.Errorf("write error")
	errSend     = fmt.Errorf("send error")
	errClose    = fmt.Errorf("close error")
	retryNever  = retry.Immediately(retry.Attempts(0))
	retryTwice  = retry.ExponentialBackoff(time.Microsecond, time.Microsecond, retry.Attempts(2))
)

func TestBatching_MissingBlobs(t *testing.T) {
	tests := []struct {
		name        string
		digests     []digest.Digest
		cas         *fakeCAS
		wantErr     error
		wantDigests []digest.Digest
	}{
		{"empty_request", nil, &fakeCAS{}, nil, nil},
		{"bad_batch", []digest.Digest{largeDigest}, &fakeCAS{}, ErrOversizedBlob, []digest.Digest{largeDigest}},
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
			u, err := NewBatchingUploader(context.Background(), test.cas, &fakeByteStreamClient{}, "", rpcCfg, rpcCfg, rpcCfg, ioCfg, retryNever)
			if err != nil {
				t.Fatalf("error creating batching uploader: %v", err)
			}
			missing, err := u.MissingBlobs(context.Background(), test.digests)
			if test.wantErr == nil && err != nil {
				t.Errorf("MissingBlobs failed: %v", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Errorf("error mismatch: got %v, want %v", err, test.wantErr)
			}
			sort.Sort(byHash(test.wantDigests))
			sort.Sort(byHash(missing))
			if diff := cmp.Diff(test.wantDigests, missing); diff != "" {
				t.Errorf("missing mismatch, (-want +got): %s", diff)
			}
			_ = u.Close()
		})
	}
}

func TestBatching_MissingBlobsConcurrent(t *testing.T) {
	cas := &fakeCAS{findMissingBlobs: func(_ context.Context, _ *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
		return &repb.FindMissingBlobsResponse{}, nil
	}}
	u, err := NewBatchingUploader(context.Background(), cas, &fakeByteStreamClient{}, "", rpcCfg, rpcCfg, rpcCfg, ioCfg, retryNever)
	if err != nil {
		t.Fatalf("error creating batching uploader: %v", err)
	}
	digests := []digest.Digest{{Hash: "a"}, {Hash: "b"}, {Hash: "c"}}
	for i := 0; i < 100; i++ {
		go func() {
			missing, err := u.MissingBlobs(context.Background(), digests)
			if err != nil {
				t.Errorf("MissingBlobs failed: %v", err)
			}
			if len(missing) > 0 {
				t.Errorf("missing found: %d", len(missing))
			}
		}()
	}
	_ = u.Close()
}

func TestBatching_MissingBlobsAbort(t *testing.T) {
	cas := &fakeCAS{findMissingBlobs: func(_ context.Context, _ *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
		return &repb.FindMissingBlobsResponse{}, nil
	}}
	u, err := NewBatchingUploader(context.Background(), cas, &fakeByteStreamClient{}, "", rpcCfg, rpcCfg, rpcCfg, ioCfg, retryNever)
	if err != nil {
		t.Fatalf("error creating batching uploader: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Canceling the context before using it should get the sending goroutine to abort.
	cancel()
	digests := []digest.Digest{{Hash: "a"}, {Hash: "b"}, {Hash: "c"}}
	missing, err := u.MissingBlobs(ctx, digests)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error mismatch: got %v, want %v", err, context.Canceled)
	}
	// No need to sort since the input as is should be returned.
	if diff := cmp.Diff(digests, missing); diff != "" {
		t.Errorf("missing mismatch, (-want +got): %s", diff)
	}
	_ = u.Close()
}

func TestStreaming_MissingBlobs(t *testing.T) {
	cas := &fakeCAS{findMissingBlobs: func(_ context.Context, _ *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
		return &repb.FindMissingBlobsResponse{}, nil
	}}
	u, err := NewStreamingUploader(context.Background(), cas, &fakeByteStreamClient{}, "", rpcCfg, rpcCfg, rpcCfg, ioCfg, retryNever)
	if err != nil {
		t.Fatalf("error creating batching uploader: %v", err)
	}
	reqChan := make(chan digest.Digest)
	ch := u.MissingBlobs(context.Background(), reqChan)

	go func() {
		for i := 0; i < 1000; i++ {
			reqChan <- digest.Digest{Hash: "a"}
		}
		close(reqChan)
	}()

	// It's not necessary to receive in a separate goroutine, but this allows
	// for testing the Close() as well, which should block until all concurrent code is properly terminated.
	go func() {
		for r := range ch {
			if r.Err != nil {
				t.Errorf("unexpected error: %v", r.Err)
			}
			if r.Missing {
				t.Errorf("unexpected missing: %s", r.Digest.Hash)
			}
		}
	}()
	_ = u.Close()
}

func TestBatching_WriteBytes(t *testing.T) {
	tests := []struct {
		name        string
		bs          *fakeByteStreamClient
		b           []byte
		wantErr     error
		wantStats   Stats
		retryPolicy *retry.BackoffPolicy
	}{
		{
			name: "no_compression",
			bs: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					bytesSent := int64(0)
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							bytesSent += int64(len(wr.Data))
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: bytesSent}, nil
						},
					}, nil
				},
			},
			b:       []byte("abs"),
			wantErr: nil,
			wantStats: Stats{
				BytesRequesetd:    3,
				BytesAttempted:    3,
				BytesMoved:        3,
				LogicalBytesMoved: 3,
				BytesStreamed:     3,
				CacheMissCount:    1,
				StreamedCount:     1,
			},
		},
		{
			name: "compression",
			bs: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					bytesSent := int64(0)
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							bytesSent += int64(len(wr.Data))
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: bytesSent}, nil
						},
					}, nil
				},
			},
			b:       []byte(strings.Repeat("abcdefg", 500)),
			wantErr: nil,
			wantStats: Stats{
				BytesRequesetd:    3500,
				BytesAttempted:    29,
				BytesMoved:        29,
				LogicalBytesMoved: 3500,
				BytesStreamed:     3500,
				CacheMissCount:    1,
				StreamedCount:     1,
			},
		},
		{
			name: "write_call_error",
			bs: &fakeByteStreamClient{
				write: func(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					return nil, errWrite
				},
			},
			b:         []byte("abc"),
			wantErr:   errWrite,
			wantStats: Stats{},
		},
		{
			name: "cache_hit",
			bs: &fakeByteStreamClient{
				write: func(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							return io.EOF
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{}, nil
						},
					}, nil
				},
			},
			b:       []byte("abc"),
			wantErr: nil,
			wantStats: Stats{
				BytesRequesetd:    3,
				BytesAttempted:    2, // matches buffer size
				BytesMoved:        2,
				LogicalBytesMoved: 2,
				BytesStreamed:     2,
				CacheHitCount:     1,
				BytesCached:       3,
				StreamedCount:     1,
			},
		},
		{
			name: "send_error",
			bs: &fakeByteStreamClient{
				write: func(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							return errSend
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{}, nil
						},
					}, nil
				},
			},
			b:       []byte("abc"),
			wantErr: ErrGRPC,
			wantStats: Stats{
				BytesRequesetd:    3,
				BytesAttempted:    2, // matches buffer size
				BytesMoved:        2,
				LogicalBytesMoved: 2,
				BytesStreamed:     2,
				CacheMissCount:    1,
				StreamedCount:     0,
			},
		},
		{
			name: "send_retry_timeout",
			bs: &fakeByteStreamClient{
				write: func(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							return status.Error(codes.Internal, "error")
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{}, nil
						},
					}, nil
				},
			},
			b:       []byte("abc"),
			wantErr: ErrGRPC,
			wantStats: Stats{
				BytesRequesetd:    3,
				BytesAttempted:    2, // matches one buffer size
				BytesMoved:        4, // matches two buffer sizes
				LogicalBytesMoved: 2,
				BytesStreamed:     2,
				CacheMissCount:    1,
				StreamedCount:     0,
			},
			retryPolicy: &retryTwice,
		},
		{
			name: "stream_close_error",
			bs: &fakeByteStreamClient{
				write: func(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return nil, errClose
						},
					}, nil
				},
			},
			b:       []byte("abc"),
			wantErr: ErrGRPC,
			wantStats: Stats{
				BytesRequesetd:    3,
				BytesAttempted:    3,
				BytesMoved:        3,
				LogicalBytesMoved: 3,
				BytesStreamed:     3,
				CacheMissCount:    1,
				StreamedCount:     1,
			},
			retryPolicy: &retryTwice,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if test.retryPolicy == nil {
				test.retryPolicy = &retryNever
			}
			u, err := NewBatchingUploader(context.Background(), &fakeCAS{}, test.bs, "", rpcCfg, rpcCfg, rpcCfg, ioCfg, *test.retryPolicy)
			if err != nil {
				t.Fatalf("error creating batching uploader: %v", err)
			}
			stats, err := u.WriteBytes(context.Background(), "", test.b, 0, true)
			if test.wantErr == nil && err != nil {
				t.Errorf("WriteBytes failed: %v", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Errorf("error mismatch: got %v, want %v", err, test.wantErr)
			}
			if diff := cmp.Diff(test.wantStats, stats); diff != "" {
				t.Errorf("stats mismatch, (-want +got): %s", diff)
			}
		})
	}
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
