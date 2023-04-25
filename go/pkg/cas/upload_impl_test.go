// Using a different package name to strictly exclude types defined here from the original package.
package cas_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/cas"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/google/go-cmp/cmp"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBatching_WriteBytes(t *testing.T) {
	tests := []struct {
		name        string
		bs          *fakeByteStreamClient
		b           []byte
		offset      int64
		finish      bool
		wantErr     error
		wantStats   *cas.Stats
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
			wantStats: &cas.Stats{
				BytesRequested:       3,
				EffectiveBytesMoved:  3,
				TotalBytesMoved:      3,
				LogicalBytesMoved:    3,
				LogicalBytesStreamed: 3,
				CacheMissCount:       1,
				StreamedCount:        1,
			},
		},
		{
			name: "compression",
			bs: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: 3500}, nil
						},
					}, nil
				},
			},
			b:       []byte(strings.Repeat("abcdefg", 500)),
			wantErr: nil,
			wantStats: &cas.Stats{
				BytesRequested:       3500,
				EffectiveBytesMoved:  29,
				TotalBytesMoved:      29,
				LogicalBytesMoved:    3500,
				LogicalBytesStreamed: 3500,
				CacheMissCount:       1,
				StreamedCount:        1,
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
			wantStats: nil,
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
			wantStats: &cas.Stats{
				BytesRequested:       3,
				EffectiveBytesMoved:  2, // matches buffer size
				TotalBytesMoved:      2,
				LogicalBytesMoved:    2,
				LogicalBytesStreamed: 2,
				CacheHitCount:        1,
				LogicalBytesCached:   3,
				StreamedCount:        1,
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
			wantErr: cas.ErrGRPC,
			wantStats: &cas.Stats{
				BytesRequested:       3,
				EffectiveBytesMoved:  2, // matches buffer size
				TotalBytesMoved:      2,
				LogicalBytesMoved:    2,
				LogicalBytesStreamed: 2,
				CacheMissCount:       1,
				StreamedCount:        0,
			},
		},
		{
			name: "send_retry_timeout",
			bs: &fakeByteStreamClient{
				write: func(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							return status.Error(codes.DeadlineExceeded, "error")
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{}, nil
						},
					}, nil
				},
			},
			b:       []byte("abc"),
			wantErr: cas.ErrGRPC,
			wantStats: &cas.Stats{
				BytesRequested:       3,
				EffectiveBytesMoved:  2, // matches one buffer size
				TotalBytesMoved:      4, // matches two buffer sizes
				LogicalBytesMoved:    2,
				LogicalBytesStreamed: 2,
				CacheMissCount:       1,
				StreamedCount:        0,
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
			wantErr: cas.ErrGRPC,
			wantStats: &cas.Stats{
				BytesRequested:       3,
				EffectiveBytesMoved:  3,
				TotalBytesMoved:      3,
				LogicalBytesMoved:    3,
				LogicalBytesStreamed: 3,
				CacheMissCount:       1,
				StreamedCount:        1,
			},
		},
		{
			name: "arbitrary_offset",
			bs: &fakeByteStreamClient{
				write: func(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							if wr.WriteOffset < 5 {
								return fmt.Errorf("mismatched offset: want 5, got %d", wr.WriteOffset)
							}
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: 3}, nil
						},
					}, nil
				},
			},
			b:      []byte("abc"),
			offset: 5,
			wantStats: &cas.Stats{
				BytesRequested:       3,
				EffectiveBytesMoved:  3,
				TotalBytesMoved:      3,
				LogicalBytesMoved:    3,
				LogicalBytesStreamed: 3,
				CacheMissCount:       1,
				StreamedCount:        1,
			},
		},
		{
			name: "finish_write",
			bs: &fakeByteStreamClient{
				write: func(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							if len(wr.Data) == 0 && !wr.FinishWrite {
								return fmt.Errorf("finish write was not set")
							}
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: 3}, nil
						},
					}, nil
				},
			},
			b:      []byte("abc"),
			finish: true,
			wantStats: &cas.Stats{
				BytesRequested:       3,
				EffectiveBytesMoved:  3,
				TotalBytesMoved:      3,
				LogicalBytesMoved:    3,
				LogicalBytesStreamed: 3,
				CacheMissCount:       1,
				StreamedCount:        1,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if test.retryPolicy == nil {
				test.retryPolicy = &retryNever
			}
			testRpcCfg := rpcCfg
			testRpcCfg.RetryPolicy = *test.retryPolicy
			u, err := cas.NewBatchingUploader(context.Background(), &fakeCAS{}, test.bs, "", testRpcCfg, testRpcCfg, testRpcCfg, ioCfg)
			if err != nil {
				t.Fatalf("error creating batching uploader: %v", err)
			}
			var stats *cas.Stats
			if test.finish {
				stats, err = u.WriteBytes(context.Background(), "", bytes.NewReader(test.b), int64(len(test.b)), test.offset)
			} else {
				stats, err = u.WriteBytesPartial(context.Background(), "", bytes.NewReader(test.b), int64(len(test.b)), test.offset)
			}
			if test.wantErr == nil && err != nil {
				t.Errorf("WriteBytes failed: %v", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Errorf("error mismatch: want %v, got %v", test.wantErr, err)
			}
			if diff := cmp.Diff(test.wantStats, stats); diff != "" {
				t.Errorf("stats mismatch, (-want +got): %s", diff)
			}
		})
	}
}

func TestBatching_Upload(t *testing.T) {
	tests := []struct {
		name string
		bsc  *fakeByteStreamClient
		cc   *fakeCAS
		fs   map[string][]byte
		wantStats cas.Stats
		wantUploaded []digest.Digest
	}{
		{
			name: "cache_hit",
			bsc: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{}, nil
						},
					}, nil
				},
			},
			cc: &fakeCAS{
				findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
					return &repb.FindMissingBlobsResponse{
						// MissingBlobDigests: in.BlobDigests,
					}, nil
				},
				batchUpdateBlobs: func(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
					return &repb.BatchUpdateBlobsResponse{
						Responses: []*repb.BatchUpdateBlobsResponse_Response{{Digest: in.Requests[0].Digest, Status: &rpcstatus.Status{}}},
					}, nil
				},
			},
			fs: map[string][]byte{
				"foo.c": []byte("int c;"),
			},
			wantStats: cas.Stats{},
			wantUploaded: []digest.Digest{},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			tmp := makeFs(t, test.fs)
			testRpcCfg := rpcCfg
			testRpcCfg.RetryPolicy = retryNever
			ctx, ctxCancel := context.WithCancel(context.Background())
			u, err := cas.NewBatchingUploader(ctx, test.cc, test.bsc, "", testRpcCfg, testRpcCfg, testRpcCfg, ioCfg)
			if err != nil {
				t.Fatalf("error creating batching uploader: %v", err)
			}
			uploaded, stats, err := u.Upload(ctx, []impath.Absolute{impath.MustAbs(tmp, "foo.c")}, symlinkopts.PreserveAllowDangling(), nil)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(test.wantStats, stats); diff != "" {
				t.Errorf("stats mismatch, (-want +got): %s", diff)
			}
			if diff := cmp.Diff(test.wantUploaded, uploaded); diff != "" {
				t.Errorf("uploaded mismatch, (-want +got): %s", diff)
			}
			ctxCancel()
			u.Wait()
		})
	}
}

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
