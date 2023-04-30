// Using a different package name to strictly exclude types defined here from the original package.
package cas_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/cas"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	glog "github.com/golang/glog"
	"github.com/google/go-cmp/cmp"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUpload_WriteBytes(t *testing.T) {
	tests := []struct {
		name        string
		bs          *fakeByteStreamClient
		b           []byte
		offset      int64
		finish      bool
		wantErr     error
		wantStats   cas.Stats
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
			wantStats: cas.Stats{
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
			wantStats: cas.Stats{
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
			wantStats: cas.Stats{},
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
			wantStats: cas.Stats{
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
			wantStats: cas.Stats{
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
			wantStats: cas.Stats{
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
			wantStats: cas.Stats{
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
			wantStats: cas.Stats{
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
			wantStats: cas.Stats{
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
			var stats cas.Stats
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

func TestUpload_Upload(t *testing.T) {
	tests := []struct {
		name         string
		fs           map[string][]byte
		root         string
		ioCfg        cas.IOConfig
		bsc          *fakeByteStreamClient
		cc           *fakeCAS
		wantStats    *cas.Stats
		wantUploaded []digest.Digest
	}{
		{
			name: "cache_hit",
			fs: map[string][]byte{
				"foo.c": []byte("int c;"),
			},
			root: "foo.c",
			ioCfg: cas.IOConfig{
				CompressionSizeThreshold: 100, // disable compression.
				BufferSize:               1,
				SmallFileSizeThreshold:   1, // ensure the blob gets streamed.
			},
			bsc: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
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
			cc: &fakeCAS{
				findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
					return &repb.FindMissingBlobsResponse{}, nil
				},
				batchUpdateBlobs: func(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
					return &repb.BatchUpdateBlobsResponse{
						Responses: []*repb.BatchUpdateBlobsResponse_Response{{Digest: in.Requests[0].Digest, Status: &rpcstatus.Status{}}},
					}, nil
				},
			},
			wantStats: &cas.Stats{
				BytesRequested:       6,
				LogicalBytesMoved:    1, // matches a single buffer size
				TotalBytesMoved:      1,
				EffectiveBytesMoved:  1,
				LogicalBytesCached:   6,
				LogicalBytesStreamed: 1,
				CacheHitCount:        1,
				StreamedCount:        1,
			},
			wantUploaded: nil,
		},
		{
			name: "batch_single_blob",
			fs: map[string][]byte{
				"foo.c": []byte("int c;"), // 6 bytes
			},
			root: "foo.c",
			ioCfg: cas.IOConfig{
				SmallFileSizeThreshold: 10, // larger than the blob to ensure it gets batched.
				LargeFileSizeThreshold: 1000,
			},
			bsc: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					var size int64
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							size += int64(len(wr.Data))
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: size}, nil
						},
					}, nil
				},
			},
			cc: &fakeCAS{
				findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
					return &repb.FindMissingBlobsResponse{MissingBlobDigests: in.BlobDigests}, nil
				},
				batchUpdateBlobs: func(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
					return &repb.BatchUpdateBlobsResponse{
						Responses: []*repb.BatchUpdateBlobsResponse_Response{{Digest: in.Requests[0].Digest, Status: &rpcstatus.Status{}}},
					}, nil
				},
			},
			wantStats: &cas.Stats{
				BytesRequested:      6,
				LogicalBytesMoved:   6,
				TotalBytesMoved:     6,
				EffectiveBytesMoved: 6,
				LogicalBytesBatched: 6,
				CacheMissCount:      1,
				BatchedCount:        1,
			},
			wantUploaded: []digest.Digest{{Hash: "62f74d0e355efb6101ee13172d05e89592d4aef21ba0e4041584d8653e60c4c3", Size: 6}},
		},
		{
			name: "stream_single_blob",
			fs: map[string][]byte{
				"foo.c": []byte("int c;"), // 6 bytes
			},
			root: "foo.c",
			ioCfg: cas.IOConfig{
				CompressionSizeThreshold: 10, // larger than the blob to avoid compression.
				SmallFileSizeThreshold:   1,  // smaller than the blob to ensure it gets streamed.
				LargeFileSizeThreshold:   2,
			},
			bsc: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					var size int64
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							size += int64(len(wr.Data))
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: size}, nil
						},
					}, nil
				},
			},
			cc: &fakeCAS{
				findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
					return &repb.FindMissingBlobsResponse{MissingBlobDigests: in.BlobDigests}, nil
				},
				batchUpdateBlobs: func(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
					return &repb.BatchUpdateBlobsResponse{
						Responses: []*repb.BatchUpdateBlobsResponse_Response{{Digest: in.Requests[0].Digest, Status: &rpcstatus.Status{}}},
					}, nil
				},
			},
			wantStats: &cas.Stats{
				BytesRequested:       6,
				LogicalBytesMoved:    6,
				TotalBytesMoved:      6,
				EffectiveBytesMoved:  6,
				LogicalBytesStreamed: 6,
				CacheMissCount:       1,
				StreamedCount:        1,
			},
			wantUploaded: []digest.Digest{{Hash: "62f74d0e355efb6101ee13172d05e89592d4aef21ba0e4041584d8653e60c4c3", Size: 6}},
		},
		{
			name: "batch_directory",
			fs: map[string][]byte{
				"foo/bar.c":   []byte("int bar;"),
				"foo/baz.c":   []byte("int baz;"),
				"foo/a/b/c.c": []byte("int c;"),
			},
			root: "foo",
			ioCfg: cas.IOConfig{
				CompressionSizeThreshold: 10000, // large enough to disable compression.
				SmallFileSizeThreshold:   1000,  // large enough to ensure all blobs are batched.
				LargeFileSizeThreshold:   1000,
			},
			bsc: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					var size int64
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							size += int64(len(wr.Data))
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: size}, nil
						},
					}, nil
				},
			},
			cc: &fakeCAS{
				findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
					return &repb.FindMissingBlobsResponse{MissingBlobDigests: in.BlobDigests}, nil
				},
				batchUpdateBlobs: func(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
					resp := make([]*repb.BatchUpdateBlobsResponse_Response, len(in.Requests))
					for i, r := range in.Requests {
						resp[i] = &repb.BatchUpdateBlobsResponse_Response{Digest: r.Digest, Status: &rpcstatus.Status{}}
					}
					return &repb.BatchUpdateBlobsResponse{
						Responses: resp,
					}, nil
				},
			},
			wantStats: &cas.Stats{
				BytesRequested:      407,
				LogicalBytesMoved:   407,
				TotalBytesMoved:     407,
				EffectiveBytesMoved: 407,
				LogicalBytesBatched: 407,
				CacheMissCount:      6,
				BatchedCount:        6,
			},
			wantUploaded: []digest.Digest{
				{Hash: "62f74d0e355efb6101ee13172d05e89592d4aef21ba0e4041584d8653e60c4c3", Size: 6},   // foo/a/b/c.c
				{Hash: "9877358cfe402635019ce7bf591e9fd86d27953b0077e1f173b7875f0043d87a", Size: 8},   // foo/bar.c
				{Hash: "6aaaeea4a97ffca961316ffc535dc101d077c89aed6885da0e8893fa497bf8c2", Size: 8},   // foo/baz.c
				{Hash: "9093edf5f915dd0a5b2181ea08180d8a129e9e231bd0341bdf188c20d0c270d5", Size: 77},  // foo/a/b
				{Hash: "8ecd5bd172610cf88adf66f171251299ac0541be80bc40c7ac081de911284624", Size: 75},  // foo/a
				{Hash: "e62b0cd1aedd5cd2249d3afb418454483777f5deecfcd2df7fc113f546340b2e", Size: 233}, // foo
			},
		},
		{
			name: "batch_stream_directory",
			fs: map[string][]byte{
				"foo/bar.c":   []byte("int bar;"),
				"foo/baz.c":   []byte("int baz;"),
				"foo/a/b/c.c": []byte("int c;"),
			},
			root: "foo",
			ioCfg: cas.IOConfig{
				CompressionSizeThreshold: 10000, // large enough to disable compression.
				SmallFileSizeThreshold:   1,
				LargeFileSizeThreshold:   2,
			},
			bsc: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					var size int64
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							size += int64(len(wr.Data))
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: size}, nil
						},
					}, nil
				},
			},
			cc: &fakeCAS{
				findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
					return &repb.FindMissingBlobsResponse{MissingBlobDigests: in.BlobDigests}, nil
				},
				batchUpdateBlobs: func(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
					resp := make([]*repb.BatchUpdateBlobsResponse_Response, len(in.Requests))
					for i, r := range in.Requests {
						resp[i] = &repb.BatchUpdateBlobsResponse_Response{Digest: r.Digest, Status: &rpcstatus.Status{}}
					}
					return &repb.BatchUpdateBlobsResponse{
						Responses: resp,
					}, nil
				},
			},
			wantStats: &cas.Stats{
				BytesRequested:       407,
				LogicalBytesMoved:    407,
				TotalBytesMoved:      407,
				EffectiveBytesMoved:  407,
				LogicalBytesStreamed: 22,  // just the files
				LogicalBytesBatched:  385, // the directories
				CacheMissCount:       6,
				StreamedCount:        3,
				BatchedCount:         3,
			},
			wantUploaded: []digest.Digest{
				{Hash: "62f74d0e355efb6101ee13172d05e89592d4aef21ba0e4041584d8653e60c4c3", Size: 6},   // foo/a/b/c.c
				{Hash: "9877358cfe402635019ce7bf591e9fd86d27953b0077e1f173b7875f0043d87a", Size: 8},   // foo/bar.c
				{Hash: "6aaaeea4a97ffca961316ffc535dc101d077c89aed6885da0e8893fa497bf8c2", Size: 8},   // foo/baz.c
				{Hash: "9093edf5f915dd0a5b2181ea08180d8a129e9e231bd0341bdf188c20d0c270d5", Size: 77},  // foo/a/b
				{Hash: "8ecd5bd172610cf88adf66f171251299ac0541be80bc40c7ac081de911284624", Size: 75},  // foo/a
				{Hash: "e62b0cd1aedd5cd2249d3afb418454483777f5deecfcd2df7fc113f546340b2e", Size: 233}, // foo
			},
		},
		// TODO: digset cache hit
		// TODO: all digestions fail (nothing is dispatched)
		// TODO: unified
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			glog.Infof("test: %s", test.name)
			tmp := makeFs(t, test.fs)
			testRpcCfg := rpcCfg
			testRpcCfg.RetryPolicy = retryNever
			if test.ioCfg.ConcurrentWalksLimit <= 0 {
				test.ioCfg.ConcurrentWalksLimit = 1
			}
			if test.ioCfg.BufferSize <= 0 {
				test.ioCfg.BufferSize = 1
			}
			if test.ioCfg.OpenFilesLimit <= 0 {
				test.ioCfg.OpenFilesLimit = 1
			}
			if test.ioCfg.OpenLargeFilesLimit <= 0 {
				test.ioCfg.OpenLargeFilesLimit = 1
			}
			ctx, ctxCancel := context.WithCancel(context.Background())
			u, err := cas.NewBatchingUploader(ctx, test.cc, test.bsc, "", testRpcCfg, testRpcCfg, testRpcCfg, test.ioCfg)
			if err != nil {
				t.Fatalf("error creating batching uploader: %v", err)
			}
			uploaded, stats, err := u.Upload(cas.UploadRequest{Path: impath.MustAbs(tmp, test.root), SymlinkOptions: symlinkopts.PreserveAllowDangling()})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(test.wantStats, stats); diff != "" {
				t.Errorf("stats mismatch, (-want +got): %s", diff)
			}
			sort.Slice(uploaded, func(i, j int) bool { return uploaded[i].Hash > uploaded[j].Hash })
			sort.Slice(test.wantUploaded, func(i, j int) bool { return test.wantUploaded[i].Hash > test.wantUploaded[j].Hash })
			if diff := cmp.Diff(test.wantUploaded, uploaded); diff != "" {
				t.Errorf("uploaded mismatch, (-want +got): %s", diff)
			}
			ctxCancel()
			u.Wait()
		})
	}
	glog.Flush()
}

func TestUpload_Abort(t *testing.T) {
	ctx, ctxCancel := context.WithCancel(context.Background())
	u, err := cas.NewBatchingUploader(ctx, &fakeCAS{}, &fakeByteStreamClient{}, "", rpcCfg, rpcCfg, rpcCfg, ioCfg)
	if err != nil {
		t.Fatalf("error creating batching uploader: %v", err)
	}
	ctxCancel()
	u.Wait()
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
