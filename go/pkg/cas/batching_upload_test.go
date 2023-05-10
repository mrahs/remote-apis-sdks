package cas_test

import (
	"context"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/cas"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"github.com/google/go-cmp/cmp"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
)

func TestUpload_Batching(t *testing.T) {
	tests := []struct {
		name         string
		fs           map[string][]byte
		root         string
		ioCfg        cas.IOConfig
		rpcCfg       *cas.GRPCConfig
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
		{
			name: "stream_unified",
			fs: map[string][]byte{
				"foo/bar1.c": []byte("int bar;"),
				"foo/bar2.c": []byte("int bar;"),
			},
			root: "foo",
			ioCfg: cas.IOConfig{
				CompressionSizeThreshold: 10000, // large enough to disable compression.
				SmallFileSizeThreshold:   1,
				LargeFileSizeThreshold:   2,
				OpenLargeFilesLimit:      2,
				OpenFilesLimit:           2,
			},
			bsc: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
					var size int64
					return &fakeByteStream_WriteClient{
						send: func(wr *bspb.WriteRequest) error {
							<-time.After(10 * time.Millisecond) // Fake high latency.
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
				BytesRequested:       176,
				LogicalBytesMoved:    168,
				TotalBytesMoved:      168,
				EffectiveBytesMoved:  168,
				LogicalBytesStreamed: 8,   // one copy
				LogicalBytesCached:   8,   // the other copy
				LogicalBytesBatched:  160, // the directory
				CacheMissCount:       2,
				CacheHitCount:        1,
				StreamedCount:        1,
				BatchedCount:         1,
			},
			wantUploaded: []digest.Digest{
				{Hash: "9877358cfe402635019ce7bf591e9fd86d27953b0077e1f173b7875f0043d87a", Size: 8},   // the file
				{Hash: "ffd7226e23f331727703bfd6ce4ebc0f503127f73eaa1ad62469749c82374ec5", Size: 160}, // the directory
			},
		},
		{
			name: "batch_unified",
			fs: map[string][]byte{
				"foo/bar1.c": []byte("int bar;"),
				"foo/bar2.c": []byte("int bar;"),
			},
			root: "foo",
			ioCfg: cas.IOConfig{
				CompressionSizeThreshold: 10000, // large enough to disable compression.
				SmallFileSizeThreshold:   10,
				LargeFileSizeThreshold:   20,
				OpenFilesLimit:           2,
			},
			rpcCfg: &cas.GRPCConfig{
				ConcurrentCallsLimit: 1,
				BytesLimit:           1000,
				ItemsLimit:           10,
				BundleTimeout:        10 * time.Millisecond,
				Timeout:              time.Second,
			},
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
				BytesRequested:      176,
				LogicalBytesMoved:   168,
				TotalBytesMoved:     168,
				EffectiveBytesMoved: 168,
				LogicalBytesCached:  8,
				LogicalBytesBatched: 168,
				CacheMissCount:      2,
				CacheHitCount:       1,
				BatchedCount:        2,
			},
			wantUploaded: []digest.Digest{
				{Hash: "9877358cfe402635019ce7bf591e9fd86d27953b0077e1f173b7875f0043d87a", Size: 8},   // the file
				{Hash: "ffd7226e23f331727703bfd6ce4ebc0f503127f73eaa1ad62469749c82374ec5", Size: 160}, // the directory
			},
		},
	}

	rpcCfg := cas.GRPCConfig{
		ConcurrentCallsLimit: 5,
		ItemsLimit:           2,
		BytesLimit:           1024,
		Timeout:              time.Second,
		BundleTimeout:        time.Millisecond,
		RetryPolicy:          retryNever,
		RetryPredicate:       func(error) bool { return true },
	}

	for _, test := range tests {
		if test.name != "batch_unified" {
			continue
		}
		test := test
		t.Run(test.name, func(t *testing.T) {
			log.Infof("test: %s", test.name)
			tmp := makeFs(t, test.fs)
			if test.rpcCfg == nil {
				test.rpcCfg = &rpcCfg
			}
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
			u, err := cas.NewBatchingUploader(ctx, test.cc, test.bsc, "", *test.rpcCfg, *test.rpcCfg, *test.rpcCfg, test.ioCfg)
			if err != nil {
				t.Fatalf("error creating batching uploader: %v", err)
			}
			uploaded, stats, err := u.Upload(ctx, cas.UploadRequest{Path: impath.MustAbs(tmp, test.root), SymlinkOptions: symlinkopts.PreserveAllowDangling()})
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
	log.Flush()
}

func TestUpload_BatchingAbort(t *testing.T) {
	ctx, ctxCancel := context.WithCancel(context.Background())
	u, err := cas.NewBatchingUploader(ctx, &fakeCAS{}, &fakeByteStreamClient{}, "", defaultRpcCfg, defaultRpcCfg, defaultRpcCfg, defaultIoCfg)
	if err != nil {
		t.Fatalf("error creating batching uploader: %v", err)
	}
	ctxCancel()
	u.Wait()
}
