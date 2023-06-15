package casng_test

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/casng"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	"github.com/google/go-cmp/cmp"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

func TestDownload_ReadBytes(t *testing.T) {
	tests := []struct {
		name        string
		resName     string
		bs          *fakeByteStreamClient
		offset      int64
		limit       int64
		retryPolicy *retry.BackoffPolicy
		wantOut     string
		wantStats   casng.Stats
		wantErr     error
	}{
		{
			name:    "no_compression",
			resName: casng.MakeReadResourceName("", "", 0),
			bs: &fakeByteStreamClient{
				read: func(ctx context.Context, in *bspb.ReadRequest, opts ...grpc.CallOption) (bspb.ByteStream_ReadClient, error) {
					return &fakeByteStreamClient_ReadClient{
						recv: func() (*bspb.ReadResponse, error) {
							return &bspb.ReadResponse{Data: []byte("foo")}, io.EOF
						},
					}, nil
				},
			},
			wantOut: "foo",
			wantStats: casng.Stats{
				LogicalBytesMoved:    3,
				TotalBytesMoved:      3,
				EffectiveBytesMoved:  3,
				LogicalBytesStreamed: 3,
				StreamedCount:        1,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			testRpcCfg := defaultRPCCfg
			if test.retryPolicy != nil {
				testRpcCfg.RetryPolicy = *test.retryPolicy
			}
			d, err := casng.NewDownloader(context.Background(), &fakeCAS{}, test.bs, "", testRpcCfg, testRpcCfg, testRpcCfg, defaultIOCfg)
			if err != nil {
				t.Fatalf("error creating downloader: %v", err)
			}
			b := bytes.Buffer{}
			w := bufio.NewWriter(&b)
			dg, stats, err := d.ReadBytes(context.Background(), test.resName, test.offset, test.limit, w)
			w.Flush()
			out := b.String()
			if test.wantErr == nil && err != nil {
				t.Errorf("ReadBytes failed: %v", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Errorf("error mismatch: want %v, got %v", test.wantErr, err)
			}
			if out != test.wantOut {
				t.Errorf("out mismatch, want %q, but got %q", test.wantOut, out)
			}
			wantDigest := digest.NewFromBlob([]byte(out))
			if diff := cmp.Diff(wantDigest, dg); diff != "" {
				t.Errorf("digest mismatch, (-want +got): %s", diff)
			}
			if diff := cmp.Diff(test.wantStats, stats); diff != "" {
				t.Errorf("stats mismatch, (-want +got): %s", diff)
			}
		})
	}
}
