package cas

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	"github.com/google/go-cmp/cmp"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBatchingWriteBytes(t *testing.T) {
	rpcCfg := RPCCfg{
		ConcurrentCallsLimit: 1,
		ItemsLimit:           1,
		BytesLimit:           1024,
		Timeout:              time.Second,
	}
	ioCfg := IOCfg{
		OpenFilesLimit:           1,
		OpenLargeFilesLimit:      1,
		SmallFileSizeThreshold:   1,
		LargeFileSizeThreshold:   1,
		CompressionSizeThreshold: 10,
		BufferSize:               2,
	}

	var (
		errWrite   = fmt.Errorf("write error")
		errSend    = fmt.Errorf("send error")
		retryNever = retry.Immediately(retry.Attempts(0))
		retryTwice = retry.ExponentialBackoff(time.Microsecond, time.Microsecond, retry.Attempts(2))
	)
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
				write: func(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					bytesSent := int64(0)
					return &fakeByteStream_WriteClient{
						send: func(wr *bsgrpc.WriteRequest) error {
							bytesSent += int64(len(wr.Data))
							return nil
						},
						closeAndRecv: func() (*bsgrpc.WriteResponse, error) {
							return &bsgrpc.WriteResponse{CommittedSize: bytesSent}, nil
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
				write: func(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					bytesSent := int64(0)
					return &fakeByteStream_WriteClient{
						send: func(wr *bsgrpc.WriteRequest) error {
							bytesSent += int64(len(wr.Data))
							return nil
						},
						closeAndRecv: func() (*bsgrpc.WriteResponse, error) {
							return &bsgrpc.WriteResponse{CommittedSize: bytesSent}, nil
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
				write: func(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
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
				write: func(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					return &fakeByteStream_WriteClient{
						send: func(wr *bsgrpc.WriteRequest) error {
							return io.EOF
						},
						closeAndRecv: func() (*bsgrpc.WriteResponse, error) {
							return &bsgrpc.WriteResponse{}, nil
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
				write: func(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					return &fakeByteStream_WriteClient{
						send: func(wr *bsgrpc.WriteRequest) error {
							return errSend
						},
						closeAndRecv: func() (*bsgrpc.WriteResponse, error) {
							return &bsgrpc.WriteResponse{}, nil
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
				write: func(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					return &fakeByteStream_WriteClient{
						send: func(wr *bsgrpc.WriteRequest) error {
							return status.Error(codes.Internal, "error")
						},
						closeAndRecv: func() (*bsgrpc.WriteResponse, error) {
							return &bsgrpc.WriteResponse{}, nil
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
		// {
		//   name: "stream_close_error",
		// },
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.retryPolicy == nil {
				test.retryPolicy = &retryNever
			}
			u, err := NewBatchingUploader(&fakeCAS{}, test.bs, rpcCfg, rpcCfg, rpcCfg, ioCfg, *test.retryPolicy)
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
	bsgrpc.ByteStreamClient
	write func(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error)
}

type fakeByteStream_WriteClient struct {
	bsgrpc.ByteStream_WriteClient
	send         func(*bsgrpc.WriteRequest) error
	closeAndRecv func() (*bsgrpc.WriteResponse, error)
}

func (s *fakeByteStreamClient) Write(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
	if s.write != nil {
		return s.write(ctx, opts...)
	}
	return &fakeByteStream_WriteClient{}, nil
}

func (s *fakeByteStream_WriteClient) Send(wr *bsgrpc.WriteRequest) error {
	if s.send != nil {
		return s.send(wr)
	}
	return nil
}

func (s *fakeByteStream_WriteClient) CloseAndRecv() (*bsgrpc.WriteResponse, error) {
	if s.closeAndRecv != nil {
		return s.closeAndRecv()
	}
	return &bsgrpc.WriteResponse{}, nil
}
