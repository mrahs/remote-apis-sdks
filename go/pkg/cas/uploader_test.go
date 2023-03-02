package cas

import (
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
)

func TestUploaderV2(t *testing.T) {
  rpcCfg := RPCCfg{
    ConcurrentCallsLimit: 1,
    ItemsLimit: 1,
    BytesLimit: 1,
    Timeout: time.Second,
  }
  ioCfg := IOCfg{
    OpenFilesLimit: 1,
    OpenLargeFilesLimit: 1,
    SmallFileSizeThreshold: 1,
    LargeFileSizeThreshold: 1,
    CompressionSizeThreshold: 1,
    BufferSize: 1,
  }
  _, err := NewBatchingUploader(
    &fakeCAS{}, rpcCfg, rpcCfg, rpcCfg, ioCfg, retry.Immediately(retry.UnlimitedAttempts))
  if err != nil {
    t.Fatalf("error creating batching uploader: %v", err)
  }
}
