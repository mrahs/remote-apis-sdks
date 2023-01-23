package client_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	rc "github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/fakes"
)

func TestDigest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tmpDir := t.TempDir()
	putFile(t, filepath.Join(tmpDir, "root", "a"), "a")
	putFile(t, filepath.Join(tmpDir, "root", "b"), "b")
	putFile(t, filepath.Join(tmpDir, "root", "subdir", "c"), "c")
	putFile(t, filepath.Join(tmpDir, "root", "subdir", "d"), "d")

	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	conn, err := e.Server.NewClientConn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	client, err := rc.NewCASClientWithConfig(ctx, conn, "instance", rc.DefaultCASClientConfig())
	if err != nil {
		t.Fatal(err)
	}

	inputs := []struct {
		input       *rc.UploadInput
		wantDigests map[string]digest.Digest
	}{
		{
			input: &rc.UploadInput{
				Path:      filepath.Join(tmpDir, "root"),
				Allowlist: []string{"a", "b", filepath.Join("subdir", "c")},
			},
			wantDigests: map[string]digest.Digest{
				".":      {Hash: "9a0af914385de712675cd780ae2dcb5e17b8943dc62cf9fc6fbf8ccd6f8c940d", Size: 230},
				"a":      {Hash: "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb", Size: 1},
				"subdir": {Hash: "2d5c8ba78600fcadae65bab790bdf1f6f88278ec4abe1dc3aa7c26e60137dfc8", Size: 75},
			},
		},
		{
			input: &rc.UploadInput{
				Path:      filepath.Join(tmpDir, "root"),
				Allowlist: []string{"a", "b", filepath.Join("subdir", "d")},
			},
			wantDigests: map[string]digest.Digest{
				".":      {Hash: "2ab9cc3c9d504c883a66da62b57eb44fc9ca57abe05e75633b435e017920d8df", Size: 230},
				"a":      {Hash: "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb", Size: 1},
				"subdir": {Hash: "ce33c7475f9ff2f2ee501eafcb2f21825b24a63de6fbabf7fbb886d606a448b9", Size: 75},
			},
		},
	}

	uploadInputs := make([]*rc.UploadInput, len(inputs))
	for i, in := range inputs {
		uploadInputs[i] = in.input
		if in.input.DigestsComputed() == nil {
			t.Fatalf("DigestCopmuted() returned nil")
		}
	}

	if _, err := client.Upload(ctx, rc.UploadOptions{}, uploadInputChanFrom(uploadInputs...)); err != nil {
		t.Fatal(err)
	}

	for i, in := range inputs {
		t.Logf("input %d", i)
		select {
		case <-in.input.DigestsComputed():
			// Good
		case <-time.After(time.Second):
			t.Errorf("Upload succeeded, but DigestsComputed() is not closed")
		}

		for relPath, wantDig := range in.wantDigests {
			gotDig, err := in.input.Digest(relPath)
			if err != nil {
				t.Error(err)
				continue
			}
			if diff := cmp.Diff(gotDig, wantDig); diff != "" {
				t.Errorf("unexpected digest for %s (-want +got):\n%s", relPath, diff)
			}
		}
	}
}

func TestStreaming(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// TODO(nodir): add tests for retries.

	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	conn, err := e.Server.NewClientConn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cfg := rc.DefaultCASClientConfig()
	cfg.BatchUpdateBlobs.MaxSizeBytes = 1
	cfg.ByteStreamWrite.MaxSizeBytes = 2 // force multiple requests in a stream
	cfg.SmallFileThreshold = 2
	cfg.LargeFileThreshold = 3
	cfg.CompressedBytestreamThreshold = 7 // between medium and large
	client, err := rc.NewCASClientWithConfig(ctx, conn, "instance", cfg)
	if err != nil {
		t.Fatal(err)
	}

	tmpDir := t.TempDir()
	largeFilePath := filepath.Join(tmpDir, "testdata", "large")
	putFile(t, largeFilePath, "laaaaaaaaaaarge")

	res, err := client.Upload(ctx, rc.UploadOptions{}, uploadInputChanFrom(
		&rc.UploadInput{Path: largeFilePath}, // large file
	))
	if err != nil {
		t.Fatalf("failed to upload: %s", err)
	}

	cas := e.Server.CAS
	if cas.WriteReqs() != 1 {
		t.Errorf("want 1 write requests, got %d", cas.WriteReqs())
	}

	fileDigest := digest.Digest{Hash: "71944dd83e7e86354c3a9284e299e0d76c0b1108be62c8e7cefa72adf22128bf", Size: 15}
	if got := cas.BlobWrites(fileDigest); got != 1 {
		t.Errorf("want 1 write of %s, got %d", fileDigest, got)
	}

	wantStats := &rc.TransferStats{
		CacheMisses: rc.DigestStat{Digests: 1, Bytes: 15},
		Streamed:    rc.DigestStat{Digests: 1, Bytes: 15},
	}
	if diff := cmp.Diff(wantStats, &res.Stats); diff != "" {
		t.Errorf("unexpected stats (-want +got):\n%s", diff)
	}

	// Upload the large file again.
	if _, err := client.Upload(ctx, rc.UploadOptions{}, uploadInputChanFrom(&rc.UploadInput{Path: largeFilePath})); err != nil {
		t.Fatalf("failed to upload: %s", err)
	}
}

func putFile(t *testing.T, path, contents string) {
	if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
}

func uploadInputChanFrom(inputs ...*rc.UploadInput) chan *rc.UploadInput {
	ch := make(chan *rc.UploadInput, len(inputs))
	for _, in := range inputs {
		ch <- in
	}
	close(ch)
	return ch
}
