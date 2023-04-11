package walker_test

import (
	"io/fs"
	"testing"
	"time"

	ep "github.com/bazelbuild/remote-apis-sdks/go/pkg/io/exppath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
)

func TestWalker(t *testing.T) {
	walker.FS = &walker.FileSystem{
		Lstat: func(path string) (fs.FileInfo, error) {
			return &fileInfo{name: "foo", size: 10, mode: 4, modTime: time.Now(), isDir: true}, nil
		},
	}

	root, _ := ep.NewAbs("/foo")
	i := 0
	err := walker.DepthFirst(root, nil, 1, func(path ep.Abs, virtualPath ep.Abs, info fs.FileInfo, err error) (walker.NextStep, error)  {
		if path.String() != root.String() {
			t.Errorf("invalid path: want %q, got %q", root, path)
		}
		if virtualPath.String() != root.String() {
			t.Errorf("invalid path: want %q, got %q", root, virtualPath)
		}
		if info.Name() != "foo" {
			t.Errorf("invalid name: want %q, got %q", "foo", info.Name())
		}
		if info.Size() != 10 {
			t.Errorf("invalid size: want %d, got %d", 10, info.Size())
		}
		if info.Mode() != 4 {
			t.Errorf("invalid mode: want %d, got %d", 4, info.Mode())
		}
		if !info.IsDir() {
			t.Errorf("invalid type: want %q, got %t", "dir", info.IsDir())
		}
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		i+=1
		return walker.Continue, nil
	})

	if err!=nil {
		t.Errorf("unexpected error: %v", err)
	}
	if i != 2 {
		t.Errorf("invalid call count: want %d, got %d", 2, i)
	}
}

type fileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
	isDir   bool
	sys     any
}

func (fi *fileInfo) Name() string {
	return fi.name
}

func (fi *fileInfo) Size() int64 {
	return fi.size
}

func (fi *fileInfo) Mode() fs.FileMode {
	return fi.mode
}

func (fi *fileInfo) ModTime() time.Time {
	return fi.modTime
}

func (fi *fileInfo) IsDir() bool {
	return fi.isDir
}

func (fi *fileInfo) Sys() any {
	return fi.sys
}
