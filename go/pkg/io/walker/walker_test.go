package walker_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	"github.com/google/go-cmp/cmp"
)

func TestWalker(t *testing.T) {
	tests := []struct {
		name    string
		fs      map[string][]byte
		filter  *walker.Filter
		pathStep map[string]walker.NextStep
		wantPathVisitCount map[string]int
		wantErr error
	}{
		{
			name: "single_file",
			fs: map[string][]byte{
				"foo.c": nil,
			},
			wantPathVisitCount: map[string]int{"foo.c": 2},
		},
		{
			name: "empty_dir",
			fs: map[string][]byte{
				"foo/": nil,
			},
			wantPathVisitCount: map[string]int{"foo": 2},
		},
		{
			name: "dir_single_file",
			fs: map[string][]byte{
				"foo/bar.c": nil,
			},
			wantPathVisitCount: map[string]int{"foo": 2, "foo/bar.c": 2},
		},
		{
			name: "single_level",
			fs: map[string][]byte{
				"foo/bar.c": nil,
				"foo/baz.c": nil,
			},
			wantPathVisitCount: map[string]int{"foo": 2, "foo/bar.c": 2, "foo/baz.c": 2},
		},
		{
			name: "two_levels_simple",
			fs: map[string][]byte{
				"foo/a.z":     nil,
				"foo/bar/b.z": nil,
			},
			wantPathVisitCount: map[string]int{"foo": 2, "foo/a.z": 2, "foo/bar": 2, "foo/bar/b.z": 2},
		},
		{
			name: "two_levels",
			fs: map[string][]byte{
				"foo/a.z":         nil,
				"foo/b.z":         nil,
				"foo/bar/c.z":     nil,
				"foo/bar/baz/d.z": nil,
				"foo/bar/baz/e.z": nil,
			},
			wantPathVisitCount: map[string]int{"foo": 2, "foo/a.z": 2, "foo/b.z": 2, "foo/bar": 2, "foo/bar/baz": 2, "foo/bar/c.z": 2, "foo/bar/baz/d.z": 2, "foo/bar/baz/e.z": 2},
		},
		{
			name: "skip_file_by_path",
			fs: map[string][]byte{
				"foo.c": nil,
			},
			filter:  &walker.Filter{Regexp: regexp.MustCompile("foo.c")},
			wantPathVisitCount: map[string]int{},
		},
		{
			name: "path_cancel",
			fs: map[string][]byte{
				"foo.c": nil,
			},
			pathStep: map[string]walker.NextStep{"foo.c": walker.Cancel},
			wantPathVisitCount: map[string]int{"foo.c": 1},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			tmp, root := makeFs(t, test.fs)
			var seq []string
			err := walker.DepthFirst(impath.MustAbs(root), test.filter, 1, func(path, virtualPath impath.Abs, info fs.FileInfo, err error) walker.NextStep  {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				p, _ := filepath.Rel(tmp, path.String())
				seq = append(seq, p)
				return test.pathStep[p]
			})
			if !errors.Is(err, test.wantErr) {
				t.Errorf("unexpected error: %v", err)
			}
			pathVisitCount := validateSequence(t, seq)
			if diff := cmp.Diff(test.wantPathVisitCount, pathVisitCount); diff != "" {
				t.Errorf("path visit count mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func makeFs(t *testing.T, files map[string][]byte) (string, string) {
	t.Helper()

	tmp := t.TempDir()
	var root string
	for name, content := range files {
		if root == "" {
			root = pathHead(t, name)
		}
		p := filepath.Join(tmp, name)
		d := p
		if !strings.HasSuffix(name, "/") {
			d = filepath.Dir(p)
		}
		if err := os.MkdirAll(d, 0766); err != nil {
			t.Fatalf("io error: %v", err)
		}
		if p == d {
			continue
		}
		if err := os.WriteFile(filepath.Join(tmp, name), content, 0666); err != nil {
			t.Fatalf("io error: %v", err)
		}
	}

	return tmp, filepath.Join(tmp, root)
}

func pathHead(t *testing.T, p string) string {
	t.Helper()

	i := strings.IndexRune(p, os.PathSeparator)
	if i < 0 {
		return p
	}
	return p[:i]
}

// validateSequence checks every path is visited twice and the second visit
// happens after all of its children are visited.
// Files are expected to have two visits back to back, while directories may
// have other paths visited in-between.
func validateSequence(t *testing.T, seq []string) map[string]int {
	t.Helper()

	t.Logf("validating sequence: %v\n", seq)
	var parent string
	var lastFile string
	pathVisitCount := map[string]int{}
	for _, p := range seq {
		t.Logf("parent: %q, path: %q\n", parent, p)
		pathVisitCount[p] += 1

		// Remove parent prefix.
		// For root, returns p as is.
		// For a second visit, child will be empty.
		child := strings.TrimLeft(p, parent)

		// If it's the second visit for this directory, move up a level.
		if child == "" {
			parent = filepath.Dir(parent)
			if parent == "." {
				parent = ""
			}
			continue
		}

		// Remove the separator from the path name.
		child = strings.TrimLeft(child, string(os.PathSeparator))

		// If it's a directory, append it to the parent to maintain the chain.
		if !strings.ContainsRune(child, '.') {
			parent = filepath.Join(parent, child)
			continue
		}

		// If it's the second visit for this file, clear it. Otherwise, remember it.
		if lastFile != "" {
			if lastFile != child {
				t.Errorf("unexpected file: want %q, got %q", lastFile, child)
			}
			lastFile = ""
			continue
		}
		lastFile = child
	}
	if parent != "" {
		t.Errorf("incomplete sequence: %q", parent)
	}
	return pathVisitCount
}
