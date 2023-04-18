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
		name               string
		paths              []string
		symlinks           map[string]string
		root               string
		filter             *walker.Filter
		pathStep           map[string]walker.NextStep
		symlinkSkip        map[string]bool
		wantPathVisitCount map[string]int
		wantErr            error
	}{
		{
			name:               "single_file",
			paths:              []string{"foo.c"},
			wantPathVisitCount: map[string]int{"foo.c": 2},
		},
		{
			name:               "empty_dir",
			paths:              []string{"foo"},
			wantPathVisitCount: map[string]int{"foo": 2},
		},
		{
			name:               "dir_single_file",
			paths:              []string{"foo/bar.c"},
			wantPathVisitCount: map[string]int{"foo": 2, "foo/bar.c": 2},
		},
		{
			name: "single_level",
			paths: []string{
				"foo/bar.c",
				"foo/baz.c",
			},
			wantPathVisitCount: map[string]int{"foo": 2, "foo/bar.c": 2, "foo/baz.c": 2},
		},
		{
			name: "two_levels_simple",
			paths: []string{
				"foo/a.z",
				"foo/bar/b.z",
			},
			wantPathVisitCount: map[string]int{"foo": 2, "foo/a.z": 2, "foo/bar": 2, "foo/bar/b.z": 2},
		},
		{
			name: "two_levels",
			paths: []string{
				"foo/a.z",
				"foo/b.z",
				"foo/bar/c.z",
				"foo/bar/baz/d.z",
				"foo/bar/baz/e.z",
			},
			wantPathVisitCount: map[string]int{
				"foo":             2,
				"foo/a.z":         2,
				"foo/b.z":         2,
				"foo/bar":         2,
				"foo/bar/baz":     2,
				"foo/bar/c.z":     2,
				"foo/bar/baz/d.z": 2,
				"foo/bar/baz/e.z": 2,
			},
		},
		{
			name:               "skip_file_by_path",
			paths:              []string{"foo.c"},
			filter:             &walker.Filter{Regexp: regexp.MustCompile("foo.c")},
			wantPathVisitCount: map[string]int{},
		},
		{
			name:               "path_cancel",
			paths:              []string{"foo.c"},
			pathStep:           map[string]walker.NextStep{"foo.c": walker.Cancel},
			wantPathVisitCount: map[string]int{"foo.c": 1},
		},
		{
			name:               "single_file_deferred",
			paths:              []string{"foo.c"},
			pathStep:           map[string]walker.NextStep{"foo.c": walker.Defer},
			wantPathVisitCount: map[string]int{"foo.c": 3},
		},
		{
			name:               "single_dir_deferred",
			paths:              []string{"foo/"},
			pathStep:           map[string]walker.NextStep{"foo": walker.Defer},
			wantPathVisitCount: map[string]int{"foo": 3},
		},
		{
			name: "deferred",
			paths: []string{
				"foo/a.z",
				"foo/b.z",
				"foo/bar/c.z",
				"foo/bar/baz/d.z",
				"foo/bar/baz/e.z",
			},
			pathStep: map[string]walker.NextStep{
				"foo/b.z":         walker.Defer,
				"foo/bar/baz/e.z": walker.Defer,
			},
			wantPathVisitCount: map[string]int{
				"foo":             2,
				"foo/a.z":         2,
				"foo/b.z":         3,
				"foo/bar":         2,
				"foo/bar/baz":     2,
				"foo/bar/c.z":     2,
				"foo/bar/baz/d.z": 2,
				"foo/bar/baz/e.z": 3,
			},
		},
		{
			name:     "file_symlink",
			symlinks: map[string]string{"foo.c": "bar.c"},
			wantPathVisitCount: map[string]int{
				"foo.c": 2,
				"bar.c": 2,
			},
		},
		{
			name:     "dir_symlink",
			paths:    []string{"foo/bar.c"},
			symlinks: map[string]string{"foo.c": "foo/"},
			root:     "foo.c",
			wantPathVisitCount: map[string]int{
				"foo.c":     2,
				"foo/bar.c": 2,
				"foo":       2,
			},
		},
		{
			name:     "nested_symlink",
			paths:    []string{"foo/bar.c"},
			symlinks: map[string]string{"foo/baz.c": "a.z"},
			root:     "foo", // Otherwise it's nondeterministic which top-level path is selected.
			wantPathVisitCount: map[string]int{
				"foo":       2,
				"foo/bar.c": 2,
				"foo/baz.c": 2,
				"a.z":       2,
			},
		},
		{
			name:        "skip_symlink",
			paths:       []string{"foo/bar.c"},
			symlinks:    map[string]string{"foo.c": "foo/"},
			symlinkSkip: map[string]bool{"foo.c": true},
			wantPathVisitCount: map[string]int{
				"foo.c": 2,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			tmp, root, pathLayout := makeFs(t, test.paths, test.symlinks)
			if test.root != "" {
				root = filepath.Join(tmp, test.root)
			}
			var seq []string
			err := walker.DepthFirst(impath.MustAbs(root), test.filter, 1, func(path, virtualPath impath.Absolute, info fs.FileInfo, err error) walker.NextStep {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return walker.Cancel
				}
				p, _ := filepath.Rel(tmp, path.String())
				seq = append(seq, p)
				next := test.pathStep[p]
				// Defer once to avoid infinite loops.
				if next == walker.Defer {
					test.pathStep[p] = walker.Continue
				}
				if info != nil && info.Mode()&fs.ModeSymlink == fs.ModeSymlink && test.symlinkSkip[p] {
					return walker.Skip
				}
				return next
			})
			if !errors.Is(err, test.wantErr) {
				t.Errorf("unexpected error: %v", err)
			}
			pathVisitCount := validateSequence(t, seq, pathLayout)
			if diff := cmp.Diff(test.wantPathVisitCount, pathVisitCount); diff != "" {
				t.Errorf("path visit count mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func makeFs(t *testing.T, paths []string, symlinks map[string]string) (string, string, map[string][]string) {
	t.Helper()

	if len(paths) == 0 && len(symlinks) == 0 {
		t.Fatalf("paths and symlinks cannot be both empty")
	}

	// Map each dir to its children.
	pathLayout := map[string][]string{}
	for _, p := range paths {
		parent := filepath.Dir(p)
		pathLayout[parent] = append(pathLayout[parent], p)
	}
	for p, trg := range symlinks {
		parent := filepath.Dir(p)
		pathLayout[parent] = append(pathLayout[parent], p)
		parent = filepath.Dir(trg)
		pathLayout[parent] = append(pathLayout[parent], trg)
	}
	// Recursively parse out parents from other parents.
	// E.g. if the map has the keys foo/bar and bar/baz, it should also include
	// the keys foo and bar as parent directories.
	moreParents := true
	for moreParents {
		moreParents = false
		for p := range pathLayout {
			parent := filepath.Dir(p)
			c := pathLayout[parent]
			if len(c) > 0 {
				continue
			}
			t.Logf("parent: %q", parent)
			if parent != "." {
				moreParents = true
			}
			pathLayout[parent] = append(c, p)
		}
	}
	// The . must be the root of all directories.
	if _, ok := pathLayout["."]; !ok {
		t.Fatalf("root not present in pathChildren at . directory; is there an absolute path in the list? %v", pathLayout)
	}
	root := pathLayout["."][0]

	tmp := t.TempDir()
	for _, p := range paths {
		createFile(t, tmp, p)
	}

	for p, trg := range symlinks {
		createFile(t, tmp, trg)
		err := os.Symlink(filepath.Join(tmp, trg), filepath.Join(tmp, p))
		if err != nil {
			t.Errorf("io error: %v", err)
		}
	}

	return tmp, filepath.Join(tmp, root), pathLayout
}

func createFile(t *testing.T, parent, p string) {
	// Check for suffix before joining since filepath.Join removes trailing slashes.
	d := p
	if !strings.HasSuffix(p, "/") {
		d = filepath.Dir(p)
	}
	if err := os.MkdirAll(filepath.Join(parent, d), 0766); err != nil {
		t.Fatalf("io error: %v", err)
	}
	if p == d {
		return
	}
	if err := os.WriteFile(filepath.Join(parent, p), nil, 0666); err != nil {
		t.Fatalf("io error: %v", err)
	}
}

// validateSequence checks that every path is visited after its children.
func validateSequence(t *testing.T, seq []string, pathLayout map[string][]string) map[string]int {
	t.Helper()

	t.Logf("validating sequence: %v\n", seq)
	var parent string
	pathVisitCount := map[string]int{}
	pendingParent := map[string]bool{}
	for _, p := range seq {
		t.Logf("parent: %q, path: %q\n", parent, p)
		pathVisitCount[p] += 1
		parent := filepath.Dir(p)
		// Parent should be visited after this child.
		pendingParent[parent] = true
		// If this child is itself a parent, mark it as done.
		delete(pendingParent, p)
	}
	delete(pendingParent, ".")
	if len(pendingParent) > 0 {
		t.Errorf("incomplete traversal: %v", pendingParent)
	}
	return pathVisitCount
}
