package casng

import (
	"context"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
)

func TestReplaceWorkingDir(t *testing.T) {
	tests := []struct {
		name     string
		path     impath.Absolute
		root     impath.Absolute
		wd       impath.Relative
		rwd      impath.Relative
		wantPath impath.Absolute
		wantErr  bool
	}{
		{
			name:     "both_set",
			path:     impath.MustAbs("/root/wd/foo"),
			root:     impath.MustAbs("/root"),
			wd:       impath.MustRel("wd"),
			rwd:      impath.MustRel("rwd"),
			wantPath: impath.MustAbs("/root/rwd/foo"),
		},
		{
			name:     "both_empty",
			path:     impath.MustAbs("/root/foo"),
			root:     impath.MustAbs("/root"),
			wd:       impath.MustRel(""),
			rwd:      impath.MustRel(""),
			wantPath: impath.MustAbs("/root/foo"),
		},
		{
			name:     "wd_empty",
			path:     impath.MustAbs("/root/foo"),
			root:     impath.MustAbs("/root"),
			wd:       impath.MustRel(""),
			rwd:      impath.MustRel("rwd"),
			wantPath: impath.MustAbs("/root/rwd/foo"),
		},
		{
			name:     "rwd_empty",
			path:     impath.MustAbs("/root/wd/foo"),
			root:     impath.MustAbs("/root"),
			wd:       impath.MustRel("wd"),
			rwd:      impath.MustRel(""),
			wantPath: impath.MustAbs("/root/foo"),
		},
		{
			name:     "root_not_prefix",
			path:     impath.MustAbs("/root/wd/foo"),
			root:     impath.MustAbs("/root2"),
			wd:       impath.MustRel("wd"),
			rwd:      impath.MustRel("rwd"),
			wantPath: impath.Absolute{},
			wantErr:  true,
		},
		{
			name:     "outside_wd",
			path:     impath.MustAbs("/root/out/foo"),
			root:     impath.MustAbs("/root"),
			wd:       impath.MustRel("out/src"),
			rwd:      impath.MustRel("rwd/a/b"),
			wantPath: impath.MustAbs("/root/rwd/a/foo"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p, err := replaceWorkingDir(context.Background(), test.path, test.root, test.wd, test.rwd)
			if err != nil && !test.wantErr {
				t.Errorf("unexpected error: %v", err)
			}
			if err == nil && test.wantErr {
				t.Errorf("expected an error, but didn't get one")
			}
			if test.wantPath.String() != p.String() {
				t.Errorf("Path mismatch: want: %q, got %q", test.wantPath, p)
			}
		})
	}
}
