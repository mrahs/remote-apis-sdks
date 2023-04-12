package impath_test

import (
	"errors"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
)

func Test_ToAbs(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
		want  string
		err   error
	}{
		{
			name:  "valid",
			parts: []string{"/foo", "bar", "baz"},
			want:  "/foo/bar/baz",
			err:   nil,
		},
		{
			name:  "not_absolute",
			parts: []string{"foo", "bar", "baz"},
			want:  "",
			err:   impath.ErrNotAbsolute,
		},
		{
			name:  "not_clean",
			parts: []string{"/foo", "bar", "..", "baz"},
			want:  "/foo/baz",
			err:   nil,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := impath.ToAbs(test.parts...)
			if !errors.Is(err, test.err) {
				t.Errorf("unexpected error: %v", err)
			}
			if got.String() != test.want {
				t.Errorf("path mismatch: want %q, got %q", test.want, got)
			}
		})
	}
}

func Test_ToRel(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
		want  string
		err   error
	}{
		{
			name:  "valid",
			parts: []string{"foo", "bar", "baz"},
			want:  "foo/bar/baz",
			err:   nil,
		},
		{
			name:  "not_relative",
			parts: []string{"/foo", "bar", "baz"},
			want:  "",
			err:   impath.ErrNotRelative,
		},
		{
			name:  "not_clean",
			parts: []string{"foo", "bar", "..", "baz"},
			want:  "foo/baz",
			err:   nil,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := impath.ToRel(test.parts...)
			if !errors.Is(err, test.err) {
				t.Errorf("unexpected error: %v", err)
			}
			if got.String() != test.want {
				t.Errorf("path mismatch: want %q, got %q", test.want, got)
			}
		})
	}
}

func Test_JoinAbs(t *testing.T) {
	tests := []struct {
		name   string
		parent impath.Abs
		parts  []impath.Rel
		want   string
		err    error
	}{
		{
			name:   "valid",
			parent: impath.MustAbs("/foo"),
			parts:  []impath.Rel{impath.MustRel("bar"), impath.MustRel("baz")},
			want:   "/foo/bar/baz",
			err:    nil,
		},
		{
			name:   "not_clean",
			parent: impath.MustAbs("/foo"),
			parts:  []impath.Rel{impath.MustRel("bar"), impath.MustRel(".."), impath.MustRel("baz")},
			want:   "/foo/baz",
			err:    nil,
		},
		{
			name:   "ivnalid",
			parent: impath.Abs{},
			parts:  []impath.Rel{impath.MustRel("bar"), impath.MustRel("baz")},
			want:   "",
			err:    impath.ErrNotAbsolute,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := impath.JoinAbs(test.parent, test.parts...)
			if !errors.Is(err, test.err) {
				t.Errorf("unexpected error: %v", err)
			}
			if got.String() != test.want {
				t.Errorf("path mismatch: want %q, got %q", test.want, got)
			}
		})
	}
}

func Test_JoinRel(t *testing.T) {
	tests := []struct {
		name  string
		parts []impath.Rel
		want  string
	}{
		{
			name:  "valid",
			parts: []impath.Rel{impath.MustRel("bar"), impath.MustRel("baz")},
			want:  "bar/baz",
		},
		{
			name:  "not_clean",
			parts: []impath.Rel{impath.MustRel("bar"), impath.MustRel(".."), impath.MustRel("baz")},
			want:  "baz",
		},
		{
			name:  "empty_parts",
			parts: []impath.Rel{impath.MustRel("bar"), {}, impath.MustRel("baz")},
			want:  "bar/baz",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got := impath.JoinRel(test.parts...)
			if got.String() != test.want {
				t.Errorf("path mismatch: want %q, got %q", test.want, got)
			}
		})
	}
}

func Test_Descendant(t *testing.T) {
	tests := []struct {
		name   string
		base   impath.Abs
		target impath.Abs
		want   string
		err    error
	}{
		{
			name:   "valid",
			base:   impath.MustAbs("/a/b"),
			target: impath.MustAbs("/a/b/c/d"),
			want:   "c/d",
			err:    nil,
		},
		{
			name:   "not_descendent",
			base:   impath.MustAbs("/a/b"),
			target: impath.MustAbs("/c/d"),
			want:   "",
			err:    impath.ErrNotDescendant,
		},
		{
			name:   "invalid_base",
			base:   impath.Abs{},
			target: impath.MustAbs("/c/d"),
			want:   "",
			err:    impath.ErrNotRelative,
		},
		{
			name:   "invalid_target",
			base:   impath.MustAbs("/c/d"),
			target: impath.Abs{},
			want:   "",
			err:    impath.ErrNotRelative,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := impath.Descendant(test.base, test.target)
			if !errors.Is(err, test.err) {
				t.Errorf("unexpected error: %v", err)
			}
			if got.String() != test.want {
				t.Errorf("path mismatch: want %q, got %q", test.want, got)
			}
		})
	}
}
