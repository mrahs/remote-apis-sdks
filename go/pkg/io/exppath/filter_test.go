package exppath_test

import (
	"path/filepath"
	"regexp"
	"testing"

	ep "github.com/bazelbuild/remote-apis-sdks/go/pkg/io/exppath"
)

func TestFilterMatch(t *testing.T) {
	f := ep.Filter{
		Regexp: regexp.MustCompile("/bar/"),
		Mode:   4,
	}
	if !f.Path(filepath.Join("foo", "bar", "baz")) {
		t.Errorf("path should have matched")
	}
	if !f.File(filepath.Join("/", "bar", "foo"), 4) {
		t.Errorf("file should have matched")
	}
}

func TestFilterString(t *testing.T) {
	tests := []struct {
		name   string
		filter ep.Filter
		want   string
	}{
		{
			name:   "empty",
			filter: ep.Filter{},
			want:   "",
		},
		{
			name:   "regexp_only",
			filter: ep.Filter{Regexp: regexp.MustCompile("/bar/.*")},
			want:   "/bar/.*;0",
		},
		{
			name:   "regexp_and_mode",
			filter: ep.Filter{Regexp: regexp.MustCompile("/bar/.*"), Mode: 4},
			want:   "/bar/.*;4",
		},
		{
			name:   "mode_only",
			filter: ep.Filter{Mode: 4},
			want:   ";4",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			id := test.filter.String()
			if id != test.want {
				t.Errorf("invalid string: want %q, got %q", test.want, id)
			}
		})
	}
}
