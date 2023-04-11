package walker_test

import (
	"path/filepath"
	"regexp"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
)

func TestFilterMatch(t *testing.T) {
	f := walker.Filter{
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
		filter walker.Filter
		want   string
	}{
		{
			name:   "empty",
			filter: walker.Filter{},
			want:   "",
		},
		{
			name:   "regexp_only",
			filter: walker.Filter{Regexp: regexp.MustCompile("/bar/.*")},
			want:   "/bar/.*;0",
		},
		{
			name:   "regexp_and_mode",
			filter: walker.Filter{Regexp: regexp.MustCompile("/bar/.*"), Mode: 4},
			want:   "/bar/.*;4",
		},
		{
			name:   "mode_only",
			filter: walker.Filter{Mode: 4},
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
