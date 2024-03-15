package casng

import (
	"context"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/google/go-cmp/cmp"
)

func TestFixKv(t *testing.T) {
	testCases := []struct {
		name  string
		input []any
		want  []any
	}{
		{
			"no_digest_no_paths",
			[]any{"foo", "bar"},
			[]any{"foo", "bar"},
		},
		{
			"digest",
			[]any{"d", &repb.Digest{}},
			[]any{"d", "/0"},
		},
		{
			"paths_unique",
			[]any{"path", "/a/b", "real_path", "/c/d"},
			[]any{"path", "/a/b", "real_path", "/c/d"},
		},
		{
			"paths_redundant",
			[]any{"path", "/a/b", "real_path", "/a/b"},
			[]any{"path", "/a/b"},
		},
		{
			"paths_redundant_more",
			[]any{"path", "/a/b", "real_path", "/a/b", "foo", 5},
			[]any{"path", "/a/b", "foo", 5},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := fixKv(tc.input...)
			if diff := cmp.Diff(got, tc.want); diff != "" {
				t.Errorf(diff)
			}
		})
	}
}

func TestFmtCtx(t *testing.T) {
	testCases := []struct {
		name  string
		trace *ctxTrace
		kv    []any
		want  string
	}{
		{
			"empty",
			nil,
			[]any{},
			"",
		},
		{
			"kv",
			nil,
			[]any{"foo", "bar"},
			"foo=bar",
		},
		{
			"kvs",
			nil,
			[]any{"foo", "bar", "a", 5},
			"foo=bar, a=5",
		},
		{
			"kv_digest_paths",
			nil,
			[]any{"foo", "bar", "d", &repb.Digest{}, "path", "/a/b", "real_path", "/a/b"},
			"foo=bar, d=/0, path=/a/b",
		},
		{
			"trace",
			&ctxTrace{
				region: "r",
				labels: map[any]any{"k": "v"},
			},
			[]any{},
			"region=r, k=v",
		},
		{
			"kv_trace",
			&ctxTrace{
				region: "r",
				labels: map[any]any{"k": "v"},
			},
			[]any{"foo", "bar"},
			"foo=bar, region=r, k=v",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := fmtCtx(context.WithValue(context.Background(), ctxKeyTrace, tc.trace), tc.kv...)
			if diff := cmp.Diff(got, tc.want); diff != "" {
				t.Errorf(diff)
			}
		})
	}
}

func TestFmtTrace(t *testing.T) {
	now := time.Now()

	testCases := []struct {
		name  string
		trace *ctxTrace
		want  string
	}{
		{
			"empty",
			nil,
			"",
		},
		{
			"single",
			&ctxTrace{
				start:  now.Add(-time.Second),
				end:    now,
				region: "r",
				labels: map[any]any{"k": "v"},
			},
			"r, 1s, k=v",
		},
		{
			"single_metric",
			&ctxTrace{
				start:   now.Add(-time.Second),
				end:     now,
				region:  "r",
				labels:  map[any]any{"k": "v"},
				metrics: map[string]metricGauge{"m1": {count: 1, max: 1, sum: 1}},
			},
			"r, 1s, k=v\n    metric: m1, c=1, avg=1.00, max=1",
		},
		{
			"with_parent",
			&ctxTrace{
				start:  now.Add(-time.Second),
				end:    now,
				region: "r",
				// prefix:  "R",
				labels:  map[any]any{"k": "v"},
				metrics: map[string]metricGauge{"m1": {count: 1, max: 1, sum: 1}},
				parentCtx: context.WithValue(context.Background(), ctxKeyTrace, &ctxTrace{
					start:  now.Add(-2 * time.Second),
					end:    now,
					region: "R",
				}),
			},
			"" +
				"r, 1s, k=v\n" +
				"    metric: m1, c=1, avg=1.00, max=1\n" +
				"R, 2s",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := fmtTrace(tc.trace)
			if diff := cmp.Diff(got, tc.want); diff != "" {
				t.Errorf(diff)
			}
		})
	}
}
