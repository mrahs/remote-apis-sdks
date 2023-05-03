package client

import (
	"testing"
	cctx "github.com/bazelbuild/remote-apis-sdks/go/pkg/context"
)

func TestCapToLimit(t *testing.T) {
	type testCase struct {
		name  string
		limit int
		input *cctx.Metadata
		want  *cctx.Metadata
	}
	tests := []testCase{
		{
			name:  "under limit",
			limit: 24,
			input: &cctx.Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
			want: &cctx.Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "actionID over limit",
			limit: 24,
			input: &cctx.Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID-12345678",
				InvocationID: "invocID*",
			},
			want: &cctx.Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "invocationID over limit",
			limit: 29,
			input: &cctx.Metadata{
				ToolName:     "toolName",
				ToolVersion:  "1.2.3",
				ActionID:     "actionID",
				InvocationID: "invocID*-12345678",
			},
			want: &cctx.Metadata{
				ToolName:     "toolName",
				ToolVersion:  "1.2.3",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "both equally over limit",
			limit: 24,
			input: &cctx.Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID-12345678",
				InvocationID: "invocID*-12345678",
			},
			want: &cctx.Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "both over limit but actionID is bigger",
			limit: 24,
			input: &cctx.Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID-123456789012345678",
				InvocationID: "invocID*-12345678",
			},
			want: &cctx.Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "both over limit but invocationID is bigger",
			limit: 24,
			input: &cctx.Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID-12345678",
				InvocationID: "invocID*-123456789012345678",
			},
			want: &cctx.Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			capToLimit(tc.input, tc.limit)
			if *tc.input != *tc.want {
				t.Errorf("Got %+v, want %+v", tc.input, tc.want)
			}
		})
	}
}
