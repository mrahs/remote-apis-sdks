//go:build !debug

package casng

import (
	"context"
)

func traceStart(ctx context.Context, region string, kv ...any) context.Context { return ctx }

// also add numeric labels to metrics.
func traceTag(ctx context.Context, kv ...any) {}

func traceEnd(ctx context.Context, kv ...any) context.Context { return ctx }
