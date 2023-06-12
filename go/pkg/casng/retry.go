package casng

import (
	"context"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
)

func withRetry(ctx context.Context, predicate retry.ShouldRetry, policy retry.BackoffPolicy, fn func() error) error {
	return retry.WithPolicy(ctx, predicate, policy, fn)
}
