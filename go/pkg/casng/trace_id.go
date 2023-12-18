package casng

import (
	"context"
	"github.com/pborman/uuid"
)

const traceIDKey = "casng_trace_id"

func ctxWithTrace(ctx context.Context) (context.Context, string) {
	if id := traceFromCtx(ctx); id != "" {
		return ctx, id
	}
	id := uuid.New()
	return context.WithValue(ctx, traceIDKey, id), id
}

func traceFromCtx(ctx context.Context) string {
	if rawID := ctx.Value(traceIDKey); rawID != nil {
		return rawID.(string)
	}
	return ""
}
