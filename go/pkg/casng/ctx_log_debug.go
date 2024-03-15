//go:build debug

package casng

import (
	"context"
	"time"

	log "github.com/golang/glog"
	"github.com/pborman/uuid"
)

func traceStart(ctx context.Context, region string, kv ...any) context.Context {
	if !verbose {
		return ctx
	}

	traceCountCh <- 1
	traceWg.Add(1)

	trace := &ctxTrace{
		id:        uuid.New(),
		region:    region,
		start:     time.Now(),
		labels:    mergeKv(nil, kv...),
		metrics:   mergeKvMetrics(nil, kv...),
		parentCtx: ctx,
	}
	if parentTrace := traceFromCtx(ctx); parentTrace != nil {
		prefix := ""
		if parentTrace.prefix != "" {
			prefix = parentTrace.prefix + "."
		}
		trace.prefix = prefix + parentTrace.region
	}
	return context.WithValue(ctx, ctxKeyTrace, trace)
}

// also add numeric labels to metrics.
func traceTag(ctx context.Context, kv ...any) {
	if !verbose {
		return
	}

	trace := traceFromCtx(ctx)
	if trace == nil {
		log.WarningDepth(1, "no trace to tag")
		return
	}
	trace.labels = mergeKv(trace.labels, kv...)
	trace.metrics = mergeKvMetrics(trace.metrics, kv...)
}

func traceEnd(ctx context.Context, kv ...any) context.Context {
	if !verbose {
		return ctx
	}

	trace := traceFromCtx(ctx)
	if trace == nil {
		log.WarningDepth(1, "no trace to end")
		return ctx
	}

	traceCountCh <- -1

	trace.labels = mergeKv(trace.labels, kv...)
	trace.metrics = mergeKvMetrics(trace.metrics, kv...)
	trace.end = time.Now()

	if trace.labels["err"] != nil {
		log.ErrorDepth(1, fmtTrace(trace))
	}

	go func() {
		collectTraceMetrics(trace)
		traceWg.Done()
	}()

	return trace.parentCtx
}
