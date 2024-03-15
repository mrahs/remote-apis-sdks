package casng

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/pborman/uuid"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// This should be set after parsing flags (not in init or variable init).
var verbose bool

// Stuff that can be used outside this file.

func debugf(ctx context.Context, msg string, kv ...any) {
	if !verbose {
		return
	}
	log.InfoDepthf(1, "%s; %s", msg, fmtCtx(ctx, kv...))
}

func serrorf(ctx context.Context, msg string, kv ...any) error {
	return fmt.Errorf("%s; %s", msg, fmtCtx(ctx, kv...))
}

func errorf(ctx context.Context, msg string, kv ...any) {
	log.ErrorDepthf(1, "%s; %s", msg, fmtCtx(ctx, kv...))
}

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
			prefix = "." + parentTrace.prefix
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

// Internal stuff that should not be used outside this file.

type ctxKey int

const (
	ctxKeyTrace ctxKey = iota

	// Temporary keys for debugging purposes.
	CtxKeyNGTree
	CtxKeyClientTree
)

type ctxTrace struct {
	id        string
	region    string
	prefix    string
	start     time.Time
	end       time.Time
	labels    map[any]any
	metrics   map[string]metricGauge
	parentCtx context.Context
}

type metricGauge struct {
	name  string
	count int64
	max   int64
	sum   int64
}

type metricDuration struct {
	name     string
	duration time.Duration
}

var (
	metricsCh    = make(chan any)
	traceWg      sync.WaitGroup
	traceCountCh = make(chan int)
	traceCount   = 0
)

func traceFromCtx(ctx context.Context) *ctxTrace {
	if ctx == nil {
		return nil
	}
	if tRaw := ctx.Value(ctxKeyTrace); tRaw != nil {
		return tRaw.(*ctxTrace)
	}
	return nil
}

// fixKv unifies digest formats and removes redundant real_path labels.
func fixKv(kv ...any) []any {
	var k any
	var p string
	j := 0
	for i := 0; i < len(kv); i++ {
		if i%2 == 0 {
			k = kv[i]
			kv[j] = k
			j++
			continue
		}

		v := kv[i]
		if d, ok := v.(*repb.Digest); ok {
			kv[j] = fmt.Sprintf("%s/%d", d.GetHash(), d.GetSizeBytes())
			j++
			continue
		}

		// This assumes "path" always comes before "real_path"
		if k == "path" {
			switch str := v.(type) {
			case string:
				p = str
			case fmt.Stringer:
				p = str.String()
			}
		} else if k == "real_path" {
			var rp string
			switch str := v.(type) {
			case string:
				rp = str
			case fmt.Stringer:
				rp = str.String()
			}
			if rp == p {
				j--
				continue
			}
		}
		kv[j] = v
		j++
	}
	return kv[:j]
}

func mergeKv(m map[any]any, kv ...any) map[any]any {
	kv = fixKv(kv...)
	if m == nil {
		m = make(map[any]any, len(kv)/2)
	}
	var k any
	for i := 0; i < len(kv); i++ {
		if i%2 == 0 {
			k = kv[i]
			continue
		}
		if kv[i] == nil {
			continue
		}

		m[k] = kv[i]
	}
	return m
}

func mergeKvMetrics(metrics map[string]metricGauge, kv ...any) map[string]metricGauge {
	if len(kv) == 0 {
		return metrics
	}
	if metrics == nil {
		metrics = make(map[string]metricGauge)
	}
	var k any
	for i := 0; i < len(kv); i++ {
		if i%2 == 0 {
			k = kv[i]
			continue
		}
		name, ok := k.(string)
		if !ok {
			continue
		}

		v, ok := kv[i].(int)
		if !ok {
			continue
		}
		value := int64(v)

		m := metrics[name]
		m.count++
		m.sum += value
		if value > m.max {
			m.max = value
		}
		metrics[name] = m
	}
	return metrics
}

func fmtCtx(ctx context.Context, kv ...any) string {
	kv = fixKv(kv...)
	labelCount := len(kv) / 2
	trace := traceFromCtx(ctx)
	if trace != nil {
		labelCount += 1 // for region
		labelCount += len(trace.labels)
	}
	if labelCount == 0 {
		return ""
	}

	labels := make([]string, 0, labelCount)

	var k any
	for i := 0; i < len(kv); i++ {
		if i%2 == 0 {
			k = kv[i]
			continue
		}
		labels = append(labels, fmt.Sprintf("%s=%v", k, kv[i]))
	}

	if trace != nil {
		labels = append(labels, "region="+trace.region)
		for k, v := range trace.labels {
			labels = append(labels, fmt.Sprintf("%s=%v", k, v))
		}
	}

	return strings.Join(labels, ", ")
}

func fmtTrace(trace *ctxTrace) string {
    if trace == nil {
        return ""
    }
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("%s, %s", trace.region, trace.end.Sub(trace.start)))
	if len(trace.labels) > 0 {
		for k, v := range trace.labels {
            sb.WriteString(", ")
			sb.WriteString(fmt.Sprintf("%s=%v", k, v))
		}
	}
	for name, m := range trace.metrics {
		sb.WriteString(fmt.Sprintf("\n    metric: %s, c=%d, avg=%.2f, max=%d", name, m.count, float64(m.sum)/float64(m.count), m.max))
	}
	if trace.parentCtx == nil {
		return sb.String()
	}
	parentTrace := traceFromCtx(trace.parentCtx)
	if parentTrace == nil {
		return sb.String()
	}
	sb.WriteRune('\n')
	sb.WriteString(fmtTrace(parentTrace))
	return sb.String()
}

func collectTraceMetrics(trace *ctxTrace) {
	fqName := trace.region
	if trace.prefix != "" {
		fqName = trace.prefix + "." + trace.region
	}

	metricsCh <- metricDuration{
		name:     fqName,
		duration: trace.end.Sub(trace.start),
	}
	for name, m := range trace.metrics {
		m.name = fmt.Sprintf("%s.%s", fqName, name)
		metricsCh <- m
	}
}

func runMetricsCollector() {
	if !verbose {
		return
	}

	durations := make(map[string]metricDuration)
	gauges := make(map[string]metricGauge)
	counts := make(map[string]int64)
	for mRaw := range metricsCh {
		if m, ok := mRaw.(metricDuration); ok {
			md := durations[m.name]
			md.duration += m.duration
			counts[m.name]++
			durations[m.name] = md
			continue
		}

		m, ok := mRaw.(metricGauge)
		if !ok {
			log.Infof("unexpected message type: %T", mRaw)
			continue
		}
		counts[m.name]++
		mg := gauges[m.name]
		mg.count++
		mg.sum += m.sum
		if m.max > mg.max {
			mg.max = m.max
		}
		gauges[m.name] = mg
	}
	log.Infof("summarizing %d metrics", len(counts))
	names := make([]string, 0, len(counts))
	for r := range counts {
		names = append(names, r)
	}
	sort.Strings(names)
	for _, n := range names {
		sb := strings.Builder{}
		if m, ok := durations[n]; ok {
			sb.WriteString(fmt.Sprintf("metric.%s, c=%d, d=%s", n, counts[n], m.duration))
		} else if m, ok := gauges[n]; ok {
			sb.WriteString(fmt.Sprintf("metric.%s, c=%d, avg=%.2f, max=%d", n, m.count, float64(m.sum)/float64(m.count), m.max))
		} else {
			log.Infof("unexpected metric name: %s", n)
			continue
		}
		log.Info(sb.String())
	}
}
