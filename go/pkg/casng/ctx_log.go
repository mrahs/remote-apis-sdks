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

// Much cheaper than log.V
// This should be set after parsing flags (not in init or variable init).
var verbose bool

// Public stuff that can be used outside this file.
func fmtKv(kv ...any) string {
	return strings.Join(fmtMergeKv(make([]string, 0, len(kv)/2), kv...), ", ")
}

func debugf(ctx context.Context, msg string, kv ...any) {
	if !verbose {
		return
	}
	depth := depthFromCtx(ctx)
	log.InfoDepthf(depth+1, "%s; %s", msg, fmtCtx(ctx, kv...))
}

func serrorf(ctx context.Context, msg string, kv ...any) error {
	return fmt.Errorf("%s; %s", msg, fmtCtx(ctx, kv...))
}

func errorf(ctx context.Context, msg string, kv ...any) {
	depth := depthFromCtx(ctx)
	log.ErrorDepthf(depth+1, "%s; %s", msg, fmtCtx(ctx, kv...))
}

// func durationf(ctx context.Context, startTime time.Time, op string, kv ...any) {
// 	if !verbose {
// 		return
// 	}
// 	endTime := time.Now()
// 	depth := depthFromCtx(ctx)
// 	log.InfoDepthf(depth+1, "duration.%s; start=%d, end=%d, d=%s, %s",
// 		op, startTime.UnixNano(), endTime.UnixNano(), endTime.Sub(startTime), fmtCtx(ctx, kv...))
// }

func ctxWithValues(ctx context.Context, kv ...any) context.Context {
	if !verbose {
		return ctx
	}
	m := metaFromCtx(ctx)
	var k any
	for i := 0; i < len(kv); i++ {
		if i%2 == 0 {
			k = kv[i]
			continue
		}
		m[k] = kv[i]
	}
	return context.WithValue(ctx, ctxKeyMeta, m)
}

func ctxWithRqID(ctx context.Context, kv ...any) context.Context {
	if !verbose {
		return ctx
	}
	if id := valFromCtx(ctx, CtxKeyRqID); id == nil {
		kv = append(kv, CtxKeyRqID, uuid.New())
	}
	return ctxWithValues(ctx, kv...)
}

func ctxWithLogDepthInc(ctx context.Context) context.Context {
	if !verbose {
		return ctx
	}
	d := depthFromCtx(ctx)
	d++
	return ctxWithValues(ctx, ctxKeyLogDepth, d)
}

func traceStart(ctx context.Context, module string, kv ...any) context.Context {
	if !verbose {
		return ctx
	}
	traceWg.Add(1)
	trace := &ctxTrace{
		parentCtx: ctx,
		region:    module,
		labels:    kvMergeMap(make(map[any]any, len(kv)/2), kv...),
		start:     time.Now(),
	}
	if parentTrace := traceFromCtx(ctx); parentTrace != nil {
		trace.id = parentTrace.id
	}
	if trace.id == "" {
		trace.id = uuid.New()
	}
	trace.metrics = mergeMetricsFromKv(trace.metrics, kv...)
	return context.WithValue(ctx, ctxKeyTrace, trace)
}

// also add numeric labels to metrics.
func traceTag(ctx context.Context, kv ...any) {
	if !verbose {
		return
	}
	trace := traceFromCtx(ctx)
	if trace == nil {
		log.InfoDepth(1, "no trace to tag")
		return
	}
	trace.labels = kvMergeMap(trace.labels, kv...)
	trace.metrics = mergeMetricsFromKv(trace.metrics, kv...)
}

func traceMetric(ctx context.Context, kv ...any) {
	if !verbose {
		return
	}
	trace := traceFromCtx(ctx)
	if trace == nil {
		log.InfoDepth(1, "no trace to add metrics to")
		return
	}
	trace.metrics = mergeMetricsFromKv(trace.metrics, kv...)
}

func traceEnd(ctx context.Context, kv ...any) context.Context {
	if !verbose {
		return ctx
	}

	trace := traceFromCtx(ctx)
	if trace == nil {
		log.InfoDepth(1, "no trace to end")
		traceWg.Done()
		return ctx
	}
	trace.labels = kvMergeMap(trace.labels, kv...)
	if err, ok := trace.labels["err"]; ok && err != nil {
		trace.hasErr = true
	}
	trace.end = time.Now()
	parentTrace := traceFromCtx(trace.parentCtx)
	if parentTrace != nil {
		parentTrace.children = append(parentTrace.children, trace)
		parentTrace.hasErr = trace.hasErr
		traceWg.Done()
		return trace.parentCtx
	}

	// if trace.hasErr {
	log.InfoDepth(1, fmtTrace(trace))
	// }

	go func() {
		collectTrace(trace)
		traceWg.Done()
	}()

	return trace.parentCtx
}

// Internal stuff that should not be used outside this file.

type ctxKey int

const (
	// Context key for invocation ID.
	CtxKeyInvID ctxKey = iota

	// Context key for command ID.
	CtxKeyCmdID

	// Context key for request ID
	CtxKeyRqID

	// Context key for route ID.
	ctxKeyRtID

	// Context key sub-request ID.
	ctxKeySqID

	// Context key for module.
	ctxKeyModule

	// Context key for adjusting log depth for glog.
	ctxKeyLogDepth

	// Context key for meta data.
	ctxKeyMeta

	// Context key for tracing.
	ctxKeyTrace

	// Temporary keys for debugging purposes.
	CtxKeyNGTree
	CtxKeyClientTree
)

type ctxMeta map[any]any

type ctxTrace struct {
	parentCtx context.Context
	id        string
	region    string
	start     time.Time
	end       time.Time
	hasErr    bool
	labels    map[any]any
	metrics   map[string]metricGauge
	children  []*ctxTrace
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
	metricsCh = make(chan any)
	traceWg   sync.WaitGroup
)

func mergeMetricsFromKv(metrics map[string]metricGauge, kv ...any) map[string]metricGauge {
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

func traceFromCtx(ctx context.Context) *ctxTrace {
	if ctx == nil {
		return nil
	}
	if tRaw := ctx.Value(ctxKeyTrace); tRaw != nil {
		return tRaw.(*ctxTrace)
	}
	return nil
}

func metaFromCtx(ctx context.Context) ctxMeta {
	m := make(ctxMeta)
	if mRaw := ctx.Value(ctxKeyMeta); mRaw != nil {
		for k, v := range mRaw.(ctxMeta) {
			m[k] = v
		}
	}
	return m
}

func valFromCtx(ctx context.Context, key any) any {
	var val any
	if mRaw := ctx.Value(ctxKeyMeta); mRaw != nil {
		m := mRaw.(ctxMeta)
		val = m[key]
	}
	return val
}

func depthFromCtx(ctx context.Context) int {
	if dRaw := valFromCtx(ctx, ctxKeyLogDepth); dRaw != nil {
		return dRaw.(int)
	}
	return 0
}

// TODO: use ctxTrace instead of ctxMeta
func fmtCtx(ctx context.Context, kv ...any) string {
	s := fmtMergeKv(make([]string, 0, 6+len(kv)/2), kv...)

	if !verbose {
		return strings.Join(s, ", ")
	}

	rawModule := valFromCtx(ctx, ctxKeyModule)
	if module, ok := rawModule.(string); ok {
		s = append(s, "m="+module)
	}

	rawInvID := valFromCtx(ctx, CtxKeyInvID)
	if invID, ok := rawInvID.(string); ok {
		s = append(s, "inv="+invID)
	}

	rawCmdID := valFromCtx(ctx, CtxKeyCmdID)
	if cmdID, ok := rawCmdID.(string); ok {
		s = append(s, "cmd="+cmdID)
	}

	rawRqID := valFromCtx(ctx, CtxKeyRqID)
	if rqID, ok := rawRqID.(string); ok {
		s = append(s, "rqid="+rqID)
	}

	rawRtID := valFromCtx(ctx, ctxKeyRtID)
	if rtID, ok := rawRtID.(string); ok {
		s = append(s, "rtid="+rtID)
	}

	rawSqID := valFromCtx(ctx, ctxKeySqID)
	if sqID, ok := rawSqID.(string); ok {
		s = append(s, "sqid="+sqID)
	}

	return strings.Join(s, ", ")
}

func fmtMergeKv(out []string, kv ...any) []string {
	var k any
	var p string
	for i := 0; i < len(kv); i++ {
		if i%2 == 0 {
			k = kv[i]
			continue
		}

		v := kv[i]
		if d, ok := v.(*repb.Digest); ok {
			out = append(out, fmt.Sprintf("%s=%s/%d", k, d.GetHash(), d.GetSizeBytes()))
			continue
		}

		if k == "path" {
			if str, ok := v.(fmt.Stringer); ok {
				p = str.String()
			}
		} else if k == "real_path" {
			if str, ok := v.(fmt.Stringer); ok && str.String() == p {
				continue
			}
		}
		out = append(out, fmt.Sprintf("%s=%v", k, v))
	}
	return out
}

func kvMergeMap(m map[any]any, kv ...any) map[any]any {
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

func fmtMap(m map[any]any) string {
	sb := strings.Builder{}
	for k, v := range m {
		if sb.Len() > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%v=%v", k, v))
	}
	return sb.String()
}

func fmtTrace(trace *ctxTrace) string {
	return fmtTraceDeep(0, trace)
}

func fmtTraceDeep(level int, trace *ctxTrace) string {
	indent := strings.Repeat(" ", level*2)
	sb := strings.Builder{}
	if level == 0 {
		sb.WriteString(fmt.Sprintf("trace.start %s\n", trace.id))
	}
	sb.WriteString(fmt.Sprintf("%s, %s, %s", trace.region, trace.end.Sub(trace.start), fmtMap(trace.labels)))
	for n, m := range trace.metrics {
		sb.WriteString(fmt.Sprintf("\n%s  metric: %s, c=%d, avg=%.2f, max=%d", indent, n, m.count, float64(m.sum)/float64(m.count), m.max))
	}
	for _, child := range trace.children {
		sb.WriteString(fmt.Sprintf("\n%s  %s", indent, fmtTraceDeep(level+1, child)))
	}
	if level == 0 {
		sb.WriteString("\ntrace.end")
	}
	return sb.String()
}

func collectTrace(trace *ctxTrace) {
	collectTraceDeep("", trace)
}

func collectTraceDeep(parentRegion string, trace *ctxTrace) {
	region := trace.region
	if parentRegion != "" {
		region = fmt.Sprintf("%s.%s", parentRegion, trace.region)
	}

	metricsCh <- metricDuration{
		name:     region,
		duration: trace.end.Sub(trace.start),
	}
	for name, m := range trace.metrics {
		m.name = fmt.Sprintf("%s.%s", region, name)
		metricsCh <- m
	}
	for _, t := range trace.children {
		collectTraceDeep(region, t)
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
