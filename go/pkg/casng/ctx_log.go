package casng

import (
	"context"
	"fmt"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/pborman/uuid"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

type ctxKey struct {
	key string
}

var (
	// Context key for invocation ID.
	CtxKeyInvID = &ctxKey{key: "invid"}
	// Context key for command ID.
	CtxKeyCmdID = &ctxKey{key: "cmdid"}
	// Context key for request ID
	CtxKeyRqID = &ctxKey{key: "rqid"}
	// Context key for route ID.
	ctxKeyRtID = &ctxKey{key: "rtid"}
	// Context key sub-request ID.
	ctxKeySqID = &ctxKey{key: "sqid"}
	// Context key for module.
	ctxKeyModule = &ctxKey{key: "module"}
	// Context key for digester walk ID.
	ctxKeyWalkID = &ctxKey{key: "wid"}
	// Context key for adjusting log depth for glog.
	ctxKeyLogDepth = &ctxKey{key: "log_depth"}

	// Temporary keys for debugging purposes.
	CtxKeyNGTree     = &ctxKey{key: "ng_tree"}
	CtxKeyClientTree = &ctxKey{key: "cl_tree"}
)

func ctxWithRqID(ctx context.Context) context.Context {
	if !log.V(4) {
		return ctx
	}
	if id := ctx.Value(CtxKeyRqID); id != nil {
		return ctx
	}
	id := uuid.New()
	return context.WithValue(ctx, CtxKeyRqID, id)
}

func ctxWithLogDepthInc(ctx context.Context) context.Context {
	if !log.V(4) {
		return ctx
	}
	d := 0
	if dRaw := ctx.Value(ctxKeyLogDepth); dRaw != nil {
		d = dRaw.(int)
	}
	d++
	return context.WithValue(ctx, ctxKeyLogDepth, d)
}

func fmtCtx(ctx context.Context, kv ...any) string {
	s := make([]string, 0, 6+len(kv)/2)

	var k any
	for i := 0; i < len(kv); i++ {
		if i%2 == 0 {
			k = kv[i]
			continue
		}
		if d, ok := kv[i].(*repb.Digest); ok {
			s = append(s, fmt.Sprintf("%s=%s/%d", k, d.GetHash(), d.GetSizeBytes()))
			continue
		}
		s = append(s, fmt.Sprintf("%s=%v", k, kv[i]))
	}

	if !log.V(4) {
		return strings.Join(s, ", ")
	}

	rawModule := ctx.Value(ctxKeyModule)
	if module, ok := rawModule.(string); ok {
		s = append(s, "m="+module)
	}

	rawInvID := ctx.Value(CtxKeyInvID)
	if invID, ok := rawInvID.(string); ok {
		s = append(s, "inv="+invID)
	}

	rawCmdID := ctx.Value(CtxKeyCmdID)
	if cmdID, ok := rawCmdID.(string); ok {
		s = append(s, "cmd="+cmdID)
	}

	rawRqID := ctx.Value(CtxKeyRqID)
	if rqID, ok := rawRqID.(string); ok {
		s = append(s, "rqid="+rqID)
	}

	rawRtID := ctx.Value(ctxKeyRtID)
	if rtID, ok := rawRtID.(string); ok {
		s = append(s, "rtid="+rtID)
	}

	rawSqID := ctx.Value(ctxKeySqID)
	if sqID, ok := rawSqID.(string); ok {
		s = append(s, "sqid="+sqID)
	}

	return strings.Join(s, ", ")
}

func ctxWithValues(ctx context.Context, kv ...any) context.Context {
	if !log.V(4) {
		return ctx
	}
	var k any
	for i := 0; i < len(kv); i++ {
		if i%2 == 0 {
			k = kv[i]
			continue
		}
		if strs, ok := kv[i].([]string); ok {
			ctx = context.WithValue(ctx, k, strings.Join(strs, "|"))
			continue
		}
		ctx = context.WithValue(ctx, k, kv[i])
	}
	return ctx
}

func durationf(ctx context.Context, startTime time.Time, op string, kv ...any) {
	if !log.V(3) {
		return
	}
	endTime := time.Now()
	// reclient uses an old version of glog that doesn't have InfoDepthf.
	log.InfoDepth(1, fmt.Sprintf("duration.%s; start=%d, end=%d, d=%s, %s", op, startTime.UnixNano(), endTime.UnixNano(), endTime.Sub(startTime), fmtCtx(ctx, kv...)))
}

func infof(ctx context.Context, level log.Level, msg string, kv ...any) {
	if !log.V(level) {
		return
	}
	depth := depthFromCtx(ctx)
	// reclient uses an old version of glog that doesn't have InfoDepthf.
	log.InfoDepth(depth+1, fmt.Sprintf("%s; %s", msg, fmtCtx(ctx, kv...)))
}

func warnf(ctx context.Context, msg string, kv ...any) {
	depth := depthFromCtx(ctx)
	// reclient uses an old version of glog that doesn't have WarningDepthf.
	log.WarningDepth(depth+1, fmt.Sprintf("%s; %s", msg, fmtCtx(ctx, kv...)))
}

func depthFromCtx(ctx context.Context) int {
	if dRaw := ctx.Value(ctxKeyLogDepth); dRaw != nil {
		return dRaw.(int)
	}
	return 0
}
