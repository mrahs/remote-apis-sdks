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

// Much cheaper than log.V
var verbose bool = bool(log.V(4))

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

func durationf(ctx context.Context, startTime time.Time, op string, kv ...any) {
	if !verbose {
		return
	}
	endTime := time.Now()
	depth := depthFromCtx(ctx)
	log.InfoDepthf(depth+1, "duration.%s; start=%d, end=%d, d=%s, %s",
		op, startTime.UnixNano(), endTime.UnixNano(), endTime.Sub(startTime), fmtCtx(ctx, kv...))
}

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
	// Context key for digester walk ID.

	ctxKeyWalkID

	// Context key for adjusting log depth for glog.
	ctxKeyLogDepth

	// Context key for meta data.
	ctxKeyMeta

	// Temporary keys for debugging purposes.
	CtxKeyNGTree
	CtxKeyClientTree
)

type ctxMeta map[any]any

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
