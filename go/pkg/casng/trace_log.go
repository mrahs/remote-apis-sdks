package casng

import (
	"context"
	"fmt"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/pborman/uuid"
)

type ctxKey struct {
	key string
}

var (
	// Context key for invocation ID.
	CtxKeyInvID = ctxKey{ key: "invid" }
	// Context key for command ID.
	CtxKeyCmdID  = ctxKey{ key: "cmdid" }
	// Context key for request ID
	CtxKeyRqID = ctxKey{ key: "rqid" }
	// Context key for route ID.
	ctxKeyRtID = ctxKey{ key: "rtid" }
	// Context key sub-request ID.
	ctxKeySqID = ctxKey{ key: "sqid" }
	// Context key for module.
	ctxKeyModule = ctxKey { key: "module" }
	// Context key for digester walk ID.
	ctxKeyWalkID = ctxKey { key: "wid" }

	// Temporary keys for debugging purposes.
	CtxKeyNGTree = ctxKey { key: "ng_tree" }
	CtxKeyClientTree = ctxKey { key: "cl_tree" }
)

func ctxWithRqID(ctx context.Context) context.Context {
	if id := rqIDFromCtx(ctx); id != "" {
		return ctx
	}
	id := uuid.New()
	return context.WithValue(ctx, CtxKeyRqID, id)
}

func rqIDFromCtx(ctx context.Context) string {
	rawID := ctx.Value(CtxKeyRqID)
	if rawID == nil {
		return ""
	}
	if id, ok := rawID.(string); ok {
		return id
	}
	return ""
}

func fmtCtx(ctx context.Context, kv ...any) string {
	s := make([]string, 0, 6 + len(kv)/2)

	var k any
	for i := 0; i < len(kv); i++ {
		if i % 2 == 0 {
			k = kv[i]
			continue
		}
		s = append(s, fmt.Sprintf("%s=%v", k ,kv[i]))
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
	var k any
	for i := 0; i < len(kv); i++ {
		if i % 2 == 0 {
			k = kv[i]
			continue
		}
		ctx = context.WithValue(ctx, k, kv[i])
	}
	return ctx
}

func logDuration(ctx context.Context, startTime time.Time, op string, kv ...any) {
	if !log.V(3) {
		return
	}
	log.Infof("duration.%s; start=%d, end=%d, %s", op, startTime.UnixNano(), time.Now().UnixNano(), fmtCtx(ctx, kv...))
}
