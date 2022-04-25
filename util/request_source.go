package util

import (
	"context"
)

// RequestSourceTypeKeyType is a dummy type to avoid naming collision in context.
type RequestSourceTypeKeyType struct{}

// RequestSourceTypeKey is used as the key of request source type in context.
var RequestSourceTypeKey = RequestSourceTypeKeyType{}

const (
	// InternalTxnOthers is the type of requests that consume low resources.
	// This reduces the size of metrics.
	InternalTxnOthers = "others"
	// InternalTxnGC is the type of GC txn.
	InternalTxnGC = "gc"
	// InternalTxnMeta is the type of the miscellaneous meta usage.
	InternalTxnMeta = InternalTxnOthers
)

const (
	// InternalRequest is the scope of internal queries
	InternalRequest = "internal_"
	// ExternalRequest is the scope of external queries
	ExternalRequest = "external_"
	// SourceUnknown keeps same with the default value(empty string)
	SourceUnknown = "unknown"
)

// RequestSource contains the source label of the request, used for tracking resource consuming.
type RequestSource struct {
	RequestSourceInternal bool
	RequestSourceType     string
}

// SetRequestSourceInternal sets the scope of the request source.
func (r *RequestSource) SetRequestSourceInternal(internal bool) {
	r.RequestSourceInternal = internal
}

// SetRequestSourceType sets the type of the request source.
func (r *RequestSource) SetRequestSourceType(tp string) {
	r.RequestSourceType = tp
}

// GetRequestSource gets the request_source field of the request.
func (r *RequestSource) GetRequestSource() string {
	// if r.RequestSourceType is not set, it's mostly possible that r.RequestSourceInternal is not set
	// to avoid internal requests be marked as external(default value), return unknown source here.
	if r == nil || r.RequestSourceType == "" {
		return SourceUnknown
	}
	if r.RequestSourceInternal {
		return InternalRequest + r.RequestSourceType
	}
	return ExternalRequest + r.RequestSourceType
}

// RequestSourceFromCtx extract source from passed context, only used by internal txns.
func RequestSourceFromCtx(ctx context.Context) string {
	if source := ctx.Value(RequestSourceTypeKey); source != nil {
		return InternalRequest + source.(string)
	}
	return SourceUnknown
}
