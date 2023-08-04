package util

import (
	"context"
	"strings"
)

// RequestSourceTypeKeyType is a dummy type to avoid naming collision in context.
type RequestSourceTypeKeyType struct{}

// RequestSourceTypeKey is used as the key of request source type in context.
var RequestSourceTypeKey = RequestSourceTypeKeyType{}

// RequestSourceKeyType is a dummy type to avoid naming collision in context.
type RequestSourceKeyType struct{}

// RequestSourceKey is used as the key of request source type in context.
var RequestSourceKey = RequestSourceKeyType{}

const (
	// InternalTxnOthers is the type of requests that consume low resources.
	// This reduces the size of metrics.
	InternalTxnOthers = "others"
	// InternalTxnGC is the type of GC txn.
	InternalTxnGC = "gc"
	// InternalTxnMeta is the type of the miscellaneous meta usage.
	InternalTxnMeta = InternalTxnOthers
)

// explicit source types.
const (
	ExplicitTypeEmpty      = ""
	ExplicitTypeLightning  = "lightning"
	ExplicitTypeBR         = "br"
	ExplicitTypeDumpling   = "dumpling"
	ExplicitTypeBackground = "background"
)

// ExplicitTypeList is the list of all explicit source types.
var ExplicitTypeList = []string{ExplicitTypeEmpty, ExplicitTypeLightning, ExplicitTypeBR, ExplicitTypeDumpling, ExplicitTypeBackground}

const (
	// InternalRequest is the scope of internal queries
	InternalRequest = "internal"
	// ExternalRequest is the scope of external queries
	ExternalRequest = "external"
	// SourceUnknown keeps same with the default value(empty string)
	SourceUnknown = "unknown"
)

// RequestSource contains the source label of the request, used for tracking resource consuming.
type RequestSource struct {
	RequestSourceInternal bool
	RequestSourceType     string
	// ExplicitRequestSourceType is a type that is set from the session variable and may be specified by the client or users.
	// It is a complement to the RequestSourceType and provides additional information about how a request was initiated.
	ExplicitRequestSourceType string
}

// SetRequestSourceInternal sets the scope of the request source.
func (r *RequestSource) SetRequestSourceInternal(internal bool) {
	r.RequestSourceInternal = internal
}

// SetRequestSourceType sets the type of the request source.
func (r *RequestSource) SetRequestSourceType(tp string) {
	r.RequestSourceType = tp
}

// SetExplicitRequestSourceType sets the type of the request source.
func (r *RequestSource) SetExplicitRequestSourceType(tp string) {
	r.ExplicitRequestSourceType = tp
}

// WithInternalSourceType create context with internal source.
func WithInternalSourceType(ctx context.Context, source string) context.Context {
	return context.WithValue(ctx, RequestSourceKey, RequestSource{
		RequestSourceInternal: true,
		RequestSourceType:     source,
	})
}

// IsRequestSourceInternal checks whether the input request source type is internal type.
func IsRequestSourceInternal(reqSrc *RequestSource) bool {
	isInternal := false
	if reqSrc != nil && IsInternalRequest(reqSrc.GetRequestSource()) {
		isInternal = true
	}
	return isInternal
}

// GetRequestSource gets the request_source field of the request.
func (r *RequestSource) GetRequestSource() string {
	source := SourceUnknown
	origin := ExternalRequest
	if r == nil || (len(r.RequestSourceType) == 0 && len(r.ExplicitRequestSourceType) == 0) {
		// if r.RequestSourceType and r.ExplicitRequestSourceType are not set, it's mostly possible that r.RequestSourceInternal is not set
		// to avoid internal requests be marked as external(default value), return unknown source here.
		return source
	}
	if r.RequestSourceInternal {
		origin = InternalRequest
	}
	labelList := make([]string, 0, 3)
	labelList = append(labelList, origin)
	if len(r.RequestSourceType) > 0 {
		source = r.RequestSourceType
	}
	labelList = append(labelList, source)
	if len(r.ExplicitRequestSourceType) > 0 && r.ExplicitRequestSourceType != r.RequestSourceType {
		labelList = append(labelList, r.ExplicitRequestSourceType)
	}

	return strings.Join(labelList, "_")
}

// RequestSourceFromCtx extract source from passed context.
func RequestSourceFromCtx(ctx context.Context) string {
	if source := ctx.Value(RequestSourceKey); source != nil {
		rs := source.(RequestSource)
		return rs.GetRequestSource()
	}
	return SourceUnknown
}

// IsInternalRequest returns the type of the request source.
func IsInternalRequest(source string) bool {
	return strings.HasPrefix(source, InternalRequest)
}

// ResourceGroupNameKeyType is the context key type of resource group name.
type resourceGroupNameKeyType struct{}

// ResourceGroupNameKey is used as the key of request source type in context.
var resourceGroupNameKey = resourceGroupNameKeyType{}

// WithResouceGroupName return a copy of the given context with a associated
// reosurce group name.
func WithResouceGroupName(ctx context.Context, groupName string) context.Context {
	return context.WithValue(ctx, resourceGroupNameKey, groupName)
}

// ResourceGroupNameFromCtx extract resource group name from passed context,
// empty string is returned is the key is not set.
func ResourceGroupNameFromCtx(ctx context.Context) string {
	if val := ctx.Value(resourceGroupNameKey); val != nil {
		return val.(string)
	}
	return ""
}
