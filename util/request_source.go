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
	ExplicitTypeDefault    = "default"
	ExplicitTypeLightning  = "lightning"
	ExplicitTypeBR         = "br"
	ExplicitTypeDumpling   = "dumpling"
	ExplicitTypeBackground = "background"
)

// ExplicitTypeList is the list of all explicit source types.
var ExplicitTypeList = []string{ExplicitTypeDefault, ExplicitTypeLightning, ExplicitTypeBR, ExplicitTypeDumpling, ExplicitTypeBackground}

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
	// ExplicitRequestSourceType is set from the session variable, it may specified by the client or users. `explicit_request_source_type`. it's a complement of RequestSourceType.
	// The value maybe "lightning", "br", "dumpling" etc.
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
	// if r.RequestSourceType is not set, it's mostly possible that r.RequestSourceInternal is not set
	// to avoid internal requests be marked as external(default value), return unknown source here.
	if r == nil || (len(r.RequestSourceType) == 0 && len(r.ExplicitRequestSourceType) == 0) {
		return SourceUnknown
	}
	clearEmpty := func(list []string) []string {
		i := 0
		for _, v := range list {
			if len(v) != 0 {
				list[i] = v
				i++
			}
		}
		return list[:i]
	}
	explicitSourceType := r.ExplicitRequestSourceType
	if len(explicitSourceType) == 0 {
		explicitSourceType = ExplicitTypeDefault
	}
	if r.RequestSourceInternal {
		labels := []string{InternalRequest, r.RequestSourceType, explicitSourceType}
		labels = clearEmpty(labels)

		return strings.Join(labels, "_")
	}
	labels := []string{ExternalRequest, r.RequestSourceType, explicitSourceType}
	labels = clearEmpty(labels)
	return strings.Join(labels, "_")
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
