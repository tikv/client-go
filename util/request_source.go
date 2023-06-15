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
	// InternalTxnTools is the type of tools usage of TiDB.
	// Do not classify different tools by now.
	InternalTxnTools = "tools"
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
	RunInBackground       bool
}

// SetRequestSourceInternal sets the scope of the request source.
func (r *RequestSource) SetRequestSourceInternal(internal bool) {
	r.RequestSourceInternal = internal
}

// SetRequestSourceType sets the type of the request source.
func (r *RequestSource) SetRequestSourceType(tp string) {
	r.RequestSourceType = tp
}

// SetRunInBackground sets the request to run in background.
func (r *RequestSource) SetRunInBackground(background bool) {
	r.RunInBackground = background
}

// RequestSourceOption is used to set the request source.
type RequestSourceOption func(*RequestSource)

// RequestRunInBackgroundOption sets the request to run in background.
var RequestRunInBackgroundOption = func(rs *RequestSource) {
	rs.SetRunInBackground(true)
}

// WithInternalSourceType create context with internal source.
func WithInternalSourceType(ctx context.Context, source string, opts ...RequestSourceOption) context.Context {
	rs := &RequestSource{
		RequestSourceInternal: true,
		RequestSourceType:     source,
	}
	for _, opt := range opts {
		opt(rs)
	}
	return context.WithValue(ctx, RequestSourceKey, *rs)
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
	if r == nil || r.RequestSourceType == "" {
		return SourceUnknown
	}
	if r.RequestSourceInternal {
		return InternalRequest + r.RequestSourceType
	}
	return ExternalRequest + r.RequestSourceType
}

// RequestSourceFromCtx extract source from passed context.
func RequestSourceFromCtx(ctx context.Context) string {
	if source := ctx.Value(RequestSourceKey); source != nil {
		rs := source.(RequestSource)
		return rs.GetRequestSource()
	}
	return SourceUnknown
}

// IsBackgroundFromCtx checks whether the request is a background job.
func IsBackgroundFromCtx(ctx context.Context) bool {
	if source := ctx.Value(RequestSourceKey); source != nil {
		rs := source.(RequestSource)
		return rs.RunInBackground
	}
	return false
}

// IsInternalRequest returns the type of the request source.
func IsInternalRequest(source string) bool {
	return strings.HasPrefix(source, InternalRequest)
}

// ResourceGroupNameKeyType is the context key type of resource group name.
type resourceGroupNameKeyType struct{}

// ResourceGroupNameKey is used as the key of request source type in context.
var resourceGroupNameKey = resourceGroupNameKeyType{}

// WithResourceGroupName return a copy of the given context with an associated
// resource group name.
func WithResourceGroupName(ctx context.Context, groupName string) context.Context {
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
