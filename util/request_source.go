package util

import (
	"context"
	"strings"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
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
	InternalTxnTools     = "tools"
	InternalTxnBR        = "br"
	InternalTxnLightning = "lightning"
	InternalTxnDumpling  = "dumpling"
	InternalTxnTTL       = "ttl"
	InternalTxnDDL       = "ddl"
	// InternalTxnMeta is the type of the miscellaneous meta usage.
	InternalTxnMeta = InternalTxnOthers
)

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
}

// SetRequestSourceInternal sets the scope of the request source.
func (r *RequestSource) SetRequestSourceInternal(internal bool) {
	r.RequestSourceInternal = internal
}

// SetRequestSourceType sets the type of the request source.
func (r *RequestSource) SetRequestSourceType(tp string) {
	r.RequestSourceType = tp
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
// TODO: Remove this function after all internal requests are migrated to use RequestSourceV2.
func (r *RequestSource) GetRequestSource() string {
	// if r.RequestSourceType is not set, it's mostly possible that r.RequestSourceInternal is not set
	// to avoid internal requests be marked as external(default value), return unknown source here.
	if r == nil || r.RequestSourceType == "" {
		return SourceUnknown
	}
	if r.RequestSourceInternal {
		return strings.Join([]string{InternalRequest, r.RequestSourceType}, "_")
	}
	return strings.Join([]string{ExternalRequest, r.RequestSourceType}, "_")
}

// GetRequestSourceV2 gets the request_source_v2 field of the request.
func (r *RequestSource) GetRequestSourceV2() *kvrpcpb.RequestSourceV2 {
	if r == nil {
		return nil
	}
	return r.ToPB()
}

// ToPB converts the request source to protobuf format.
func (r *RequestSource) ToPB() *kvrpcpb.RequestSourceV2 {
	if r == nil {
		return nil
	}
	origin := ExternalRequest
	if r.RequestSourceInternal {
		origin = InternalRequest
	}
	var typ kvrpcpb.RequestSourceType
	tag := strings.ToLower(r.RequestSourceType)
	switch tag {
	case InternalTxnBR:
		typ = kvrpcpb.RequestSourceType_BR
	case InternalTxnLightning:
		typ = kvrpcpb.RequestSourceType_Lightning
	case InternalTxnDumpling:
		typ = kvrpcpb.RequestSourceType_Dumpling
	}
	return &kvrpcpb.RequestSourceV2{
		Origin:     origin,
		Tag:        tag,
		SourceType: typ,
	}
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
