package util

// ResourceGroupTagParams are some necessary parameters used to generate ResourceGroupTag.
type ResourceGroupTagParams struct {
	// FirstKey is the first key of current TiKV request.
	FirstKey []byte
}

// ResourceGroupTagFactory is used to generate the ResourceGroupTag required for each request.
type ResourceGroupTagFactory func(params ResourceGroupTagParams) []byte
