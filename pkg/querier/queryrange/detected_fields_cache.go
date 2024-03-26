package queryrange

import (
	"context"
	"flag"

	"github.com/go-kit/log"
	"github.com/grafana/loki/pkg/querier/queryrange/queryrangebase"
	"github.com/grafana/loki/pkg/storage/chunk/cache"
)

type DetectedFieldsCacheConfig struct {
	queryrangebase.ResultsCacheConfig `yaml:",inline"`
}

// RegisterFlags registers flags.
func (cfg *DetectedFieldsCacheConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix(f, "frontend.detected-fields-results-cache.")
}

func (cfg *DetectedFieldsCacheConfig) Validate() error {
	return cfg.ResultsCacheConfig.Validate()
}

func NewDetectedFieldsCacheMiddleware(
	logger log.Logger,
	limits Limits,
	merger queryrangebase.Merger,
	c cache.Cache,
	cacheGenNumberLoader queryrangebase.CacheGenNumberLoader,
	shouldCache queryrangebase.ShouldCacheFn,
	parallelismForReq queryrangebase.ParallelismForReqFn,
	retentionEnabled bool,
	transformer UserIDTransformer,
	metrics *queryrangebase.ResultsCacheMetrics,
) (queryrangebase.Middleware, error) {
	return queryrangebase.NewResultsCacheMiddleware(
		logger,
		c,
		cacheKeyLabels{limits, transformer},
		limits,
		merger,
		labelsExtractor{},
		cacheGenNumberLoader,
		func(ctx context.Context, r queryrangebase.Request) bool {
			return shouldCacheMetadataReq(ctx, logger, shouldCache, r, limits)
		},
		parallelismForReq,
		retentionEnabled,
		true,
		metrics,
	)
}
