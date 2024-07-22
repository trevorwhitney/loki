package metric

import (
	"flag"
	"time"

	"github.com/grafana/dskit/backoff"
	"github.com/prometheus/common/config"
)

type AggregationConfig struct {
	Enabled             bool                    `yaml:"enabled,omitempty" doc:"description=Whether the pattern ingester metric aggregation is enabled."`
	LogPushObservations bool                    `yaml:"log_push_observations,omitempty" doc:"description=Whether to log push observations."`
	DownsamplePeriod    time.Duration           `yaml:"downsample_period"`
	LokiAddr            string                  `yaml:"loki_address,omitempty" doc:"description=The address of the Loki instance to push aggregated metrics to."`
	WriteTimeout        time.Duration           `yaml:"timeout,omitempty" doc:"description=The timeout for writing to Loki."`
	HTTPClientConfig    config.HTTPClientConfig `yaml:"http_client_config,omitempty" doc:"description=The HTTP client configuration for pushing metrics to Loki."`
	UseTLS              bool                    `yaml:"use_tls,omitempty" doc:"description=Whether to use TLS for pushing metrics to Loki."`
	BackoffConfig       backoff.Config          `yaml:"backoff_config,omitempty" doc:"description=The backoff configuration for pushing metrics to Loki."`
}

// RegisterFlags registers pattern ingester related flags.
func (cfg *AggregationConfig) RegisterFlags(fs *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix(fs, "")
}

func (cfg *AggregationConfig) RegisterFlagsWithPrefix(fs *flag.FlagSet, prefix string) {
	fs.BoolVar(
		&cfg.Enabled,
		prefix+"metric-aggregation.enabled",
		false,
		"Flag to enable or disable metric aggregation.",
	)
	fs.BoolVar(
		&cfg.LogPushObservations,
		prefix+"metric-aggregation.log-push-observations",
		false,
		"Flag to enable or disable logging of push observations.",
	)
	fs.DurationVar(
		&cfg.DownsamplePeriod,
		prefix+"metric-aggregation.downsample-period",
		10*time.Second,
		"How often to downsample metrics from raw push observations.",
	)
	fs.DurationVar(
		&cfg.WriteTimeout,
		prefix+"metric-aggregation.timeout",
		10*time.Second,
		"How long to wait write response from Loki",
	)

	fs.BoolVar(
		&cfg.UseTLS,
		prefix+"metric-aggregation.tls",
		false,
		"Does the loki connection use TLS?",
	)

	cfg.BackoffConfig.RegisterFlagsWithPrefix(prefix+"metric-aggregation", fs)
}
