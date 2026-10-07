// Package archive serves LogQL over an archive-and-replay bucket: gzipped
// OTLP JSON objects in five-minute event-time partitions, as written by the
// archive ingester. It implements the store interface of the v1 querier, so
// the standard LogQL engine, query frontend and HTTP API run unchanged on top
// of it.
package archive

import (
	"flag"
)

// Config configures the archive store behind the archive-querier target.
// The archive bucket itself is configured by dataobj_flight.archive, which
// the dataobj-flight target shares.
type Config struct {
	// Enabled turns the archive-querier target on.
	Enabled bool `yaml:"enabled"`

	// MaxMetadataObjects bounds how many objects a label, label values or
	// series request decodes. Objects are sampled evenly across the time
	// range when the range holds more.
	MaxMetadataObjects int `yaml:"max_metadata_objects"`

	// PrefetchPartitions is how many five-minute partitions a log query
	// decodes ahead of the one being read. Log queries stop decoding once
	// the engine has its limit, so this bounds wasted work.
	PrefetchPartitions int `yaml:"prefetch_partitions"`

	// ObjectConcurrency bounds how many objects one request fetches at
	// once. Decoding is bounded separately by dataobj_flight.archive.concurrency.
	ObjectConcurrency int `yaml:"object_concurrency"`
}

// RegisterFlags registers the flags under the archive-querier prefix.
func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&c.Enabled, "archive-querier.enabled", false, "Experimental: serve LogQL over the archive configured by dataobj_flight.archive through the archive-querier target.")
	f.IntVar(&c.MaxMetadataObjects, "archive-querier.max-metadata-objects", 256, "Experimental: maximum archive objects decoded to answer a labels, label values or series request; sampled evenly across the range when exceeded.")
	f.IntVar(&c.PrefetchPartitions, "archive-querier.prefetch-partitions", 2, "Experimental: five-minute archive partitions decoded ahead of the one a log query is reading.")
	f.IntVar(&c.ObjectConcurrency, "archive-querier.object-concurrency", 32, "Experimental: archive objects fetched at once per request.")
}

func (c *Config) applyDefaults() {
	if c.MaxMetadataObjects <= 0 {
		c.MaxMetadataObjects = 256
	}
	if c.PrefetchPartitions <= 0 {
		c.PrefetchPartitions = 1
	}
	if c.ObjectConcurrency <= 0 {
		c.ObjectConcurrency = 32
	}
}
