package arrowflight

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"

	"github.com/grafana/loki/v3/pkg/storage/bucket"
	lokiring "github.com/grafana/loki/v3/pkg/util/ring"
)

// Config configures the dataobj-flight target: the Arrow Flight scan service
// that serves a cell's data objects (through the metastore) and, optionally,
// an archive bucket as the archive_logs table.
type Config struct {
	// Tenant served when a request carries no X-Scope-OrgID, and used for
	// schema discovery at startup.
	Tenant string `yaml:"tenant"`
	// SchemaWindow is how far back schema discovery looks.
	SchemaWindow time.Duration `yaml:"schema_window"`
	// ExtraLabels and ExtraMetadata are columns always present in the logs
	// table, in addition to what discovery finds.
	ExtraLabels   flagext.StringSliceCSV `yaml:"extra_labels"`
	ExtraMetadata flagext.StringSliceCSV `yaml:"extra_metadata"`
	// PrefetchBytes is passed to the data object reader when opening objects.
	PrefetchBytes int64 `yaml:"prefetch_bytes"`
	// MaxCachedObjects bounds the opened data objects kept in memory;
	// StreamsCacheBytes bounds the cached in-memory streams tables used to
	// join labels into log rows. 0 means the package defaults.
	MaxCachedObjects  int   `yaml:"max_cached_objects"`
	StreamsCacheBytes int64 `yaml:"streams_cache_bytes"`
	// MaxConcurrentScans bounds the DoGet calls served at once by this
	// instance; further calls queue. 0 leaves scans unbounded.
	MaxConcurrentScans int `yaml:"max_concurrent_scans"`
	// DiskCache optionally caches object store reads on local disk so a
	// repeated query measures CPU without object store traffic.
	DiskCache DiskCacheConfig `yaml:"disk_cache"`

	// RingEnabled makes the target join a ring of dataobj-flight instances
	// and stamp every planned endpoint with the ring members that should
	// serve it, so a Flight client spreads DoGet calls across the pool.
	RingEnabled bool `yaml:"ring_enabled"`
	// Ring configures the dataobj-flight ring. Its replication factor is the
	// number of candidate servers offered per endpoint.
	Ring lokiring.RingConfig `yaml:"ring" doc:"description=Ring of dataobj-flight instances, used to place scan endpoints when ring_enabled is true."`

	Archive ArchiveServerConfig `yaml:"archive"`
}

// ArchiveServerConfig configures the archive_logs table of the target.
type ArchiveServerConfig struct {
	Enabled bool `yaml:"enabled"`
	// Backend is the object store type of the archive bucket: gcs, s3,
	// azure, filesystem, ...
	Backend string        `yaml:"backend"`
	Storage bucket.Config `yaml:"storage"`
	// Prefix within the bucket that holds the tenant's partitions, for
	// example replay-archive/logs/tenant=12345/signal=logs.
	Prefix string `yaml:"prefix"`
	// Layout is "hive" (year=/month=/...) or "plain" (YYYY/MM/...).
	Layout string `yaml:"layout"`
	// IndexLabels are the OTLP resource attributes promoted to labels for
	// native OTLP objects. Empty means Loki's default list.
	IndexLabels flagext.StringSliceCSV `yaml:"index_labels"`
	// LabelColumns fixes the label columns instead of discovering them.
	LabelColumns flagext.StringSliceCSV `yaml:"label_columns"`
	// MetadataColumns are structured metadata keys exposed as flat columns.
	MetadataColumns flagext.StringSliceCSV `yaml:"metadata_columns"`
	// ObjectsPerTicket and Concurrency bound the scan fan-out.
	ObjectsPerTicket int `yaml:"objects_per_ticket"`
	Concurrency      int `yaml:"concurrency"`
}

// RegisterFlags registers the target's flags under the dataobj-flight. prefix.
func (c *Config) RegisterFlags(f *flag.FlagSet) {
	const prefix = "dataobj-flight."
	f.StringVar(&c.Tenant, prefix+"tenant", "", "Experimental: Tenant served when a request carries no X-Scope-OrgID; also used for schema discovery at startup.")
	f.DurationVar(&c.SchemaWindow, prefix+"schema-window", 24*time.Hour, "Experimental: How far back schema discovery looks for label names and metadata keys.")
	f.Var(&c.ExtraLabels, prefix+"extra-labels", "Experimental: Comma-separated label columns always present in the logs table.")
	f.Var(&c.ExtraMetadata, prefix+"extra-metadata", "Experimental: Comma-separated structured metadata columns always present in the logs table.")
	f.Int64Var(&c.PrefetchBytes, prefix+"prefetch-bytes", 0, "Experimental: Bytes to prefetch when opening a data object.")
	f.IntVar(&c.MaxCachedObjects, prefix+"max-cached-objects", DefaultMaxCachedObjects, "Experimental: Opened data objects kept in memory by the scan service.")
	f.Int64Var(&c.StreamsCacheBytes, prefix+"streams-cache-bytes", DefaultStreamsCacheBytes, "Experimental: Bytes of in-memory streams tables (stream ID to labels, per object) kept for joining labels into log rows; least recently used tables are dropped past it.")
	f.IntVar(&c.MaxConcurrentScans, prefix+"max-concurrent-scans", 16, "Experimental: Maximum DoGet scans served at once by this instance; further scans wait for a slot. 0 means unbounded.")
	f.StringVar(&c.DiskCache.Dir, prefix+"disk-cache.dir", "", "Experimental: Directory for a read-through disk cache of object store reads (data objects, metastore index and archive). Empty disables the cache.")
	f.Int64Var(&c.DiskCache.MaxSizeBytes, prefix+"disk-cache.max-size-bytes", 0, "Experimental: Maximum bytes kept in the disk cache before least recently used entries are evicted. 0 means 10 GiB.")
	f.Var(&c.DiskCache.Buckets, prefix+"disk-cache.buckets", "Experimental: Comma-separated stores that go through the disk cache: data (data objects and metastore index), archive. Empty means both. Leave out a store whose working set is far larger than the cache.")
	f.BoolVar(&c.RingEnabled, prefix+"ring.enabled", false, "Experimental: Join a ring of dataobj-flight instances and put the ring members that should serve each planned endpoint on it as Flight locations.")
	c.Ring.RegisterFlagsWithPrefix(prefix, "collectors/", f)

	a := &c.Archive
	f.BoolVar(&a.Enabled, prefix+"archive.enabled", false, "Experimental: Serve an archive bucket as the archive_logs table.")
	f.StringVar(&a.Backend, prefix+"archive.backend", bucket.GCS, "Experimental: Object store backend of the archive bucket (gcs, s3, azure, filesystem).")
	a.Storage.RegisterFlagsWithPrefix(prefix+"archive.", f)
	f.StringVar(&a.Prefix, prefix+"archive.prefix", "", "Experimental: Prefix of the tenant's partitions within the archive bucket, for example replay-archive/logs/tenant=12345/signal=logs.")
	f.StringVar(&a.Layout, prefix+"archive.layout", ArchiveLayoutHive, "Experimental: Partition layout of the archive: hive (year=/month=/day=/hour=/minute=) or plain (YYYY/MM/DD/HH/mm).")
	f.Var(&a.IndexLabels, prefix+"archive.index-labels", "Experimental: Comma-separated OTLP resource attributes promoted to labels for native OTLP objects. Empty means Loki's default list.")
	f.Var(&a.LabelColumns, prefix+"archive.label-columns", "Experimental: Comma-separated label columns of archive_logs; discovered from sample objects when empty.")
	f.Var(&a.MetadataColumns, prefix+"archive.metadata-columns", "Experimental: Comma-separated structured metadata keys exposed as columns of archive_logs.")
	f.IntVar(&a.ObjectsPerTicket, prefix+"archive.objects-per-ticket", 32, "Experimental: Archive objects served per Flight endpoint.")
	f.IntVar(&a.Concurrency, prefix+"archive.concurrency", 0, "Experimental: Archive objects decoded concurrently across scans. 0 means GOMAXPROCS.")
}

// NewArchiveSourceFromConfig opens the archive bucket described by cfg and
// returns the archive_logs source.
//
// With a cache directory configured, reads of archive objects go through
// the disk cache described by cache (see [NewDiskCacheBucket]).
func NewArchiveSourceFromConfig(ctx context.Context, cfg ArchiveServerConfig, cache DiskCacheConfig, cacheMetrics *DiskCacheMetrics, metrics *Metrics, logger log.Logger) (*ArchiveSource, error) {
	if cfg.Prefix == "" {
		return nil, fmt.Errorf("dataobj-flight.archive.prefix is required")
	}
	b, err := bucket.NewClient(ctx, cfg.Backend, cfg.Storage, "dataobj-flight-archive", logger, nil)
	if err != nil {
		return nil, fmt.Errorf("opening archive bucket: %w", err)
	}
	cached, err := NewDiskCacheBucket(b, "archive", cache, cacheMetrics, logger)
	if err != nil {
		return nil, err
	}
	return NewArchiveSource(ctx, ArchiveConfig{
		Bucket:           cached,
		Prefix:           cfg.Prefix,
		Layout:           cfg.Layout,
		IndexLabels:      cfg.IndexLabels,
		LabelColumns:     cfg.LabelColumns,
		MetadataColumns:  metadataColumnsOrDefault(cfg.MetadataColumns),
		ObjectsPerTicket: cfg.ObjectsPerTicket,
		Concurrency:      cfg.Concurrency,
		Metrics:          metrics,
		Logger:           logger,
	})
}

func metadataColumnsOrDefault(cols []string) []string {
	if len(cols) == 0 {
		return nil // ArchiveConfig applies DefaultArchiveMetadataColumns
	}
	return cols
}
