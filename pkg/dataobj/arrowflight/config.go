package arrowflight

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"

	"github.com/grafana/loki/v3/pkg/storage/bucket"
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
func NewArchiveSourceFromConfig(ctx context.Context, cfg ArchiveServerConfig, logger log.Logger) (*ArchiveSource, error) {
	if cfg.Prefix == "" {
		return nil, fmt.Errorf("dataobj-flight.archive.prefix is required")
	}
	b, err := bucket.NewClient(ctx, cfg.Backend, cfg.Storage, "dataobj-flight-archive", logger, nil)
	if err != nil {
		return nil, fmt.Errorf("opening archive bucket: %w", err)
	}
	return NewArchiveSource(ctx, ArchiveConfig{
		Bucket:           b,
		Prefix:           cfg.Prefix,
		Layout:           cfg.Layout,
		IndexLabels:      cfg.IndexLabels,
		LabelColumns:     cfg.LabelColumns,
		MetadataColumns:  metadataColumnsOrDefault(cfg.MetadataColumns),
		ObjectsPerTicket: cfg.ObjectsPerTicket,
		Concurrency:      cfg.Concurrency,
		Logger:           logger,
	})
}

func metadataColumnsOrDefault(cols []string) []string {
	if len(cols) == 0 {
		return nil // ArchiveConfig applies DefaultArchiveMetadataColumns
	}
	return cols
}
