package bench

import (
	"context"
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
)

var (
	storageDir = "storage"
	workingDir = "workingdir"
)

// Store represents a storage backend for log data
type Store interface {
	// Write writes a batch of streams to the store
	Write(ctx context.Context, streams []logproto.Stream) error
	// Name returns the name of the store implementation
	Name() string
	// Close flushes any remaining data and closes resources
	Close() error
}

// Builder helps construct test datasets using multiple stores
type Builder struct {
	stores []Store
	gen    *Generator
	dir    string

	// archive receives the streams matched by archiveRules instead of the
	// live stores, modelling an archive rule (a stream selector) that sends
	// those streams to the archive bucket rather than to Loki.
	archive      Store
	archiveRules [][]*labels.Matcher
	archived     []bool // per stream index, computed on the first batch
}

// NewBuilder creates a new Builder
func NewBuilder(dir string, opt Opt, stores ...Store) *Builder {
	return &Builder{
		stores: stores,
		gen:    NewGenerator(opt),
		dir:    dir,
	}
}

// Generate generates and stores the specified amount of data across all stores
// WithArchive routes every stream matching one of the rules (LogQL stream
// selectors such as `{service_name="nginx"}`) to the archive store and
// withholds it from the live stores.
func (b *Builder) WithArchive(store Store, rules []string) error {
	for _, rule := range rules {
		rule = strings.TrimSpace(rule)
		if rule == "" {
			continue
		}
		matchers, err := syntax.ParseMatchers(rule, true)
		if err != nil {
			return fmt.Errorf("parsing archive rule %q: %w", rule, err)
		}
		b.archiveRules = append(b.archiveRules, matchers)
	}
	if len(b.archiveRules) > 0 {
		b.archive = store
	}
	return nil
}

// ArchiveRules returns the configured archive rules as strings.
func (b *Builder) ArchiveRules() []string {
	out := make([]string, 0, len(b.archiveRules))
	for _, rule := range b.archiveRules {
		parts := make([]string, 0, len(rule))
		for _, m := range rule {
			parts = append(parts, m.String())
		}
		out = append(out, "{"+strings.Join(parts, ", ")+"}")
	}
	return out
}

// matchesArchiveRule reports whether a stream's labels match any rule.
func (b *Builder) matchesArchiveRule(labelString string) (bool, error) {
	if len(b.archiveRules) == 0 {
		return false, nil
	}
	lbls, err := syntax.ParseLabels(labelString)
	if err != nil {
		return false, fmt.Errorf("parsing stream labels %q: %w", labelString, err)
	}
	for _, rule := range b.archiveRules {
		matched := true
		for _, m := range rule {
			if !m.Matches(lbls.Get(m.Name)) {
				matched = false
				break
			}
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

// splitBatch separates a batch into live and archived streams. The split is
// decided once, on the first batch, because stream order is stable.
func (b *Builder) splitBatch(streams []logproto.Stream) (live, archived []logproto.Stream, err error) {
	if b.archive == nil {
		return streams, nil, nil
	}
	if b.archived == nil {
		b.archived = make([]bool, len(streams))
		for i, stream := range streams {
			b.archived[i], err = b.matchesArchiveRule(stream.Labels)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	for i, stream := range streams {
		if i < len(b.archived) && b.archived[i] {
			archived = append(archived, stream)
		} else {
			live = append(live, stream)
		}
	}
	return live, archived, nil
}

func (b *Builder) Generate(ctx context.Context, targetSize int64) error {
	fmt.Printf("Generating %s of data\n", formatBytes(targetSize))
	// Save the generator config once at the root directory
	if err := SaveConfig(b.dir, &b.gen.config); err != nil {
		return fmt.Errorf("failed to save generator config: %w", err)
	}

	var totalSize int64
	lastProgress := 0

	for batch := range b.gen.Batches() {
		live, archived, err := b.splitBatch(batch.Streams)
		if err != nil {
			return err
		}

		// Write live streams to all stores, archived ones to the archive.
		for _, store := range b.stores {
			if err := store.Write(ctx, live); err != nil {
				return fmt.Errorf("failed to write to store %s: %w", store.Name(), err)
			}
		}
		if len(archived) > 0 {
			if err := b.archive.Write(ctx, archived); err != nil {
				return fmt.Errorf("failed to write to store %s: %w", b.archive.Name(), err)
			}
		}
		totalSize += int64(batch.Size())
		// Report progress every 5%
		progress := int(float64(totalSize) / float64(targetSize) * 100)
		if progress/5 > lastProgress/5 {
			fmt.Printf("Generated %d%% (%s/%s)\n", progress, formatBytes(totalSize), formatBytes(targetSize))
			lastProgress = progress
		}

		if totalSize >= targetSize {
			break
		}
	}

	// Close all stores to ensure data is flushed
	for _, store := range b.stores {
		if err := store.Close(); err != nil {
			return fmt.Errorf("failed to close store %s: %w", store.Name(), err)
		}
	}
	if b.archive != nil {
		if err := b.archive.Close(); err != nil {
			return fmt.Errorf("failed to close store %s: %w", b.archive.Name(), err)
		}
	}

	fmt.Printf("Generated 100%% (%s/%s)\n", formatBytes(totalSize), formatBytes(targetSize))

	// Generate and save dataset metadata
	fmt.Println("Generating dataset metadata...")
	// Archived streams are not in the live stores, so they are kept out of
	// the selectors the query generators draw from and listed separately.
	liveMeta := make([]StreamMetadata, 0, len(b.gen.StreamsMeta))
	var archivedSelectors []string
	for i, meta := range b.gen.StreamsMeta {
		if i < len(b.archived) && b.archived[i] {
			archivedSelectors = append(archivedSelectors, meta.Labels)
			continue
		}
		liveMeta = append(liveMeta, meta)
	}
	metadata := BuildMetadata(&b.gen.config, liveMeta)
	metadata.ArchiveRules = b.ArchiveRules()
	metadata.ArchivedSelectors = archivedSelectors
	if err := SaveMetadata(b.dir, metadata); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}
	fmt.Printf("Saved metadata: %d streams, %d formats, %d applications, %d archived streams\n",
		metadata.Statistics.TotalStreams,
		len(metadata.ByFormat),
		len(metadata.ByServiceName),
		len(metadata.ArchivedSelectors))

	return nil
}

// formatBytes converts bytes to a human readable string
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
