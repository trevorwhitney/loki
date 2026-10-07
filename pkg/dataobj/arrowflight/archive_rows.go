package arrowflight

import (
	"context"
	"sort"
	"time"
)

// ArchiveRow is one normalised archive log record in the shape Loki would
// have ingested: stream labels, the line and its structured metadata. It is
// the row-oriented view of the archive for LogQL consumers; the Arrow
// scanner is the columnar one.
type ArchiveRow struct {
	Timestamp time.Time
	Line      string
	Labels    map[string]string
	// Metadata is the structured metadata of the record. Keys derived from
	// the log record and its scope win over keys derived from the resource.
	Metadata map[string]string
}

// ArchiveObjectStats accounts for one object read through
// [ArchiveSource.ReadObjectRows].
type ArchiveObjectStats struct {
	CompressedBytes   int64
	UncompressedBytes int64
}

// ListObjects returns the archive objects whose five-minute partition
// intersects [start, end), sorted by name (partition time, then uuidv7).
func (a *ArchiveSource) ListObjects(ctx context.Context, start, end time.Time) ([]string, error) {
	if !end.After(start) {
		return nil, nil
	}
	// Partition paths are formatted in UTC; callers may pass local times.
	return a.listObjects(ctx, start.UTC(), end.UTC().Add(-time.Nanosecond))
}

// PartitionTime returns the five-minute partition an object belongs to.
func (a *ArchiveSource) PartitionTime(name string) (time.Time, bool) {
	return archivePartitionTime(a.cfg.Prefix, name, archiveLayouts[a.cfg.Layout].minute)
}

// PartitionDuration is the event-time width of one archive partition.
func (a *ArchiveSource) PartitionDuration() time.Duration { return archiveBucketDuration }

// ReadObjectRows fetches, decompresses, decodes and normalises one object.
// Reads are bounded by the source's Concurrency across all callers.
func (a *ArchiveSource) ReadObjectRows(ctx context.Context, name string) ([]ArchiveRow, ArchiveObjectStats, error) {
	rows, st, err := a.readObjectStats(ctx, name)
	if err != nil {
		return nil, ArchiveObjectStats{}, err
	}
	out := make([]ArchiveRow, len(rows))
	for i := range rows {
		r := &rows[i]
		meta := make(map[string]string, len(r.resource)+len(r.log))
		for k, v := range r.resource {
			meta[k] = v
		}
		for k, v := range r.log {
			meta[k] = v
		}
		out[i] = ArchiveRow{
			Timestamp: r.ts,
			Line:      r.line,
			Labels:    r.labels,
			Metadata:  meta,
		}
	}
	return out, ArchiveObjectStats{CompressedBytes: st.compressedBytes, UncompressedBytes: st.uncompressedBytes}, nil
}

// LabelNames returns the stream label names the source discovered (or was
// configured with), sorted.
func (a *ArchiveSource) LabelNames() []string {
	var names []string
	for _, b := range a.schema.bindings {
		if b.Kind == columnKindLabel {
			names = append(names, b.Source)
		}
	}
	sort.Strings(names)
	return names
}

// MetadataKeys returns the structured metadata keys exposed as flat columns.
func (a *ArchiveSource) MetadataKeys() []string {
	var names []string
	for _, b := range a.schema.bindings {
		if b.Kind == columnKindMetadata {
			names = append(names, b.Source)
		}
	}
	sort.Strings(names)
	return names
}
