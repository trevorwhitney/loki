package bench

import (
	"compress/gzip"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
)

const (
	// ArchiveDir is the directory under the dataset root that holds archive
	// objects, mirroring the layout of the archive-and-replay bucket:
	// <ArchiveDir>/<tenant>/YYYY/MM/DD/HH/mm/<uuidv7>.json.gz.
	ArchiveDir = "archive"

	// ArchiveConvertedPushScopeAttribute marks objects that the archive
	// ingester produced from Loki push requests rather than native OTLP. For
	// those, every resource attribute is a stream label.
	ArchiveConvertedPushScopeAttribute = "__al_archive_ingester_converted_push_request__"

	// archiveBucketDuration is the time bucket of one archive partition.
	archiveBucketDuration = 5 * time.Minute

	// archiveObjectTargetBytes bounds the uncompressed size of one object.
	archiveObjectTargetBytes = 1 << 20
)

// ArchiveStore writes streams the way the archive-and-replay ingester does:
// gzipped OTLP JSON objects, one JSON document per object, partitioned by the
// event time of the entries in five-minute buckets. Stream labels become
// resource attributes, structured metadata becomes log record attributes
// (trace and span IDs become the record's own fields), and the scope carries
// the converted-push marker.
type ArchiveStore struct {
	dir     string
	tenant  string
	buckets map[time.Time]*archiveBucket
	objects int
}

type archiveBucket struct {
	start   time.Time
	streams map[string]*logproto.Stream
	bytes   int
}

// NewArchiveStore creates an archive store rooted at <dir>/archive/<tenant>.
func NewArchiveStore(dir, tenant string) (*ArchiveStore, error) {
	root := filepath.Join(dir, ArchiveDir, tenant)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("creating archive directory: %w", err)
	}
	return &ArchiveStore{
		dir:     root,
		tenant:  tenant,
		buckets: make(map[time.Time]*archiveBucket),
	}, nil
}

// Name implements Store.
func (s *ArchiveStore) Name() string { return "archive" }

// Write implements Store. Entries are routed to their five-minute bucket;
// buckets are flushed as objects once they reach the target size.
func (s *ArchiveStore) Write(_ context.Context, streams []logproto.Stream) error {
	for _, stream := range streams {
		for _, entry := range stream.Entries {
			start := entry.Timestamp.UTC().Truncate(archiveBucketDuration)
			b, ok := s.buckets[start]
			if !ok {
				b = &archiveBucket{start: start, streams: make(map[string]*logproto.Stream)}
				s.buckets[start] = b
			}
			st, ok := b.streams[stream.Labels]
			if !ok {
				st = &logproto.Stream{Labels: stream.Labels}
				b.streams[stream.Labels] = st
				b.bytes += len(stream.Labels)
			}
			st.Entries = append(st.Entries, entry)
			b.bytes += len(entry.Line)
			for _, md := range entry.StructuredMetadata {
				b.bytes += len(md.Name) + len(md.Value)
			}
			if b.bytes >= archiveObjectTargetBytes {
				if err := s.flush(b); err != nil {
					return err
				}
				delete(s.buckets, start)
			}
		}
	}
	return nil
}

// Close implements Store: flushes every open bucket.
func (s *ArchiveStore) Close() error {
	starts := make([]time.Time, 0, len(s.buckets))
	for start := range s.buckets {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	for _, start := range starts {
		if err := s.flush(s.buckets[start]); err != nil {
			return err
		}
		delete(s.buckets, start)
	}
	fmt.Printf("Archive: wrote %d objects under %s\n", s.objects, s.dir)
	return nil
}

func (s *ArchiveStore) flush(b *archiveBucket) error {
	if len(b.streams) == 0 {
		return nil
	}
	labelsOrder := make([]string, 0, len(b.streams))
	for l := range b.streams {
		labelsOrder = append(labelsOrder, l)
	}
	sort.Strings(labelsOrder)

	ld := plog.NewLogs()
	for _, l := range labelsOrder {
		stream := b.streams[l]
		if err := appendStreamAsResourceLogs(ld, stream); err != nil {
			return err
		}
	}

	data, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
	if err != nil {
		return fmt.Errorf("marshalling archive object: %w", err)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generating object id: %w", err)
	}
	dir := filepath.Join(s.dir, b.start.Format("2006/01/02/15/04"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating partition directory: %w", err)
	}
	f, err := os.Create(filepath.Join(dir, id.String()+".json.gz"))
	if err != nil {
		return fmt.Errorf("creating archive object: %w", err)
	}
	gz := gzip.NewWriter(f)
	if _, err := gz.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("writing archive object: %w", err)
	}
	if err := gz.Close(); err != nil {
		_ = f.Close()
		return fmt.Errorf("closing gzip writer: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing archive object: %w", err)
	}
	s.objects++
	return nil
}

// appendStreamAsResourceLogs converts one Loki stream into one ResourceLogs
// entry, the shape the archive ingester writes for converted push requests.
func appendStreamAsResourceLogs(ld plog.Logs, stream *logproto.Stream) error {
	lbls, err := syntax.ParseLabels(stream.Labels)
	if err != nil {
		return fmt.Errorf("parsing stream labels %q: %w", stream.Labels, err)
	}
	rl := ld.ResourceLogs().AppendEmpty()
	attrs := rl.Resource().Attributes()
	lbls.Range(func(l labels.Label) {
		attrs.PutStr(l.Name, l.Value)
	})
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().Attributes().PutStr(ArchiveConvertedPushScopeAttribute, "true")

	records := sl.LogRecords()
	records.EnsureCapacity(len(stream.Entries))
	for _, entry := range stream.Entries {
		lr := records.AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(entry.Timestamp))
		lr.Body().SetStr(entry.Line)
		for _, md := range entry.StructuredMetadata {
			switch md.Name {
			case "trace_id":
				if b, err := hex.DecodeString(md.Value); err == nil && len(b) == 16 {
					lr.SetTraceID(pcommon.TraceID(b))
					continue
				}
			case "span_id":
				if b, err := hex.DecodeString(md.Value); err == nil && len(b) == 8 {
					lr.SetSpanID(pcommon.SpanID(b))
					continue
				}
			}
			lr.Attributes().PutStr(md.Name, md.Value)
		}
	}
	return nil
}
