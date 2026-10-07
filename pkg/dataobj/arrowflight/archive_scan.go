package arrowflight

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/otlptranslator"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/grafana/loki/pkg/push"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight/scanpb"
	"github.com/grafana/loki/v3/pkg/loghttp/push/otlplabels"
)

// archiveRow is one log record after Loki's OTLP normalisation: the same
// shape Loki would have ingested (stream labels, line, structured metadata),
// with the metadata split by where it came from.
type archiveRow struct {
	ts       time.Time
	observed time.Time
	line     string
	labels   map[string]string
	// resource holds metadata derived from resource attributes, log holds
	// metadata derived from scope and log attributes plus record fields.
	resource map[string]string
	log      map[string]string
}

func (r *archiveRow) metadata(key string) (string, bool) {
	if v, ok := r.log[key]; ok {
		return v, true
	}
	v, ok := r.resource[key]
	return v, ok
}

// archiveObjectStats accounts for one object read.
type archiveObjectStats struct {
	compressedBytes   int64
	uncompressedBytes int64
}

// readObject fetches, decompresses, decodes and normalises one object.
func (a *ArchiveSource) readObject(ctx context.Context, name string) ([]archiveRow, error) {
	rows, _, err := a.readObjectStats(ctx, name)
	return rows, err
}

// readObjectStats is readObject with the compressed and uncompressed sizes
// of the object, for callers that account for bytes read per query.
func (a *ArchiveSource) readObjectStats(ctx context.Context, name string) ([]archiveRow, archiveObjectStats, error) {
	var st archiveObjectStats
	select {
	case a.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, st, ctx.Err()
	}
	defer func() { <-a.sem }()

	// Fetch the whole compressed object first so fetch (network) and decode
	// (CPU) are accounted separately; objects are small.
	fetchStart := time.Now()
	rc, err := a.cfg.Bucket.Get(ctx, name)
	if err != nil {
		return nil, st, err
	}
	compressed, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return nil, st, fmt.Errorf("reading object: %w", err)
	}
	m := a.cfg.Metrics
	m.archiveFetchDuration.Observe(time.Since(fetchStart).Seconds())
	m.archiveObjectsFetched.Inc()
	m.archiveCompressedBytes.Add(float64(len(compressed)))
	st.compressedBytes = int64(len(compressed))

	decodeStart := time.Now()
	gz, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, st, fmt.Errorf("gunzip: %w", err)
	}
	raw, err := io.ReadAll(gz)
	if err != nil {
		return nil, st, fmt.Errorf("gunzip: %w", err)
	}
	m.archiveUncompressedByte.Add(float64(len(raw)))
	st.uncompressedBytes = int64(len(raw))
	ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs(raw)
	if err != nil {
		return nil, st, fmt.Errorf("decoding OTLP JSON: %w", err)
	}
	rows, err := a.normalize(ld)
	m.archiveDecodeDuration.Observe(time.Since(decodeStart).Seconds())
	m.archiveRecordsDecoded.Add(float64(len(rows)))
	return rows, st, err
}

// normalize applies Loki's OTLP-to-stream mapping to every record.
func (a *ArchiveSource) normalize(ld plog.Logs) ([]archiveRow, error) {
	rows := make([]archiveRow, 0, ld.LogRecordCount())
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			// Converted push requests already carry Loki's labels (including
			// service_name), so service name discovery must not run on them:
			// it would overwrite service_name with unknown_service.
			cfg, discover := a.native, a.cfg.DiscoverServiceName
			if _, ok := sls.At(j).Scope().Attributes().Get(ArchiveConvertedPushScopeAttribute); ok {
				cfg, discover = a.converted, nil
			}
			res, err := otlplabels.ResourceAttrsToStreamLabels(rl.Resource().Attributes(), cfg, discover)
			if err != nil {
				return nil, err
			}
			scope, err := otlplabels.ScopeAttrsToStructuredMetadata(sls, j, cfg)
			if err != nil {
				return nil, err
			}
			resourceMeta := adaptersToMap(res.StructuredMetadata, nil)

			records := sls.At(j).LogRecords()
			for k := 0; k < records.Len(); k++ {
				lr := records.At(k)
				logResult, err := otlplabels.LogAttrsToLabels(lr, cfg)
				if err != nil {
					return nil, err
				}
				labels := make(map[string]string, len(res.StreamLabels)+len(logResult.IndexLabels))
				for n, v := range res.StreamLabels {
					labels[string(n)] = string(v)
				}
				for n, v := range logResult.IndexLabels {
					labels[string(n)] = string(v)
				}
				logMeta := adaptersToMap(scope.StructuredMetadata, nil)
				logMeta = adaptersToMap(logResult.StructuredMetadata, logMeta)

				var observed time.Time
				if lr.ObservedTimestamp() != 0 {
					observed = time.Unix(0, int64(lr.ObservedTimestamp())).UTC()
				}
				ts := observed
				if lr.Timestamp() != 0 {
					ts = time.Unix(0, int64(lr.Timestamp())).UTC()
				}
				rows = append(rows, archiveRow{
					ts:       ts,
					observed: observed,
					line:     lr.Body().AsString(),
					labels:   labels,
					resource: resourceMeta,
					log:      logMeta,
				})
			}
		}
	}
	return rows, nil
}

func adaptersToMap(in push.LabelsAdapter, into map[string]string) map[string]string {
	if into == nil {
		into = make(map[string]string, len(in))
	}
	for _, l := range in {
		into[l.Name] = l.Value
	}
	return into
}

// normalizeLabelName applies the same key normalisation Loki applies to
// OTLP attribute names (dots become underscores, and so on).
func normalizeLabelName(attr string) string {
	namer := otlptranslator.LabelNamer{}
	n, err := namer.Build(attr)
	if err != nil {
		return strings.ReplaceAll(attr, ".", "_")
	}
	return n
}

// archiveFilter evaluates the pushed predicates on a normalised row. Every
// predicate the server understands is applied exactly; unknown columns pass
// (the client re-applies all filters anyway).
type archiveFilter func(*archiveRow) bool

func (a *ArchiveSource) compileFilter(preds []*scanpb.Predicate) archiveFilter {
	var checks []archiveFilter
	for _, p := range preds {
		b, ok := a.schema.binding(p.GetColumn())
		if !ok {
			continue
		}

		switch {
		case b.Kind == columnKindFixed && b.Source == ColumnTimestamp:
			ns, ok := literalInt64(p.GetValues()[0])
			if !ok || len(p.GetValues()) != 1 {
				continue
			}
			op := p.GetOp()
			checks = append(checks, func(r *archiveRow) bool {
				v := r.ts.UnixNano()
				switch op {
				case scanpb.Op_OP_GT:
					return v > ns
				case scanpb.Op_OP_GTE:
					return v >= ns
				case scanpb.Op_OP_LT:
					return v < ns
				case scanpb.Op_OP_LTE:
					return v <= ns
				case scanpb.Op_OP_EQ:
					return v == ns
				}
				return true
			})
		case b.Kind == columnKindLabel || b.Kind == columnKindMetadata:
			values := make([]string, 0, len(p.GetValues()))
			for _, l := range p.GetValues() {
				if s, ok := literalString(l); ok {
					values = append(values, s)
				}
			}
			if len(values) == 0 {
				continue
			}
			source, kind, op := b.Source, b.Kind, p.GetOp()
			checks = append(checks, func(r *archiveRow) bool {
				var v string
				var present bool
				if kind == columnKindLabel {
					v, present = r.labels[source]
				} else {
					v, present = r.metadata(source)
				}
				if !present {
					return false
				}
				switch op {
				case scanpb.Op_OP_EQ, scanpb.Op_OP_IN:
					return slices.Contains(values, v)
				case scanpb.Op_OP_GT:
					return v > values[0]
				case scanpb.Op_OP_GTE:
					return v >= values[0]
				case scanpb.Op_OP_LT:
					return v < values[0]
				case scanpb.Op_OP_LTE:
					return v <= values[0]
				}
				return true
			})
		}
	}
	return func(r *archiveRow) bool {
		for _, c := range checks {
			if !c(r) {
				return false
			}
		}
		return true
	}
}

// archiveScanner streams the rows of a ticket's objects as record batches in
// the projected schema.
type archiveScanner struct {
	src    *ArchiveSource
	schema *arrow.Schema
	paths  []string
	filter archiveFilter
	alloc  memory.Allocator

	next    int
	pending []arrow.RecordBatch
}

func newArchiveScanner(src *ArchiveSource, schema *arrow.Schema, paths []string, req *scanpb.ScanRequest) *archiveScanner {
	return &archiveScanner{
		src:    src,
		schema: schema,
		paths:  paths,
		filter: src.compileFilter(req.GetPredicates()),
		alloc:  memory.DefaultAllocator,
	}
}

func (s *archiveScanner) Schema() *arrow.Schema { return s.schema }

func (s *archiveScanner) Next(ctx context.Context) (arrow.RecordBatch, error) {
	for len(s.pending) == 0 {
		if s.next >= len(s.paths) {
			return nil, io.EOF
		}
		name := s.paths[s.next]
		s.next++
		rows, err := s.src.readObject(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		s.pending = s.build(rows)
	}
	rec := s.pending[0]
	s.pending = s.pending[1:]
	return rec, nil
}

func (s *archiveScanner) Close() error {
	for _, rec := range s.pending {
		rec.Release()
	}
	s.pending = nil
	return nil
}

// build turns normalised rows into batches of the projected schema.
func (s *archiveScanner) build(rows []archiveRow) []arrow.RecordBatch {
	var out []arrow.RecordBatch
	b := array.NewRecordBuilder(s.alloc, s.schema)
	defer b.Release()

	n := 0
	flush := func() {
		if n == 0 {
			return
		}
		out = append(out, b.NewRecordBatch())
		n = 0
	}

	matched := 0
	for i := range rows {
		r := &rows[i]
		if !s.filter(r) {
			continue
		}
		matched++
		for fi, f := range s.schema.Fields() {
			binding, _ := s.src.schema.binding(f.Name)
			s.appendValue(b.Field(fi), binding, r)
		}
		n++
		if n >= s.src.cfg.BatchRows {
			flush()
		}
	}
	flush()
	s.src.cfg.Metrics.archiveRecordsMatched.Add(float64(matched))
	return out
}

func (s *archiveScanner) appendValue(fb array.Builder, binding columnBinding, r *archiveRow) {
	switch binding.Kind {
	case columnKindLabel:
		v, ok := r.labels[binding.Source]
		appendOptionalString(fb, v, ok)
		return
	case columnKindMetadata:
		v, ok := r.metadata(binding.Source)
		appendOptionalString(fb, v, ok)
		return
	}
	switch binding.Source {
	case ColumnTimestamp:
		fb.(*array.TimestampBuilder).Append(arrow.Timestamp(r.ts.UnixNano()))
	case ColumnObservedTimestamp:
		if r.observed.IsZero() {
			fb.AppendNull()
		} else {
			fb.(*array.TimestampBuilder).Append(arrow.Timestamp(r.observed.UnixNano()))
		}
	case ColumnMessage:
		fb.(*array.StringBuilder).Append(r.line)
	case ColumnResourceAttributes:
		appendMap(fb.(*array.MapBuilder), r.resource)
	case ColumnLogAttributes:
		appendMap(fb.(*array.MapBuilder), r.log)
	default:
		fb.AppendNull()
	}
}

func appendOptionalString(fb array.Builder, v string, ok bool) {
	sb := fb.(*array.StringBuilder)
	if !ok {
		sb.AppendNull()
		return
	}
	sb.Append(v)
}

func appendMap(mb *array.MapBuilder, m map[string]string) {
	mb.Append(true)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	kb := mb.KeyBuilder().(*array.StringBuilder)
	ib := mb.ItemBuilder().(*array.StringBuilder)
	for _, k := range keys {
		kb.Append(k)
		ib.Append(m[k])
	}
}
