package archive

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel/stats"
	"github.com/grafana/loki/v3/pkg/storage/chunk"
	indexstats "github.com/grafana/loki/v3/pkg/storage/stores/index/stats"
	"github.com/grafana/loki/v3/pkg/util"
)

// ErrShardingNotSupported is returned for shard requests: the archive has no
// index to split a query by, so the query frontend must not shard.
var ErrShardingNotSupported = errors.New("archive store does not support query sharding")

// Store answers the v1 querier's store interface from an archive. Every
// request lists the partitions of its time range, decodes their objects and
// runs the LogQL pipeline or sample extractor over the normalised rows.
type Store struct {
	src    *arrowflight.ArchiveSource
	cfg    Config
	logger log.Logger
}

// NewStore creates a store over src.
func NewStore(src *arrowflight.ArchiveSource, cfg Config, logger log.Logger) *Store {
	cfg.applyDefaults()
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Store{src: src, cfg: cfg, logger: logger}
}

// partitions lists the objects of [start, end) grouped by five-minute
// partition, oldest first.
func (s *Store) partitions(ctx context.Context, start, end time.Time) ([][]string, error) {
	objects, err := s.src.ListObjects(ctx, start, end)
	if err != nil {
		return nil, err
	}
	type group struct {
		t     time.Time
		paths []string
	}
	byTime := map[time.Time]*group{}
	var groups []*group
	for _, name := range objects {
		t, _ := s.src.PartitionTime(name)
		g, ok := byTime[t]
		if !ok {
			g = &group{t: t}
			byTime[t] = g
			groups = append(groups, g)
		}
		g.paths = append(g.paths, name)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].t.Before(groups[j].t) })
	out := make([][]string, len(groups))
	for i, g := range groups {
		out[i] = g.paths
	}
	return out, nil
}

// readObjects decodes paths concurrently and hands each object's rows to fn
// in arrival order. fn runs on one goroutine at a time.
func (s *Store) readObjects(ctx context.Context, paths []string, fn func(rows []arrowflight.ArchiveRow)) error {
	st := stats.FromContext(ctx)
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(s.cfg.ObjectConcurrency)
	var mu sync.Mutex
	for _, name := range paths {
		g.Go(func() error {
			rows, ost, err := s.src.ReadObjectRows(ctx, name)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			st.AddChunksDownloaded(1)
			st.AddCompressedBytes(ost.CompressedBytes)
			st.AddDecompressedLines(int64(len(rows)))
			fn(rows)
			return nil
		})
	}
	return g.Wait()
}

// rowLabels builds sorted stream labels and structured metadata for a row.
type rowLabels struct {
	sb  labels.ScratchBuilder
	mb  labels.ScratchBuilder
	hit map[uint64]bool
}

func newRowLabels(hint int) *rowLabels {
	return &rowLabels{
		sb:  labels.NewScratchBuilder(hint),
		mb:  labels.NewScratchBuilder(hint),
		hit: map[uint64]bool{},
	}
}

func (r *rowLabels) stream(row *arrowflight.ArchiveRow) labels.Labels {
	r.sb.Reset()
	for k, v := range row.Labels {
		r.sb.Add(k, v)
	}
	r.sb.Sort()
	return r.sb.Labels()
}

func (r *rowLabels) metadata(row *arrowflight.ArchiveRow) labels.Labels {
	if len(row.Metadata) == 0 {
		return labels.EmptyLabels()
	}
	r.mb.Reset()
	for k, v := range row.Metadata {
		r.mb.Add(k, v)
	}
	r.mb.Sort()
	return r.mb.Labels()
}

// matches evaluates the stream matchers, remembering the verdict per label
// set since a partition holds few distinct streams and many rows.
func (r *rowLabels) matches(ms []*labels.Matcher, lbls labels.Labels) bool {
	if len(ms) == 0 {
		return true
	}
	h := lbls.Hash()
	if v, ok := r.hit[h]; ok {
		return v
	}
	v := true
	for _, m := range ms {
		if !m.Matches(lbls.Get(m.Name)) {
			v = false
			break
		}
	}
	r.hit[h] = v
	return v
}

func inRange(ts, start, end time.Time) bool {
	return !ts.Before(start) && ts.Before(end)
}

// SelectLogs implements the querier store: a log query over the archive.
func (s *Store) SelectLogs(ctx context.Context, req logql.SelectLogParams) (iter.EntryIterator, error) {
	expr, err := req.LogSelector()
	if err != nil {
		return nil, err
	}
	pipeline, err := expr.Pipeline()
	if err != nil {
		return nil, err
	}
	matchers := expr.Matchers()

	parts, err := s.partitions(ctx, req.Start, req.End)
	if err != nil {
		return nil, err
	}
	if req.Direction == logproto.BACKWARD {
		for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
			parts[i], parts[j] = parts[j], parts[i]
		}
	}
	level.Debug(s.logger).Log("msg", "archive log query planned", "query", req.Selector, "from", req.Start.Format(time.RFC3339), "to", req.End.Format(time.RFC3339), "partitions", len(parts))

	st := stats.FromContext(ctx)
	start, end, direction := req.Start, req.End, req.Direction
	var mu sync.Mutex // pipeline caches per-stream state and is not goroutine safe
	load := func(ctx context.Context, paths []string) (iter.EntryIterator, error) {
		streams := map[string]*logproto.Stream{}
		rl := newRowLabels(16)
		err := s.readObjects(ctx, paths, func(rows []arrowflight.ArchiveRow) {
			mu.Lock()
			defer mu.Unlock()
			for i := range rows {
				row := &rows[i]
				if !inRange(row.Timestamp, start, end) {
					continue
				}
				st.AddDecompressedBytes(int64(len(row.Line)))
				lbls := rl.stream(row)
				if !rl.matches(matchers, lbls) {
					continue
				}
				sp := pipeline.ForStream(lbls)
				meta := rl.metadata(row)
				line, res, ok := sp.ProcessString(row.Timestamp.UnixNano(), row.Line, meta)
				if !ok {
					continue
				}
				st.AddPostFilterLines(1)
				key := res.String()
				stream, ok := streams[key]
				if !ok {
					stream = &logproto.Stream{Labels: key, Hash: sp.BaseLabels().Hash()}
					streams[key] = stream
				}
				stream.Entries = append(stream.Entries, logproto.Entry{
					Timestamp:          row.Timestamp,
					Line:               line,
					StructuredMetadata: logproto.FromLabelsToLabelAdapters(res.StructuredMetadata()),
					Parsed:             logproto.FromLabelsToLabelAdapters(res.Parsed()),
				})
			}
		})
		if err != nil {
			return nil, err
		}
		if len(streams) == 0 {
			return iter.NoopEntryIterator, nil
		}
		out := make([]logproto.Stream, 0, len(streams))
		for _, stream := range streams {
			sortEntries(stream.Entries, direction)
			out = append(out, *stream)
		}
		return iter.NewStreamsIterator(out, direction), nil
	}
	return newLazyEntryIterator(ctx, parts, s.cfg.PrefetchPartitions, load), nil
}

func sortEntries(entries []logproto.Entry, direction logproto.Direction) {
	if direction == logproto.BACKWARD {
		sort.SliceStable(entries, func(i, j int) bool { return entries[i].Timestamp.After(entries[j].Timestamp) })
		return
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].Timestamp.Before(entries[j].Timestamp) })
}

// SelectSamples implements the querier store: a metric query over the
// archive. Samples are materialised before the iterator is returned because
// stream-ordered output needs every partition; the query frontend's
// split-by-interval bounds how much one request holds.
func (s *Store) SelectSamples(ctx context.Context, req logql.SelectSampleParams) (iter.SampleIterator, error) {
	expr, err := req.Expr()
	if err != nil {
		return nil, err
	}
	extractor, err := expr.Extractor()
	if err != nil {
		return nil, err
	}
	selector, err := expr.Selector()
	if err != nil {
		return nil, err
	}
	matchers := selector.Matchers()

	parts, err := s.partitions(ctx, req.Start, req.End)
	if err != nil {
		return nil, err
	}
	level.Debug(s.logger).Log("msg", "archive sample query planned", "query", req.Selector, "from", req.Start.Format(time.RFC3339), "to", req.End.Format(time.RFC3339), "partitions", len(parts))

	st := stats.FromContext(ctx)
	series := map[string]*logproto.Series{}
	rl := newRowLabels(16)
	var hasher util.SampleHasher
	referencedMetadata := false
	for _, paths := range parts {
		err := s.readObjects(ctx, paths, func(rows []arrowflight.ArchiveRow) {
			for i := range rows {
				row := &rows[i]
				if !inRange(row.Timestamp, req.Start, req.End) {
					continue
				}
				st.AddDecompressedBytes(int64(len(row.Line)))
				lbls := rl.stream(row)
				if !rl.matches(matchers, lbls) {
					continue
				}
				se := extractor.ForStream(lbls)
				referencedMetadata = referencedMetadata || se.ReferencedStructuredMetadata()
				meta := rl.metadata(row)
				sample, ok := se.ProcessString(row.Timestamp.UnixNano(), row.Line, meta)
				if !ok {
					continue
				}
				st.AddPostFilterLines(1)
				key := sample.Labels.String()
				ser, ok := series[key]
				if !ok {
					ser = &logproto.Series{Labels: key, StreamHash: se.BaseLabels().Hash()}
					series[key] = ser
				}
				ser.Samples = append(ser.Samples, logproto.Sample{
					Timestamp: row.Timestamp.UnixNano(),
					Value:     sample.Value,
					Hash:      hasher.Hash(key, []byte(row.Line)),
				})
			}
		})
		if err != nil {
			return nil, err
		}
	}
	if referencedMetadata {
		st.SetQueryReferencedStructuredMetadata()
	}
	if len(series) == 0 {
		return iter.NoopSampleIterator, nil
	}
	out := make([]logproto.Series, 0, len(series))
	for _, ser := range series {
		sort.Slice(ser.Samples, func(i, j int) bool { return ser.Samples[i].Timestamp < ser.Samples[j].Timestamp })
		out = append(out, *ser)
	}
	switch req.Order {
	case logproto.SAMPLE_ORDER_BY_STREAM:
		sort.Slice(out, func(i, j int) bool { return out[i].Labels < out[j].Labels })
		return iter.NewStreamFirstMultiSeriesIterator(out), nil
	default:
		return iter.NewTimestampFirstMultiSeriesIterator(out), nil
	}
}

// sampleObjects bounds a metadata request to MaxMetadataObjects objects
// spread evenly over the range.
func (s *Store) sampleObjects(ctx context.Context, start, end time.Time) ([]string, error) {
	objects, err := s.src.ListObjects(ctx, start, end)
	if err != nil {
		return nil, err
	}
	maxN := s.cfg.MaxMetadataObjects
	if len(objects) <= maxN {
		return objects, nil
	}
	out := make([]string, 0, maxN)
	for i := 0; i < maxN; i++ {
		out = append(out, objects[i*len(objects)/maxN])
	}
	return out, nil
}

// streamsIn collects the distinct stream label sets of the sampled objects
// that match the matchers.
func (s *Store) streamsIn(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) ([]labels.Labels, error) {
	objects, err := s.sampleObjects(ctx, start, end)
	if err != nil {
		return nil, err
	}
	rl := newRowLabels(16)
	seen := map[uint64]labels.Labels{}
	err = s.readObjects(ctx, objects, func(rows []arrowflight.ArchiveRow) {
		for i := range rows {
			row := &rows[i]
			if !inRange(row.Timestamp, start, end) {
				continue
			}
			lbls := rl.stream(row)
			h := lbls.Hash()
			if _, ok := seen[h]; ok {
				continue
			}
			if !rl.matches(matchers, lbls) {
				continue
			}
			seen[h] = lbls.Copy()
		}
	})
	if err != nil {
		return nil, err
	}
	out := make([]labels.Labels, 0, len(seen))
	for _, l := range seen {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool { return labels.Compare(out[i], out[j]) < 0 })
	return out, nil
}

// SelectSeries implements the querier store.
func (s *Store) SelectSeries(ctx context.Context, req logql.SelectLogParams) ([]logproto.SeriesIdentifier, error) {
	var matchers []*labels.Matcher
	if req.Selector != "" {
		var expr syntax.LogSelectorExpr
		var err error
		if req.Plan != nil {
			expr, err = req.LogSelector()
		} else {
			expr, err = syntax.ParseLogSelector(req.Selector, true)
		}
		if err != nil {
			return nil, err
		}
		matchers = expr.Matchers()
	}
	streams, err := s.streamsIn(ctx, req.Start, req.End, matchers)
	if err != nil {
		return nil, err
	}
	out := make([]logproto.SeriesIdentifier, 0, len(streams))
	for _, l := range streams {
		out = append(out, logproto.SeriesIdentifierFromLabels(l))
	}
	return out, nil
}

// LabelNamesForMetricName implements the querier store. Without matchers the
// names come from the source's label discovery and cost no reads.
func (s *Store) LabelNamesForMetricName(ctx context.Context, _ string, from, through model.Time, _ string, matchers ...*labels.Matcher) ([]string, error) {
	if len(matchers) == 0 {
		return s.src.LabelNames(), nil
	}
	streams, err := s.streamsIn(ctx, from.Time(), through.Time(), matchers)
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	for _, l := range streams {
		l.Range(func(lb labels.Label) { seen[lb.Name] = struct{}{} })
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

// LabelValuesForMetricName implements the querier store.
func (s *Store) LabelValuesForMetricName(ctx context.Context, _ string, from, through model.Time, _ string, labelName string, matchers ...*labels.Matcher) ([]string, error) {
	streams, err := s.streamsIn(ctx, from.Time(), through.Time(), matchers)
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	for _, l := range streams {
		if v := l.Get(labelName); v != "" {
			seen[v] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out, nil
}

// Stats implements the querier store. The archive has no index, so the only
// cheap figure is the object count; bytes stay zero, which also keeps the
// frontend's dynamic sharding at one shard.
func (s *Store) Stats(ctx context.Context, _ string, from, through model.Time, _ ...*labels.Matcher) (*indexstats.Stats, error) {
	objects, err := s.src.ListObjects(ctx, from.Time(), through.Time())
	if err != nil {
		return nil, err
	}
	return &indexstats.Stats{Chunks: uint64(len(objects))}, nil
}

// Volume implements the querier store with an empty response: volume needs
// an index the archive does not have.
func (s *Store) Volume(_ context.Context, _ string, _, _ model.Time, limit int32, _ []string, _ string, _ ...*labels.Matcher) (*logproto.VolumeResponse, error) {
	return &logproto.VolumeResponse{Limit: limit}, nil
}

// GetShards implements the querier store.
func (s *Store) GetShards(_ context.Context, _ string, _, _ model.Time, _ uint64, _ chunk.Predicate) (*logproto.ShardsResponse, error) {
	return nil, ErrShardingNotSupported
}
