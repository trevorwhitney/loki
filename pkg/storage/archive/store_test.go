package archive

import (
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/grafana/loki/v3/pkg/logqlmodel/stats"
)

var testBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// testEntry is one record of a synthetic archive object.
type testEntry struct {
	labels map[string]string
	ts     time.Time
	line   string
	meta   map[string]string
}

// writeArchiveObject writes one gzipped OTLP JSON object in the plain layout
// under dir/prefix, in the shape the archive ingester writes for converted
// push requests: every resource attribute is a stream label and record
// attributes are structured metadata.
func writeArchiveObject(t *testing.T, dir, prefix string, partition time.Time, name string, entries []testEntry) {
	t.Helper()
	ld := plog.NewLogs()
	byStream := map[string][]testEntry{}
	var order []string
	for _, e := range entries {
		key := fmt.Sprint(e.labels)
		if _, ok := byStream[key]; !ok {
			order = append(order, key)
		}
		byStream[key] = append(byStream[key], e)
	}
	for _, key := range order {
		es := byStream[key]
		rl := ld.ResourceLogs().AppendEmpty()
		for k, v := range es[0].labels {
			rl.Resource().Attributes().PutStr(k, v)
		}
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().Attributes().PutStr(arrowflight.ArchiveConvertedPushScopeAttribute, "true")
		for _, e := range es {
			lr := sl.LogRecords().AppendEmpty()
			lr.SetTimestamp(pcommon.NewTimestampFromTime(e.ts))
			lr.Body().SetStr(e.line)
			for k, v := range e.meta {
				lr.Attributes().PutStr(k, v)
			}
		}
	}
	data, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
	require.NoError(t, err)

	objDir := filepath.Join(dir, prefix, partition.Format("2006/01/02/15/04"))
	require.NoError(t, os.MkdirAll(objDir, 0o755))
	f, err := os.Create(filepath.Join(objDir, name+".json.gz"))
	require.NoError(t, err)
	gz := gzip.NewWriter(f)
	_, err = gz.Write(data)
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	require.NoError(t, f.Close())
}

// newTestStore writes six five-minute partitions with two streams each:
// {app="a", env="prod"} logfmt lines and {app="b", env="prod"} JSON lines,
// ten entries per stream per partition, ordered newest first inside the
// object to exercise sorting.
func newTestStore(t *testing.T, cfg Config) (*Store, int) {
	t.Helper()
	dir := t.TempDir()
	const prefix = "tenant"
	objects := 0
	for p := 0; p < 6; p++ {
		partition := testBase.Add(time.Duration(p) * 5 * time.Minute)
		var entries []testEntry
		for i := 9; i >= 0; i-- {
			ts := partition.Add(time.Duration(i) * 20 * time.Second)
			entries = append(entries, testEntry{
				labels: map[string]string{"app": "a", "env": "prod"},
				ts:     ts,
				line:   fmt.Sprintf(`level=info msg="hello world" n=%d partition=%d`, i, p),
				meta:   map[string]string{"trace_id": fmt.Sprintf("%02d%02d", p, i)},
			})
			entries = append(entries, testEntry{
				labels: map[string]string{"app": "b", "env": "prod"},
				ts:     ts.Add(time.Second),
				line:   fmt.Sprintf(`{"level":"%s","n":%d}`, map[bool]string{true: "error", false: "info"}[i%2 == 0], i),
			})
		}
		writeArchiveObject(t, dir, prefix, partition, fmt.Sprintf("obj-%d", p), entries)
		objects++
	}

	bucket, err := filesystem.NewBucket(dir)
	require.NoError(t, err)
	src, err := arrowflight.NewArchiveSource(context.Background(), arrowflight.ArchiveConfig{
		Bucket: bucket,
		Prefix: prefix,
		Layout: arrowflight.ArchiveLayoutPlain,
		Logger: log.NewNopLogger(),
	})
	require.NoError(t, err)
	return NewStore(src, cfg, log.NewNopLogger()), objects
}

func testContext(t *testing.T) (context.Context, *stats.Context) {
	t.Helper()
	ctx := user.InjectOrgID(context.Background(), "tenant")
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	t.Cleanup(cancel)
	_, ctx = stats.NewContext(ctx)
	return ctx, stats.FromContext(ctx)
}

func runQuery(t *testing.T, store *Store, ctx context.Context, qs string, start, end time.Time, step time.Duration, direction logproto.Direction, limit uint32) logqlmodel.Result {
	t.Helper()
	eng := logql.NewEngine(logql.EngineOpts{}, store, logql.NoLimits, log.NewNopLogger())
	params, err := logql.NewLiteralParams(qs, start, end, step, 0, direction, limit, nil, nil)
	require.NoError(t, err)
	res, err := eng.Query(params).Exec(ctx)
	require.NoError(t, err)
	return res
}

func flattenEntries(streams logqlmodel.Streams) []logproto.Entry {
	var out []logproto.Entry
	for _, s := range streams {
		out = append(out, s.Entries...)
	}
	return out
}

func TestSelectLogsOrderAndFilter(t *testing.T) {
	store, _ := newTestStore(t, Config{})
	start, end := testBase, testBase.Add(30*time.Minute)

	for _, direction := range []logproto.Direction{logproto.FORWARD, logproto.BACKWARD} {
		t.Run(direction.String(), func(t *testing.T) {
			ctx, _ := testContext(t)
			// Structured metadata is part of a result stream's identity, so
			// drop the per-entry trace id to get one stream whose order we
			// can check.
			res := runQuery(t, store, ctx, `{app="a"} |= "hello" | drop trace_id`, start, end, 0, direction, 1000)
			streams, ok := res.Data.(logqlmodel.Streams)
			require.True(t, ok)
			require.Len(t, streams, 1)
			require.Equal(t, `{app="a", env="prod"}`, streams[0].Labels)
			require.Len(t, streams[0].Entries, 60)

			var last time.Time
			for i, e := range streams[0].Entries {
				if i > 0 {
					if direction == logproto.FORWARD {
						require.True(t, e.Timestamp.After(last), "entry %d out of order", i)
					} else {
						require.True(t, e.Timestamp.Before(last), "entry %d out of order", i)
					}
				}
				last = e.Timestamp
			}
			require.Equal(t, int64(120), res.Statistics.Summary.TotalLinesProcessed)
			require.Equal(t, int64(6), res.Statistics.Querier.Store.TotalChunksDownloaded)

			// Without the drop, every entry carries its trace id as structured metadata.
			res = runQuery(t, store, ctx, `{app="a"}`, start, end, 0, direction, 1000)
			entries := flattenEntries(res.Data.(logqlmodel.Streams))
			require.Len(t, entries, 60)
			for _, e := range entries {
				require.Len(t, e.StructuredMetadata, 1)
				require.Equal(t, "trace_id", e.StructuredMetadata[0].Name)
			}
		})
	}
}

func TestSelectLogsLimitStopsDecodingEarly(t *testing.T) {
	store, objects := newTestStore(t, Config{PrefetchPartitions: 1})
	ctx, _ := testContext(t)
	res := runQuery(t, store, ctx, `{app="a"} | drop trace_id`, testBase, testBase.Add(30*time.Minute), 0, logproto.BACKWARD, 5)
	streams := res.Data.(logqlmodel.Streams)
	require.Len(t, streams, 1)
	require.Len(t, streams[0].Entries, 5)
	require.Equal(t, testBase.Add(25*time.Minute).Add(180*time.Second), streams[0].Entries[0].Timestamp)
	// One partition answers the limit; at most the prefetched neighbours are read too.
	downloaded := res.Statistics.Querier.Store.TotalChunksDownloaded
	require.Less(t, downloaded, int64(objects))
	require.GreaterOrEqual(t, downloaded, int64(1))
}

func TestSelectLogsParsers(t *testing.T) {
	store, _ := newTestStore(t, Config{})
	start, end := testBase, testBase.Add(30*time.Minute)

	ctx, _ := testContext(t)
	res := runQuery(t, store, ctx, `{app="b"} | json | level="error"`, start, end, 0, logproto.FORWARD, 1000)
	streams := res.Data.(logqlmodel.Streams)
	require.Len(t, flattenEntries(streams), 30)
	for _, s := range streams {
		require.Contains(t, s.Labels, `level="error"`)
		require.Contains(t, s.Labels, `n="`)
	}

	res = runQuery(t, store, ctx, `{app="a"} | logfmt | n >= 8`, start, end, 0, logproto.FORWARD, 1000)
	require.Len(t, flattenEntries(res.Data.(logqlmodel.Streams)), 12)
}

func TestSelectSamples(t *testing.T) {
	store, _ := newTestStore(t, Config{})
	ctx, _ := testContext(t)
	start, end := testBase, testBase.Add(25*time.Minute)

	res := runQuery(t, store, ctx, `sum by (app) (count_over_time({env="prod"}[5m]))`, start, end, 5*time.Minute, logproto.FORWARD, 0)
	matrix, ok := res.Data.(promql.Matrix)
	require.True(t, ok)
	require.Len(t, matrix, 2)
	sort.Slice(matrix, func(i, j int) bool { return labels.Compare(matrix[i].Metric, matrix[j].Metric) < 0 })
	// app="a" has an entry at exactly start, so its first window (start-5m, start] holds one entry;
	// app="b" entries are one second later and its first point is the second step.
	require.Equal(t, "a", matrix[0].Metric.Get("app"))
	require.Len(t, matrix[0].Floats, 6)
	require.Equal(t, float64(1), matrix[0].Floats[0].F)
	for _, p := range matrix[0].Floats[1:] {
		require.Equal(t, float64(10), p.F)
	}
	require.Equal(t, "b", matrix[1].Metric.Get("app"))
	require.Len(t, matrix[1].Floats, 5)
	for _, p := range matrix[1].Floats {
		require.Equal(t, float64(10), p.F)
	}
	require.Equal(t, int64(120), res.Statistics.Summary.TotalLinesProcessed)

	res = runQuery(t, store, ctx, `sum(rate({app="b"} | json | level="error" [10m]))`, end, end, 0, logproto.FORWARD, 0)
	vector, ok := res.Data.(promql.Vector)
	require.True(t, ok)
	require.Len(t, vector, 1)
	require.InDelta(t, float64(10)/600, vector[0].F, 1e-9)
}

func TestSeriesAndLabels(t *testing.T) {
	store, _ := newTestStore(t, Config{MaxMetadataObjects: 2})
	ctx, _ := testContext(t)
	start, end := testBase, testBase.Add(30*time.Minute)

	series, err := store.SelectSeries(ctx, logql.SelectLogParams{QueryRequest: &logproto.QueryRequest{
		Selector: `{env="prod"}`, Start: start, End: end, Limit: 1000,
	}})
	require.NoError(t, err)
	require.Len(t, series, 2)

	names, err := store.LabelNamesForMetricName(ctx, "tenant", 0, 0, "logs")
	require.NoError(t, err)
	require.Subset(t, names, []string{"app", "env"})

	values, err := store.LabelValuesForMetricName(ctx, "tenant", modelTime(start), modelTime(end), "logs", "app")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, values)

	values, err = store.LabelValuesForMetricName(ctx, "tenant", modelTime(start), modelTime(end), "logs", "app", labels.MustNewMatcher(labels.MatchEqual, "app", "b"))
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, values)
}

// TestDevArchiveSlice runs a query over a copied archive slice when one is
// present: pkg/logql/bench/archive-dev/<tenant>/YYYY/MM/DD/HH/mm/*.json.gz
// (not committed). The first directory under archive-dev is the tenant.
func TestDevArchiveSlice(t *testing.T) {
	dir := filepath.Join("..", "..", "logql", "bench", "archive-dev")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Skip("dev archive slice not present")
	}
	var tenant string
	for _, e := range entries {
		if e.IsDir() {
			tenant = e.Name()
			break
		}
	}
	if tenant == "" {
		t.Skip("dev archive slice holds no tenant directory")
	}
	bucket, err := filesystem.NewBucket(dir)
	require.NoError(t, err)
	src, err := arrowflight.NewArchiveSource(context.Background(), arrowflight.ArchiveConfig{
		Bucket: bucket, Prefix: tenant, Layout: arrowflight.ArchiveLayoutPlain, Logger: log.NewNopLogger(),
	})
	require.NoError(t, err)
	store := NewStore(src, Config{}, log.NewNopLogger())
	ctx, _ := testContext(t)

	start := time.Date(2026, 2, 17, 6, 50, 0, 0, time.UTC)
	end := start.Add(10 * time.Minute)
	res := runQuery(t, store, ctx, `sum(count_over_time({service_name=~".+"}[10m]))`, end, end, 0, logproto.FORWARD, 0)
	vector := res.Data.(promql.Vector)
	require.Len(t, vector, 1)
	require.Greater(t, vector[0].F, float64(0))
	t.Logf("dev slice: %.0f lines in 10m, %d objects, %d lines processed", vector[0].F, res.Statistics.Querier.Store.TotalChunksDownloaded, res.Statistics.Summary.TotalLinesProcessed)

	res = runQuery(t, store, ctx, `{service_name=~".+"}`, start, end, 0, logproto.BACKWARD, 10)
	streams := res.Data.(logqlmodel.Streams)
	total := 0
	for _, s := range streams {
		total += len(s.Entries)
	}
	require.Equal(t, 10, total)
}

func modelTime(t time.Time) model.Time { return model.TimeFromUnixNano(t.UnixNano()) }
