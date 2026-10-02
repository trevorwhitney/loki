package bench

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight"
	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight/scanpb"
	"github.com/grafana/loki/v3/pkg/dataobj/metastore"
	"github.com/grafana/loki/v3/pkg/logproto"
)

// TestMetastoreCatalog checks that the index-driven catalog (what the Loki
// dataobj-flight target uses) plans through the metastore the bench data
// object store writes, and returns the same rows as the walking catalog.
func TestMetastoreCatalog(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	gen := NewGenerator(DefaultOpt().WithNumStreams(26).WithTimeSpread(2 * time.Hour))
	var streams []logproto.Stream
	for b := range gen.Batches() {
		streams = b.Streams
		break
	}
	store, err := NewDataObjStore(dir, "t1")
	require.NoError(t, err)
	require.NoError(t, store.Write(ctx, streams))
	require.NoError(t, store.Close())

	bucket, err := filesystem.NewBucket(filepath.Join(dir, storageDir, "dataobj"))
	require.NoError(t, err)
	logger := log.NewNopLogger()

	walking, err := arrowflight.OpenCatalog(ctx, bucket, "objects", logger)
	require.NoError(t, err)

	ms := metastore.NewObjectMetastore(bucket, metastore.Config{IndexStoragePrefix: "index/v0"}, logger, metastore.NewObjectMetastoreMetrics(prometheus.NewRegistry()))
	// Schema discovery looks back from now; the generated data is dated
	// 2024, so the window must reach it.
	window := time.Since(gen.config.StartTime) + 48*time.Hour
	indexed, err := arrowflight.NewMetastoreCatalog(ctx, arrowflight.MetastoreCatalogConfig{
		Bucket:       bucket,
		Metastore:    ms,
		Tenant:       "t1",
		SchemaWindow: window,
		Logger:       logger,
	})
	require.NoError(t, err)

	// Same label columns discovered through the index as by walking.
	ws, _ := walking.Table(arrowflight.TableLogs)
	is, _ := indexed.Table(arrowflight.TableLogs)
	require.Equal(t, ws.LabelColumns(), is.LabelColumns())

	start := gen.config.StartTime
	columns := []string{"timestamp", "message", "service_name", "pod", "level", "trace_id"}
	window2 := []*scanpb.Predicate{
		{Column: "timestamp", Op: scanpb.Op_OP_GTE, Values: []*scanpb.Literal{{Value: &scanpb.Literal_TimestampNs{TimestampNs: start.UnixNano()}}}},
		{Column: "timestamp", Op: scanpb.Op_OP_LT, Values: []*scanpb.Literal{{Value: &scanpb.Literal_TimestampNs{TimestampNs: start.Add(3 * time.Hour).UnixNano()}}}},
	}

	t.Run("time range only", func(t *testing.T) {
		a := scanRows(t, walking, &scanpb.ScanRequest{Table: arrowflight.TableLogs, Columns: columns, Predicates: window2})
		b := scanRows(t, indexed, &scanpb.ScanRequest{Table: arrowflight.TableLogs, Columns: columns, Predicates: window2})
		require.NotEmpty(t, a)
		require.Equal(t, a, b)
	})

	t.Run("label predicate resolved to stream ids by the metastore", func(t *testing.T) {
		preds := append(window2, &scanpb.Predicate{Column: "service_name", Op: scanpb.Op_OP_IN, Values: []*scanpb.Literal{
			{Value: &scanpb.Literal_StringValue{StringValue: "nginx"}},
			{Value: &scanpb.Literal_StringValue{StringValue: "kafka"}},
		}})
		a := scanRows(t, walking, &scanpb.ScanRequest{Table: arrowflight.TableLogs, Columns: columns, Predicates: preds})
		b := scanRows(t, indexed, &scanpb.ScanRequest{Table: arrowflight.TableLogs, Columns: columns, Predicates: preds})
		require.NotEmpty(t, a)
		require.Equal(t, a, b)

		tickets, err := indexed.Plan(ctx, &scanpb.ScanRequest{Table: arrowflight.TableLogs, Columns: columns, Predicates: preds})
		require.NoError(t, err)
		require.NotEmpty(t, tickets)
		require.NotEmpty(t, tickets[0].StreamIds, "metastore should resolve the label predicate to stream ids")
	})

	t.Run("streams table", func(t *testing.T) {
		cols := []string{"stream_id", "service_name", "rows"}
		preds := []*scanpb.Predicate{
			{Column: "min_timestamp", Op: scanpb.Op_OP_GTE, Values: []*scanpb.Literal{{Value: &scanpb.Literal_TimestampNs{TimestampNs: start.UnixNano()}}}},
			{Column: "min_timestamp", Op: scanpb.Op_OP_LT, Values: []*scanpb.Literal{{Value: &scanpb.Literal_TimestampNs{TimestampNs: start.Add(3 * time.Hour).UnixNano()}}}},
		}
		tickets, err := indexed.Plan(ctx, &scanpb.ScanRequest{Table: arrowflight.TableStreams, Columns: cols, Predicates: preds})
		require.NoError(t, err)
		require.NotEmpty(t, tickets, "streams scan should plan one ticket per object")
		a := scanRows(t, walking, &scanpb.ScanRequest{Table: arrowflight.TableStreams, Columns: cols})
		b := scanRows(t, indexed, &scanpb.ScanRequest{Table: arrowflight.TableStreams, Columns: cols, Predicates: preds})
		require.NotEmpty(t, a)
		require.Equal(t, a, b)
	})

	t.Run("time range is mandatory", func(t *testing.T) {
		_, err := indexed.Plan(ctx, &scanpb.ScanRequest{Table: arrowflight.TableLogs, Columns: columns})
		require.ErrorContains(t, err, "requires a lower timestamp bound")
	})
}
