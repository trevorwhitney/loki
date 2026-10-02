package bench

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight"
	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight/scanpb"
	"github.com/grafana/loki/v3/pkg/logproto"
)

// TestArchiveStoreRoundTrip writes the same generated streams to a data
// object store and to the archive store, then scans both through the Flight
// sources and expects the same rows: the archive mapping (labels as resource
// attributes, metadata as log attributes, trace and span ids as record
// fields) round-trips through Loki's OTLP normalisation.
func TestArchiveStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	gen := NewGenerator(DefaultOpt().WithNumStreams(26).WithTimeSpread(2 * time.Hour))
	var batch *Batch
	for b := range gen.Batches() {
		batch = b
		break
	}
	var streams []logproto.Stream
	for _, s := range batch.Streams {
		if strings.Contains(s.Labels, `service_name="nginx"`) {
			streams = append(streams, s)
		}
	}
	require.NotEmpty(t, streams)

	dataobjStore, err := NewDataObjStore(dir, "t1")
	require.NoError(t, err)
	archiveStore, err := NewArchiveStore(dir, "t1")
	require.NoError(t, err)
	require.NoError(t, dataobjStore.Write(ctx, streams))
	require.NoError(t, archiveStore.Write(ctx, streams))
	require.NoError(t, dataobjStore.Close())
	require.NoError(t, archiveStore.Close())

	liveBucket, err := filesystem.NewBucket(filepath.Join(dir, storageDir, "dataobj"))
	require.NoError(t, err)
	live, err := arrowflight.OpenCatalog(ctx, liveBucket, "objects", log.NewNopLogger())
	require.NoError(t, err)

	archiveBucket, err := filesystem.NewBucket(filepath.Join(dir, ArchiveDir))
	require.NoError(t, err)
	archive, err := arrowflight.NewArchiveSource(ctx, arrowflight.ArchiveConfig{
		Bucket:          archiveBucket,
		Prefix:          "t1",
		MetadataColumns: []string{"trace_id", "span_id", "level", "detected_level", "resource_hostname"},
		Logger:          log.NewNopLogger(),
	})
	require.NoError(t, err)

	columns := []string{"timestamp", "message", "service_name", "cluster", "namespace", "pod", "env", "level", "detected_level", "trace_id", "span_id", "resource_hostname"}
	start := gen.config.StartTime
	window := []*scanpb.Predicate{
		{Column: "timestamp", Op: scanpb.Op_OP_GTE, Values: []*scanpb.Literal{{Value: &scanpb.Literal_TimestampNs{TimestampNs: start.UnixNano()}}}},
		{Column: "timestamp", Op: scanpb.Op_OP_LT, Values: []*scanpb.Literal{{Value: &scanpb.Literal_TimestampNs{TimestampNs: start.Add(3 * time.Hour).UnixNano()}}}},
	}

	liveRows := scanRows(t, live, &scanpb.ScanRequest{Table: arrowflight.TableLogs, Columns: columns, Predicates: window})
	archiveRows := scanRows(t, archive, &scanpb.ScanRequest{Table: arrowflight.TableArchiveLogs, Columns: columns, Predicates: window})
	require.NotEmpty(t, liveRows)
	require.Equal(t, len(liveRows), len(archiveRows))
	require.Equal(t, liveRows, archiveRows)

	// A metadata predicate prunes the same rows on both sides.
	filter := append(slices.Clone(window), &scanpb.Predicate{Column: "level", Op: scanpb.Op_OP_EQ, Values: []*scanpb.Literal{{Value: &scanpb.Literal_StringValue{StringValue: "error"}}}})
	liveErrors := scanRows(t, live, &scanpb.ScanRequest{Table: arrowflight.TableLogs, Columns: columns, Predicates: filter})
	archiveErrors := scanRows(t, archive, &scanpb.ScanRequest{Table: arrowflight.TableArchiveLogs, Columns: columns, Predicates: filter})
	require.NotEmpty(t, liveErrors)
	require.Less(t, len(liveErrors), len(liveRows))
	require.Equal(t, liveErrors, archiveErrors)
}

// scanRows plans and scans a request directly against a source and returns
// every row rendered as one string per column, sorted.
func scanRows(t *testing.T, src arrowflight.Source, req *scanpb.ScanRequest) []string {
	t.Helper()
	ctx := context.Background()
	tickets, err := src.Plan(ctx, req)
	require.NoError(t, err)

	var rows []string
	for _, tkt := range tickets {
		sc, err := src.Scan(ctx, tkt)
		require.NoError(t, err)
		for {
			rec, err := sc.Next(ctx)
			if err != nil {
				break
			}
			for i := 0; i < int(rec.NumRows()); i++ {
				var parts []string
				for c := 0; c < int(rec.NumCols()); c++ {
					parts = append(parts, renderValue(rec.Column(c), i))
				}
				rows = append(rows, strings.Join(parts, "|"))
			}
			rec.Release()
		}
		require.NoError(t, sc.Close())
	}
	sort.Strings(rows)
	return rows
}

func renderValue(col arrow.Array, i int) string {
	if col.IsNull(i) {
		return "<null>"
	}
	switch c := col.(type) {
	case *array.Timestamp:
		return fmt.Sprintf("%d", int64(c.Value(i)))
	case *array.String:
		return c.Value(i)
	default:
		return col.ValueStr(i)
	}
}
