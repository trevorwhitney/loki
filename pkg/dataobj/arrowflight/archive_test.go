package arrowflight_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight"
	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight/scanpb"
)

var archiveT0 = time.Date(2026, 2, 17, 6, 55, 0, 0, time.UTC)

// writeArchiveObject writes ld as the archive ingester would: gzipped OTLP
// JSON under <root>/<tenant>/YYYY/MM/DD/HH/mm/<name>.json.gz.
func writeArchiveObject(t *testing.T, root, tenant string, bucket time.Time, name string, ld plog.Logs) {
	t.Helper()
	data, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
	require.NoError(t, err)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err = gz.Write(data)
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	dir := filepath.Join(root, tenant, bucket.Format("2006/01/02/15/04"))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+".json.gz"), buf.Bytes(), 0o644))
}

// buildArchiveFixture writes one converted push-request object and one
// native OTLP object into two five-minute partitions.
func buildArchiveFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	converted := plog.NewLogs()
	rl := converted.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("cluster", "c1")
	rl.Resource().Attributes().PutStr("service_name", "nginx")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().Attributes().PutStr(arrowflight.ArchiveConvertedPushScopeAttribute, "true")
	for i, level := range []string{"error", "info", "error"} {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(archiveT0.Add(time.Duration(i) * time.Minute)))
		lr.Body().SetStr("GET / 200 " + level)
		lr.Attributes().PutStr("level", level)
		lr.SetTraceID(pcommon.TraceID([16]byte{byte(i + 1)}))
	}
	writeArchiveObject(t, root, "t1", archiveT0, "01-converted", converted)

	native := plog.NewLogs()
	rl = native.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "adservice")
	rl.Resource().Attributes().PutStr("k8s.namespace.name", "demo")
	rl.Resource().Attributes().PutStr("host.name", "node-7")
	sl = rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(archiveT0.Add(7 * time.Minute)))
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(archiveT0.Add(8 * time.Minute)))
	lr.SetSeverityText("ERROR")
	lr.SetSeverityNumber(plog.SeverityNumberError)
	lr.Body().SetStr("ad lookup failed")
	lr.Attributes().PutStr("http.status", "500")
	writeArchiveObject(t, root, "t1", archiveT0.Add(5*time.Minute), "02-native", native)

	return root
}

func startArchiveServer(t *testing.T, root, tenant string) flight.Client {
	t.Helper()
	bucket, err := filesystem.NewBucket(root)
	require.NoError(t, err)
	src, err := arrowflight.NewArchiveSource(context.Background(), arrowflight.ArchiveConfig{
		Bucket: bucket,
		Prefix: tenant,
		Logger: log.NewNopLogger(),
	})
	require.NoError(t, err)

	srv := flight.NewServerWithMiddleware(nil)
	require.NoError(t, srv.Init("127.0.0.1:0"))
	srv.RegisterFlightService(arrowflight.NewServer(log.NewNopLogger(), src))
	go func() { _ = srv.Serve() }()
	t.Cleanup(srv.Shutdown)

	client, err := flight.NewClientWithMiddleware(srv.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestArchive_Schema(t *testing.T) {
	client := startArchiveServer(t, buildArchiveFixture(t), "t1")
	res, err := client.GetSchema(context.Background(), &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{arrowflight.TableArchiveLogs}})
	require.NoError(t, err)
	schema, err := flight.DeserializeSchema(res.Schema, nil)
	require.NoError(t, err)

	var names []string
	for _, f := range schema.Fields() {
		names = append(names, f.Name)
	}
	// Envelope, discovered converted-push labels, default promoted labels
	// (normalised), promoted metadata keys.
	for _, want := range []string{"timestamp", "message", "observed_timestamp", "resource_attributes", "log_attributes",
		"cluster", "service_name", "k8s_namespace_name", "trace_id", "severity_text", "severity_number"} {
		require.Contains(t, names, want)
	}
	require.NotContains(t, names, "level", "non-promoted log attributes live in log_attributes")
	require.NotContains(t, names, "host_name", "non-promoted resource attributes live in resource_attributes")
}

func TestArchive_RequiresTimeRange(t *testing.T) {
	client := startArchiveServer(t, buildArchiveFixture(t), "t1")
	cmd, err := proto.Marshal(&scanpb.ScanRequest{Table: arrowflight.TableArchiveLogs, Columns: []string{"message"}})
	require.NoError(t, err)
	_, err = client.GetFlightInfo(context.Background(), &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: cmd})
	require.ErrorContains(t, err, "requires a lower timestamp bound")
}

func TestArchive_Scan(t *testing.T) {
	client := startArchiveServer(t, buildArchiveFixture(t), "t1")

	window := []*scanpb.Predicate{
		{Column: "timestamp", Op: scanpb.Op_OP_GTE, Values: []*scanpb.Literal{ts(archiveT0)}},
		{Column: "timestamp", Op: scanpb.Op_OP_LT, Values: []*scanpb.Literal{ts(archiveT0.Add(10 * time.Minute))}},
	}

	t.Run("both objects, labels and metadata mapped like Loki ingest", func(t *testing.T) {
		schema, recs := scan(t, client, &scanpb.ScanRequest{
			Table:      arrowflight.TableArchiveLogs,
			Columns:    []string{"timestamp", "message", "service_name", "cluster", "k8s_namespace_name", "trace_id", "severity_text", "log_attributes", "resource_attributes"},
			Predicates: window,
		})
		require.EqualValues(t, 4, totalRows(recs))
		require.Equal(t, map[string]int{"nginx": 3, "adservice": 1}, count(stringColumn(t, schema, recs, "service_name")))
		require.Equal(t, map[string]int{"c1": 3, "<null>": 1}, count(stringColumn(t, schema, recs, "cluster")))
		require.Equal(t, map[string]int{"demo": 1, "<null>": 3}, count(stringColumn(t, schema, recs, "k8s_namespace_name")))
		require.Equal(t, map[string]int{"ERROR": 1, "<null>": 3}, count(stringColumn(t, schema, recs, "severity_text")))

		traceIDs := stringColumn(t, schema, recs, "trace_id")
		require.Contains(t, traceIDs, "01000000000000000000000000000000")

		// Non-promoted attributes land in the maps, with Loki's key normalisation.
		var logAttrs, resAttrs []string
		for _, rec := range recs {
			la := rec.Column(schema.FieldIndices("log_attributes")[0])
			ra := rec.Column(schema.FieldIndices("resource_attributes")[0])
			for i := 0; i < int(rec.NumRows()); i++ {
				logAttrs = append(logAttrs, la.ValueStr(i))
				resAttrs = append(resAttrs, ra.ValueStr(i))
			}
		}
		require.True(t, containsAny(logAttrs, "level", "error"), "log_attributes should carry level: %v", logAttrs)
		require.True(t, containsAny(logAttrs, "http_status", "500"), "log_attributes should carry http_status: %v", logAttrs)
		require.True(t, containsAny(resAttrs, "host_name", "node-7"), "resource_attributes should carry host_name: %v", resAttrs)
	})

	t.Run("time range selects partitions and rows", func(t *testing.T) {
		_, recs := scan(t, client, &scanpb.ScanRequest{
			Table:   arrowflight.TableArchiveLogs,
			Columns: []string{"timestamp"},
			Predicates: []*scanpb.Predicate{
				{Column: "timestamp", Op: scanpb.Op_OP_GTE, Values: []*scanpb.Literal{ts(archiveT0.Add(1 * time.Minute))}},
				{Column: "timestamp", Op: scanpb.Op_OP_LTE, Values: []*scanpb.Literal{ts(archiveT0.Add(4 * time.Minute))}},
			},
		})
		require.EqualValues(t, 2, totalRows(recs), "rows at +1m and +2m; the native object's partition is outside the range")
	})

	t.Run("label and metadata predicates", func(t *testing.T) {
		schema, recs := scan(t, client, &scanpb.ScanRequest{
			Table:   arrowflight.TableArchiveLogs,
			Columns: []string{"message", "service_name"},
			Predicates: append(window,
				&scanpb.Predicate{Column: "service_name", Op: scanpb.Op_OP_IN, Values: []*scanpb.Literal{str("nginx"), str("missing")}},
				&scanpb.Predicate{Column: "trace_id", Op: scanpb.Op_OP_EQ, Values: []*scanpb.Literal{str("03000000000000000000000000000000")}},
			),
		})
		require.EqualValues(t, 1, totalRows(recs))
		require.Equal(t, []string{"GET / 200 error"}, stringColumn(t, schema, recs, "message"))
	})
}

func containsAny(values []string, key, value string) bool {
	for _, v := range values {
		if strings.Contains(v, key) && strings.Contains(v, value) {
			return true
		}
	}
	return false
}
