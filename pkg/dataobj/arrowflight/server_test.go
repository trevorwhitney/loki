package arrowflight_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight"
	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight/scanpb"
	"github.com/grafana/loki/v3/pkg/dataobj/logsobj"
	"github.com/grafana/loki/v3/pkg/logproto"
)

var testBase = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// Test fixture: 18 log lines across 3 streams.
//
//	{app="nginx", env="prod"}    10 lines, level alternates info/error
//	{app="postgres", env="prod"}  5 lines, db_system="postgres"
//	{app="redis"}                 3 lines, no structured metadata
func buildTestBucket(t *testing.T) objstore.Bucket {
	t.Helper()
	ctx := context.Background()

	builder, err := logsobj.NewBuilder(logsobj.BuilderBaseConfig{
		TargetPageSize:          2048,
		TargetObjectSize:        1 << 20,
		TargetSectionSize:       8 << 10,
		BufferSize:              2048 * 8,
		SectionStripeMergeLimit: 2,
	}, nil, logsobj.NewBuilderMetrics(), log.NewNopLogger(), nil)
	require.NoError(t, err)

	var nginx logproto.Stream
	nginx.Labels = `{app="nginx", env="prod"}`
	for i := range 10 {
		level := "info"
		if i%2 == 1 {
			level = "error"
		}
		nginx.Entries = append(nginx.Entries, logproto.Entry{
			Timestamp:          testBase.Add(time.Duration(i) * time.Second),
			Line:               "nginx request",
			StructuredMetadata: []logproto.LabelAdapter{{Name: "level", Value: level}},
		})
	}

	var postgres logproto.Stream
	postgres.Labels = `{app="postgres", env="prod"}`
	for i := range 5 {
		postgres.Entries = append(postgres.Entries, logproto.Entry{
			Timestamp:          testBase.Add(time.Duration(i) * time.Second),
			Line:               "postgres query",
			StructuredMetadata: []logproto.LabelAdapter{{Name: "db_system", Value: "postgres"}},
		})
	}

	var redis logproto.Stream
	redis.Labels = `{app="redis"}`
	for i := range 3 {
		redis.Entries = append(redis.Entries, logproto.Entry{
			Timestamp: testBase.Add(time.Duration(i) * time.Second),
			Line:      "redis command",
		})
	}

	for _, s := range []logproto.Stream{nginx, postgres, redis} {
		require.NoError(t, builder.Append("tenant", s, time.Now()))
	}

	obj, closer, err := builder.Flush()
	require.NoError(t, err)
	defer closer.Close()

	rc, err := obj.Reader(ctx)
	require.NoError(t, err)
	defer rc.Close()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)

	bucket, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, bucket.Upload(ctx, "objects/ab/test-object", bytes.NewReader(data)))
	return bucket
}

func startServer(t *testing.T) flight.Client {
	t.Helper()
	ctx := context.Background()

	catalog, err := arrowflight.OpenCatalog(ctx, buildTestBucket(t), "objects", log.NewNopLogger())
	require.NoError(t, err)

	srv := flight.NewServerWithMiddleware(nil)
	require.NoError(t, srv.Init("127.0.0.1:0"))
	srv.RegisterFlightService(arrowflight.NewServer(log.NewNopLogger(), catalog))
	go func() { _ = srv.Serve() }()
	t.Cleanup(srv.Shutdown)

	client, err := flight.NewClientWithMiddleware(srv.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// scan plans req with GetFlightInfo and reads every endpoint with DoGet.
func scan(t *testing.T, client flight.Client, req *scanpb.ScanRequest) (*arrow.Schema, []arrow.RecordBatch) {
	t.Helper()
	ctx := context.Background()

	cmd, err := proto.Marshal(req)
	require.NoError(t, err)

	info, err := client.GetFlightInfo(ctx, &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: cmd})
	require.NoError(t, err)
	require.NotEmpty(t, info.Endpoint)

	schema, err := flight.DeserializeSchema(info.Schema, memory.DefaultAllocator)
	require.NoError(t, err)

	var recs []arrow.RecordBatch
	for _, ep := range info.Endpoint {
		stream, err := client.DoGet(ctx, ep.Ticket)
		require.NoError(t, err)

		rdr, err := flight.NewRecordReader(stream)
		require.NoError(t, err)
		require.True(t, schema.Equal(rdr.Schema()), "endpoint schema %s differs from planned schema %s", rdr.Schema(), schema)
		for rdr.Next() {
			rec := rdr.RecordBatch()
			rec.Retain()
			recs = append(recs, rec)
		}
		require.NoError(t, rdr.Err())
		rdr.Release()
	}
	t.Cleanup(func() {
		for _, rec := range recs {
			rec.Release()
		}
	})
	return schema, recs
}

func totalRows(recs []arrow.RecordBatch) int64 {
	var n int64
	for _, rec := range recs {
		n += rec.NumRows()
	}
	return n
}

// stringColumn concatenates the named string column across batches. Nulls are
// returned as "<null>".
func stringColumn(t *testing.T, schema *arrow.Schema, recs []arrow.RecordBatch, name string) []string {
	t.Helper()
	idx := schema.FieldIndices(name)
	require.Len(t, idx, 1, "column %s", name)

	var out []string
	for _, rec := range recs {
		col, ok := rec.Column(idx[0]).(*array.String)
		require.True(t, ok, "column %s is %T, want *array.String", name, rec.Column(idx[0]))
		for i := range col.Len() {
			if col.IsNull(i) {
				out = append(out, "<null>")
			} else {
				out = append(out, col.Value(i))
			}
		}
	}
	return out
}

func count(values []string) map[string]int {
	m := make(map[string]int)
	for _, v := range values {
		m[v]++
	}
	return m
}

func str(s string) *scanpb.Literal {
	return &scanpb.Literal{Value: &scanpb.Literal_StringValue{StringValue: s}}
}

func ts(t time.Time) *scanpb.Literal {
	return &scanpb.Literal{Value: &scanpb.Literal_TimestampNs{TimestampNs: t.UnixNano()}}
}

func TestServer_GetSchema(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	logsRes, err := client.GetSchema(ctx, &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{"logs"}})
	require.NoError(t, err)
	logsSchema, err := flight.DeserializeSchema(logsRes.Schema, memory.DefaultAllocator)
	require.NoError(t, err)

	var names []string
	for _, f := range logsSchema.Fields() {
		names = append(names, f.Name)
	}
	require.Equal(t, []string{"stream_id", "timestamp", "message", "app", "env", "db_system", "level"}, names)
	require.True(t, arrow.TypeEqual(arrow.FixedWidthTypes.Timestamp_ns, logsSchema.Field(1).Type))

	streamsRes, err := client.GetSchema(ctx, &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{"streams"}})
	require.NoError(t, err)
	streamsSchema, err := flight.DeserializeSchema(streamsRes.Schema, memory.DefaultAllocator)
	require.NoError(t, err)

	names = nil
	for _, f := range streamsSchema.Fields() {
		names = append(names, f.Name)
	}
	require.Equal(t, []string{"stream_id", "min_timestamp", "max_timestamp", "rows", "uncompressed_size", "app", "env"}, names)

	_, err = client.GetSchema(ctx, &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{"nope"}})
	require.Error(t, err)
}

func TestServer_ScanLogs(t *testing.T) {
	client := startServer(t)

	t.Run("all columns joins labels into every row", func(t *testing.T) {
		schema, recs := scan(t, client, &scanpb.ScanRequest{Table: "logs"})
		require.Equal(t, int64(18), totalRows(recs))
		require.Equal(t, map[string]int{"nginx": 10, "postgres": 5, "redis": 3}, count(stringColumn(t, schema, recs, "app")))
		require.Equal(t, map[string]int{"prod": 15, "<null>": 3}, count(stringColumn(t, schema, recs, "env")))
		require.Equal(t, map[string]int{"info": 5, "error": 5, "<null>": 8}, count(stringColumn(t, schema, recs, "level")))
	})

	t.Run("projection preserves requested order", func(t *testing.T) {
		schema, recs := scan(t, client, &scanpb.ScanRequest{Table: "logs", Columns: []string{"level", "message", "app"}})
		require.Equal(t, 3, schema.NumFields())
		require.Equal(t, "level", schema.Field(0).Name)
		require.Equal(t, "message", schema.Field(1).Name)
		require.Equal(t, "app", schema.Field(2).Name)
		require.Equal(t, int64(18), totalRows(recs))
	})

	t.Run("label-only projection still returns every row", func(t *testing.T) {
		schema, recs := scan(t, client, &scanpb.ScanRequest{Table: "logs", Columns: []string{"app"}})
		require.Equal(t, int64(18), totalRows(recs))
		require.Equal(t, map[string]int{"nginx": 10, "postgres": 5, "redis": 3}, count(stringColumn(t, schema, recs, "app")))
	})

	t.Run("label equality prunes to matching streams", func(t *testing.T) {
		schema, recs := scan(t, client, &scanpb.ScanRequest{
			Table:      "logs",
			Columns:    []string{"app", "message"},
			Predicates: []*scanpb.Predicate{{Column: "app", Op: scanpb.Op_OP_EQ, Values: []*scanpb.Literal{str("nginx")}}},
		})
		require.Equal(t, int64(10), totalRows(recs))
		require.Equal(t, map[string]int{"nginx": 10}, count(stringColumn(t, schema, recs, "app")))
	})

	t.Run("label IN prunes to matching streams", func(t *testing.T) {
		schema, recs := scan(t, client, &scanpb.ScanRequest{
			Table:      "logs",
			Columns:    []string{"app"},
			Predicates: []*scanpb.Predicate{{Column: "app", Op: scanpb.Op_OP_IN, Values: []*scanpb.Literal{str("postgres"), str("redis")}}},
		})
		require.Equal(t, int64(8), totalRows(recs))
		require.Equal(t, map[string]int{"postgres": 5, "redis": 3}, count(stringColumn(t, schema, recs, "app")))
	})

	t.Run("metadata equality filters rows", func(t *testing.T) {
		schema, recs := scan(t, client, &scanpb.ScanRequest{
			Table:      "logs",
			Columns:    []string{"app", "level"},
			Predicates: []*scanpb.Predicate{{Column: "level", Op: scanpb.Op_OP_EQ, Values: []*scanpb.Literal{str("error")}}},
		})
		require.Equal(t, int64(5), totalRows(recs))
		require.Equal(t, map[string]int{"error": 5}, count(stringColumn(t, schema, recs, "level")))
		require.Equal(t, map[string]int{"nginx": 5}, count(stringColumn(t, schema, recs, "app")))
	})

	t.Run("timestamp range filters rows", func(t *testing.T) {
		_, recs := scan(t, client, &scanpb.ScanRequest{
			Table:   "logs",
			Columns: []string{"timestamp"},
			Predicates: []*scanpb.Predicate{
				{Column: "timestamp", Op: scanpb.Op_OP_GTE, Values: []*scanpb.Literal{ts(testBase.Add(2 * time.Second))}},
				{Column: "timestamp", Op: scanpb.Op_OP_LT, Values: []*scanpb.Literal{ts(testBase.Add(4 * time.Second))}},
			},
		})
		// Seconds 2 and 3 of every stream: nginx 2, postgres 2, redis 1.
		require.Equal(t, int64(5), totalRows(recs))
	})

	t.Run("combined label and metadata predicates", func(t *testing.T) {
		_, recs := scan(t, client, &scanpb.ScanRequest{
			Table:   "logs",
			Columns: []string{"message"},
			Predicates: []*scanpb.Predicate{
				{Column: "env", Op: scanpb.Op_OP_EQ, Values: []*scanpb.Literal{str("prod")}},
				{Column: "level", Op: scanpb.Op_OP_EQ, Values: []*scanpb.Literal{str("info")}},
			},
		})
		require.Equal(t, int64(5), totalRows(recs))
	})

	t.Run("unmatched label yields an empty stream with the right schema", func(t *testing.T) {
		schema, recs := scan(t, client, &scanpb.ScanRequest{
			Table:      "logs",
			Columns:    []string{"app", "message"},
			Predicates: []*scanpb.Predicate{{Column: "app", Op: scanpb.Op_OP_EQ, Values: []*scanpb.Literal{str("missing")}}},
		})
		require.Equal(t, int64(0), totalRows(recs))
		require.Equal(t, 2, schema.NumFields())
	})

	t.Run("unknown column is rejected", func(t *testing.T) {
		cmd, err := proto.Marshal(&scanpb.ScanRequest{Table: "logs", Columns: []string{"nope"}})
		require.NoError(t, err)
		_, err = client.GetFlightInfo(context.Background(), &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: cmd})
		require.Error(t, err)
	})
}

func TestServer_ScanStreams(t *testing.T) {
	client := startServer(t)

	t.Run("all streams", func(t *testing.T) {
		schema, recs := scan(t, client, &scanpb.ScanRequest{Table: "streams"})
		require.Equal(t, int64(3), totalRows(recs))
		require.Equal(t, map[string]int{"nginx": 1, "postgres": 1, "redis": 1}, count(stringColumn(t, schema, recs, "app")))

		var rows int64
		idx := schema.FieldIndices("rows")[0]
		for _, rec := range recs {
			col := rec.Column(idx).(*array.Int64)
			for i := range col.Len() {
				rows += col.Value(i)
			}
		}
		require.Equal(t, int64(18), rows)
	})

	t.Run("label IN filters streams", func(t *testing.T) {
		schema, recs := scan(t, client, &scanpb.ScanRequest{
			Table:      "streams",
			Columns:    []string{"app", "env"},
			Predicates: []*scanpb.Predicate{{Column: "app", Op: scanpb.Op_OP_IN, Values: []*scanpb.Literal{str("nginx"), str("redis")}}},
		})
		require.Equal(t, int64(2), totalRows(recs))
		require.Equal(t, map[string]int{"nginx": 1, "redis": 1}, count(stringColumn(t, schema, recs, "app")))
	})

	t.Run("fixed column comparison filters streams", func(t *testing.T) {
		schema, recs := scan(t, client, &scanpb.ScanRequest{
			Table:      "streams",
			Columns:    []string{"app"},
			Predicates: []*scanpb.Predicate{{Column: "rows", Op: scanpb.Op_OP_GT, Values: []*scanpb.Literal{{Value: &scanpb.Literal_Int64Value{Int64Value: 4}}}}},
		})
		require.Equal(t, int64(2), totalRows(recs))
		require.Equal(t, map[string]int{"nginx": 1, "postgres": 1}, count(stringColumn(t, schema, recs, "app")))
	})
}
