package bench

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
)

var (
	translateStart  = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	translateEnd    = translateStart.Add(24 * time.Hour)
	translateSchema = arrowflight.NewLogsSchema([]string{"app", "env"}, []string{"detected_level", "trace_id"})
)

func logParams(t *testing.T, query string) logql.Params {
	t.Helper()
	p, err := logql.NewLiteralParams(query, translateStart, translateEnd, 0, 0, logproto.BACKWARD, 1000, nil, nil)
	require.NoError(t, err)
	return p
}

func metricParams(t *testing.T, query string, step time.Duration) logql.Params {
	t.Helper()
	end := translateEnd
	if step == 0 {
		end = translateStart // instant query
	}
	p, err := logql.NewLiteralParams(query, translateStart, end, step, 0, logproto.BACKWARD, 1000, nil, nil)
	require.NoError(t, err)
	return p
}

func TestTranslateLogQL(t *testing.T) {
	for _, tc := range []struct {
		name     string
		params   func(t *testing.T) logql.Params
		contains []string
		notes    int
		wantErr  error
	}{
		{
			name:     "plain selector",
			params:   func(t *testing.T) logql.Params { return logParams(t, `{app="nginx", env="prod"}`) },
			contains: []string{`"app" = 'nginx'`, `"env" = 'prod'`, `"timestamp" >= '2024-01-01T00:00:00Z'`, `"timestamp" < '2024-01-02T00:00:00Z'`, `ORDER BY "timestamp" DESC`, `LIMIT 1000`},
		},
		{
			name: "line filter and metadata filter",
			params: func(t *testing.T) logql.Params {
				return logParams(t, `{app="nginx"} |= "level" | detected_level="error"`)
			},
			contains: []string{`strpos("message", 'level') > 0`, `"detected_level" = 'error'`},
			notes:    1,
		},
		{
			name:     "regex alternation becomes IN",
			params:   func(t *testing.T) logql.Params { return logParams(t, `{app="nginx"} | detected_level=~"error|warn"`) },
			contains: []string{`"detected_level" IN ('error', 'warn')`},
		},
		{
			name:     "regex line filter is left to DataFusion",
			params:   func(t *testing.T) logql.Params { return logParams(t, `{app="nginx"} |~ "(?i)error"`) },
			contains: []string{`regexp_like("message", '(?i)error')`},
			notes:    1,
		},
		{
			name:     "unknown label equality matches nothing",
			params:   func(t *testing.T) logql.Params { return logParams(t, `{app="nginx", missing="x"}`) },
			contains: []string{`FALSE`},
		},
		{
			name:    "parser stage is unsupported",
			params:  func(t *testing.T) logql.Params { return logParams(t, `{app="nginx"} | json`) },
			wantErr: errSQLUnsupported,
		},
		{
			name: "count over time range query tiles by step",
			params: func(t *testing.T) logql.Params {
				return metricParams(t, `sum(count_over_time({app="nginx"}[15m]))`, time.Minute)
			},
			contains: []string{`AS "__bucket"`, `count(*) AS "__v"`, `GROUP BY "__bucket"`, `"timestamp" > '2023-12-31T23:45:00Z'`, `"timestamp" <= '2024-01-02T00:00:00Z'`},
		},
		{
			name: "sum by groups on the label column",
			params: func(t *testing.T) logql.Params {
				return metricParams(t, `sum by (app) (rate({env="prod"}[15m]))`, time.Minute)
			},
			contains: []string{`"app" AS "__g0"`, `GROUP BY "__bucket", "__g0"`},
		},
		{
			name:     "instant query uses a single window",
			params:   func(t *testing.T) logql.Params { return metricParams(t, `sum(bytes_over_time({app="nginx"}[30m]))`, 0) },
			contains: []string{`sum(octet_length("message")) AS "__v"`, `"timestamp" > '2023-12-31T23:30:00Z'`, `"timestamp" <= '2024-01-01T00:00:00Z'`},
		},
		{
			name: "range not a multiple of step is unsupported",
			params: func(t *testing.T) logql.Params {
				return metricParams(t, `sum(count_over_time({app="nginx"}[15m]))`, 7*time.Minute)
			},
			wantErr: errSQLUnsupported,
		},
		{
			name: "topk is unsupported",
			params: func(t *testing.T) logql.Params {
				return metricParams(t, `topk(3, sum by (app) (count_over_time({env="prod"}[15m])))`, time.Minute)
			},
			wantErr: errSQLUnsupported,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := translateLogQL(tc.params(t), translateSchema)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			for _, want := range tc.contains {
				require.Contains(t, plan.SQL, want)
			}
			require.Len(t, plan.Notes, tc.notes, "notes: %v", plan.Notes)
		})
	}
}

func row(kv ...any) map[string]json.RawMessage {
	m := make(map[string]json.RawMessage, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		b, _ := json.Marshal(kv[i+1])
		m[kv[i].(string)] = b
	}
	return m
}

func TestSQLPlan_logsResult(t *testing.T) {
	plan, err := translateLogQL(logParams(t, `{app="nginx"}`), translateSchema)
	require.NoError(t, err)

	t2 := translateStart.Add(2 * time.Second).UnixNano()
	t1 := translateStart.Add(1 * time.Second).UnixNano()
	rows := []map[string]json.RawMessage{
		row("__ts", t2, "message", "b", "app", "nginx", "env", "prod", "detected_level", "error"),
		row("__ts", t1, "message", "a", "app", "nginx", "env", "prod", "detected_level", "error"),
		row("__ts", t1, "message", "c", "app", "nginx"),
	}

	streams, err := plan.logsResult(rows)
	require.NoError(t, err)
	require.Len(t, streams, 2)

	// Streams are sorted by their full label string, which includes
	// structured metadata ("," sorts before "}"), and entries keep SQL order.
	require.Equal(t, `{app="nginx", detected_level="error", env="prod"}`, streams[0].Labels)
	require.Len(t, streams[0].Entries, 2)
	require.Equal(t, "b", streams[0].Entries[0].Line)
	require.Equal(t, time.Unix(0, t2), streams[0].Entries[0].Timestamp)
	require.EqualValues(t, []logproto.LabelAdapter{{Name: "detected_level", Value: "error"}}, streams[0].Entries[0].StructuredMetadata)

	require.Equal(t, `{app="nginx"}`, streams[1].Labels)
	require.Len(t, streams[1].Entries, 1)
	require.Nil(t, streams[1].Entries[0].StructuredMetadata)
}

func TestSQLPlan_logsResult_metadataCollision(t *testing.T) {
	schema := arrowflight.NewLogsSchema([]string{"app", "service_name"}, []string{"service_name"})
	plan, err := translateLogQL(logParams(t, `{app="nginx"}`), schema)
	require.NoError(t, err)

	metaCol, ok := schema.MetadataColumn("service_name")
	require.True(t, ok)
	require.Equal(t, "metadata_service_name", metaCol)

	streams, err := plan.logsResult([]map[string]json.RawMessage{
		row("__ts", translateStart.UnixNano(), "message", "x", "app", "nginx", "service_name", "svc", metaCol, "svc"),
	})
	require.NoError(t, err)
	require.Len(t, streams, 1)
	require.Equal(t, `{app="nginx", service_name="svc", service_name_extracted="svc"}`, streams[0].Labels)
	require.EqualValues(t, []logproto.LabelAdapter{{Name: "service_name_extracted", Value: "svc"}}, streams[0].Entries[0].StructuredMetadata)
}

func TestSQLPlan_metricResult(t *testing.T) {
	t.Run("range query sums step buckets over the window", func(t *testing.T) {
		// 3 minute range, 1 minute step, evaluated at start .. start+4m.
		p, err := logql.NewLiteralParams(`sum(count_over_time({app="nginx"}[3m]))`, translateStart, translateStart.Add(4*time.Minute), time.Minute, 0, logproto.BACKWARD, 0, nil, nil)
		require.NoError(t, err)
		plan, err := translateLogQL(p, translateSchema)
		require.NoError(t, err)

		bucket := func(end time.Time) int64 { return end.UnixNano() }
		rows := []map[string]json.RawMessage{
			row("__bucket", bucket(translateStart.Add(-2*time.Minute)), "__v", 1),  // in window of start only
			row("__bucket", bucket(translateStart), "__v", 10),                     // start, +1m, +2m
			row("__bucket", bucket(translateStart.Add(3*time.Minute)), "__v", 100), // +3m, +4m
		}
		v, err := plan.metricResult(rows)
		require.NoError(t, err)

		matrix, ok := v.(promql.Matrix)
		require.True(t, ok)
		require.Len(t, matrix, 1)
		require.Equal(t, labels.EmptyLabels(), matrix[0].Metric)

		ms := func(d time.Duration) int64 { return translateStart.Add(d).UnixMilli() }
		require.Equal(t, []promql.FPoint{
			{T: ms(0), F: 11},
			{T: ms(1 * time.Minute), F: 10},
			{T: ms(2 * time.Minute), F: 10},
			{T: ms(3 * time.Minute), F: 100},
			{T: ms(4 * time.Minute), F: 100},
		}, matrix[0].Floats)
	})

	t.Run("rate divides by range seconds and groups by label", func(t *testing.T) {
		plan, err := translateLogQL(metricParams(t, `sum by (app) (rate({env="prod"}[30m]))`, 0), translateSchema)
		require.NoError(t, err)

		rows := []map[string]json.RawMessage{
			row("__g0", "nginx", "__v", 1800),
			row("__g0", "db", "__v", 900),
		}
		v, err := plan.metricResult(rows)
		require.NoError(t, err)

		vector, ok := v.(promql.Vector)
		require.True(t, ok)
		require.Len(t, vector, 2)
		require.Equal(t, labels.FromStrings("app", "db"), vector[0].Metric)
		require.InDelta(t, 0.5, vector[0].F, 1e-9)
		require.Equal(t, labels.FromStrings("app", "nginx"), vector[1].Metric)
		require.InDelta(t, 1.0, vector[1].F, 1e-9)
		require.Equal(t, translateStart.UnixMilli(), vector[1].T)
	})

	t.Run("no rows gives an empty result", func(t *testing.T) {
		plan, err := translateLogQL(metricParams(t, `sum(count_over_time({app="nginx"}[15m]))`, time.Minute), translateSchema)
		require.NoError(t, err)
		v, err := plan.metricResult(nil)
		require.NoError(t, err)
		require.Empty(t, v.(promql.Matrix))
	})
}
