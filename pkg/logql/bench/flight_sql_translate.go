package bench

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
)

// errSQLUnsupported marks LogQL features outside the subset this translator
// can express as SQL over the Flight server's logs table.
var errSQLUnsupported = errors.New("not translatable to SQL")

func unsupported(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errSQLUnsupported, fmt.Sprintf(format, args...))
}

type sqlPlanKind int

const (
	sqlPlanLogs sqlPlanKind = iota
	sqlPlanMetric
)

// sqlPlan is a LogQL query translated to one SQL statement plus everything
// needed to turn the rows back into a LogQL result.
type sqlPlan struct {
	SQL  string
	Kind sqlPlanKind

	// Notes lists the parts of the query that DataFusion evaluates itself
	// because the Flight server cannot prune on them.
	Notes []string

	// Log queries: SQL column name to label name / metadata key.
	labelCols map[string]string
	metaCols  map[string]string

	// Metric queries.
	op         string
	start, end time.Time
	step, rng  time.Duration
	groupNames []string // sorted grouping labels from the query
	groupCols  []string // SQL alias per groupName, "" when the label exists nowhere
}

func sqlIdent(name string) string             { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }
func sqlString(s string) string               { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func sqlTime(t time.Time) string              { return sqlString(t.UTC().Format(time.RFC3339Nano)) }
func sqlBool(b bool) string                   { return map[bool]string{true: "TRUE", false: "FALSE"}[b] }
func sortedKeys(m map[string]string) []string { return slices.Sorted(mapsKeys(m)) }

func mapsKeys(m map[string]string) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

// translateLogQL converts a LogQL query into a SQL plan, or returns an error
// wrapping errSQLUnsupported when the query uses features outside the subset.
func translateLogQL(p logql.Params, schema *arrowflight.TableSchema) (*sqlPlan, error) {
	expr, err := syntax.ParseExpr(p.QueryString())
	if err != nil {
		return nil, err
	}

	switch e := expr.(type) {
	case syntax.LogSelectorExpr:
		return translateLogQuery(e, p, schema)
	case syntax.SampleExpr:
		return translateMetricQuery(e, p, schema)
	}
	return nil, unsupported("expression type %T", expr)
}

// sqlWhere accumulates WHERE clauses while walking a log selector.
type sqlWhere struct {
	clauses []string
	notes   []string
}

func (w *sqlWhere) add(clause string, note string) {
	w.clauses = append(w.clauses, clause)
	if note != "" && !slices.Contains(w.notes, note) {
		w.notes = append(w.notes, note)
	}
}

// translateSelector handles stream matchers, line filters and string label
// filters. Parsers, formatters and every other stage are unsupported.
func translateSelector(e syntax.LogSelectorExpr, schema *arrowflight.TableSchema) (*sqlWhere, error) {
	var (
		matchers []*labels.Matcher
		stages   syntax.MultiStageExpr
	)
	switch e := e.(type) {
	case *syntax.MatchersExpr:
		matchers = e.Mts
	case *syntax.PipelineExpr:
		matchers = e.Left.Mts
		stages = e.MultiStages
	default:
		return nil, unsupported("selector %T", e)
	}

	w := &sqlWhere{}
	for _, m := range matchers {
		col, ok := schema.LabelColumn(m.Name)
		clause, note := matcherClause(col, ok, m)
		w.add(clause, note)
	}

	for _, stage := range stages {
		switch st := stage.(type) {
		case *syntax.LineFilterExpr:
			if err := w.addLineFilters(st); err != nil {
				return nil, err
			}
		case *syntax.LabelFilterExpr:
			var m *labels.Matcher
			switch f := st.LabelFilterer.(type) {
			case *log.StringLabelFilter:
				m = f.Matcher
			case *log.LineFilterLabelFilter:
				m = f.Matcher
			default:
				return nil, unsupported("label filter %T", st.LabelFilterer)
			}
			// A label filter sees stream labels and structured metadata
			// alike. When both carry the same name the metadata key is
			// renamed with an _extracted suffix, so the bare name always
			// refers to the stream label.
			col, ok := schema.LabelColumn(m.Name)
			if !ok {
				col, ok = schema.MetadataColumn(m.Name)
			}
			clause, note := matcherClause(col, ok, m)
			w.add(clause, note)
		default:
			return nil, unsupported("pipeline stage %T", stage)
		}
	}
	return w, nil
}

// addLineFilters walks a chain of line filters. `|= "a" |= "b"` parses as
// LineFilterExpr{b, Left: LineFilterExpr{a}}.
func (w *sqlWhere) addLineFilters(e *syntax.LineFilterExpr) error {
	if e.Left != nil {
		if err := w.addLineFilters(e.Left); err != nil {
			return err
		}
	}
	if e.Or != nil || e.IsOrChild {
		return unsupported("line filter with 'or'")
	}
	if e.Op != "" {
		return unsupported("line filter function %q", e.Op)
	}

	msg := sqlIdent(arrowflight.ColumnMessage)
	switch e.Ty {
	case log.LineMatchEqual:
		w.add(fmt.Sprintf("strpos(%s, %s) > 0", msg, sqlString(e.Match)), "line filter evaluated by DataFusion")
	case log.LineMatchNotEqual:
		w.add(fmt.Sprintf("strpos(%s, %s) = 0", msg, sqlString(e.Match)), "line filter evaluated by DataFusion")
	case log.LineMatchRegexp:
		w.add(fmt.Sprintf("regexp_like(%s, %s)", msg, sqlString(e.Match)), "regex line filter evaluated by DataFusion")
	case log.LineMatchNotRegexp:
		w.add(fmt.Sprintf("NOT regexp_like(%s, %s)", msg, sqlString(e.Match)), "regex line filter evaluated by DataFusion")
	default:
		return unsupported("line filter type %v", e.Ty)
	}
	return nil
}

// matcherClause renders a label matcher against a nullable string column. A
// column that exists in no data object (exists=false) behaves as the empty
// string, which is how LogQL treats a missing label.
func matcherClause(col string, exists bool, m *labels.Matcher) (clause string, note string) {
	ref := sqlIdent(col)
	switch m.Type {
	case labels.MatchEqual:
		if !exists {
			return sqlBool(m.Value == ""), ""
		}
		if m.Value == "" {
			return fmt.Sprintf("coalesce(%s, '') = ''", ref), ""
		}
		return fmt.Sprintf("%s = %s", ref, sqlString(m.Value)), ""

	case labels.MatchNotEqual:
		if !exists {
			return sqlBool(m.Value != ""), ""
		}
		return fmt.Sprintf("coalesce(%s, '') <> %s", ref, sqlString(m.Value)), ""

	case labels.MatchRegexp, labels.MatchNotRegexp:
		negate := m.Type == labels.MatchNotRegexp
		if !exists {
			return sqlBool(m.Matches("")), ""
		}
		if alts, ok := literalAlternation(m.Value); ok {
			quoted := make([]string, len(alts))
			for i, a := range alts {
				quoted[i] = sqlString(a)
			}
			list := strings.Join(quoted, ", ")
			if negate {
				return fmt.Sprintf("coalesce(%s, '') NOT IN (%s)", ref, list), ""
			}
			return fmt.Sprintf("%s IN (%s)", ref, list), ""
		}
		clause = fmt.Sprintf("regexp_like(coalesce(%s, ''), %s)", ref, sqlString("^(?:"+m.Value+")$"))
		if negate {
			clause = "NOT " + clause
		}
		return clause, "regex label matcher evaluated by DataFusion"
	}
	return "FALSE", ""
}

// literalAlternation reports whether a regex is a plain alternation of
// non-empty literals such as `error|warn`, which maps to IN.
func literalAlternation(re string) ([]string, bool) {
	parts := strings.Split(re, "|")
	for _, p := range parts {
		if p == "" || regexp.QuoteMeta(p) != p {
			return nil, false
		}
	}
	return parts, true
}

func translateLogQuery(e syntax.LogSelectorExpr, p logql.Params, schema *arrowflight.TableSchema) (*sqlPlan, error) {
	if p.Interval() != 0 {
		return nil, unsupported("log query interval")
	}

	w, err := translateSelector(e, schema)
	if err != nil {
		return nil, err
	}

	plan := &sqlPlan{
		Kind:      sqlPlanLogs,
		Notes:     w.notes,
		labelCols: schema.LabelColumns(),
		metaCols:  schema.MetadataColumns(),
	}

	cols := []string{
		sqlIdent(arrowflight.ColumnStreamID),
		fmt.Sprintf(`CAST(%s AS BIGINT) AS "__ts"`, sqlIdent(arrowflight.ColumnTimestamp)),
		sqlIdent(arrowflight.ColumnMessage),
	}
	for _, c := range sortedKeys(plan.labelCols) {
		cols = append(cols, sqlIdent(c))
	}
	for _, c := range sortedKeys(plan.metaCols) {
		cols = append(cols, sqlIdent(c))
	}

	ts := sqlIdent(arrowflight.ColumnTimestamp)
	where := append([]string{
		fmt.Sprintf("%s >= %s", ts, sqlTime(p.Start())),
		fmt.Sprintf("%s < %s", ts, sqlTime(p.End())),
	}, w.clauses...)

	order := "DESC"
	if p.Direction() == logproto.FORWARD {
		order = "ASC"
	}

	plan.SQL = fmt.Sprintf("SELECT %s FROM logs WHERE %s ORDER BY %s %s, %s %s LIMIT %d",
		strings.Join(cols, ", "), strings.Join(where, " AND "),
		ts, order, sqlIdent(arrowflight.ColumnStreamID), order, p.Limit())
	return plan, nil
}

func translateMetricQuery(e syntax.SampleExpr, p logql.Params, schema *arrowflight.TableSchema) (*sqlPlan, error) {
	vec, ok := e.(*syntax.VectorAggregationExpr)
	if !ok || vec.Operation != syntax.OpTypeSum || vec.Params != 0 {
		return nil, unsupported("metric expression %T must be sum(...)", e)
	}
	if vec.Grouping != nil && vec.Grouping.Without {
		return nil, unsupported("sum without (...)")
	}
	ra, ok := vec.Left.(*syntax.RangeAggregationExpr)
	if !ok || ra.Grouping != nil || ra.Params != nil {
		return nil, unsupported("range aggregation %T", vec.Left)
	}
	switch ra.Operation {
	case syntax.OpRangeTypeCount, syntax.OpRangeTypeRate, syntax.OpRangeTypeBytes:
	default:
		return nil, unsupported("range aggregation %s", ra.Operation)
	}
	if ra.Left.Unwrap != nil || ra.Left.Offset != 0 {
		return nil, unsupported("unwrap or offset")
	}

	w, err := translateSelector(ra.Left.Left, schema)
	if err != nil {
		return nil, err
	}

	plan := &sqlPlan{
		Kind:  sqlPlanMetric,
		Notes: w.notes,
		op:    ra.Operation,
		start: p.Start(),
		end:   p.End(),
		step:  p.Step(),
		rng:   ra.Left.Interval,
	}

	var selectCols, groupBy []string
	if vec.Grouping != nil {
		plan.groupNames = slices.Clone(vec.Grouping.Groups)
		sort.Strings(plan.groupNames)
	}
	for i, name := range plan.groupNames {
		col, ok := schema.LabelColumn(name)
		if !ok {
			col, ok = schema.MetadataColumn(name)
		}
		if !ok {
			plan.groupCols = append(plan.groupCols, "")
			continue
		}
		alias := fmt.Sprintf("__g%d", i)
		plan.groupCols = append(plan.groupCols, alias)
		selectCols = append(selectCols, fmt.Sprintf("%s AS %s", sqlIdent(col), sqlIdent(alias)))
		groupBy = append(groupBy, sqlIdent(alias))
	}

	agg := "count(*)"
	if ra.Operation == syntax.OpRangeTypeBytes {
		agg = fmt.Sprintf("sum(octet_length(%s))", sqlIdent(arrowflight.ColumnMessage))
	}

	ts := sqlIdent(arrowflight.ColumnTimestamp)
	var where []string

	if plan.step == 0 {
		// Instant query: one window (end-range, end].
		where = []string{
			fmt.Sprintf("%s > %s", ts, sqlTime(plan.end.Add(-plan.rng))),
			fmt.Sprintf("%s <= %s", ts, sqlTime(plan.end)),
		}
	} else {
		if plan.step < 0 || plan.rng%plan.step != 0 {
			return nil, unsupported("range %s is not a multiple of step %s", plan.rng, plan.step)
		}
		// Range query: tile (start-range, end] with buckets of width step
		// whose ends land on the evaluation times start + i*step. A row at
		// timestamp ts belongs to the bucket ending at the smallest such
		// time >= ts, which is what the integer arithmetic below computes.
		origin := plan.start.Add(-plan.rng).UnixNano()
		stepNs := plan.step.Nanoseconds()
		selectCols = append([]string{fmt.Sprintf(
			`((CAST(%s AS BIGINT) - 1 - %d) / %d) * %d + %d AS "__bucket"`,
			ts, origin, stepNs, stepNs, origin+stepNs,
		)}, selectCols...)
		groupBy = append([]string{`"__bucket"`}, groupBy...)
		where = []string{
			fmt.Sprintf("%s > %s", ts, sqlTime(plan.start.Add(-plan.rng))),
			fmt.Sprintf("%s <= %s", ts, sqlTime(plan.end)),
		}
	}
	where = append(where, w.clauses...)

	selectCols = append(selectCols, agg+` AS "__v"`)
	plan.SQL = fmt.Sprintf("SELECT %s FROM logs WHERE %s", strings.Join(selectCols, ", "), strings.Join(where, " AND "))
	if len(groupBy) > 0 {
		plan.SQL += " GROUP BY " + strings.Join(groupBy, ", ")
	}
	return plan, nil
}

// rawString decodes a JSON string cell. Absent or null cells return ok=false.
func rawString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

func rawInt64(raw json.RawMessage) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
}

func rawFloat(raw json.RawMessage) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(string(raw)), 64)
}

// logsResult turns rows of a log plan into Loki streams, grouped by the full
// label set (stream labels plus structured metadata) like the query engines.
func (plan *sqlPlan) logsResult(rows []map[string]json.RawMessage) (logqlmodel.Streams, error) {
	streams := make(map[string]*logproto.Stream)

	for _, row := range rows {
		ts, err := rawInt64(row["__ts"])
		if err != nil {
			return nil, fmt.Errorf("decoding timestamp: %w", err)
		}
		line, _ := rawString(row[arrowflight.ColumnMessage])

		var lbls, meta []labels.Label
		streamNames := make(map[string]struct{})
		for col, name := range plan.labelCols {
			if v, ok := rawString(row[col]); ok {
				lbls = append(lbls, labels.Label{Name: name, Value: v})
				streamNames[name] = struct{}{}
			}
		}
		for col, name := range plan.metaCols {
			if v, ok := rawString(row[col]); ok {
				// Loki renames structured metadata that collides with a
				// stream label of the same entry.
				if _, collides := streamNames[name]; collides {
					name += "_extracted"
				}
				meta = append(meta, labels.Label{Name: name, Value: v})
			}
		}

		all := labels.New(slices.Concat(lbls, meta)...)
		key := all.String()

		entry := logproto.Entry{Timestamp: time.Unix(0, ts), Line: line}
		if len(meta) > 0 {
			entry.StructuredMetadata = logproto.FromLabelsToLabelAdapters(labels.New(meta...))
		}

		s, ok := streams[key]
		if !ok {
			s = &logproto.Stream{Labels: key}
			streams[key] = s
		}
		s.Entries = append(s.Entries, entry)
	}

	result := make(logqlmodel.Streams, 0, len(streams))
	for _, s := range streams {
		result = append(result, *s)
	}
	sort.Sort(result)
	return result, nil
}

// metricResult turns rows of a metric plan into a Vector (instant) or Matrix
// (range) with LogQL's windowing semantics: the point at time t covers
// (t-range, t], and steps with no samples produce no point.
func (plan *sqlPlan) metricResult(rows []map[string]json.RawMessage) (parser.Value, error) {
	type series struct {
		metric  labels.Labels
		buckets map[int64]float64 // bucket end (ns) -> value
	}
	bySeries := make(map[string]*series)

	for _, row := range rows {
		v, err := rawFloat(row["__v"])
		if err != nil {
			return nil, fmt.Errorf("decoding value: %w", err)
		}

		var lbls []labels.Label
		for i, name := range plan.groupNames {
			if plan.groupCols[i] == "" {
				continue
			}
			if val, ok := rawString(row[plan.groupCols[i]]); ok {
				lbls = append(lbls, labels.Label{Name: name, Value: val})
			}
		}
		metric := labels.EmptyLabels()
		if len(lbls) > 0 {
			metric = labels.New(lbls...)
		}

		bucketEnd := plan.end.UnixNano()
		if plan.step != 0 {
			if bucketEnd, err = rawInt64(row["__bucket"]); err != nil {
				return nil, fmt.Errorf("decoding bucket: %w", err)
			}
		}

		key := metric.String()
		s, ok := bySeries[key]
		if !ok {
			s = &series{metric: metric, buckets: make(map[int64]float64)}
			bySeries[key] = s
		}
		s.buckets[bucketEnd] += v
	}

	keys := slices.Sorted(mapsKeys(func() map[string]string {
		m := make(map[string]string, len(bySeries))
		for k := range bySeries {
			m[k] = ""
		}
		return m
	}()))

	scale := 1.0
	if plan.op == syntax.OpRangeTypeRate {
		scale = 1 / plan.rng.Seconds()
	}

	if plan.step == 0 {
		vector := make(promql.Vector, 0, len(keys))
		for _, k := range keys {
			s := bySeries[k]
			vector = append(vector, promql.Sample{
				Metric: s.metric,
				T:      plan.end.UnixMilli(),
				F:      s.buckets[plan.end.UnixNano()] * scale,
			})
		}
		return vector, nil
	}

	bucketsPerWindow := int(plan.rng / plan.step)
	matrix := make(promql.Matrix, 0, len(keys))
	for _, k := range keys {
		s := bySeries[k]
		var points []promql.FPoint
		for t := plan.start; !t.After(plan.end); t = t.Add(plan.step) {
			var (
				sum   float64
				found bool
			)
			for i := 1; i <= bucketsPerWindow; i++ {
				end := t.Add(-plan.rng).Add(time.Duration(i) * plan.step).UnixNano()
				if v, ok := s.buckets[end]; ok {
					sum += v
					found = true
				}
			}
			if found {
				points = append(points, promql.FPoint{T: t.UnixMilli(), F: sum * scale})
			}
		}
		if len(points) > 0 {
			matrix = append(matrix, promql.Series{Metric: s.metric, Floats: points})
		}
	}
	return matrix, nil
}
