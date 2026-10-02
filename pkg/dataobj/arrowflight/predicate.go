package arrowflight

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/scalar"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight/scanpb"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/logs"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/streams"
)

// Predicate translation is best-effort: anything that cannot be expressed as
// a section predicate is dropped, because clients re-apply every filter. The
// one thing translation must never do is drop rows that match, so a predicate
// is only produced when its semantics are exactly those of the request.

func literalString(l *scanpb.Literal) (string, bool) {
	switch v := l.GetValue().(type) {
	case *scanpb.Literal_StringValue:
		return v.StringValue, true
	case *scanpb.Literal_BytesValue:
		return string(v.BytesValue), true
	}
	return "", false
}

func literalInt64(l *scanpb.Literal) (int64, bool) {
	switch v := l.GetValue().(type) {
	case *scanpb.Literal_Int64Value:
		return v.Int64Value, true
	case *scanpb.Literal_Uint64Value:
		if v.Uint64Value > math.MaxInt64 {
			return 0, false
		}
		return int64(v.Uint64Value), true
	case *scanpb.Literal_TimestampNs:
		return v.TimestampNs, true
	}
	return 0, false
}

func stringScalars(lits []*scanpb.Literal) ([]scalar.Scalar, bool) {
	out := make([]scalar.Scalar, 0, len(lits))
	for _, l := range lits {
		s, ok := literalString(l)
		if !ok {
			return nil, false
		}
		out = append(out, scalar.NewStringScalar(s))
	}
	return out, len(out) > 0
}

func int64Scalars(lits []*scanpb.Literal) ([]scalar.Scalar, bool) {
	out := make([]scalar.Scalar, 0, len(lits))
	for _, l := range lits {
		v, ok := literalInt64(l)
		if !ok {
			return nil, false
		}
		out = append(out, scalar.NewInt64Scalar(v))
	}
	return out, len(out) > 0
}

func timestampScalars(lits []*scanpb.Literal) ([]scalar.Scalar, bool) {
	out := make([]scalar.Scalar, 0, len(lits))
	for _, l := range lits {
		v, ok := literalInt64(l)
		if !ok {
			return nil, false
		}
		out = append(out, scalar.NewTimestampScalar(arrow.Timestamp(v), arrow.FixedWidthTypes.Timestamp_ns))
	}
	return out, len(out) > 0
}

// labelFilter returns an in-memory filter over a stream's labels for an
// equality or membership predicate. Other operators are not supported.
func labelFilter(name string, p *scanpb.Predicate) (func(map[string]string) bool, bool) {
	switch p.GetOp() {
	case scanpb.Op_OP_EQ:
		if len(p.GetValues()) != 1 {
			return nil, false
		}
		want, ok := literalString(p.GetValues()[0])
		if !ok {
			return nil, false
		}
		return func(lbls map[string]string) bool {
			got, ok := lbls[name]
			return ok && got == want
		}, true

	case scanpb.Op_OP_IN:
		set := make(map[string]struct{}, len(p.GetValues()))
		for _, l := range p.GetValues() {
			s, ok := literalString(l)
			if !ok {
				return nil, false
			}
			set[s] = struct{}{}
		}
		return func(lbls map[string]string) bool {
			got, ok := lbls[name]
			if !ok {
				return false
			}
			_, ok = set[got]
			return ok
		}, true
	}
	return nil, false
}

// logsCompare builds a logs predicate comparing col against vals with op.
func logsCompare(col *logs.Column, op scanpb.Op, vals []scalar.Scalar) (logs.Predicate, bool) {
	if op == scanpb.Op_OP_IN {
		return logs.InPredicate{Column: col, Values: vals}, true
	}
	if len(vals) != 1 {
		return nil, false
	}
	switch op {
	case scanpb.Op_OP_EQ:
		return logs.EqualPredicate{Column: col, Value: vals[0]}, true
	case scanpb.Op_OP_GT:
		return logs.GreaterThanPredicate{Column: col, Value: vals[0]}, true
	case scanpb.Op_OP_GTE:
		return logs.NotPredicate{Inner: logs.LessThanPredicate{Column: col, Value: vals[0]}}, true
	case scanpb.Op_OP_LT:
		return logs.LessThanPredicate{Column: col, Value: vals[0]}, true
	case scanpb.Op_OP_LTE:
		return logs.NotPredicate{Inner: logs.GreaterThanPredicate{Column: col, Value: vals[0]}}, true
	}
	return nil, false
}

// streamsCompare builds a streams predicate comparing col against vals with op.
func streamsCompare(col *streams.Column, op scanpb.Op, vals []scalar.Scalar) (streams.Predicate, bool) {
	if op == scanpb.Op_OP_IN {
		return streams.InPredicate{Column: col, Values: vals}, true
	}
	if len(vals) != 1 {
		return nil, false
	}
	switch op {
	case scanpb.Op_OP_EQ:
		return streams.EqualPredicate{Column: col, Value: vals[0]}, true
	case scanpb.Op_OP_GT:
		return streams.GreaterThanPredicate{Column: col, Value: vals[0]}, true
	case scanpb.Op_OP_GTE:
		return streams.NotPredicate{Inner: streams.LessThanPredicate{Column: col, Value: vals[0]}}, true
	case scanpb.Op_OP_LT:
		return streams.LessThanPredicate{Column: col, Value: vals[0]}, true
	case scanpb.Op_OP_LTE:
		return streams.NotPredicate{Inner: streams.GreaterThanPredicate{Column: col, Value: vals[0]}}, true
	}
	return nil, false
}

// isMembership reports whether op can only be satisfied by a non-null value
// equal to one of the literals. Such predicates match nothing when the column
// is absent from a section.
func isMembership(op scanpb.Op) bool {
	return op == scanpb.Op_OP_EQ || op == scanpb.Op_OP_IN
}

// logsPredicates translates request predicates into a single logs section
// predicate. It returns empty=true when the request provably matches no row
// of this section, and a nil predicate when nothing could be pushed down.
func logsPredicates(ts *TableSchema, sec *logs.Section, st *streamsTable, preds []*scanpb.Predicate) (logs.Predicate, bool, error) {
	var (
		streamIDCol  *logs.Column
		timestampCol *logs.Column
		metadataCols = make(map[string]*logs.Column)
	)
	for _, col := range sec.Columns() {
		switch col.Type {
		case logs.ColumnTypeStreamID:
			streamIDCol = col
		case logs.ColumnTypeTimestamp:
			timestampCol = col
		case logs.ColumnTypeMetadata:
			metadataCols[col.Name] = col
		}
	}

	var (
		out           []logs.Predicate
		streamFilters []func(map[string]string) bool
	)

	for _, p := range preds {
		b, ok := ts.binding(p.GetColumn())
		if !ok {
			return nil, false, fmt.Errorf("unknown column %q in predicate", p.GetColumn())
		}

		switch b.Kind {
		case columnKindLabel:
			if f, ok := labelFilter(b.Source, p); ok {
				streamFilters = append(streamFilters, f)
			}

		case columnKindMetadata:
			col, present := metadataCols[b.Source]
			if !present {
				if isMembership(p.GetOp()) {
					return nil, true, nil
				}
				continue
			}
			if vals, ok := stringScalars(p.GetValues()); ok {
				if pred, ok := logsCompare(col, p.GetOp(), vals); ok {
					out = append(out, pred)
				}
			}

		case columnKindFixed:
			switch b.Source {
			case ColumnStreamID:
				if streamIDCol == nil {
					continue
				}
				if vals, ok := int64Scalars(p.GetValues()); ok {
					if pred, ok := logsCompare(streamIDCol, p.GetOp(), vals); ok {
						out = append(out, pred)
					}
				}
			case ColumnTimestamp:
				if timestampCol == nil {
					continue
				}
				if vals, ok := timestampScalars(p.GetValues()); ok {
					if pred, ok := logsCompare(timestampCol, p.GetOp(), vals); ok {
						out = append(out, pred)
					}
				}
			}
		}
	}

	if len(streamFilters) > 0 {
		if st == nil || streamIDCol == nil {
			return nil, false, fmt.Errorf("label predicates require a streams section and a stream ID column")
		}
		ids := st.matchingIDs(func(lbls map[string]string) bool {
			for _, f := range streamFilters {
				if !f(lbls) {
					return false
				}
			}
			return true
		})
		if len(ids) == 0 {
			return nil, true, nil
		}
		vals := make([]scalar.Scalar, len(ids))
		for i, id := range ids {
			vals[i] = scalar.NewInt64Scalar(id)
		}
		out = append(out, logs.InPredicate{Column: streamIDCol, Values: vals})
	}

	return andLogs(out), false, nil
}

func andLogs(ps []logs.Predicate) logs.Predicate {
	if len(ps) == 0 {
		return nil
	}
	acc := ps[0]
	for _, p := range ps[1:] {
		acc = logs.AndPredicate{Left: acc, Right: p}
	}
	return acc
}

// streamsPredicates translates request predicates into a single streams
// section predicate, with the same contract as [logsPredicates].
func streamsPredicates(ts *TableSchema, sec *streams.Section, preds []*scanpb.Predicate) (streams.Predicate, bool, error) {
	fixedCols := make(map[streams.ColumnType]*streams.Column)
	labelCols := make(map[string]*streams.Column)
	for _, col := range sec.Columns() {
		if col.Type == streams.ColumnTypeLabel {
			labelCols[col.Name] = col
		} else {
			fixedCols[col.Type] = col
		}
	}

	var out []streams.Predicate
	for _, p := range preds {
		b, ok := ts.binding(p.GetColumn())
		if !ok {
			return nil, false, fmt.Errorf("unknown column %q in predicate", p.GetColumn())
		}

		switch b.Kind {
		case columnKindLabel:
			col, present := labelCols[b.Source]
			if !present {
				if isMembership(p.GetOp()) {
					return nil, true, nil
				}
				continue
			}
			if vals, ok := stringScalars(p.GetValues()); ok {
				if pred, ok := streamsCompare(col, p.GetOp(), vals); ok {
					out = append(out, pred)
				}
			}

		case columnKindFixed:
			var (
				col  *streams.Column
				vals []scalar.Scalar
				ok   bool
			)
			switch b.Source {
			case ColumnStreamID:
				col = fixedCols[streams.ColumnTypeStreamID]
				vals, ok = int64Scalars(p.GetValues())
			case ColumnMinTimestamp:
				col = fixedCols[streams.ColumnTypeMinTimestamp]
				vals, ok = timestampScalars(p.GetValues())
			case ColumnMaxTimestamp:
				col = fixedCols[streams.ColumnTypeMaxTimestamp]
				vals, ok = timestampScalars(p.GetValues())
			case ColumnRows:
				col = fixedCols[streams.ColumnTypeRows]
				vals, ok = int64Scalars(p.GetValues())
			case ColumnUncompressedSize:
				col = fixedCols[streams.ColumnTypeUncompressedSize]
				vals, ok = int64Scalars(p.GetValues())
			}
			if col == nil || !ok {
				continue
			}
			if pred, ok := streamsCompare(col, p.GetOp(), vals); ok {
				out = append(out, pred)
			}
		}
	}

	return andStreams(out), false, nil
}

func andStreams(ps []streams.Predicate) streams.Predicate {
	if len(ps) == 0 {
		return nil
	}
	acc := ps[0]
	for _, p := range ps[1:] {
		acc = streams.AndPredicate{Left: acc, Right: p}
	}
	return acc
}
