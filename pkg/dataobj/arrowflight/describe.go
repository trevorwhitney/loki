package arrowflight

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight/scanpb"
)

// describePredicates renders the pushed-down predicates of a scan request in
// SQL-like form for debug logs, so that it is visible from the server side
// which filters a client managed to push down (for example a time range).
func describePredicates(preds []*scanpb.Predicate) string {
	if len(preds) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(preds))
	for _, p := range preds {
		values := make([]string, 0, len(p.GetValues()))
		for _, v := range p.GetValues() {
			values = append(values, describeLiteral(v))
		}
		if p.GetOp() == scanpb.Op_OP_IN {
			parts = append(parts, fmt.Sprintf("%s IN (%s)", p.GetColumn(), strings.Join(values, ", ")))
			continue
		}
		op := strings.TrimPrefix(p.GetOp().String(), "OP_")
		parts = append(parts, fmt.Sprintf("%s %s %s", p.GetColumn(), op, strings.Join(values, ", ")))
	}
	return strings.Join(parts, " AND ")
}

func describeLiteral(l *scanpb.Literal) string {
	switch v := l.GetValue().(type) {
	case *scanpb.Literal_StringValue:
		return strconv.Quote(v.StringValue)
	case *scanpb.Literal_Int64Value:
		return strconv.FormatInt(v.Int64Value, 10)
	case *scanpb.Literal_Uint64Value:
		return strconv.FormatUint(v.Uint64Value, 10)
	case *scanpb.Literal_TimestampNs:
		return time.Unix(0, v.TimestampNs).UTC().Format(time.RFC3339Nano)
	case *scanpb.Literal_BytesValue:
		return fmt.Sprintf("0x%x", v.BytesValue)
	default:
		return "?"
	}
}
