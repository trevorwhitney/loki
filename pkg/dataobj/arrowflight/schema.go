// Package arrowflight exposes Loki data objects to external Arrow Flight
// clients, such as a DataFusion TableProvider, as two SQL-shaped tables:
//
//   - logs: one row per log record, with the labels of the owning stream
//     joined in and one column per structured metadata key.
//   - streams: one row per stream, with its labels and per-stream statistics.
//
// Clients plan a scan with [scanpb.ScanRequest] (projection plus a conjunction
// of predicates), receive one Flight endpoint per data object section, and
// stream Arrow record batches from each endpoint with DoGet. Predicates are a
// pruning hint; the server never guarantees that every returned row matches,
// so clients must re-apply the filters.
package arrowflight

import (
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
)

// Names of the tables served by the Flight server.
const (
	TableLogs    = "logs"
	TableStreams = "streams"
)

// Names of the fixed (non-label, non-metadata) columns.
const (
	ColumnStreamID         = "stream_id"
	ColumnTimestamp        = "timestamp"
	ColumnMessage          = "message"
	ColumnMinTimestamp     = "min_timestamp"
	ColumnMaxTimestamp     = "max_timestamp"
	ColumnRows             = "rows"
	ColumnUncompressedSize = "uncompressed_size"
)

// Prefixes applied to label and structured metadata keys whose names collide
// with an existing column.
const (
	labelPrefix    = "label_"
	metadataPrefix = "metadata_"
)

type columnKind int

const (
	columnKindFixed columnKind = iota
	columnKindLabel
	columnKindMetadata
)

// columnBinding records where the values of a SQL column come from: a fixed
// section column, a stream label, or a structured metadata key.
type columnBinding struct {
	Kind   columnKind
	Source string
}

// TableSchema is the Arrow schema of a served table together with the binding
// of each field to its source in the data object.
type TableSchema struct {
	Name   string
	Schema *arrow.Schema

	bindings map[string]columnBinding
}

// binding returns the binding of the named SQL column.
func (t *TableSchema) binding(name string) (columnBinding, bool) {
	b, ok := t.bindings[name]
	return b, ok
}

// Project returns the schema of the given columns, in order. An empty list
// selects every column of the table.
func (t *TableSchema) Project(columns []string) (*arrow.Schema, error) {
	if len(columns) == 0 {
		return t.Schema, nil
	}

	fields := make([]arrow.Field, 0, len(columns))
	for _, name := range columns {
		idx := t.Schema.FieldIndices(name)
		if len(idx) == 0 {
			return nil, fmt.Errorf("unknown column %q in table %s", name, t.Name)
		}
		fields = append(fields, t.Schema.Field(idx[0]))
	}
	return arrow.NewSchema(fields, nil), nil
}

var (
	logsFixedFields = []arrow.Field{
		{Name: ColumnStreamID, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: ColumnTimestamp, Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true},
		{Name: ColumnMessage, Type: arrow.BinaryTypes.String, Nullable: true},
	}

	streamsFixedFields = []arrow.Field{
		{Name: ColumnStreamID, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: ColumnMinTimestamp, Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true},
		{Name: ColumnMaxTimestamp, Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true},
		{Name: ColumnRows, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: ColumnUncompressedSize, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}
)

// NewLogsSchema builds the schema of the logs table from the union of label
// names and structured metadata keys found across all data objects.
func NewLogsSchema(labelNames, metadataKeys []string) *TableSchema {
	return buildTableSchema(TableLogs, logsFixedFields, labelNames, metadataKeys)
}

// NewStreamsSchema builds the schema of the streams table from the union of
// label names found across all data objects.
func NewStreamsSchema(labelNames []string) *TableSchema {
	return buildTableSchema(TableStreams, streamsFixedFields, labelNames, nil)
}

func buildTableSchema(name string, fixed []arrow.Field, labelNames, metadataKeys []string) *TableSchema {
	ts := &TableSchema{Name: name, bindings: make(map[string]columnBinding)}

	fields := make([]arrow.Field, 0, len(fixed)+len(labelNames)+len(metadataKeys))
	for _, f := range fixed {
		fields = append(fields, f)
		ts.bindings[f.Name] = columnBinding{Kind: columnKindFixed, Source: f.Name}
	}

	// Labels and metadata keys are sorted so the schema is stable across
	// restarts regardless of discovery order. A name that collides with an
	// already-bound column is prefixed until it is unique.
	add := func(kind columnKind, prefix string, names []string) {
		names = slices.Clone(names)
		slices.Sort(names)
		names = slices.Compact(names)

		for _, source := range names {
			sqlName := source
			for {
				if _, taken := ts.bindings[sqlName]; !taken {
					break
				}
				sqlName = prefix + sqlName
			}
			fields = append(fields, arrow.Field{Name: sqlName, Type: arrow.BinaryTypes.String, Nullable: true})
			ts.bindings[sqlName] = columnBinding{Kind: kind, Source: source}
		}
	}
	add(columnKindLabel, labelPrefix, labelNames)
	add(columnKindMetadata, metadataPrefix, metadataKeys)

	ts.Schema = arrow.NewSchema(fields, nil)
	return ts
}

// LabelColumn returns the SQL column bound to the given stream label name.
func (t *TableSchema) LabelColumn(name string) (string, bool) {
	return t.columnFor(columnKindLabel, name)
}

// MetadataColumn returns the SQL column bound to the given structured
// metadata key.
func (t *TableSchema) MetadataColumn(name string) (string, bool) {
	return t.columnFor(columnKindMetadata, name)
}

// LabelColumns returns every label-bound column as SQL column name to label
// name.
func (t *TableSchema) LabelColumns() map[string]string {
	return t.columnsOf(columnKindLabel)
}

// MetadataColumns returns every metadata-bound column as SQL column name to
// structured metadata key.
func (t *TableSchema) MetadataColumns() map[string]string {
	return t.columnsOf(columnKindMetadata)
}

func (t *TableSchema) columnFor(kind columnKind, source string) (string, bool) {
	for name, b := range t.bindings {
		if b.Kind == kind && b.Source == source {
			return name, true
		}
	}
	return "", false
}

func (t *TableSchema) columnsOf(kind columnKind) map[string]string {
	out := make(map[string]string)
	for name, b := range t.bindings {
		if b.Kind == kind {
			out[name] = b.Source
		}
	}
	return out
}
