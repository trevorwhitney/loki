package arrowflight

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/grafana/loki/v3/pkg/dataobj"
	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight/scanpb"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/logs"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/streams"
)

// defaultBatchSize is the number of rows requested from section readers per
// call. Pages in data objects are typically a few thousand rows.
const defaultBatchSize = 4096

// Scanner streams Arrow record batches for one Flight ticket. Every batch
// matches [Scanner.Schema]; callers must release batches after use.
type Scanner interface {
	Schema() *arrow.Schema
	// Next returns the next batch, or io.EOF when the scan is complete.
	Next(ctx context.Context) (arrow.RecordBatch, error)
	Close() error
}

// Scan opens a Scanner for the section and request described by tkt.
func (c *Catalog) Scan(ctx context.Context, tkt *scanpb.Ticket) (Scanner, error) {
	req := tkt.GetRequest()
	if req == nil {
		return nil, errors.New("ticket has no scan request")
	}

	ts, ok := c.tables[req.GetTable()]
	if !ok {
		return nil, fmt.Errorf("unknown table %q", req.GetTable())
	}

	schema, err := ts.Project(req.GetColumns())
	if err != nil {
		return nil, err
	}

	info, sec, err := c.section(ctx, tkt.GetObjectPath(), int(tkt.GetSectionIndex()))
	if err != nil {
		return nil, err
	}

	switch req.GetTable() {
	case TableLogs:
		if !logs.CheckSection(sec) {
			return nil, fmt.Errorf("section %d of %s is not a logs section", tkt.GetSectionIndex(), tkt.GetObjectPath())
		}
		return c.newLogsScanner(ctx, ts, schema, info, sec, req)

	case TableStreams:
		if !streams.CheckSection(sec) {
			return nil, fmt.Errorf("section %d of %s is not a streams section", tkt.GetSectionIndex(), tkt.GetObjectPath())
		}
		return newStreamsScanner(ctx, ts, schema, sec, req)
	}

	return nil, fmt.Errorf("unknown table %q", req.GetTable())
}

// emptyScanner is returned when predicates prove that a section holds no
// matching rows. It still carries the schema so the Flight stream is valid.
type emptyScanner struct{ schema *arrow.Schema }

func (s *emptyScanner) Schema() *arrow.Schema                           { return s.schema }
func (s *emptyScanner) Next(context.Context) (arrow.RecordBatch, error) { return nil, io.EOF }
func (s *emptyScanner) Close() error                                    { return nil }

// outputColumn describes how one field of the output schema is produced.
type outputColumn struct {
	// readerIndex is the index of the column in records returned by the
	// section reader, or -1 if the column is not read from the section.
	readerIndex int
	// label is set when the column is a stream label joined in from the
	// streams section.
	label string
}

// buildBatch assembles an output record from a reader record using the given
// output column plan. Columns absent from the section are filled with nulls.
// Label columns are looked up per row in st via the stream ID column at
// streamIDIndex.
func buildBatch(schema *arrow.Schema, outputs []outputColumn, rec arrow.RecordBatch, st *streamsTable, streamIDIndex int, alloc memory.Allocator) arrow.RecordBatch {
	n := int(rec.NumRows())
	arrs := make([]arrow.Array, len(outputs))

	var streamIDs *array.Int64
	if streamIDIndex >= 0 {
		streamIDs = rec.Column(streamIDIndex).(*array.Int64)
	}

	for i, oc := range outputs {
		switch {
		case oc.label != "":
			b := array.NewStringBuilder(alloc)
			b.Reserve(n)
			for r := range n {
				if streamIDs.IsNull(r) {
					b.AppendNull()
					continue
				}
				if v, ok := st.label(streamIDs.Value(r), oc.label); ok {
					b.Append(v)
				} else {
					b.AppendNull()
				}
			}
			arrs[i] = b.NewArray()
			b.Release()

		case oc.readerIndex >= 0:
			arr := rec.Column(oc.readerIndex)
			arr.Retain()
			arrs[i] = arr

		default:
			arrs[i] = array.MakeArrayOfNull(alloc, schema.Field(i).Type, n)
		}
	}

	out := array.NewRecordBatch(schema, arrs, int64(n))
	for _, arr := range arrs {
		arr.Release() // NewRecordBatch retains its columns.
	}
	return out
}

type logsScanner struct {
	schema        *arrow.Schema
	reader        *logs.Reader
	streams       *streamsTable
	outputs       []outputColumn
	streamIDIndex int
	alloc         memory.Allocator
	done          bool
}

func (c *objectCache) newLogsScanner(ctx context.Context, ts *TableSchema, schema *arrow.Schema, info *objectInfo, sec *dataobj.Section, req *scanpb.ScanRequest) (Scanner, error) {
	logsSec, err := logs.Open(ctx, sec)
	if err != nil {
		return nil, fmt.Errorf("opening logs section: %w", err)
	}

	var (
		streamIDCol  *logs.Column
		timestampCol *logs.Column
		messageCol   *logs.Column
		metadataCols = make(map[string]*logs.Column)
	)
	for _, col := range logsSec.Columns() {
		switch col.Type {
		case logs.ColumnTypeStreamID:
			streamIDCol = col
		case logs.ColumnTypeTimestamp:
			timestampCol = col
		case logs.ColumnTypeMessage:
			messageCol = col
		case logs.ColumnTypeMetadata:
			metadataCols[col.Name] = col
		}
	}
	if streamIDCol == nil {
		return nil, errors.New("logs section has no stream ID column")
	}

	var (
		readCols  []*logs.Column
		readIndex = make(map[*logs.Column]int)
	)
	addRead := func(col *logs.Column) int {
		if col == nil {
			return -1
		}
		if i, ok := readIndex[col]; ok {
			return i
		}
		readIndex[col] = len(readCols)
		readCols = append(readCols, col)
		return len(readCols) - 1
	}

	var (
		outputs    = make([]outputColumn, schema.NumFields())
		needLabels bool
	)
	for i, f := range schema.Fields() {
		b, ok := ts.binding(f.Name)
		if !ok {
			return nil, fmt.Errorf("column %q has no binding in table %s", f.Name, ts.Name)
		}

		switch b.Kind {
		case columnKindFixed:
			switch b.Source {
			case ColumnStreamID:
				outputs[i] = outputColumn{readerIndex: addRead(streamIDCol)}
			case ColumnTimestamp:
				outputs[i] = outputColumn{readerIndex: addRead(timestampCol)}
			case ColumnMessage:
				outputs[i] = outputColumn{readerIndex: addRead(messageCol)}
			default:
				outputs[i] = outputColumn{readerIndex: -1}
			}
		case columnKindMetadata:
			outputs[i] = outputColumn{readerIndex: addRead(metadataCols[b.Source])}
		case columnKindLabel:
			needLabels = true
			outputs[i] = outputColumn{readerIndex: -1, label: b.Source}
		}
	}

	// The streams table is only needed for projected label columns and label
	// predicates, and only for those labels. When the request already names
	// the stream IDs (the metastore resolved the label predicates), only
	// those streams are loaded.
	wantLabels := map[string]struct{}{}
	for _, o := range outputs {
		if o.label != "" {
			wantLabels[o.label] = struct{}{}
		}
	}
	var wantIDs []int64
	for _, p := range req.GetPredicates() {
		b, ok := ts.binding(p.GetColumn())
		if !ok {
			continue
		}
		switch {
		case b.Kind == columnKindLabel:
			wantLabels[b.Source] = struct{}{}
		case b.Kind == columnKindFixed && b.Source == ColumnStreamID && p.GetOp() == scanpb.Op_OP_IN:
			for _, lit := range p.GetValues() {
				if v, ok := lit.GetValue().(*scanpb.Literal_Int64Value); ok {
					wantIDs = append(wantIDs, v.Int64Value)
				}
			}
		}
	}
	needStreams := needLabels || len(wantLabels) > 0

	var st *streamsTable
	if needStreams {
		want := streamsWant{ids: wantIDs, labels: slices.Sorted(maps.Keys(wantLabels))}
		st, err = c.streamsFor(ctx, info, sec.Tenant, want)
		if err != nil {
			return nil, err
		}
	}

	streamIDIndex := -1
	if needLabels {
		streamIDIndex = addRead(streamIDCol)
	}

	pred, empty, err := logsPredicates(ts, logsSec, st, req.GetPredicates())
	if err != nil {
		return nil, err
	}
	if empty {
		return &emptyScanner{schema: schema}, nil
	}

	// A projection with no section-backed columns (for example, only labels
	// or only absent metadata keys) still needs a column to drive row counts.
	if len(readCols) == 0 {
		addRead(streamIDCol)
	}

	opts := logs.ReaderOptions{Columns: readCols, Allocator: memory.DefaultAllocator}
	if pred != nil {
		opts.Predicates = []logs.Predicate{pred}
	}
	reader := logs.NewReader(opts)
	if err := reader.Open(ctx); err != nil {
		return nil, fmt.Errorf("opening logs reader: %w", err)
	}

	return &logsScanner{
		schema:        schema,
		reader:        reader,
		streams:       st,
		outputs:       outputs,
		streamIDIndex: streamIDIndex,
		alloc:         memory.DefaultAllocator,
	}, nil
}

func (s *logsScanner) Schema() *arrow.Schema { return s.schema }

func (s *logsScanner) Next(ctx context.Context) (arrow.RecordBatch, error) {
	if s.done {
		return nil, io.EOF
	}

	for {
		rec, err := s.reader.Read(ctx, defaultBatchSize)
		if rec == nil {
			if err == nil {
				// No rows in this chunk matched the predicates; keep reading.
				continue
			}
			return nil, err
		}

		out := buildBatch(s.schema, s.outputs, rec, s.streams, s.streamIDIndex, s.alloc)
		rec.Release()

		if errors.Is(err, io.EOF) {
			s.done = true
		} else if err != nil {
			out.Release()
			return nil, err
		}
		return out, nil
	}
}

func (s *logsScanner) Close() error { return s.reader.Close() }

type streamsScanner struct {
	schema  *arrow.Schema
	reader  *streams.Reader
	outputs []outputColumn
	alloc   memory.Allocator
	done    bool
}

func newStreamsScanner(ctx context.Context, ts *TableSchema, schema *arrow.Schema, sec *dataobj.Section, req *scanpb.ScanRequest) (Scanner, error) {
	streamsSec, err := streams.Open(ctx, sec)
	if err != nil {
		return nil, fmt.Errorf("opening streams section: %w", err)
	}

	fixedCols := make(map[streams.ColumnType]*streams.Column)
	labelCols := make(map[string]*streams.Column)
	for _, col := range streamsSec.Columns() {
		if col.Type == streams.ColumnTypeLabel {
			labelCols[col.Name] = col
		} else {
			fixedCols[col.Type] = col
		}
	}

	var (
		readCols  []*streams.Column
		readIndex = make(map[*streams.Column]int)
	)
	addRead := func(col *streams.Column) int {
		if col == nil {
			return -1
		}
		if i, ok := readIndex[col]; ok {
			return i
		}
		readIndex[col] = len(readCols)
		readCols = append(readCols, col)
		return len(readCols) - 1
	}

	fixedByName := map[string]streams.ColumnType{
		ColumnStreamID:         streams.ColumnTypeStreamID,
		ColumnMinTimestamp:     streams.ColumnTypeMinTimestamp,
		ColumnMaxTimestamp:     streams.ColumnTypeMaxTimestamp,
		ColumnRows:             streams.ColumnTypeRows,
		ColumnUncompressedSize: streams.ColumnTypeUncompressedSize,
	}

	outputs := make([]outputColumn, schema.NumFields())
	for i, f := range schema.Fields() {
		b, ok := ts.binding(f.Name)
		if !ok {
			return nil, fmt.Errorf("column %q has no binding in table %s", f.Name, ts.Name)
		}
		switch b.Kind {
		case columnKindFixed:
			outputs[i] = outputColumn{readerIndex: addRead(fixedCols[fixedByName[b.Source]])}
		case columnKindLabel:
			outputs[i] = outputColumn{readerIndex: addRead(labelCols[b.Source])}
		default:
			outputs[i] = outputColumn{readerIndex: -1}
		}
	}

	pred, empty, err := streamsPredicates(ts, streamsSec, req.GetPredicates())
	if err != nil {
		return nil, err
	}
	if empty {
		return &emptyScanner{schema: schema}, nil
	}

	if len(readCols) == 0 {
		if idCol := fixedCols[streams.ColumnTypeStreamID]; idCol != nil {
			addRead(idCol)
		} else {
			return nil, errors.New("streams section has no stream ID column")
		}
	}

	opts := streams.ReaderOptions{Columns: readCols, Allocator: memory.DefaultAllocator}
	if pred != nil {
		opts.Predicates = []streams.Predicate{pred}
	}
	reader := streams.NewReader(opts)
	if err := reader.Open(ctx); err != nil {
		return nil, fmt.Errorf("opening streams reader: %w", err)
	}

	return &streamsScanner{
		schema:  schema,
		reader:  reader,
		outputs: outputs,
		alloc:   memory.DefaultAllocator,
	}, nil
}

func (s *streamsScanner) Schema() *arrow.Schema { return s.schema }

func (s *streamsScanner) Next(ctx context.Context) (arrow.RecordBatch, error) {
	if s.done {
		return nil, io.EOF
	}

	for {
		rec, err := s.reader.Read(ctx, defaultBatchSize)
		if rec == nil {
			if err == nil {
				continue
			}
			return nil, err
		}

		out := buildBatch(s.schema, s.outputs, rec, nil, -1, s.alloc)
		rec.Release()

		if errors.Is(err, io.EOF) {
			s.done = true
		} else if err != nil {
			out.Release()
			return nil, err
		}
		return out, nil
	}
}

func (s *streamsScanner) Close() error { return s.reader.Close() }
