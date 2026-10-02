package arrowflight

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/thanos-io/objstore"

	"github.com/grafana/loki/v3/pkg/dataobj"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/logs"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/streams"
)

// Partition is one independently scannable unit of a table: a single section
// of a single data object.
type Partition struct {
	ObjectPath   string
	SectionIndex int
}

type objectInfo struct {
	Path   string
	Object *dataobj.Object
}

// streamsTable is an in-memory copy of a streams section. It is used to join
// labels into log rows and to resolve label predicates into stream IDs.
type streamsTable struct {
	ids    []int64
	labels []map[string]string // parallel to ids
	byID   map[int64]int
}

// label returns the value of the named label for the given stream.
func (st *streamsTable) label(id int64, name string) (string, bool) {
	idx, ok := st.byID[id]
	if !ok {
		return "", false
	}
	v, ok := st.labels[idx][name]
	return v, ok
}

// matchingIDs returns the IDs of every stream whose labels satisfy keep.
func (st *streamsTable) matchingIDs(keep func(labels map[string]string) bool) []int64 {
	var ids []int64
	for i, lbls := range st.labels {
		if keep(lbls) {
			ids = append(ids, st.ids[i])
		}
	}
	return ids
}

// Catalog holds the set of data objects served by a [Server] and the inferred
// schemas of the logs and streams tables.
type Catalog struct {
	bucket objstore.BucketReader
	logger log.Logger

	objects map[string]*objectInfo
	paths   []string // sorted keys of objects
	tables  map[string]*TableSchema

	streamsMu    sync.Mutex
	streamsCache map[string]*streamsTable // keyed by object path and section index
}

// OpenCatalog discovers every data object under prefix in bucket and infers
// the table schemas from the union of columns across all of them.
func OpenCatalog(ctx context.Context, bucket objstore.BucketReader, prefix string, logger log.Logger) (*Catalog, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	c := &Catalog{
		bucket:       bucket,
		logger:       logger,
		objects:      make(map[string]*objectInfo),
		tables:       make(map[string]*TableSchema),
		streamsCache: make(map[string]*streamsTable),
	}

	var paths []string
	err := bucket.Iter(ctx, prefix, func(name string) error {
		paths = append(paths, name)
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return nil, fmt.Errorf("listing objects under %q: %w", prefix, err)
	}
	slices.Sort(paths)

	labelNames := make(map[string]struct{})
	metadataKeys := make(map[string]struct{})

	for _, path := range paths {
		obj, err := dataobj.FromBucket(ctx, bucket, path, 0)
		if err != nil {
			return nil, fmt.Errorf("opening data object %s: %w", path, err)
		}

		var numLogs, numStreams int
		for _, sec := range obj.Sections() {
			switch {
			case logs.CheckSection(sec):
				numLogs++
				ls, err := logs.Open(ctx, sec)
				if err != nil {
					return nil, fmt.Errorf("opening logs section of %s: %w", path, err)
				}
				for _, col := range ls.Columns() {
					if col.Type == logs.ColumnTypeMetadata {
						metadataKeys[col.Name] = struct{}{}
					}
				}

			case streams.CheckSection(sec):
				numStreams++
				ss, err := streams.Open(ctx, sec)
				if err != nil {
					return nil, fmt.Errorf("opening streams section of %s: %w", path, err)
				}
				for _, col := range ss.Columns() {
					if col.Type == streams.ColumnTypeLabel {
						labelNames[col.Name] = struct{}{}
					}
				}
			}
		}

		level.Debug(logger).Log("msg", "discovered data object", "path", path, "logs_sections", numLogs, "streams_sections", numStreams)
		c.objects[path] = &objectInfo{Path: path, Object: obj}
		c.paths = append(c.paths, path)
	}

	labels := slices.Sorted(maps.Keys(labelNames))
	metadata := slices.Sorted(maps.Keys(metadataKeys))
	c.tables[TableLogs] = NewLogsSchema(labels, metadata)
	c.tables[TableStreams] = NewStreamsSchema(labels)

	level.Info(logger).Log("msg", "catalog opened", "objects", len(c.paths), "labels", len(labels), "metadata_keys", len(metadata))
	return c, nil
}

// Tables returns the names of the served tables in a stable order.
func (c *Catalog) Tables() []string {
	return slices.Sorted(maps.Keys(c.tables))
}

// Table returns the schema of the named table.
func (c *Catalog) Table(name string) (*TableSchema, bool) {
	ts, ok := c.tables[name]
	return ts, ok
}

// Partitions returns every section that contributes rows to the named table.
func (c *Catalog) Partitions(table string) ([]Partition, error) {
	var check func(*dataobj.Section) bool
	switch table {
	case TableLogs:
		check = logs.CheckSection
	case TableStreams:
		check = streams.CheckSection
	default:
		return nil, fmt.Errorf("unknown table %q", table)
	}

	var parts []Partition
	for _, path := range c.paths {
		for i, sec := range c.objects[path].Object.Sections() {
			if check(sec) {
				parts = append(parts, Partition{ObjectPath: path, SectionIndex: i})
			}
		}
	}
	return parts, nil
}

// section resolves a partition to its data object and section.
func (c *Catalog) section(path string, index int) (*objectInfo, *dataobj.Section, error) {
	info, ok := c.objects[path]
	if !ok {
		return nil, nil, fmt.Errorf("unknown data object %q", path)
	}
	sections := info.Object.Sections()
	if index < 0 || index >= len(sections) {
		return nil, nil, fmt.Errorf("section index %d out of range for %s (%d sections)", index, path, len(sections))
	}
	return info, sections[index], nil
}

// streamsFor returns the in-memory streams table of the given tenant within
// the data object, loading and caching it on first use.
func (c *Catalog) streamsFor(ctx context.Context, info *objectInfo, tenant string) (*streamsTable, error) {
	var (
		streamsSec *dataobj.Section
		streamsIdx int
	)
	for i, sec := range info.Object.Sections() {
		if streams.CheckSection(sec) && sec.Tenant == tenant {
			streamsSec, streamsIdx = sec, i
			break
		}
	}
	if streamsSec == nil {
		return nil, fmt.Errorf("data object %s has no streams section for tenant %q", info.Path, tenant)
	}

	key := fmt.Sprintf("%s#%d", info.Path, streamsIdx)

	c.streamsMu.Lock()
	defer c.streamsMu.Unlock()

	if st, ok := c.streamsCache[key]; ok {
		return st, nil
	}

	st, err := loadStreamsTable(ctx, streamsSec)
	if err != nil {
		return nil, fmt.Errorf("loading streams section of %s: %w", info.Path, err)
	}
	c.streamsCache[key] = st
	return st, nil
}

func loadStreamsTable(ctx context.Context, sec *dataobj.Section) (*streamsTable, error) {
	ss, err := streams.Open(ctx, sec)
	if err != nil {
		return nil, err
	}

	var (
		idCol     *streams.Column
		labelCols []*streams.Column
	)
	for _, col := range ss.Columns() {
		switch col.Type {
		case streams.ColumnTypeStreamID:
			idCol = col
		case streams.ColumnTypeLabel:
			labelCols = append(labelCols, col)
		}
	}
	if idCol == nil {
		return nil, errors.New("streams section has no stream ID column")
	}

	reader := streams.NewReader(streams.ReaderOptions{
		Columns: append([]*streams.Column{idCol}, labelCols...),
	})
	defer reader.Close()

	if err := reader.Open(ctx); err != nil {
		return nil, fmt.Errorf("opening streams reader: %w", err)
	}

	st := &streamsTable{byID: make(map[int64]int)}
	for {
		rec, err := reader.Read(ctx, 1024)
		if rec != nil {
			ids := rec.Column(0).(*array.Int64)
			for r := range ids.Len() {
				lbls := make(map[string]string, len(labelCols))
				for j, col := range labelCols {
					arr := rec.Column(j + 1).(*array.String)
					if arr.IsValid(r) {
						lbls[col.Name] = arr.Value(r)
					}
				}
				st.byID[ids.Value(r)] = len(st.ids)
				st.ids = append(st.ids, ids.Value(r))
				st.labels = append(st.labels, lbls)
			}
			rec.Release()
		}
		if errors.Is(err, io.EOF) {
			return st, nil
		} else if err != nil {
			return nil, fmt.Errorf("reading streams: %w", err)
		}
	}
}
