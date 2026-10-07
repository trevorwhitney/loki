package arrowflight

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
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
	bytes  int64 // estimated memory footprint, for the cache bound
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

// objectCache opens data objects on demand and keeps them, together with the
// in-memory streams tables used to join labels into log rows. It is shared by
// the walking [Catalog] and the [MetastoreCatalog].
type objectCache struct {
	bucket        objstore.BucketReader
	logger        log.Logger
	prefetchBytes int64
	// maxObjects bounds the opened objects kept by a cache that discovers
	// objects per request (the metastore catalog); the walking catalog holds
	// all of its objects regardless.
	maxObjects int
	// maxStreamsBytes bounds the estimated size of the cached streams tables.
	maxStreamsBytes int64

	mu           sync.Mutex
	objects      map[string]*objectInfo
	streamsCache map[string]*list.Element // key -> *streamsEntry in streamsLRU
	streamsLRU   *list.List               // front is most recently used
	streamsBytes int64
}

type streamsEntry struct {
	key   string
	table *streamsTable
}

// Defaults for the caches. A tenant with hundreds of thousands of streams
// per object makes a full streams table tens of MB, so the byte bound, not
// the object count, is what keeps a wide scan inside the pod's memory.
const (
	DefaultMaxCachedObjects  = 256
	DefaultStreamsCacheBytes = 256 << 20
)

func newObjectCache(bucket objstore.BucketReader, logger log.Logger) *objectCache {
	return &objectCache{
		bucket:          bucket,
		logger:          logger,
		maxObjects:      DefaultMaxCachedObjects,
		maxStreamsBytes: DefaultStreamsCacheBytes,
		objects:         make(map[string]*objectInfo),
		streamsCache:    make(map[string]*list.Element),
		streamsLRU:      list.New(),
	}
}

// add registers an already opened object.
func (oc *objectCache) add(info *objectInfo) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.objects[info.Path] = info
}

// object returns the opened object at path, opening it from the bucket on
// first use.
func (oc *objectCache) object(ctx context.Context, path string) (*objectInfo, error) {
	oc.mu.Lock()
	info, ok := oc.objects[path]
	oc.mu.Unlock()
	if ok {
		return info, nil
	}

	obj, err := dataobj.FromBucket(ctx, oc.bucket, path, oc.prefetchBytes)
	if err != nil {
		return nil, fmt.Errorf("opening data object %s: %w", path, err)
	}
	info = &objectInfo{Path: path, Object: obj}

	oc.mu.Lock()
	defer oc.mu.Unlock()
	if oc.maxObjects > 0 && len(oc.objects) >= oc.maxObjects {
		oc.objects = make(map[string]*objectInfo)
		oc.streamsCache = make(map[string]*list.Element)
		oc.streamsLRU.Init()
		oc.streamsBytes = 0
	}
	if existing, ok := oc.objects[path]; ok {
		return existing, nil
	}
	oc.objects[path] = info
	return info, nil
}

// section resolves an object path and section index.
func (oc *objectCache) section(ctx context.Context, path string, index int) (*objectInfo, *dataobj.Section, error) {
	info, err := oc.object(ctx, path)
	if err != nil {
		return nil, nil, err
	}
	sections := info.Object.Sections()
	if index < 0 || index >= len(sections) {
		return nil, nil, fmt.Errorf("section index %d out of range for %s (%d sections)", index, path, len(sections))
	}
	return info, sections[index], nil
}

// Catalog holds the set of data objects served by a [Server] and the inferred
// schemas of the logs and streams tables. It discovers objects by walking a
// bucket prefix once; see [MetastoreCatalog] for index-driven discovery.
type Catalog struct {
	*objectCache
	logger log.Logger

	paths  []string // sorted keys of objects
	tables map[string]*TableSchema
}

// OpenCatalog discovers every data object under prefix in bucket and infers
// the table schemas from the union of columns across all of them.
func OpenCatalog(ctx context.Context, bucket objstore.BucketReader, prefix string, logger log.Logger) (*Catalog, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	c := &Catalog{
		objectCache: newObjectCache(bucket, logger),
		logger:      logger,
		tables:      make(map[string]*TableSchema),
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
		c.add(&objectInfo{Path: path, Object: obj})
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

// streamsWant narrows what a streams table is loaded for: only the named
// label columns are read (nil means all), and when IDs are given only those
// streams are kept. A scan whose stream IDs the metastore already resolved
// needs a table of a few streams and one or two labels, not the whole
// section, which for a large tenant is tens of MB per object.
type streamsWant struct {
	ids    []int64
	labels []string
}

func (w streamsWant) cacheKey(path string, section int) string {
	return fmt.Sprintf("%s#%d|%s", path, section, strings.Join(w.labels, ","))
}

// streamsFor returns the in-memory streams table of the given tenant within
// the data object. Tables without an ID filter are cached (bounded by
// maxStreamsBytes, least recently used first); filtered ones are built per
// scan, they are small and specific to the ticket.
func (c *objectCache) streamsFor(ctx context.Context, info *objectInfo, tenant string, want streamsWant) (*streamsTable, error) {
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

	cacheable := len(want.ids) == 0
	key := want.cacheKey(info.Path, streamsIdx)
	if cacheable {
		c.mu.Lock()
		if el, ok := c.streamsCache[key]; ok {
			c.streamsLRU.MoveToFront(el)
			st := el.Value.(*streamsEntry).table
			c.mu.Unlock()
			return st, nil
		}
		c.mu.Unlock()
	}

	st, err := loadStreamsTable(ctx, streamsSec, want)
	if err != nil {
		return nil, fmt.Errorf("loading streams section of %s: %w", info.Path, err)
	}
	if cacheable {
		c.mu.Lock()
		if _, ok := c.streamsCache[key]; !ok {
			c.streamsCache[key] = c.streamsLRU.PushFront(&streamsEntry{key: key, table: st})
			c.streamsBytes += st.bytes
			for c.maxStreamsBytes > 0 && c.streamsBytes > c.maxStreamsBytes && c.streamsLRU.Len() > 1 {
				el := c.streamsLRU.Back()
				e := el.Value.(*streamsEntry)
				c.streamsLRU.Remove(el)
				delete(c.streamsCache, e.key)
				c.streamsBytes -= e.table.bytes
			}
		}
		c.mu.Unlock()
	}
	return st, nil
}

func loadStreamsTable(ctx context.Context, sec *dataobj.Section, want streamsWant) (*streamsTable, error) {
	ss, err := streams.Open(ctx, sec)
	if err != nil {
		return nil, err
	}

	var wantLabels map[string]struct{}
	if want.labels != nil {
		wantLabels = make(map[string]struct{}, len(want.labels))
		for _, l := range want.labels {
			wantLabels[l] = struct{}{}
		}
	}
	var keep map[int64]struct{}
	if len(want.ids) > 0 {
		keep = make(map[int64]struct{}, len(want.ids))
		for _, id := range want.ids {
			keep[id] = struct{}{}
		}
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
			if wantLabels != nil {
				if _, ok := wantLabels[col.Name]; !ok {
					continue
				}
			}
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
				id := ids.Value(r)
				if keep != nil {
					if _, ok := keep[id]; !ok {
						continue
					}
				}
				lbls := make(map[string]string, len(labelCols))
				st.bytes += 48 // id, index entry, map header
				for j, col := range labelCols {
					arr := rec.Column(j + 1).(*array.String)
					if arr.IsValid(r) {
						v := arr.Value(r)
						lbls[col.Name] = v
						st.bytes += int64(len(col.Name) + len(v) + 32)
					}
				}
				st.byID[id] = len(st.ids)
				st.ids = append(st.ids, id)
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
