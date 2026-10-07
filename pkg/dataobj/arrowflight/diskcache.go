package arrowflight

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
)

// DiskCacheConfig configures the optional read-through disk cache in front
// of the object stores the scan service reads. It exists to separate the
// object-store part of a query's cost (requests, fetch time) from the CPU
// part: a query run twice reads everything from local disk the second time,
// so the two runs bracket the cost of query in place.
type DiskCacheConfig struct {
	// Dir is the directory that holds cached ranges. Empty disables the cache.
	Dir string `yaml:"dir"`
	// MaxSizeBytes bounds the bytes kept on disk; the least recently used
	// entries are evicted past it. 0 means 10 GiB.
	MaxSizeBytes int64 `yaml:"max_size_bytes"`
	// Buckets names the stores that go through the cache ("data" for data
	// objects and the metastore index, "archive" for archive objects). Empty
	// means all. A store whose working set is far larger than the cache is
	// better left uncached: once the cache is full every miss also costs a
	// rename and an unlink, and on a node's ephemeral disk that is IOPS
	// bound (a full-cell scan slowed 6x when its caches filled).
	Buckets flagext.StringSliceCSV `yaml:"buckets"`
}

// caches reports whether the named store should go through the cache.
func (c DiskCacheConfig) caches(name string) bool {
	if c.Dir == "" {
		return false
	}
	if len(c.Buckets) == 0 {
		return true
	}
	for _, b := range c.Buckets {
		if strings.EqualFold(strings.TrimSpace(b), name) {
			return true
		}
	}
	return false
}

const defaultDiskCacheSize = 10 << 30

// DiskCacheMetrics counts cache traffic per bucket.
type DiskCacheMetrics struct {
	hits      *prometheus.CounterVec
	misses    *prometheus.CounterVec
	bytes     *prometheus.GaugeVec
	errors    *prometheus.CounterVec
	evictions *prometheus.CounterVec
}

// NewDiskCacheMetrics registers the disk cache metrics with reg.
func NewDiskCacheMetrics(reg prometheus.Registerer) *DiskCacheMetrics {
	m := &DiskCacheMetrics{
		hits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "loki", Subsystem: "dataobj_flight", Name: "disk_cache_hits_total",
			Help: "Object store reads answered from the local disk cache.",
		}, []string{"bucket"}),
		misses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "loki", Subsystem: "dataobj_flight", Name: "disk_cache_misses_total",
			Help: "Object store reads that went to the bucket and were added to the disk cache.",
		}, []string{"bucket"}),
		bytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "loki", Subsystem: "dataobj_flight", Name: "disk_cache_bytes",
			Help: "Bytes held in the disk cache.",
		}, []string{"bucket"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "loki", Subsystem: "dataobj_flight", Name: "disk_cache_errors_total",
			Help: "Disk cache operations that failed and fell through to the bucket.",
		}, []string{"bucket"}),
		evictions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "loki", Subsystem: "dataobj_flight", Name: "disk_cache_evictions_total",
			Help: "Entries evicted from the disk cache because it was over its size bound. A high rate means the working set does not fit.",
		}, []string{"bucket"}),
	}
	if reg != nil {
		reg.MustRegister(m.hits, m.misses, m.bytes, m.errors, m.evictions)
	}
	return m
}

// diskCacheBucket is an objstore.Bucket whose Get and GetRange are served
// from files under a directory once they have been read from the inner
// bucket. Entries are keyed by object name, offset and length, so a hit
// needs exactly the same range: the data object reader asks for the same
// ranges when a query repeats, and the archive reads whole objects.
//
// Objects are immutable, so entries never go stale. Writes, deletes and
// listings pass through uncached.
type diskCacheBucket struct {
	objstore.Bucket

	name    string
	dir     string
	maxSize int64
	logger  log.Logger
	metrics *DiskCacheMetrics

	mu      sync.Mutex
	entries map[string]*list.Element // key -> *cacheEntry in lru
	lru     *list.List               // front is most recently used
	total   int64
}

type cacheEntry struct {
	key  string
	size int64
}

// NewDiskCacheBucket wraps inner with a read-through disk cache rooted at
// cfg.Dir/name. Existing files under that directory are adopted, so a cache
// survives a container restart when the directory is a volume.
func NewDiskCacheBucket(inner objstore.Bucket, name string, cfg DiskCacheConfig, metrics *DiskCacheMetrics, logger log.Logger) (objstore.Bucket, error) {
	if !cfg.caches(name) {
		return inner, nil
	}
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if metrics == nil {
		metrics = NewDiskCacheMetrics(nil)
	}
	maxSize := cfg.MaxSizeBytes
	if maxSize <= 0 {
		maxSize = defaultDiskCacheSize
	}
	dir := filepath.Join(cfg.Dir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating disk cache directory %s: %w", dir, err)
	}
	b := &diskCacheBucket{
		Bucket:  inner,
		name:    name,
		dir:     dir,
		maxSize: maxSize,
		logger:  logger,
		metrics: metrics,
		entries: make(map[string]*list.Element),
		lru:     list.New(),
	}
	if err := b.adopt(); err != nil {
		return nil, err
	}
	level.Info(logger).Log("msg", "disk cache enabled", "bucket", name, "dir", dir, "max_size", maxSize, "adopted_entries", len(b.entries), "adopted_bytes", b.total)
	return b, nil
}

// adopt registers files already present in the cache directory.
func (b *diskCacheBucket) adopt() error {
	return filepath.WalkDir(b.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ".tmp" {
			return os.Remove(path)
		}
		b.insert(filepath.Base(path), info.Size())
		return nil
	})
}

func cacheKey(name string, off, length int64) string {
	sum := sha256.Sum256([]byte(name + "\x00" + strconv.FormatInt(off, 10) + "\x00" + strconv.FormatInt(length, 10)))
	return hex.EncodeToString(sum[:])
}

func (b *diskCacheBucket) path(key string) string {
	return filepath.Join(b.dir, key[:2], key)
}

// Get implements objstore.BucketReader.
func (b *diskCacheBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.cached(cacheKey(name, 0, -1), func() (io.ReadCloser, error) { return b.Bucket.Get(ctx, name) })
}

// GetRange implements objstore.BucketReader.
func (b *diskCacheBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.cached(cacheKey(name, off, length), func() (io.ReadCloser, error) { return b.Bucket.GetRange(ctx, name, off, length) })
}

// cached serves key from disk, or from fetch while writing the bytes to
// disk. A reader that is closed before it reaches EOF leaves no entry.
func (b *diskCacheBucket) cached(key string, fetch func() (io.ReadCloser, error)) (io.ReadCloser, error) {
	if f, err := os.Open(b.path(key)); err == nil {
		b.touch(key)
		b.metrics.hits.WithLabelValues(b.name).Inc()
		return f, nil
	}

	rc, err := fetch()
	if err != nil {
		return nil, err
	}
	b.metrics.misses.WithLabelValues(b.name).Inc()

	if err := os.MkdirAll(filepath.Dir(b.path(key)), 0o755); err != nil {
		b.metrics.errors.WithLabelValues(b.name).Inc()
		level.Warn(b.logger).Log("msg", "disk cache unavailable, serving from bucket", "err", err)
		return rc, nil
	}
	tmp, err := os.CreateTemp(filepath.Dir(b.path(key)), key+".*.tmp")
	if err != nil {
		b.metrics.errors.WithLabelValues(b.name).Inc()
		level.Warn(b.logger).Log("msg", "disk cache unavailable, serving from bucket", "err", err)
		return rc, nil
	}
	return &fillingReader{
		Reader: io.TeeReader(rc, tmp),
		src:    rc,
		tmp:    tmp,
		commit: func(size int64) {
			if err := os.Rename(tmp.Name(), b.path(key)); err != nil {
				b.metrics.errors.WithLabelValues(b.name).Inc()
				_ = os.Remove(tmp.Name())
				return
			}
			b.insert(key, size)
			b.evict()
		},
		abort: func() { _ = os.Remove(tmp.Name()) },
	}, nil
}

// fillingReader copies what the caller reads into tmp and commits the file
// once the source is exhausted.
type fillingReader struct {
	io.Reader
	src    io.ReadCloser
	tmp    *os.File
	size   int64
	eof    bool
	commit func(size int64)
	abort  func()
}

func (r *fillingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.size += int64(n)
	if errors.Is(err, io.EOF) {
		r.eof = true
	}
	return n, err
}

func (r *fillingReader) Close() error {
	cerr := r.src.Close()
	if !r.eof {
		// Drain so a partially consumed range still becomes an entry; the
		// server reads whole pages anyway, and a LIMIT query that stops
		// early would otherwise cache nothing.
		if _, err := io.Copy(io.Discard, r); err != nil {
			r.eof = false
		}
	}
	if err := r.tmp.Close(); err != nil || !r.eof {
		r.abort()
		return cerr
	}
	r.commit(r.size)
	return cerr
}

func (b *diskCacheBucket) insert(key string, size int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if el, ok := b.entries[key]; ok {
		b.total += size - el.Value.(*cacheEntry).size
		el.Value.(*cacheEntry).size = size
		b.lru.MoveToFront(el)
	} else {
		b.entries[key] = b.lru.PushFront(&cacheEntry{key: key, size: size})
		b.total += size
	}
	b.metrics.bytes.WithLabelValues(b.name).Set(float64(b.total))
}

func (b *diskCacheBucket) touch(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if el, ok := b.entries[key]; ok {
		b.lru.MoveToFront(el)
	}
}

// evictLowWater is the fraction of maxSize an eviction pass trims the cache
// to, so a full cache does not evict one entry per insert.
const evictLowWater = 0.9

// evict drops least recently used entries until the cache is under
// evictLowWater x maxSize. Victims are chosen under the lock and their files
// removed outside it: with the cache full and dozens of scans missing at
// once, deleting under the lock serialized every insert on disk I/O (a 24 h
// scan slowed 6x once the cache filled).
func (b *diskCacheBucket) evict() {
	b.mu.Lock()
	if b.total <= b.maxSize {
		b.mu.Unlock()
		return
	}
	target := int64(float64(b.maxSize) * evictLowWater)
	var victims []string
	for b.total > target {
		el := b.lru.Back()
		if el == nil {
			break
		}
		e := el.Value.(*cacheEntry)
		b.lru.Remove(el)
		delete(b.entries, e.key)
		b.total -= e.size
		victims = append(victims, e.key)
	}
	b.metrics.bytes.WithLabelValues(b.name).Set(float64(b.total))
	b.mu.Unlock()

	for _, key := range victims {
		if err := os.Remove(b.path(key)); err != nil && !errors.Is(err, os.ErrNotExist) {
			b.metrics.errors.WithLabelValues(b.name).Inc()
		}
	}
	b.metrics.evictions.WithLabelValues(b.name).Add(float64(len(victims)))
}

// Size returns the bytes held by the cache.
func (b *diskCacheBucket) Size() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.total
}
