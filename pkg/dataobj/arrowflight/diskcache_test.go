package arrowflight

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

// countingBucket counts reads that reach the inner bucket.
type countingBucket struct {
	objstore.Bucket
	gets, ranges int
}

func (c *countingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	c.gets++
	return c.Bucket.Get(ctx, name)
}

func (c *countingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	c.ranges++
	return c.Bucket.GetRange(ctx, name, off, length)
}

func newTestCache(t *testing.T, dir string, maxSize int64) (*countingBucket, objstore.Bucket, *DiskCacheMetrics) {
	t.Helper()
	inner := &countingBucket{Bucket: objstore.NewInMemBucket()}
	require.NoError(t, inner.Upload(context.Background(), "obj", strings.NewReader("0123456789abcdef")))
	require.NoError(t, inner.Upload(context.Background(), "other", strings.NewReader("zyxwvutsrqponmlk")))
	metrics := NewDiskCacheMetrics(prometheus.NewRegistry())
	cached, err := NewDiskCacheBucket(inner, "test", DiskCacheConfig{Dir: dir, MaxSizeBytes: maxSize}, metrics, log.NewNopLogger())
	require.NoError(t, err)
	return inner, cached, metrics
}

// reader returns a helper that consumes a Get/GetRange result whole.
func reader(t *testing.T) func(rc io.ReadCloser, err error) string {
	return func(rc io.ReadCloser, err error) string {
		t.Helper()
		require.NoError(t, err)
		data, err := io.ReadAll(rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
		return string(data)
	}
}

func TestDiskCacheBucket(t *testing.T) {
	ctx := context.Background()

	t.Run("disabled without a directory", func(t *testing.T) {
		inner := objstore.NewInMemBucket()
		cached, err := NewDiskCacheBucket(inner, "test", DiskCacheConfig{}, nil, nil)
		require.NoError(t, err)
		require.Same(t, inner, cached)
	})

	t.Run("only the named buckets are cached", func(t *testing.T) {
		inner := objstore.NewInMemBucket()
		cfg := DiskCacheConfig{Dir: t.TempDir(), Buckets: []string{"archive"}}
		data, err := NewDiskCacheBucket(inner, "data", cfg, nil, nil)
		require.NoError(t, err)
		require.Same(t, inner, data, "data is not in the list and bypasses the cache")
		archive, err := NewDiskCacheBucket(inner, "archive", cfg, nil, nil)
		require.NoError(t, err)
		require.NotSame(t, inner, archive)
	})

	t.Run("ranges and whole objects are cached by key", func(t *testing.T) {
		read := reader(t)
		inner, cached, metrics := newTestCache(t, t.TempDir(), 0)

		require.Equal(t, "4567", read(cached.GetRange(ctx, "obj", 4, 4)))
		require.Equal(t, "4567", read(cached.GetRange(ctx, "obj", 4, 4)))
		require.Equal(t, 1, inner.ranges, "second read of the same range must hit")

		// A different range of the same object is a different entry.
		require.Equal(t, "89ab", read(cached.GetRange(ctx, "obj", 8, 4)))
		require.Equal(t, 2, inner.ranges)

		require.Equal(t, "0123456789abcdef", read(cached.Get(ctx, "obj")))
		require.Equal(t, "0123456789abcdef", read(cached.Get(ctx, "obj")))
		require.Equal(t, 1, inner.gets)

		require.Equal(t, float64(2), testutil.ToFloat64(metrics.hits.WithLabelValues("test")))
		require.Equal(t, float64(3), testutil.ToFloat64(metrics.misses.WithLabelValues("test")))
		require.Equal(t, float64(4+4+16), testutil.ToFloat64(metrics.bytes.WithLabelValues("test")))
	})

	t.Run("a partially read range is still cached whole", func(t *testing.T) {
		read := reader(t)
		inner, cached, _ := newTestCache(t, t.TempDir(), 0)

		rc, err := cached.GetRange(ctx, "obj", 0, 8)
		require.NoError(t, err)
		buf := make([]byte, 3)
		_, err = io.ReadFull(rc, buf)
		require.NoError(t, err)
		require.NoError(t, rc.Close())

		require.Equal(t, "01234567", read(cached.GetRange(ctx, "obj", 0, 8)))
		require.Equal(t, 1, inner.ranges)
	})

	t.Run("least recently used entries are evicted past the size", func(t *testing.T) {
		read := reader(t)
		inner, cached, metrics := newTestCache(t, t.TempDir(), 10)

		require.Equal(t, "0123", read(cached.GetRange(ctx, "obj", 0, 4)))   // 4 bytes
		require.Equal(t, "4567", read(cached.GetRange(ctx, "obj", 4, 4)))   // 8 bytes
		require.Equal(t, "0123", read(cached.GetRange(ctx, "obj", 0, 4)))   // touch first
		require.Equal(t, "zyxw", read(cached.GetRange(ctx, "other", 0, 4))) // 12 bytes > 10: evict down to 9
		require.Equal(t, 3, inner.ranges)
		require.Equal(t, float64(8), testutil.ToFloat64(metrics.bytes.WithLabelValues("test")))
		require.Equal(t, float64(1), testutil.ToFloat64(metrics.evictions.WithLabelValues("test")))

		require.Equal(t, "0123", read(cached.GetRange(ctx, "obj", 0, 4)))
		require.Equal(t, 3, inner.ranges, "touched entry survives")
		require.Equal(t, "4567", read(cached.GetRange(ctx, "obj", 4, 4)))
		require.Equal(t, 4, inner.ranges, "evicted entry is fetched again")
	})

	t.Run("entries survive a restart", func(t *testing.T) {
		read := reader(t)
		dir := t.TempDir()
		inner, cached, _ := newTestCache(t, dir, 0)
		require.Equal(t, "4567", read(cached.GetRange(ctx, "obj", 4, 4)))
		require.Equal(t, 1, inner.ranges)

		inner2 := &countingBucket{Bucket: objstore.NewInMemBucket()}
		reopened, err := NewDiskCacheBucket(inner2, "test", DiskCacheConfig{Dir: dir}, nil, nil)
		require.NoError(t, err)
		require.Equal(t, "4567", read(reopened.GetRange(ctx, "obj", 4, 4)))
		require.Equal(t, 0, inner2.ranges, "adopted entry served from disk")
		require.Equal(t, int64(4), reopened.(*diskCacheBucket).Size())

		entries, err := filepath.Glob(filepath.Join(dir, "test", "*", "*"))
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})

	t.Run("inner errors pass through", func(t *testing.T) {
		_, cached, metrics := newTestCache(t, t.TempDir(), 0)
		_, err := cached.Get(ctx, "missing")
		require.Error(t, err)
		require.Equal(t, float64(0), testutil.ToFloat64(metrics.misses.WithLabelValues("test")))
	})

	t.Run("writes are not cached", func(t *testing.T) {
		read := reader(t)
		_, cached, _ := newTestCache(t, t.TempDir(), 0)
		require.NoError(t, cached.Upload(ctx, "new", bytes.NewReader([]byte("fresh"))))
		require.Equal(t, "fresh", read(cached.Get(ctx, "new")))
	})
}
