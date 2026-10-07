package arrowflight

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/loki/v3/pkg/dataobj/logsobj"
	"github.com/grafana/loki/v3/pkg/logproto"
)

// streamsFixture writes one data object with three streams of tenant
// "tenant" and returns an object cache over it plus the opened object.
func streamsFixture(t *testing.T) (*objectCache, *objectInfo) {
	t.Helper()
	ctx := context.Background()
	builder, err := logsobj.NewBuilder(logsobj.BuilderBaseConfig{
		TargetPageSize:          2048,
		TargetObjectSize:        1 << 20,
		TargetSectionSize:       8 << 10,
		BufferSize:              2048 * 8,
		SectionStripeMergeLimit: 2,
	}, nil, logsobj.NewBuilderMetrics(), log.NewNopLogger(), nil)
	require.NoError(t, err)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, s := range []struct{ labels, line string }{
		{`{app="nginx", env="prod"}`, "nginx request"},
		{`{app="postgres", env="prod"}`, "postgres query"},
		{`{app="redis"}`, "redis command"},
	} {
		stream := logproto.Stream{Labels: s.labels}
		for i := range 3 {
			stream.Entries = append(stream.Entries, logproto.Entry{Timestamp: base.Add(time.Duration(i) * time.Second), Line: s.line})
		}
		require.NoError(t, builder.Append("tenant", stream, time.Now()))
	}
	obj, closer, err := builder.Flush()
	require.NoError(t, err)
	defer closer.Close()
	rc, err := obj.Reader(ctx)
	require.NoError(t, err)
	defer rc.Close()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)

	bucket, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, bucket.Upload(ctx, "objects/ab/test-object", bytes.NewReader(data)))

	oc := newObjectCache(bucket, log.NewNopLogger())
	info, err := oc.object(ctx, "objects/ab/test-object")
	require.NoError(t, err)
	return oc, info
}

func TestStreamsForNarrowsAndCaches(t *testing.T) {
	ctx := context.Background()
	oc, info := streamsFixture(t)

	full, err := oc.streamsFor(ctx, info, "tenant", streamsWant{})
	require.NoError(t, err)
	require.Len(t, full.ids, 3)
	var nginxID int64 = -1
	for i, lbls := range full.labels {
		if lbls["app"] == "nginx" {
			nginxID = full.ids[i]
			require.Equal(t, "prod", lbls["env"], "a full table carries every label")
		}
	}
	require.NotEqual(t, int64(-1), nginxID)
	require.Positive(t, full.bytes)
	require.Len(t, oc.streamsCache, 1, "the full table is cached")

	// Only the requested label columns are loaded, under a key of their own.
	onlyApp, err := oc.streamsFor(ctx, info, "tenant", streamsWant{labels: []string{"app"}})
	require.NoError(t, err)
	require.Len(t, onlyApp.ids, 3)
	for _, lbls := range onlyApp.labels {
		_, hasEnv := lbls["env"]
		require.False(t, hasEnv)
		require.NotEmpty(t, lbls["app"])
	}
	require.Less(t, onlyApp.bytes, full.bytes)
	require.Len(t, oc.streamsCache, 2)

	again, err := oc.streamsFor(ctx, info, "tenant", streamsWant{labels: []string{"app"}})
	require.NoError(t, err)
	require.Same(t, onlyApp, again, "same labels hit the cache")

	// A table for named stream IDs holds only those streams and is not cached.
	one, err := oc.streamsFor(ctx, info, "tenant", streamsWant{ids: []int64{nginxID}, labels: []string{"app", "env"}})
	require.NoError(t, err)
	require.Equal(t, []int64{nginxID}, one.ids)
	v, ok := one.label(nginxID, "env")
	require.True(t, ok)
	require.Equal(t, "prod", v)
	require.Len(t, oc.streamsCache, 2, "filtered tables are per ticket and not cached")
	require.Equal(t, []int64{nginxID}, one.matchingIDs(func(l map[string]string) bool { return l["app"] == "nginx" }))
	require.Empty(t, one.matchingIDs(func(l map[string]string) bool { return l["app"] == "redis" }), "streams outside the ticket are not visible")
}

func TestStreamsCacheIsBoundedByBytes(t *testing.T) {
	ctx := context.Background()
	oc, info := streamsFixture(t)

	full, err := oc.streamsFor(ctx, info, "tenant", streamsWant{})
	require.NoError(t, err)
	// Room for exactly one full table: adding a second cached table evicts
	// the least recently used one.
	oc.maxStreamsBytes = full.bytes + 1
	onlyApp, err := oc.streamsFor(ctx, info, "tenant", streamsWant{labels: []string{"app"}})
	require.NoError(t, err)
	require.Len(t, oc.streamsCache, 1)
	require.LessOrEqual(t, oc.streamsBytes, oc.maxStreamsBytes)
	_, fullCached := oc.streamsCache[streamsWant{}.cacheKey(info.Path, 1)]
	_ = fullCached // which key survives depends on section layout; assert on the entry instead
	var survivor *streamsTable
	for _, el := range oc.streamsCache {
		survivor = el.Value.(*streamsEntry).table
	}
	require.Same(t, onlyApp, survivor, "the most recently used table survives")
}
