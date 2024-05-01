package main

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/index"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/tsdb"
	tsdb_index "github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/tsdb/index"
)

func analyze(indexShipper indexshipper.IndexShipper, tableName string, tenants []string) error {
	var (
		series             int
		chunks             int
		seriesRes          []tsdb.Series
		chunkRes           []tsdb.ChunkRef
		maxChunksPerSeries int
		seriesOver1kChunks int
	)
	for _, tenant := range tenants {
		fmt.Printf("analyzing tenant %s\n", tenant)
		err := indexShipper.ForEach(
			context.Background(),
			tableName,
			tenant,
			index.ForEachIndexCallback(func(isMultiTenantIndex bool, idx index.Index) error {
				if isMultiTenantIndex {
					return nil
				}

				casted := idx.(*tsdb.TSDBFile)
				seriesRes = seriesRes[:0]
				chunkRes = chunkRes[:0]

				res, err := casted.Series(
					context.Background(),
					tenant,
					model.Earliest,
					model.Latest,
					seriesRes, nil,
					labels.MustNewMatcher(labels.MatchEqual, "", ""),
				)
				if err != nil {
					return err
				}

				series += len(res)

				chunkRes, err := casted.GetChunkRefs(
					context.Background(),
					tenant,
					model.Earliest,
					model.Latest,
					chunkRes, nil,
					labels.MustNewMatcher(labels.MatchEqual, "", ""),
				)
				if err != nil {
					return err
				}

				chunks += len(chunkRes)

				err = casted.Index.(*tsdb.TSDBIndex).ForSeries(
					context.Background(),
					"", nil,
					model.Earliest,
					model.Latest,
					func(ls labels.Labels, fp model.Fingerprint, chks []tsdb_index.ChunkMeta) (stop bool) {
						for _, chk := range chks {
							from := time.UnixMilli(chk.MinTime)
							to := time.UnixMilli(chk.MaxTime)
							fmt.Printf("from: %s, to: %s\n", from.Format("2006-01-02T15:04:05"), to.Format("2006-01-02T15:04:05"))

							expected := (chk.MaxTime - chk.MinTime) / (10 * time.Second).Milliseconds()
							var maxKB, maxEntries uint32
							for _, s := range chk.Samples {
								if s.KB > maxKB {
									maxKB = s.KB
								}
								if s.Entries > maxEntries {
									maxEntries = s.Entries
								}
							}

							fmt.Printf("series: %v, entries: %d, bytes: %d\n", ls, chk.Entries, chk.KB)
							fmt.Printf("expected %d samples, found %d\n", expected, len(chk.Samples))
							fmt.Printf("max entries: %d, max bytes: %d\n", maxEntries, maxKB)
						}
						if len(chks) > maxChunksPerSeries {
							maxChunksPerSeries = len(chks)
							if len(chks) > 1000 {
								seriesOver1kChunks++
							}
						}
						return false
					},
					labels.MustNewMatcher(labels.MatchEqual, "", ""),
				)

				if err != nil {
					return err
				}

				return nil
			}),
		)
		if err != nil {
			return err
		}
	}

	fmt.Printf("analyzed %d series and %d chunks for an average of %f chunks per series. max chunks/series was %d. number of series with over 1k chunks: %d\n", series, chunks, float64(chunks)/float64(series), maxChunksPerSeries, seriesOver1kChunks)

	return nil
}
