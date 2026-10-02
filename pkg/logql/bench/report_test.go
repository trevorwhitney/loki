package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/grafana/loki/v3/pkg/logqlmodel"
)

// reportRow is one execution of one test case against one store.
type reportRow struct {
	Case     TestCase
	Store    string
	Status   string // pass, fail, skip
	Duration time.Duration
	Result   logqlmodel.Result
}

// reportCollector gathers per-store timings during TestStorageEquality and
// renders them as a markdown table so the v2 engine and the Flight/DataFusion
// path can be compared on identical queries and data.
type reportCollector struct {
	mu   sync.Mutex
	rows []reportRow
}

func (r *reportCollector) add(row reportRow) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// The baseline store runs once per comparison; keep its first timing.
	for _, existing := range r.rows {
		if existing.Store == row.Store && existing.Case.Equal(row.Case) && existing.Case.Source == row.Case.Source {
			return
		}
	}
	r.rows = append(r.rows, row)
}

func testStatus(t *testing.T) string {
	switch {
	case t.Skipped():
		return "skip"
	case t.Failed():
		return "fail"
	}
	return "pass"
}

func (r *reportCollector) write(path string, stores []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	type caseKey struct{ Source, Query string }
	byCase := make(map[caseKey][]reportRow)
	var order []caseKey
	for _, row := range r.rows {
		k := caseKey{Source: row.Case.Source, Query: row.Case.Query}
		if _, ok := byCase[k]; !ok {
			order = append(order, k)
		}
		byCase[k] = append(byCase[k], row)
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].Source != order[j].Source {
			return order[i].Source < order[j].Source
		}
		return order[i].Query < order[j].Query
	})

	var b strings.Builder
	fmt.Fprintf(&b, "# Storage equality report\n\n")
	fmt.Fprintf(&b, "Generated %s. Stores: %s.\n\n", time.Now().Format(time.RFC3339), strings.Join(stores, ", "))
	fmt.Fprintf(&b, "Exec time is wall time of `Query(...).Exec` as seen by the test. ")
	fmt.Fprintf(&b, "For `%s` the bytes column is Arrow Flight wire bytes received by DataFusion; for other stores it is bytes processed as reported by the engine.\n\n", StoreFlightSQL)
	fmt.Fprintf(&b, "| Source | Kind | Query | Store | Status | Exec (ms) | Lines processed | Bytes | Notes |\n")
	fmt.Fprintf(&b, "|---|---|---|---|---|---:|---:|---:|---|\n")

	for _, k := range order {
		rows := byCase[k]
		sort.Slice(rows, func(i, j int) bool { return rows[i].Store < rows[j].Store })
		for _, row := range rows {
			summary := row.Result.Statistics.Summary
			fmt.Fprintf(&b, "| %s | %s | `%s` | %s | %s | %.1f | %d | %s | %s |\n",
				row.Case.Source, row.Case.Kind(), strings.ReplaceAll(row.Case.Query, "|", "\\|"),
				row.Store, row.Status,
				float64(row.Duration.Microseconds())/1000,
				summary.TotalLinesProcessed,
				humanize.Bytes(uint64(max(summary.TotalBytesProcessed, 0))),
				strings.Join(row.Result.Warnings, "; "),
			)
		}
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}
