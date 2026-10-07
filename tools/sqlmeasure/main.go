// Command sqlmeasure measures what a query costs in a Loki cell: it runs a
// matrix of SQL queries against the dataobj-sql shadow (Grafana's Postgres
// datasource) and the equivalent LogQL against the live query path, through
// Grafana's query API, and reads Prometheus around each run for the scan
// service's and the queriers' CPU, object store requests, bytes and memory.
// From the deltas it derives wall time, CPU seconds, GETs, throughput and a
// dollar estimate from pluggable unit prices.
//
// Grafana and Prometheus are reached through the gcx CLI by default (the
// path gcx and Explore use, with gcx's authentication), or directly over
// HTTP with -grafana and -token.
//
//	go run ./tools/sqlmeasure -context dev -namespace <ns> -flight-job <ns>/<flight-deployment> \
//	    -sql-datasource <uid> -from 2026-09-16T00:00:00Z -windows 6h,12h,24h \
//	    -suites archive,logql -out results.jsonl
//
// Metrics are sampled every 30 s in the cell, so the harness idles -settle
// before a query (a clean "before" sample) and waits -slack after it (a
// sample that includes the whole query). Run one query at a time against a
// quiet shadow; the baseline row is shared with everybody else's queries and
// is corrected with the CPU rate observed while idling.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// query is one cell of the matrix.
type query struct {
	Suite string `json:"suite"` // archive, logs, logql, join
	Name  string `json:"name"`
	// SQL (archive, logs, join) or LogQL (logql).
	Expr string `json:"expr"`
	// Grafana format for SQL (table, time_series); query type for LogQL
	// (instant, range) plus step for range metric queries.
	Format    string `json:"format,omitempty"`
	QueryType string `json:"query_type,omitempty"`
	Step      string `json:"step,omitempty"`
	MaxLines  int    `json:"max_lines,omitempty"`
}

const needle = "Finished processing all tenants"

// matrix builds the queries for one window. Archive queries scan the whole
// archive (one tenant, all archive rules); logs queries scope to the asserts
// app, which is what the archive holds, plus one unscoped count; LogQL mirrors
// the logs queries on the live path. W is the window as a LogQL duration.
//
// LogQL selectors carry a no-op matcher on a label no stream has, unique to
// each run, so Loki's results cache does not answer a repeated query or share
// the split sub-queries of overlapping windows:
// the measurement wants the cold path, and the live side's chunk and index
// caches stay in play the same way the shadow's disk cache does.
func matrix(suites map[string]bool, w time.Duration, nonce string) []query {
	W := logqlDuration(w)
	tf := `$__timeFilter("timestamp")`
	var qs []query
	add := func(q query) {
		if suites[q.Suite] {
			qs = append(qs, q)
		}
	}
	for _, table := range []string{"archive_logs", "logs"} {
		suite := "archive"
		scope := ""
		if table == "logs" {
			suite = "logs"
			scope = " AND app = 'asserts'"
		}
		add(query{Suite: suite, Name: "count", Format: "table",
			Expr: fmt.Sprintf(`SELECT count(*) AS n FROM %s WHERE %s%s`, table, tf, scope)})
		add(query{Suite: suite, Name: "per hour by service", Format: "table",
			Expr: fmt.Sprintf(`SELECT date_trunc('hour', "timestamp") AS h, service_name, count(*) AS n FROM %s WHERE %s%s GROUP BY 1, 2 ORDER BY 1, 2`, table, tf, scope)})
		add(query{Suite: suite, Name: "latest 100", Format: "table",
			Expr: fmt.Sprintf(`SELECT "timestamp", service_name, message FROM %s WHERE %s%s ORDER BY 1 DESC LIMIT 100`, table, tf, scope)})
		add(query{Suite: suite, Name: "needle count", Format: "table",
			Expr: fmt.Sprintf(`SELECT count(*) AS n FROM %s WHERE %s%s AND message LIKE '%%%s%%'`, table, tf, scope, needle)})
		if table == "logs" {
			// Last in the suite: the unscoped count is the widest query and
			// the one most likely to fail or run on after a client timeout.
			add(query{Suite: suite, Name: "count all streams", Format: "table",
				Expr: fmt.Sprintf(`SELECT count(*) AS n FROM %s WHERE %s`, table, tf)})
		}
	}
	add(query{Suite: "logql", Name: "count", QueryType: "instant",
		Expr: fmt.Sprintf(`sum(count_over_time({app="asserts"%s}[%s]))`, nonce, W)})
	add(query{Suite: "logql", Name: "per hour by service", QueryType: "range", Step: "1h",
		Expr: fmt.Sprintf(`sum by (service_name) (count_over_time({app="asserts"%s}[1h]))`, nonce)})
	add(query{Suite: "logql", Name: "latest 100", QueryType: "range", MaxLines: 100,
		Expr: fmt.Sprintf(`{app="asserts"%s}`, nonce)})
	add(query{Suite: "logql", Name: "needle count", QueryType: "instant",
		Expr: fmt.Sprintf(`sum(count_over_time({app="asserts"%s} |= "%s" [%s]))`, nonce, needle, W)})
	// Last for the same reason as the SQL one: through Grafana it 502s at
	// about 60 s on wide windows while Loki keeps executing it for up to the
	// datasource timeout, which would poison the next measurement.
	add(query{Suite: "logql", Name: "count all streams", QueryType: "instant",
		Expr: fmt.Sprintf(`sum(count_over_time({service_name=~".+"%s}[%s]))`, nonce, W)})
	// Both tables carry the trace id in the message text (the archive's
	// trace_id column is empty for this tenant), so the join key is
	// extracted on both sides.
	add(query{Suite: "join", Name: "live x archive by trace", Format: "table",
		Expr: fmt.Sprintf(`SELECT count(*) AS joined, count(DISTINCT l.tid) AS traces FROM (SELECT regexp_match(message, 'trace_id=([0-9a-f]{32})')[1] AS tid FROM logs WHERE %s AND app = 'asserts') l JOIN (SELECT DISTINCT regexp_match(message, 'trace_id=([0-9a-f]{32})')[1] AS tid FROM archive_logs WHERE %s) a ON l.tid = a.tid`, tf, tf)})
	return qs
}

func logqlDuration(d time.Duration) string {
	if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dm", int(d.Minutes()))
}

// result is one measured run.
type result struct {
	Suite  string    `json:"suite"`
	Name   string    `json:"name"`
	Window string    `json:"window"`
	From   time.Time `json:"from"`
	To     time.Time `json:"to"`
	Run    int       `json:"run"`
	Start  time.Time `json:"start"`
	End    time.Time `json:"end"`
	WallS  float64   `json:"wall_s"`
	Rows   int       `json:"result_rows"`
	Error  string    `json:"error,omitempty"`
	Expr   string    `json:"expr"`

	// Response statistics (Loki path only).
	ExecTimeS      float64 `json:"exec_time_s,omitempty"`
	BytesProcessed float64 `json:"bytes_processed,omitempty"`
	LinesProcessed float64 `json:"lines_processed,omitempty"`

	// Deltas on the measured side: the Flight scan service for SQL suites,
	// the queriers and frontends for logql.
	CPUSeconds      float64 `json:"cpu_s"`
	BackgroundCPU   float64 `json:"bg_cpu_per_s"` // CPU rate while idling before the query
	CPUAdjusted     float64 `json:"cpu_s_adjusted"`
	Gets            float64 `json:"gets"`
	PeakRSSBytes    float64 `json:"peak_rss_bytes"`
	GatewayCPU      float64 `json:"gateway_cpu_s,omitempty"`
	GatewayPeakRSS  float64 `json:"gateway_peak_rss_bytes,omitempty"`
	ArchiveGZBytes  float64 `json:"archive_gz_bytes,omitempty"`
	ArchiveRawBytes float64 `json:"archive_raw_bytes,omitempty"`
	ArchiveObjects  float64 `json:"archive_objects,omitempty"`
	FetchSeconds    float64 `json:"archive_fetch_s,omitempty"`
	DecodeSeconds   float64 `json:"archive_decode_s,omitempty"`
	RowsServed      float64 `json:"rows_served,omitempty"`
	BytesServed     float64 `json:"bytes_served,omitempty"`
	Tickets         float64 `json:"tickets,omitempty"`
	FrontendBytes   float64 `json:"frontend_bytes_processed,omitempty"`
	CacheHits       float64 `json:"disk_cache_hits,omitempty"`
	CacheMisses     float64 `json:"disk_cache_misses,omitempty"`

	CostUSD    float64 `json:"cost_usd"`
	MemCostUSD float64 `json:"mem_cost_usd"`
}

type config struct {
	context, grafana, token               string
	sqlDS, lokiDS, promDS                 string
	namespace, flightJob, frontendJob     string
	baselineJobs, gatewayPods, flightPods string
	cpuPrice, getPrice, memPrice          float64
	settle, slack                         time.Duration
	idleBelow                             float64
	idleWait                              time.Duration
	runs                                  int
}

func main() {
	var cfg config
	flag.StringVar(&cfg.context, "context", "dev", "gcx context used for every Grafana and Prometheus call.")
	flag.StringVar(&cfg.grafana, "grafana", "", "Grafana base URL; set to call Grafana over HTTP instead of through gcx.")
	flag.StringVar(&cfg.token, "token", os.Getenv("GRAFANA_TOKEN"), "Bearer token for -grafana.")
	flag.StringVar(&cfg.sqlDS, "sql-datasource", "", "UID of the Postgres datasource pointed at dataobj-sql (required).")
	flag.StringVar(&cfg.lokiDS, "loki-datasource", "grafanacloud-logs", "UID of the Loki datasource for the logql suite.")
	flag.StringVar(&cfg.promDS, "prom", "grafanacloud-prom", "UID of the Prometheus datasource that scrapes the cell.")
	flag.StringVar(&cfg.namespace, "namespace", "", "Namespace of the cell (required; pod memory series and default job labels).")
	flag.StringVar(&cfg.flightJob, "flight-job", "", "job label of the Flight scan service (required, usually <namespace>/<deployment>).")
	flag.StringVar(&cfg.flightPods, "flight-pods", "shadow-dataobj-flight.*", "Pod regex of the Flight scan service.")
	flag.StringVar(&cfg.gatewayPods, "gateway-pods", "shadow-sql-gateway.*", "Pod regex of the dataobj-sql gateway.")
	flag.StringVar(&cfg.baselineJobs, "baseline-jobs", "", "Comma-separated job labels measured for the logql suite. Defaults to <namespace>/querier,<namespace>/query-frontend.")
	flag.StringVar(&cfg.frontendJob, "frontend-job", "", "job label whose loki_logql_querystats_bytes_processed_total is read (frontend only: queriers double count). Defaults to <namespace>/query-frontend.")
	from := flag.String("from", "", "Start of every window, RFC3339.")
	windows := flag.String("windows", "6h,12h,24h", "Comma-separated window sizes, each starting at -from.")
	suites := flag.String("suites", "archive,logs,logql,join", "Comma-separated suites to run.")
	only := flag.String("queries", "", "Only run queries whose name contains this.")
	flag.IntVar(&cfg.runs, "runs", 1, "Runs per query (the first is cold, later ones are warm when a cache is on).")
	flag.DurationVar(&cfg.settle, "settle", 45*time.Second, "Idle time before each query, so the before sample is clean (scrape interval is 30s).")
	flag.DurationVar(&cfg.slack, "slack", 90*time.Second, "Wait after each query before reading the after sample. Counter increments of a query show up in samples up to ~30s after it returns, on top of the 30s scrape interval.")
	flag.Float64Var(&cfg.idleBelow, "idle-below", 1.0, "Do not start a query until the measured jobs' CPU rate over the settle period is below this many cores (a previous query may still be running server-side after a client timeout).")
	flag.DurationVar(&cfg.idleWait, "idle-wait", 10*time.Minute, "Longest to wait for the measured jobs to go idle before starting anyway.")
	flag.Float64Var(&cfg.cpuPrice, "cpu-price", 0.045, "USD per vCPU-hour.")
	flag.Float64Var(&cfg.getPrice, "get-price", 0.0004, "USD per 1000 object storage GET/LIST requests (GCS class B).")
	flag.Float64Var(&cfg.memPrice, "mem-price", 0.0042, "USD per GB-hour of memory, reported separately (peak RSS x wall).")
	out := flag.String("out", "", "Append one JSON line per run to this file.")
	recompute := flag.String("recompute", "", "Re-derive the metric deltas of every run in this JSONL file from Prometheus (using each run's recorded start/end), print the table and, with -out, write the corrected runs. Use after a run: samples arrive in Prometheus with some lag, so deltas read live can be short.")
	flag.Parse()

	for name, v := range map[string]string{"-namespace": cfg.namespace, "-flight-job": cfg.flightJob, "-sql-datasource": cfg.sqlDS} {
		if v == "" {
			fatal("%s is required", name)
		}
	}
	if cfg.frontendJob == "" {
		cfg.frontendJob = cfg.namespace + "/query-frontend"
	}
	if cfg.baselineJobs == "" {
		cfg.baselineJobs = cfg.namespace + "/querier," + cfg.frontendJob
	}

	if *recompute != "" {
		rows, err := readRuns(*recompute)
		if err != nil {
			fatal("reading %s: %v", *recompute, err)
		}
		var sink io.Writer = io.Discard
		if *out != "" {
			f, err := os.Create(*out)
			if err != nil {
				fatal("opening -out: %v", err)
			}
			defer f.Close()
			sink = f
		}
		enc := json.NewEncoder(sink)
		for i := range rows {
			cfg.collect(&rows[i])
			_ = enc.Encode(rows[i])
			fmt.Fprintf(os.Stderr, "%-8s %-24s %-4s run %d: wall %.1fs cpu %.1fs gets %.0f\n", rows[i].Suite, rows[i].Name, rows[i].Window, rows[i].Run, rows[i].WallS, rows[i].CPUSeconds, rows[i].Gets)
		}
		printMarkdown(cfg, rows)
		return
	}

	if *from == "" {
		fatal("-from is required")
	}
	start, err := time.Parse(time.RFC3339, *from)
	if err != nil {
		fatal("parsing -from: %v", err)
	}
	var sizes []time.Duration
	for _, w := range strings.Split(*windows, ",") {
		d, err := time.ParseDuration(strings.TrimSpace(w))
		if err != nil {
			fatal("parsing window %q: %v", w, err)
		}
		sizes = append(sizes, d)
	}
	suiteSet := map[string]bool{}
	for _, s := range strings.Split(*suites, ",") {
		suiteSet[strings.TrimSpace(s)] = true
	}

	var sink io.Writer = io.Discard
	if *out != "" {
		f, err := os.OpenFile(*out, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fatal("opening -out: %v", err)
		}
		defer f.Close()
		sink = f
	}
	enc := json.NewEncoder(sink)

	// The placeholder becomes a per-run matcher in measure: Loki caches the
	// aligned sub-queries a query is split into, so two windows with the
	// same selector would share them.
	const noncePlaceholder = "__NONCE__"
	var results []result
	for _, w := range sizes {
		for _, q := range matrix(suiteSet, w, noncePlaceholder) {
			if *only != "" && !strings.Contains(q.Name, *only) {
				continue
			}
			for run := 1; run <= cfg.runs; run++ {
				r := measure(cfg, q, w, start, start.Add(w), run)
				results = append(results, r)
				_ = enc.Encode(r)
				fmt.Fprintf(os.Stderr, "%-8s %-24s %-4s run %d: wall %.1fs cpu %.1fs gets %.0f rows %d %s\n", r.Suite, r.Name, r.Window, run, r.WallS, r.CPUSeconds, r.Gets, r.Rows, r.Error)
			}
		}
	}
	printMarkdown(cfg, results)
}

// measure runs one query with a clean before sample and a complete after
// sample around it, and derives the deltas.
func measure(cfg config, q query, w time.Duration, from, to time.Time, run int) result {
	baseline := q.Suite == "logql"
	if baseline {
		q.Expr = strings.ReplaceAll(q.Expr, "__NONCE__", fmt.Sprintf(`, __sqlmeasure__!="%d"`, time.Now().UnixNano()))
	}
	r := result{Suite: q.Suite, Name: q.Name, Window: w.String(), From: from, To: to, Run: run, Expr: q.Expr}

	// Idle so the sample before the query reflects a quiet service, and
	// measure the background CPU rate over that idle period. If the jobs are
	// still busy (a previous query running on after a client timeout), keep
	// waiting up to idleWait.
	cpuExpr := cfg.cpuExpr(baseline)
	idleStart := time.Now()
	time.Sleep(cfg.settle)
	r.Start = time.Now()
	bgDelta := cfg.delta(cpuExpr, idleStart, r.Start)
	deadline := time.Now().Add(cfg.idleWait)
	for cfg.settle > 0 && bgDelta/cfg.settle.Seconds() > cfg.idleBelow && time.Now().Before(deadline) {
		fmt.Fprintf(os.Stderr, "%-8s %-24s %-4s: measured jobs busy (%.1f cores), waiting\n", q.Suite, q.Name, w, bgDelta/cfg.settle.Seconds())
		idleStart = time.Now()
		time.Sleep(cfg.settle)
		r.Start = time.Now()
		bgDelta = cfg.delta(cpuExpr, idleStart, r.Start)
	}
	if cfg.settle > 0 {
		r.BackgroundCPU = bgDelta / cfg.settle.Seconds()
	}

	var err error
	if baseline {
		r.Rows, r.ExecTimeS, r.BytesProcessed, r.LinesProcessed, err = cfg.runLogQL(q, from, to)
	} else {
		r.Rows, err = cfg.runSQL(q, from, to)
	}
	r.End = time.Now()
	r.WallS = r.End.Sub(r.Start).Seconds()
	if err != nil {
		r.Error = truncate(err.Error(), 200)
	}
	time.Sleep(cfg.slack)
	cfg.collect(&r)
	return r
}

// collect derives every metric of a run from Prometheus using the run's
// recorded start and end: counters as v(end + slack) - v(start), the
// background rate over the settle period before start, gauges as the maximum
// over the run. Safe to call again later, when all samples have arrived.
func (cfg config) collect(r *result) {
	baseline := isLivePath(r.Suite)
	cpuExpr := cfg.cpuExpr(baseline)
	after := r.End.Add(cfg.slack)
	if cfg.settle > 0 {
		r.BackgroundCPU = cfg.delta(cpuExpr, r.Start.Add(-cfg.settle), r.Start) / cfg.settle.Seconds()
	}
	r.CPUSeconds = cfg.delta(cpuExpr, r.Start, after)
	// Only the shared live path is corrected for other traffic; the shadow is
	// idle between runs and the correction would only pick up the previous
	// run's trailing samples.
	r.CPUAdjusted = r.CPUSeconds
	if baseline {
		r.CPUAdjusted = math.Max(0, r.CPUSeconds-r.BackgroundCPU*after.Sub(r.Start).Seconds())
	}
	rng := fmt.Sprintf("%ds", int(after.Sub(r.Start).Seconds())+30)
	if baseline {
		jobs := jobRegex(cfg.baselineJobs)
		r.Gets = cfg.delta(fmt.Sprintf(`loki_objstore_bucket_operations_total{job=~%q,operation=~"get|get_range|iter|attributes"}`, jobs), r.Start, after)
		r.FrontendBytes = cfg.delta(fmt.Sprintf(`loki_logql_querystats_bytes_processed_total{job=%q}`, cfg.frontendJob), r.Start, after)
		r.PeakRSSBytes = cfg.instant(fmt.Sprintf(`max(max_over_time(process_resident_memory_bytes{job=~%q}[%s]))`, jobs, rng), after)
	} else {
		J := fmt.Sprintf(`job=%q`, cfg.flightJob)
		d := func(metric string) float64 { return cfg.delta(fmt.Sprintf(`%s{%s}`, metric, J), r.Start, after) }
		r.Gets = cfg.delta(fmt.Sprintf(`loki_objstore_bucket_operations_total{%s,operation=~"get|get_range|iter|attributes"}`, J), r.Start, after)
		r.ArchiveGZBytes = d("loki_dataobj_flight_archive_compressed_bytes_total")
		r.ArchiveRawBytes = d("loki_dataobj_flight_archive_uncompressed_bytes_total")
		r.ArchiveObjects = d("loki_dataobj_flight_archive_objects_fetched_total")
		r.FetchSeconds = d("loki_dataobj_flight_archive_fetch_duration_seconds_sum")
		r.DecodeSeconds = d("loki_dataobj_flight_archive_decode_duration_seconds_sum")
		r.RowsServed = d("loki_dataobj_flight_rows_served_total")
		r.BytesServed = d("loki_dataobj_flight_bytes_served_total")
		r.Tickets = d("loki_dataobj_flight_planned_tickets_sum")
		r.CacheHits = d("loki_dataobj_flight_disk_cache_hits_total")
		r.CacheMisses = d("loki_dataobj_flight_disk_cache_misses_total")
		r.PeakRSSBytes = cfg.instant(fmt.Sprintf(`max(max_over_time(container_memory_working_set_bytes{namespace=%q,pod=~%q,container!="",container!="POD"}[%s]))`, cfg.namespace, cfg.flightPods, rng), after)
		r.GatewayPeakRSS = cfg.instant(fmt.Sprintf(`max(max_over_time(container_memory_working_set_bytes{namespace=%q,pod=~%q,container!="",container!="POD"}[%s]))`, cfg.namespace, cfg.gatewayPods, rng), after)
		r.GatewayCPU = cfg.delta(fmt.Sprintf(`container_cpu_usage_seconds_total{namespace=%q,pod=~%q,container!="",container!="POD"}`, cfg.namespace, cfg.gatewayPods), r.Start, after)
	}
	if r.Suite == "archive-logql" {
		// The shadow query-frontend (selected with -gateway-pods for this
		// suite) is part of the archive LogQL path: its CPU counts.
		r.CPUSeconds += r.GatewayCPU
		r.CPUAdjusted = r.CPUSeconds
	}
	r.CostUSD = r.CPUAdjusted/3600*cfg.cpuPrice + r.Gets/1000*cfg.getPrice
	r.MemCostUSD = r.PeakRSSBytes / 1e9 * r.WallS / 3600 * cfg.memPrice
}

// isLivePath reports whether a suite runs on the cell's live query path
// (queriers and frontends) rather than on the shadow.
func isLivePath(suite string) bool {
	return suite == "logql" || suite == "live-logql"
}

// readRuns loads the JSON lines written by -out.
func readRuns(path string) ([]result, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rows []result
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var r result
		if err := json.Unmarshal(line, &r); err != nil {
			return nil, err
		}
		rows = append(rows, r)
	}
	return rows, nil
}

// cpuExpr is the counter selector of the measured side (summed per pod by delta).
func (cfg config) cpuExpr(baseline bool) string {
	if baseline {
		return fmt.Sprintf(`process_cpu_seconds_total{job=~%q}`, jobRegex(cfg.baselineJobs))
	}
	return fmt.Sprintf(`process_cpu_seconds_total{job=%q}`, cfg.flightJob)
}

func jobRegex(csv string) string {
	var parts []string
	for _, j := range strings.Split(csv, ",") {
		parts = append(parts, strings.TrimSpace(j))
	}
	return strings.Join(parts, "|")
}

// delta is the growth of a counter selector between a and b, summed per
// pod: a pod that appears in between counts from zero, a pod that
// disappears is dropped, and a counter that went backwards (restart) counts
// what it has at b. A plain difference of sums is wrong whenever the pod set
// changes, which it does on the autoscaled live path and on a restarted
// shadow.
func (cfg config) delta(selector string, a, b time.Time) float64 {
	before := cfg.byPod(selector, a)
	after := cfg.byPod(selector, b)
	var sum float64
	for pod, v := range after {
		prev, ok := before[pod]
		if !ok || v < prev {
			sum += v
			continue
		}
		sum += v - prev
	}
	return sum
}

// byPod evaluates sum by (pod) (selector) at t.
func (cfg config) byPod(selector string, t time.Time) map[string]float64 {
	expr := fmt.Sprintf(`sum by (pod) (%s)`, selector)
	out := map[string]float64{}
	for _, r := range cfg.query(expr, t) {
		out[r.pod] += r.value
	}
	return out
}

type sampleResult struct {
	pod   string
	value float64
}

// query evaluates expr at t and returns one entry per result series.
func (cfg config) query(expr string, t time.Time) []sampleResult {
	var raw []byte
	var err error
	if cfg.grafana != "" {
		u := fmt.Sprintf("%s/api/datasources/proxy/uid/%s/api/v1/query?query=%s&time=%d", strings.TrimRight(cfg.grafana, "/"), cfg.promDS, url.QueryEscape(expr), t.Unix())
		raw, err = cfg.httpGet(u)
	} else {
		raw, err = gcx(cfg.context, "metrics", "query", "-d", cfg.promDS, expr, "--from", strconv.FormatInt(t.Unix(), 10), "--to", strconv.FormatInt(t.Unix(), 10), "--step", "1m", "-o", "json")
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "prometheus query failed: %v\n", err)
		return nil
	}
	var parsed struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(jsonBody(raw), &parsed); err != nil {
		fmt.Fprintf(os.Stderr, "prometheus response: %v: %s\n", err, truncate(string(raw), 200))
		return nil
	}
	var out []sampleResult
	for _, r := range parsed.Data.Result {
		sample := r.Value
		if len(r.Values) > 0 {
			sample = r.Values[len(r.Values)-1]
		}
		if len(sample) == 2 {
			if s, ok := sample[1].(string); ok {
				v, _ := strconv.ParseFloat(s, 64)
				out = append(out, sampleResult{pod: r.Metric["pod"], value: v})
			}
		}
	}
	return out
}

// instant evaluates expr at t and sums the result vector.
func (cfg config) instant(expr string, t time.Time) float64 {
	var sum float64
	for _, r := range cfg.query(expr, t) {
		sum += r.value
	}
	return sum
}

// runSQL runs a SQL query through Grafana's query API and returns the rows.
func (cfg config) runSQL(q query, from, to time.Time) (int, error) {
	body := map[string]any{
		"from": fmt.Sprintf("%d", from.UnixMilli()),
		"to":   fmt.Sprintf("%d", to.UnixMilli()),
		"queries": []map[string]any{{
			"refId":         "A",
			"datasource":    map[string]string{"uid": cfg.sqlDS, "type": "grafana-postgresql-datasource"},
			"rawSql":        q.Expr,
			"format":        q.Format,
			"rawQuery":      true,
			"editorMode":    "code",
			"intervalMs":    60000,
			"maxDataPoints": 10000,
		}},
	}
	res, err := cfg.dsQuery(body)
	if err != nil {
		return 0, err
	}
	return res.rows, nil
}

// runLogQL runs a LogQL query through Grafana's query API and returns the
// rows plus the summary statistics Loki reports.
func (cfg config) runLogQL(q query, from, to time.Time) (rows int, execTime, bytesProcessed, linesProcessed float64, err error) {
	qm := map[string]any{
		"refId":      "A",
		"datasource": map[string]string{"uid": cfg.lokiDS, "type": "loki"},
		"expr":       q.Expr,
		"queryType":  q.QueryType,
		"direction":  "backward",
	}
	if q.MaxLines > 0 {
		qm["maxLines"] = q.MaxLines
	}
	if q.Step != "" {
		qm["step"] = q.Step
	}
	body := map[string]any{
		"from":    fmt.Sprintf("%d", from.UnixMilli()),
		"to":      fmt.Sprintf("%d", to.UnixMilli()),
		"queries": []map[string]any{qm},
	}
	res, err := cfg.dsQuery(body)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return res.rows, res.stats["Summary: exec time"], res.stats["Summary: total bytes processed"], res.stats["Summary: total lines processed"], nil
}

type dsResult struct {
	rows  int
	stats map[string]float64
}

// dsQuery posts to /api/ds/query and parses frames and Loki statistics.
func (cfg config) dsQuery(body map[string]any) (dsResult, error) {
	raw, _ := json.Marshal(body)
	var data []byte
	var err error
	if cfg.grafana != "" {
		data, err = cfg.httpPost(strings.TrimRight(cfg.grafana, "/")+"/api/ds/query", raw)
	} else {
		f, ferr := os.CreateTemp("", "sqlmeasure-*.json")
		if ferr != nil {
			return dsResult{}, ferr
		}
		defer os.Remove(f.Name())
		_, _ = f.Write(raw)
		_ = f.Close()
		data, err = gcx(cfg.context, "api", "/api/ds/query", "-d", "@"+f.Name())
	}
	if err != nil && len(data) == 0 {
		return dsResult{}, err
	}
	var parsed struct {
		Error *struct {
			Details string `json:"details"`
			Summary string `json:"summary"`
		} `json:"error"`
		Results map[string]struct {
			Error  string `json:"error"`
			Frames []struct {
				Schema struct {
					Meta struct {
						Stats []struct {
							DisplayName string  `json:"displayName"`
							Value       float64 `json:"value"`
						} `json:"stats"`
					} `json:"meta"`
				} `json:"schema"`
				Data struct {
					Values [][]any `json:"values"`
				} `json:"data"`
			} `json:"frames"`
		} `json:"results"`
	}
	if err := json.Unmarshal(jsonBody(data), &parsed); err != nil {
		return dsResult{}, fmt.Errorf("unparseable response: %s", truncate(string(data), 300))
	}
	if parsed.Error != nil {
		// gcx wraps non-2xx responses; the datasource error is in details.
		return dsResult{}, fmt.Errorf("%s", datasourceError(parsed.Error.Details, parsed.Error.Summary))
	}
	res, ok := parsed.Results["A"]
	if !ok {
		return dsResult{}, fmt.Errorf("no result A: %s", truncate(string(data), 300))
	}
	if res.Error != "" {
		return dsResult{}, fmt.Errorf("%s", res.Error)
	}
	out := dsResult{stats: map[string]float64{}}
	for _, f := range res.Frames {
		if len(f.Data.Values) > 0 {
			out.rows += len(f.Data.Values[0])
		}
		for _, s := range f.Schema.Meta.Stats {
			out.stats[s.DisplayName] += s.Value
		}
	}
	return out, nil
}

// datasourceError pulls the datasource's own message out of gcx's wrapped
// HTTP error when it is there.
func datasourceError(details, summary string) string {
	if i := strings.Index(details, "{"); i >= 0 {
		var inner struct {
			Results map[string]struct {
				Error string `json:"error"`
			} `json:"results"`
		}
		if json.Unmarshal([]byte(details[i:]), &inner) == nil {
			for _, r := range inner.Results {
				if r.Error != "" {
					return r.Error
				}
			}
		}
	}
	if details != "" {
		return details
	}
	return summary
}

// gcx runs the gcx CLI and returns stdout (plus stderr on failure).
func gcx(context string, args ...string) ([]byte, error) {
	cmd := exec.Command("gcx", append([]string{"--context", context}, args...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	out := stdout.Bytes()
	if err != nil {
		if len(out) == 0 {
			out = stderr.Bytes()
		}
		return out, fmt.Errorf("gcx %s: %w: %s", args[0], err, truncate(stderr.String(), 200))
	}
	return out, nil
}

// jsonBody strips the hint line gcx prints before JSON in agent mode.
func jsonBody(raw []byte) []byte {
	if i := bytes.IndexByte(raw, '{'); i > 0 {
		return raw[i:]
	}
	return raw
}

func (cfg config) httpGet(u string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	return cfg.httpDo(req)
}

func (cfg config) httpPost(u string, body []byte) ([]byte, error) {
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return cfg.httpDo(req)
}

func (cfg config) httpDo(req *http.Request) ([]byte, error) {
	if cfg.token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.token)
	}
	client := &http.Client{Timeout: 30 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return data, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return data, nil
}

func printMarkdown(cfg config, results []result) {
	fmt.Printf("\nPrices: $%.3f/vCPU-h, $%.4f/1k object-store requests, $%.4f/GB-h memory (reported separately).\n", cfg.cpuPrice, cfg.getPrice, cfg.memPrice)
	fmt.Println("CPU is the process CPU of the measured side (Flight scan pods for SQL, queriers + frontends for LogQL) minus the idle rate observed before the query.")
	fmt.Println()
	fmt.Println("| suite | query | window | run | wall | result rows | CPU-s | CPU-s adj | GETs | GB scanned | MB/s | CPU-s/GB | $/query | $/GB | peak RSS | mem $ | note |")
	fmt.Println("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
	for _, r := range results {
		if r.Error != "" {
			fmt.Printf("| %s | %s | %s | %d | %.1fs | | | | | | | | | | | | error: %s |\n", r.Suite, r.Name, r.Window, r.Run, r.WallS, truncate(r.Error, 80))
			continue
		}
		// Volume: archive uncompressed bytes decoded, Loki bytes processed,
		// or Arrow bytes served for data objects (read by page, not whole).
		gb := r.ArchiveRawBytes / 1e9
		note := "archive uncompressed"
		if isLivePath(r.Suite) {
			gb = r.BytesProcessed / 1e9
			note = fmt.Sprintf("Loki bytes processed; exec %.1fs", r.ExecTimeS)
		} else if r.Suite == "archive-logql" {
			note = fmt.Sprintf("archive uncompressed; Loki bytes processed %.2f GB; exec %.1fs; frontend %.1f CPU-s", r.BytesProcessed/1e9, r.ExecTimeS, r.GatewayCPU)
		} else if r.ArchiveRawBytes == 0 {
			gb = r.BytesServed / 1e9
			note = "Arrow bytes served"
		}
		if r.Suite == "join" {
			note = fmt.Sprintf("archive %.1f GB raw + %.0f MB served", r.ArchiveRawBytes/1e9, r.BytesServed/1e6)
		}
		mbps := 0.0
		if r.WallS > 0 {
			mbps = gb * 1e3 / r.WallS
		}
		fmt.Printf("| %s | %s | %s | %d | %.1fs | %d | %.1f | %.1f | %.0f | %.2f | %.0f | %s | $%.5f | %s | %.2f GB | $%.5f | %s |\n",
			r.Suite, r.Name, r.Window, r.Run, r.WallS, r.Rows, r.CPUSeconds, r.CPUAdjusted, r.Gets, gb, mbps, ratio(r.CPUAdjusted, gb, "%.1f"), r.CostUSD, ratio(r.CostUSD, gb, "$%.5f"), r.PeakRSSBytes/1e9, r.MemCostUSD, note)
	}
}

func ratio(a, b float64, format string) string {
	if b <= 0 || math.IsNaN(a) || math.IsNaN(b) {
		return "-"
	}
	return fmt.Sprintf(format, a/b)
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
