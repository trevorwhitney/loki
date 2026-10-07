// k6 driver for the query-in-place cost matrix. Runs the same queries as
// tools/sqlmeasure (archive, logs, join over SQL; LogQL on the live path),
// one at a time with a settle period before each, and prints one JSON line
// per run with start/end timestamps. Metrics are then derived from
// Prometheus with `sqlmeasure -recompute <jsonl>` exactly as for laptop runs.
//
// Runs inside the cell so it reaches the gateway over pgwire directly. The
// first four are required:
//   K6_PG_DSN      postgres://loki@<sql-gateway>.<namespace>.svc.cluster.local:5432/datafusion?sslmode=disable
//   K6_LOKI_URL    http://query-frontend.<namespace>.svc.cluster.local:3100
//   K6_ARCHIVE_LOKI_URL  http://<archive-query-frontend>.<namespace>.svc.cluster.local:3100
//   K6_TENANT      <tenant id>
//   K6_ARCHIVE_SELECTOR  {service_name=~".+"}   K6_LIVE_SELECTOR  {app="asserts"}
//   K6_FROM        2026-09-16T00:00:00Z
//   K6_WINDOWS     6h,12h,24h
//   K6_SUITES      archive,logs,join,logql   (plus archive-logql,live-logql: the same LogQL
//                  over the archive querier and over the live chunk path, all streams)
//   K6_RUNS        1
//   K6_SETTLE_S    60
//   K6_SKIP_ALL_STREAMS_ABOVE_H  12   (the full-cell count only up to this window)
import sql from "k6/x/sql";
import driver from "k6/x/sql/driver/postgres";
import http from "k6/http";
import { sleep } from "k6";

// One VU, one iteration, and a generous ceiling: the matrix settles 60 s before every
// query and a 24 h logs query can take many minutes (k6's default maxDuration is 10 m).
export const options = {
  scenarios: { matrix: { executor: "shared-iterations", vus: 1, iterations: 1, maxDuration: "8h" } },
  setupTimeout: "2m", teardownTimeout: "2m",
};

const env = (k, d) => (__ENV[k] !== undefined && __ENV[k] !== "" ? __ENV[k] : d);
const required = (k) => {
  const v = env(k);
  if (v === undefined) throw new Error(`${k} must be set`);
  return v;
};
const PG_DSN = required("K6_PG_DSN");
const LOKI_URL = required("K6_LOKI_URL");
const ARCHIVE_LOKI_URL = required("K6_ARCHIVE_LOKI_URL");
const TENANT = required("K6_TENANT");
// Stream selectors of the two LogQL comparison suites. The archive holds only the streams
// the tenant's archive rule selects, so on the archive every stream is in scope while the
// live path must be narrowed to the same rule.
const ARCHIVE_SELECTOR = env("K6_ARCHIVE_SELECTOR", `{service_name=~".+"}`);
const LIVE_SELECTOR = env("K6_LIVE_SELECTOR", `{app="asserts"}`);
const FROM = new Date(env("K6_FROM", "2026-09-16T00:00:00Z"));
const WINDOWS = env("K6_WINDOWS", "6h,12h,24h").split(",").map((w) => parseInt(w, 10));
const SUITES = new Set(env("K6_SUITES", "archive,logs,join,logql").split(","));
const RUNS = parseInt(env("K6_RUNS", "1"), 10);
const SETTLE_S = parseInt(env("K6_SETTLE_S", "60"), 10);
const SKIP_ALL_STREAMS_ABOVE_H = parseInt(env("K6_SKIP_ALL_STREAMS_ABOVE_H", "12"), 10);
const NEEDLE = "Finished processing all tenants";

const db = sql.open(driver, PG_DSN);
export function teardown() { db.close(); }

const iso = (d) => d.toISOString().replace(/\.\d{3}Z$/, "Z");
const tf = (from, to) => `"timestamp" BETWEEN '${iso(from)}' AND '${iso(to)}'`;

function matrix(hours, from, to) {
  const t = tf(from, to);
  const q = [];
  const add = (suite, name, expr, extra) => { if (SUITES.has(suite)) q.push(Object.assign({ suite, name, expr }, extra || {})); };
  for (const table of ["archive_logs", "logs"]) {
    const suite = table === "logs" ? "logs" : "archive";
    const scope = table === "logs" ? " AND app = 'asserts'" : "";
    add(suite, "count", `SELECT count(*) AS n FROM ${table} WHERE ${t}${scope}`);
    add(suite, "per hour by service", `SELECT date_trunc('hour', "timestamp") AS h, service_name, count(*) AS n FROM ${table} WHERE ${t}${scope} GROUP BY 1, 2 ORDER BY 1, 2`);
    add(suite, "latest 100", `SELECT "timestamp", service_name, message FROM ${table} WHERE ${t}${scope} ORDER BY 1 DESC LIMIT 100`);
    add(suite, "needle count", `SELECT count(*) AS n FROM ${table} WHERE ${t}${scope} AND message LIKE '%${NEEDLE}%'`);
  }
  add("join", "live x archive by trace", `SELECT count(*) AS joined, count(DISTINCT l.tid) AS traces FROM (SELECT regexp_match(message, 'trace_id=([0-9a-f]{32})')[1] AS tid FROM logs WHERE ${t} AND app = 'asserts') l JOIN (SELECT DISTINCT regexp_match(message, 'trace_id=([0-9a-f]{32})')[1] AS tid FROM archive_logs WHERE ${t}) a ON l.tid = a.tid`);
  if (hours <= SKIP_ALL_STREAMS_ABOVE_H) add("logs", "count all streams", `SELECT count(*) AS n FROM logs WHERE ${t}`);
  // LogQL mirrors of the logs suite; the nonce matcher defeats Loki's results cache per run.
  const W = `${hours}h`;
  add("logql", "count", `sum(count_over_time({app="asserts"__NONCE__}[${W}]))`, { kind: "instant" });
  add("logql", "per hour by service", `sum by (service_name) (count_over_time({app="asserts"__NONCE__}[1h]))`, { kind: "range", step: "1h" });
  add("logql", "latest 100", `{app="asserts"__NONCE__}`, { kind: "range", limit: 100 });
  add("logql", "needle count", `sum(count_over_time({app="asserts"__NONCE__} |= "${NEEDLE}" [${W}]))`, { kind: "instant" });
  if (hours <= SKIP_ALL_STREAMS_ABOVE_H) add("logql", "count all streams", `sum(count_over_time({service_name=~".+"__NONCE__}[${W}]))`, { kind: "instant" });
  // LogQL over the archive (archive-querier behind the shadow frontend) and the identical
  // queries on the live chunk path, over every stream of the tenant, for a like-for-like
  // latency and cost comparison. The live side is capped like the all-streams count above.
  for (const suite of ["archive-logql", "live-logql"]) {
    if (suite === "live-logql" && hours > SKIP_ALL_STREAMS_ABOVE_H) continue;
    const sel = (suite === "archive-logql" ? ARCHIVE_SELECTOR : LIVE_SELECTOR).replace(/}$/, "__NONCE__}");
    add(suite, "count", `sum(count_over_time(${sel}[${W}]))`, { kind: "instant" });
    add(suite, "per hour by service", `sum by (service_name) (count_over_time(${sel}[1h]))`, { kind: "range", step: "1h" });
    add(suite, "latest 100", sel, { kind: "range", limit: 100 });
    add(suite, "needle count", `sum(count_over_time(${sel} |= "${NEEDLE}" [${W}]))`, { kind: "instant" });
    add(suite, "logfmt level per hour", `sum by (level) (count_over_time(${sel} | logfmt | level != "" [1h]))`, { kind: "range", step: "1h" });
  }
  return q;
}

function runSQL(expr) {
  const rows = db.query(expr);
  return { rows: rows.length, error: "" };
}

function runLogQL(q, from, to) {
  const expr = q.expr.replace("__NONCE__", `, __sqlmeasure__!="${Date.now()}"`);
  const headers = { "X-Scope-OrgID": TENANT };
  const base = q.suite === "archive-logql" ? ARCHIVE_LOKI_URL : LOKI_URL;
  let url, params;
  if (q.kind === "instant") {
    url = `${base}/loki/api/v1/query?query=${encodeURIComponent(expr)}&time=${to.getTime() * 1e6}`;
  } else {
    params = `query=${encodeURIComponent(expr)}&start=${from.getTime() * 1e6}&end=${to.getTime() * 1e6}&direction=backward`;
    if (q.step) params += `&step=${q.step}`;
    if (q.limit) params += `&limit=${q.limit}`;
    url = `${base}/loki/api/v1/query_range?${params}`;
  }
  const res = http.get(url, { headers, timeout: "30m", tags: { suite: q.suite, name: q.name } });
  let rows = 0, stats = {};
  if (res.status === 200) {
    try {
      const body = res.json();
      rows = (body.data.result || []).length;
      stats = (body.data.stats && body.data.stats.summary) || {};
    } catch (e) { /* leave rows 0 */ }
  }
  return { rows, error: res.status === 200 ? "" : `HTTP ${res.status}: ${String(res.body).slice(0, 200)}`, stats, expr };
}

export default function () {
  for (const hours of WINDOWS) {
    const from = FROM, to = new Date(FROM.getTime() + hours * 3600e3);
    for (const q of matrix(hours, from, to)) {
      for (let run = 1; run <= RUNS; run++) {
        sleep(SETTLE_S);
        const start = new Date();
        let r;
        try {
          r = q.suite === "logql" || q.suite.endsWith("-logql") ? runLogQL(q, from, to) : runSQL(q.expr);
        } catch (e) {
          r = { rows: 0, error: String(e).slice(0, 300) };
        }
        const end = new Date();
        const row = {
          suite: q.suite, name: q.name, window: `${hours}h0m0s`, from: iso(from), to: iso(to), run,
          start: start.toISOString(), end: end.toISOString(), wall_s: (end - start) / 1000,
          result_rows: r.rows, error: r.error || "", expr: r.expr || q.expr,
          exec_time_s: r.stats ? r.stats.execTime || 0 : 0,
          bytes_processed: r.stats ? r.stats.totalBytesProcessed || 0 : 0,
          lines_processed: r.stats ? r.stats.totalLinesProcessed || 0 : 0,
          cpu_s: 0, bg_cpu_per_s: 0, cpu_s_adjusted: 0, gets: 0, peak_rss_bytes: 0, cost_usd: 0, mem_cost_usd: 0,
          driver: "k6",
        };
        console.log("SQLMEASURE " + JSON.stringify(row));
      }
    }
  }
  console.log("SQLMEASURE_DONE");
}
