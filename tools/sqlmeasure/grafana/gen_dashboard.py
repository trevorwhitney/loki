# Generates the gcx resources in this directory (folders/, dashboards/) for the
# query-in-place cost and speed dashboard. Edit, rerun, push:
#   python3 tools/sqlmeasure/grafana/gen_dashboard.py
#   gcx --context dev resources validate -p tools/sqlmeasure/grafana
#   gcx --context dev resources push -p tools/sqlmeasure/grafana
import json, sys
import os
OUT=os.path.dirname(os.path.abspath(__file__))
FOLDER_UID="loki-dataobj-sql"
DASH_UID="dataobj-sql-cost"
DS={"type":"prometheus","uid":"${datasource}"}
SEL='cluster="$cluster", namespace="$namespace", job="$job"'
RI="[$__rate_interval]"
def rate(m, extra=""): return f'rate({m}{{{SEL}{extra}}}{RI})'
def srate(m, extra=""): return f'sum({rate(m, extra)})'
CPU=srate("process_cpu_seconds_total")
GETS=srate("loki_objstore_bucket_operations_total", ', operation=~"get|get_range|iter|attributes"')
COMP=srate("loki_dataobj_flight_archive_compressed_bytes_total")
UNCOMP=srate("loki_dataobj_flight_archive_uncompressed_bytes_total")
OBJS=srate("loki_dataobj_flight_archive_objects_fetched_total")
COST_RATE=f'({CPU} * $cpu_price / 3600 + {GETS} * $get_price / 1000)'

pid=[0]
def nid():
    pid[0]+=1; return pid[0]
def target(expr, legend, ref="A", instant=False):
    t={"datasource":DS,"expr":expr,"legendFormat":legend,"refId":ref,"range":not instant,"instant":instant}
    return t
def ts(title, targets, x,y,w,h, unit="short", desc="", stack=False, min0=True, overrides=None, maxv=None, legend="bottom"):
    p={"type":"timeseries","title":title,"description":desc,"datasource":DS,"id":nid(),
       "gridPos":{"x":x,"y":y,"w":w,"h":h},
       "fieldConfig":{"defaults":{"unit":unit,"custom":{"lineWidth":1,"fillOpacity":10 if not stack else 35,"stacking":{"mode":"normal" if stack else "none","group":"A"},"showPoints":"never","spanNulls":True}},"overrides":overrides or []},
       "options":{"legend":{"displayMode":"table","placement":legend,"calcs":["mean","max"]},"tooltip":{"mode":"multi","sort":"desc"}},
       "targets":[target(e,l,chr(65+i)) for i,(e,l) in enumerate(targets)]}
    if min0: p["fieldConfig"]["defaults"]["min"]=0
    if maxv is not None: p["fieldConfig"]["defaults"]["max"]=maxv
    return p
def stat(title, expr, x,y,w,h, unit="short", desc="", decimals=None, color="blue"):
    d={"unit":unit,"color":{"mode":"fixed","fixedColor":color}}
    if decimals is not None: d["decimals"]=decimals
    return {"type":"stat","title":title,"description":desc,"datasource":DS,"id":nid(),
            "gridPos":{"x":x,"y":y,"w":w,"h":h},
            "fieldConfig":{"defaults":d,"overrides":[]},
            "options":{"reduceOptions":{"calcs":["mean"],"fields":"","values":False},"colorMode":"value","graphMode":"area","textMode":"value","orientation":"auto"},
            "targets":[target(expr,"",'A')]}
def row(title,y,collapsed=False):
    return {"type":"row","title":title,"id":nid(),"gridPos":{"x":0,"y":y,"w":24,"h":1},"collapsed":collapsed,"panels":[]}
def heatmap(title, expr, legend, x,y,w,h, unit="Bps", desc=""):
    t=target(expr, legend, 'A'); t["format"]="heatmap"
    return {"type":"heatmap","title":title,"description":desc,"datasource":DS,"id":nid(),
            "gridPos":{"x":x,"y":y,"w":w,"h":h},
            "options":{"calculate":False,"yAxis":{"unit":unit,"decimals":0},"color":{"mode":"scheme","scheme":"Spectral","steps":64,"reverse":False},
                       "cellGap":1,"legend":{"show":True},"tooltip":{"mode":"single","yHistogram":True},"exemplars":{"color":"rgba(255,0,255,0.7)"}},
            "targets":[t]}
def text(md,x,y,w,h,title=""):
    return {"type":"text","title":title,"id":nid(),"gridPos":{"x":x,"y":y,"w":w,"h":h},"options":{"mode":"markdown","content":md}}

panels=[]
y=0
panels.append(text(
"**Query-in-place cost model.** `cost = CPU-s × $/vCPU-h + object-store requests × $/1k + bytes × $/GB egress`. "
"To first order the cost of a query is linear in compressed bytes scanned and independent of speed; speed (MB/s per core) sets how many cores a latency target needs, i.e. fleet size. "
"Rows 1 and 2 read the Flight scan service's `loki_dataobj_flight_*` series for **$job** and are empty until the shadow deployment runs. "
"Row 3 reads the live Loki query path in **$namespace** today so the two can be compared on the same axes. Prices are the `cpu_price` and `get_price` variables. "
"Numbers are per-process rates, so run one query at a time on the shadow pod, or read them as averages.", 0,y,24,3))
y+=3

# ---- Resources (Trevor, 2026-10-06): raw CPU and memory of both sides, totals then per pod
QJ_SEL='cluster="$cluster", namespace="$namespace", job=~"$querier_job"'
panels.append(row("Resources — raw CPU and memory",y)); y+=1
panels.append(ts("CPU usage — Flight scan service (cores, total)", [(CPU, "total")], 0,y,12,8, unit="short"))
panels.append(ts("Memory usage — Flight scan service (RSS, total)", [(f'sum(process_resident_memory_bytes{{{SEL}}})', "total")], 12,y,12,8, unit="bytes"))
y+=8
panels.append(ts("CPU usage — queriers (cores)", [(f'sum by (job) (rate(process_cpu_seconds_total{{{QJ_SEL}}}{RI}))', "{{job}}")], 0,y,12,8, unit="short"))
panels.append(ts("Memory usage — queriers (RSS)", [(f'sum by (job) (process_resident_memory_bytes{{{QJ_SEL}}})', "{{job}}")], 12,y,12,8, unit="bytes"))
y+=8
panels.append(row("Resources — per pod",y)); y+=1
panels.append(ts("CPU usage per pod — Flight scan service (cores)", [(f'sum by (pod) (rate(process_cpu_seconds_total{{{SEL}}}{RI}))', "{{pod}}")], 0,y,12,8, unit="short", legend="right"))
panels.append(ts("Memory usage per pod — Flight scan service (RSS)", [(f'sum by (pod) (process_resident_memory_bytes{{{SEL}}})', "{{pod}}")], 12,y,12,8, unit="bytes", legend="right"))
y+=8
panels.append(ts("CPU usage per pod — queriers (cores)", [(f'sum by (pod) (rate(process_cpu_seconds_total{{{QJ_SEL}}}{RI}))', "{{pod}}")], 0,y,12,8, unit="short", legend="right"))
panels.append(ts("Memory usage per pod — queriers (RSS)", [(f'sum by (pod) (process_resident_memory_bytes{{{QJ_SEL}}})', "{{pod}}")], 12,y,12,8, unit="bytes", legend="right"))
y+=8

# ---- Row 0: side by side, live queriers vs Flight, bytes throughput
QJ='job=~"$querier_job"'
QR=f'cluster=~"$cluster", {QJ}'
panels.append(row("Bytes throughput side by side — live queriers ($querier_job) vs Flight scan service ($job)",y)); y+=1
panels.append(ts("Bytes throughput per query — live queriers", [
    (f'histogram_quantile(0.01, sum by (le, type, range) (job_type_range:loki_logql_querystats_bytes_processed_per_seconds_bucket:sum_rate{{{QR}}}))', "{{type}} - {{range}} p1"),
    (f'sum(job_type_range:loki_logql_querystats_bytes_processed_per_seconds:50quantile{{{QR}}}) by (type, range)', "{{type}} - {{range}} p50"),
    (f'sum(job_type_range:loki_logql_querystats_bytes_processed_per_seconds:99quantile{{{QR}}}) by (type, range)', "{{type}} - {{range}} p99"),
    (f'sum(job_type_range:loki_logql_querystats_bytes_processed_per_seconds:avg{{{QR}}}) by (type, range)', "{{type}} - {{range}} avg"),
    ], 0,y,12,9, unit="Bps", min0=True,
    desc="Same panel as 'Bytes Throughput Querier' on the Loki / Queries dashboard, pointed at this cell's queriers: distribution of bytes processed per second per query (recording rules over loki_logql_querystats_bytes_processed_per_seconds). Uncompressed bytes."))
panels.append(ts("Bytes throughput — Flight scan service", [
    (UNCOMP, "archive uncompressed decoded / s"),
    (COMP, "archive compressed fetched / s"),
    ('sum('+rate("loki_dataobj_flight_bytes_served_total")+')', "Arrow bytes served to gateway / s"),
    (f'{UNCOMP} / ({CPU} > 0)', "uncompressed per busy core / s"),
    ], 12,y,12,9, unit="Bps", min0=True,
    desc="Fleet decode rate of the shadow Flight pods. The service has no per-query histogram yet; while one query runs at a time (how the harness measures) this IS that query's bytes throughput, so read it against the live panel's percentiles. 'per busy core' divides by the cores in use so the two sides can be compared at equal CPU."))
y+=9
panels.append(heatmap("Bytes throughput per query heatmap — live queriers", f'sum(rate(loki_logql_querystats_bytes_processed_per_seconds_bucket{{{QR}}}[$__rate_interval])) by (le)', "{{le}}", 0,y,12,9,
    desc="Same as 'Bytes Throughput Querier' heatmap (panel 18) on the Loki / Queries dashboard, for this cell."))
panels.append(ts("Bytes throughput per pod — Flight scan service", [
    ('sum by (pod) ('+rate("loki_dataobj_flight_archive_uncompressed_bytes_total")+')', "{{pod}} archive uncompressed"),
    ('sum by (pod) ('+rate("loki_dataobj_flight_bytes_served_total")+')', "{{pod}} Arrow served"),
    ], 12,y,12,9, unit="Bps", min0=True, stack=True,
    desc="The same rate split by replica (stacked): shows how evenly the ring spreads a query across the pool."))
y+=9

GW='cluster="$cluster", namespace="$namespace", pod=~"shadow-sql-gateway.*"'
panels.append(ts("Query latency — live path (query-frontend)", [
    (f'histogram_quantile(0.50, sum by (le, route) (rate(loki_request_duration_seconds_bucket{{cluster="$cluster", namespace="$namespace", job=~"$frontend_job", route=~"loki_api_v1_query|loki_api_v1_query_range"}}{RI})))', "p50 {{route}}"),
    (f'histogram_quantile(0.99, sum by (le, route) (rate(loki_request_duration_seconds_bucket{{cluster="$cluster", namespace="$namespace", job=~"$frontend_job", route=~"loki_api_v1_query|loki_api_v1_query_range"}}{RI})))', "p99 {{route}}"),
    ], 0,y,12,8, unit="s",
    desc="End-to-end LogQL request latency at the query-frontend: planning, splitting, every sub-query, merge."))
panels.append(ts("Query latency — SQL gateway (per statement)", [
    (f'histogram_quantile(0.50, sum by (le, kind) (rate(dataobj_sql_statement_duration_seconds_bucket{{{GW}}}{RI})))', "p50 {{kind}}"),
    (f'histogram_quantile(0.99, sum by (le, kind) (rate(dataobj_sql_statement_duration_seconds_bucket{{{GW}}}{RI})))', "p99 {{kind}}"),
    (f'sum(dataobj_sql_statements_in_flight{{{GW}}})', "statements in flight"),
    ], 12,y,12,8, unit="s",
    desc="The SQL counterpart of the frontend panel: one observation per statement on dataobj-sql, from parse to the last result row (planning, every Flight DoGet the plan fans out into, and DataFusion's sort/join/aggregate). kind=query are data queries, kind=catalog Grafana's schema probes. 'Scan duration per endpoint' below is per ticket, thousands of which make up one statement, so it is not latency. Needs a gateway image with --metrics-addr."))
y+=8

# ---- Row 1: cost
panels.append(row("Cost — Flight scan service ($job)",y)); y+=1
# Divisions guard against an idle service (rate 0) so panels read "no data" instead of "∞".
COMP_GB=f'({COMP} / 1e9 > 0)'
UNCOMP_GB=f'({UNCOMP} / 1e9 > 0)'
panels.append(stat("$ / compressed GB", f'{COST_RATE} / {COMP_GB}', 0,y,8,4, unit="currencyUSD", decimals=4, color="green",
    desc="Marginal cost of scanning the archive: (CPU + GET spend per second) / (compressed bytes fetched per second). Speed-independent to first order."))
panels.append(stat("$ / uncompressed GB", f'{COST_RATE} / {UNCOMP_GB}', 8,y,8,4, unit="currencyUSD", decimals=5, color="green",
    desc="Same spend divided by the uncompressed bytes decoded, the unit the live Loki path reports (bytes processed). Compare with the baseline row."))
panels.append(stat("$ / hour scanning", COST_RATE+" * 3600", 16,y,8,4, unit="currencyUSD", decimals=3, color="green",
    desc="Spend rate of this process at the configured prices while it is busy."))
panels.append(stat("CPU-s / compressed GB", f'{CPU} / {COMP_GB}', 0,y+4,8,4, unit="short", decimals=0, color="orange",
    desc="Locally ~190 for the row-oriented archive (every record in the window is decoded, whatever the query shape)."))
panels.append(stat("CPU-s / uncompressed GB", f'{CPU} / {UNCOMP_GB}', 8,y+4,8,4, unit="short", decimals=1, color="orange",
    desc="CPU seconds per GB of decoded archive data; the live path measured ~62 CPU-s per GB processed on 2026-10-02."))
panels.append(stat("CPU cores busy", CPU, 16,y+4,8,4, unit="short", decimals=2, color="orange",
    desc="Average cores busy over the dashboard range. CPU-s / wall-s of a query is how many cores it actually used."))
y+=8
panels.append(ts("Bytes scanned / s", [
    (COMP, "archive compressed (fetched)"),
    (UNCOMP, "archive uncompressed (decoded)"),
    ('sum by (table) ('+rate("loki_dataobj_flight_bytes_served_total")+')', "served to gateway: {{table}}"),
    ], 0,y,12,8, unit="Bps", desc="For archive_logs cost follows compressed bytes fetched. For logs (data objects) it follows Arrow bytes served, since objects are read by column and page."))
panels.append(ts("Object-store requests / s", [
    ('sum by (operation) ('+rate("loki_objstore_bucket_operations_total")+')', "{{operation}}"),
    (OBJS, "archive objects fetched"),
    ], 12,y,12,8, unit="reqps", desc="Loki registers one shared objstore counter per process (empty bucket label), so data-object and archive GETs are summed. 'archive objects fetched' isolates the archive side."))
y+=8
panels.append(ts("Cost rate by driver ($/h)", [
    (f'{CPU} * $cpu_price', "CPU"),
    (f'{GETS} * $get_price / 1000 * 3600', "object-store requests"),
    ], 0,y,12,8, unit="currencyUSD", stack=True, desc="Which lever matters. With ~2.5 KB archive objects, GET fees were expected to be the same order as CPU."))
panels.append(ts("Mean archive object size", [
    (f'{COMP} / {OBJS}', "compressed bytes per object"),
    (f'{UNCOMP} / {OBJS}', "uncompressed bytes per object"),
    ], 12,y,12,8, unit="bytes", desc="Small objects mean cost is driven by request count, not bytes. Batching objects is the lever."))
y+=8

# ---- Row 2: speed
panels.append(row("Speed — Flight scan service ($job)",y)); y+=1
panels.append(ts("Throughput per core", [
    (f'{COMP} / {CPU}', "compressed bytes / core-s"),
    (f'{UNCOMP} / {CPU}', "uncompressed bytes / core-s"),
    ], 0,y,12,8, unit="Bps", desc="Decode rate per core. Locally ~5 MB/s compressed, ~37 MB/s uncompressed. Cores needed = (bytes in window) / (this × target seconds)."))
panels.append(ts("Scan duration per endpoint (DoGet)", [
    ('histogram_quantile(0.50, sum by (le, table) ('+rate("loki_dataobj_flight_scan_duration_seconds_bucket")+'))', "p50 {{table}}"),
    ('histogram_quantile(0.99, sum by (le, table) ('+rate("loki_dataobj_flight_scan_duration_seconds_bucket")+'))', "p99 {{table}}"),
    ], 12,y,12,8, unit="s", desc="Open to last batch for one ticket. The gateway runs tickets in parallel, so query wall time ≈ max over tickets plus gateway work."))
y+=8
panels.append(ts("Planning", [
    ('histogram_quantile(0.99, sum by (le, table) ('+rate("loki_dataobj_flight_plan_duration_seconds_bucket")+'))', "plan p99 {{table}}"),
    ('histogram_quantile(0.99, sum by (le) ('+rate("loki_dataobj_flight_archive_list_duration_seconds_bucket")+'))', "archive list p99"),
    ], 0,y,8,8, unit="s", desc="Fixed per-query overhead: metastore lookup or archive listing. ~25 ms locally; dominates only for windows under a few minutes."))
panels.append(ts("Tickets per plan", [
    ('sum by (table) ('+rate("loki_dataobj_flight_planned_tickets_sum")+') / sum by (table) ('+rate("loki_dataobj_flight_planned_tickets_count")+')', "{{table}}"),
    ], 8,y,8,8, unit="short", desc="Parallelism available to the gateway: sections for data objects, object bundles for the archive."))
panels.append(ts("Archive time split: decode vs fetch", [
    (srate("loki_dataobj_flight_archive_decode_duration_seconds_sum")+' / ('+srate("loki_dataobj_flight_archive_decode_duration_seconds_sum")+' + '+srate("loki_dataobj_flight_archive_fetch_duration_seconds_sum")+')', "decode (CPU) share"),
    (srate("loki_dataobj_flight_archive_fetch_duration_seconds_sum")+' / ('+srate("loki_dataobj_flight_archive_decode_duration_seconds_sum")+' + '+srate("loki_dataobj_flight_archive_fetch_duration_seconds_sum")+')', "fetch (network) share"),
    ], 16,y,8,8, unit="percentunit", stack=True, maxv=1, desc="If fetch dominates, add concurrency; if decode dominates, the per-core rate is the ceiling."))
y+=8
panels.append(ts("Rows and records / s", [
    ('sum by (table) ('+rate("loki_dataobj_flight_rows_served_total")+')', "rows served {{table}}"),
    (srate("loki_dataobj_flight_archive_records_decoded_total"), "archive records decoded"),
    (srate("loki_dataobj_flight_archive_records_matched_total"), "archive records matched"),
    ], 0,y,8,8, unit="short", desc="decoded ≫ matched means the archive layout forces decoding of records the predicate later drops."))
panels.append(ts("Memory", [
    ('sum('+f'process_resident_memory_bytes{{{SEL}}}'+')', "RSS"),
    ('sum(container_memory_working_set_bytes{cluster="$cluster", namespace="$namespace", pod=~"shadow-dataobj-flight.*", container!=""})', "working set (cgroup)"),
    ], 8,y,8,8, unit="bytes"))
panels.append(ts("Scan errors / s", [
    ('sum by (table) ('+rate("loki_dataobj_flight_scan_errors_total")+')', "{{table}}"),
    ], 16,y,8,8, unit="short"))
y+=8
panels.append(ts("Scans in flight per pod", [
    (f'sum by (pod) (loki_dataobj_flight_scans_in_flight{{{SEL}}})', "{{pod}}"),
    ], 0,y,8,8, unit="short", desc="Concurrent DoGet scans per replica, bounded by -dataobj-flight.max-concurrent-scans. The gateway's --max-partitions bounds the total per query."))
panels.append(ts("Scan queue wait", [
    ('histogram_quantile(0.50, sum by (le) ('+rate("loki_dataobj_flight_scan_queue_duration_seconds_bucket")+'))', "p50"),
    ('histogram_quantile(0.99, sum by (le) ('+rate("loki_dataobj_flight_scan_queue_duration_seconds_bucket")+'))', "p99"),
    ], 8,y,8,8, unit="s", desc="Time a DoGet waited for a scan slot. Sustained waits mean the servers, not the gateway, are the bottleneck: add replicas or slots."))
panels.append(ts("Disk cache", [
    ('sum by (bucket) ('+rate("loki_dataobj_flight_disk_cache_hits_total")+')', "hits/s {{bucket}}"),
    ('sum by (bucket) ('+rate("loki_dataobj_flight_disk_cache_misses_total")+')', "misses/s {{bucket}}"),
    ], 16,y,8,8, unit="short", desc="Read-through disk cache of object store reads (-dataobj-flight.disk-cache.dir). A warm run of the same query is all hits: its CPU is the decode cost with no object store traffic."))
y+=8

# ---- Row 3: baseline
BQ='cluster="$cluster", namespace="$namespace", job=~"$querier_job"'
BF='cluster="$cluster", namespace="$namespace", job=~"$frontend_job"'
BCPU=f'sum(rate(process_cpu_seconds_total{{{BQ}}}{RI}))'
BBYTES=f'sum(rate(loki_logql_querystats_bytes_processed_total{{{BF}}}{RI}))'
BGETS=f'sum(rate(loki_objstore_bucket_operations_total{{{BQ}, operation=~"get|get_range|iter|attributes"}}{RI}))'
panels.append(row("Baseline — live Loki query path in $namespace (queriers $querier_job, frontends $frontend_job)",y)); y+=1
panels.append(stat("Baseline $ per GB processed (uncompressed)", f'({BCPU} * $cpu_price / 3600 + {BGETS} * $get_price / 1000) / ({BBYTES} / 1e9)', 0,y,6,4, unit="currencyUSD", decimals=4, color="purple",
    desc="Querier CPU + GET spend over LogQL bytes processed as recorded by the query-frontend (queriers also record it; counting both would double it). LogQL reports uncompressed bytes, the Flight row reports compressed: compare against the uncompressed line in 'Bytes scanned'. Includes queries answered from ingesters and caches, so it is a floor for the store path."))
panels.append(stat("Baseline CPU-s per GB processed", f'{BCPU} / ({BBYTES} / 1e9)', 6,y,6,4, unit="short", decimals=0, color="purple"))
panels.append(stat("Querier cores in use", BCPU, 12,y,6,4, unit="short", decimals=1, color="purple"))
panels.append(stat("LogQL bytes processed / s", BBYTES, 18,y,6,4, unit="Bps", color="purple"))
y+=4
panels.append(ts("Query latency (query-frontend)", [
    (f'histogram_quantile(0.50, sum by (le, route) (rate(loki_request_duration_seconds_bucket{{{BF}, route=~"loki_api_v1_query|loki_api_v1_query_range"}}{RI})))', "p50 {{route}}"),
    (f'histogram_quantile(0.99, sum by (le, route) (rate(loki_request_duration_seconds_bucket{{{BF}, route=~"loki_api_v1_query|loki_api_v1_query_range"}}{RI})))', "p99 {{route}}"),
    ], 0,y,12,8, unit="s"))
panels.append(ts("Per-query throughput distribution (LogQL)", [
    (f'histogram_quantile(0.50, sum by (le) (rate(loki_logql_querystats_bytes_processed_per_seconds_bucket{{cluster="$cluster", namespace="$namespace", job=~"$frontend_job"}}{RI})))', "p50 bytes/s per query"),
    (f'histogram_quantile(0.90, sum by (le) (rate(loki_logql_querystats_bytes_processed_per_seconds_bucket{{cluster="$cluster", namespace="$namespace", job=~"$frontend_job"}}{RI})))', "p90 bytes/s per query"),
    ], 12,y,12,8, unit="Bps", desc="What a LogQL query achieves end to end today (uncompressed bytes). The Flight equivalent is 'Throughput per core' × cores used."))
y+=8
panels.append(ts("Query rate by route (query-frontend)", [
    (f'sum by (route, status_code) (rate(loki_request_duration_seconds_count{{{BF}, route=~"loki_api_v1_query|loki_api_v1_query_range"}}{RI}))', "{{route}} {{status_code}}"),
    ], 0,y,8,8, unit="reqps"))
panels.append(ts("Querier object-store requests / s", [
    (f'sum by (operation) (rate(loki_objstore_bucket_operations_total{{{BQ}}}{RI}))', "{{operation}}"),
    ], 8,y,8,8, unit="reqps"))
panels.append(ts("Querier bytes fetched from object store / s", [
    (f'sum(rate(loki_objstore_bucket_operation_fetched_bytes_total{{{BQ}}}{RI}))', "fetched"),
    ], 16,y,8,8, unit="Bps"))
y+=8

def qvar(name,label,query,current,multi=False,include_all=False,regex=""):
    v={"type":"query","name":name,"label":label,"datasource":DS,"query":{"query":query,"refId":"var"},"refresh":2,"sort":1,
       "multi":multi,"includeAll":include_all,"regex":regex,
       "current":{"text":current,"value":current,"selected":True},"options":[]}
    if include_all: v["allValue"]=".*"
    return v
templating=[
    {"type":"datasource","name":"datasource","label":"Prometheus","query":"prometheus","current":{"text":"","value":"","selected":False},"refresh":1,"regex":"","options":[]},
    qvar("cluster","Cluster",'label_values(up{namespace=~"loki-.*"}, cluster)',""),
    qvar("namespace","Namespace",'label_values(up{cluster="$cluster", namespace=~"loki-.*"}, namespace)',""),
    qvar("job","Flight scan job",'label_values(up{cluster="$cluster", namespace="$namespace"}, job)',""),
    qvar("querier_job","Baseline querier job",'label_values(up{cluster="$cluster", namespace="$namespace", container="querier"}, job)',"$__all",multi=True,include_all=True),
    qvar("frontend_job","Baseline frontend job",'label_values(up{cluster="$cluster", namespace="$namespace", container="query-frontend"}, job)',"$__all",multi=True,include_all=True),
    {"type":"textbox","name":"cpu_price","label":"$ per vCPU-hour","query":"0.045","current":{"text":"0.045","value":"0.045","selected":True},"options":[]},
    {"type":"textbox","name":"get_price","label":"$ per 1k object-store requests","query":"0.0004","current":{"text":"0.0004","value":"0.0004","selected":True},"options":[]},
]
spec={
  "uid":DASH_UID,"title":"Loki dataobj SQL: query-in-place cost and speed","tags":["loki","dataobj","sql","cost"],
  "description":"Cost ($/GB, $/query drivers) and speed (MB/s per core, latency) of the Arrow Flight + DataFusion query-in-place prototype, next to the live Loki query path in the same cell.",
  "timezone":"browser","editable":True,"graphTooltip":1,"schemaVersion":39,"version":1,"refresh":"30s",
  "time":{"from":"now-3h","to":"now"},
  "templating":{"list":templating},
  "annotations":{"list":[{"builtIn":1,"type":"dashboard","name":"Annotations & Alerts","datasource":{"type":"grafana","uid":"-- Grafana --"},"enable":True,"hide":True,"iconColor":"rgba(0, 211, 255, 1)"}]},
  "links":[{"title":"Measurement harness (tools/sqlmeasure)","type":"link","url":"https://github.com/grafana/loki/tree/main/tools/sqlmeasure","targetBlank":True}],
  "panels":panels,
}
dash={"apiVersion":"dashboard.grafana.app/v0alpha1","kind":"Dashboard",
      "metadata":{"name":DASH_UID,"annotations":{"grafana.app/folder":FOLDER_UID}},
      "spec":spec}
folder={"apiVersion":"folder.grafana.app/v1","kind":"Folder","metadata":{"name":FOLDER_UID,"annotations":{"grafana.app/folder":"users"}},
        "spec":{"title":"Loki dataobj SQL (query in place)","description":"Dashboards for the Arrow Flight + DataFusion query-in-place prototype and its shadow deployment."}}
json.dump(dash,open(f"{OUT}/dashboards/{DASH_UID}.json","w"),indent=2)
json.dump(folder,open(f"{OUT}/folders/{FOLDER_UID}.json","w"),indent=2)
print("panels:",len(panels),"rows of height",y)
