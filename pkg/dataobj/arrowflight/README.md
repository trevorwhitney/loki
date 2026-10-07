# arrowflight: data objects over Arrow Flight

This package is a proof of concept that exposes Loki data objects to an
external query engine through [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html).
The reference client is a DataFusion `TableProvider` in
[`tools/dataobj-sql`](../../../tools/dataobj-sql), and the server binary is
[`tools/dataobj-flight`](../../../tools/dataobj-flight).

The design goal is the "Loki as a distributed scan layer" model: Loki prunes
and returns Arrow record batches, and the query engine lives elsewhere.

## Tables

The server infers two tables from the union of columns across every data
object it discovers.

| Table     | Columns                                                                                  |
|-----------|------------------------------------------------------------------------------------------|
| `logs`    | `stream_id`, `timestamp`, `message`, one `Utf8` column per stream label, one per structured metadata key |
| `streams` | `stream_id`, `min_timestamp`, `max_timestamp`, `rows`, `uncompressed_size`, one `Utf8` column per stream label |

Stream labels are joined into `logs` rows by the server, so the common query
needs no join and label filters prune at the stream level. The `streams` table
answers series-style questions without touching log data, and `stream_id` is
present in both tables so an explicit join is still possible.

Label and metadata keys become bare column names. A key that collides with an
existing column is prefixed with `label_` or `metadata_` (for example a
structured metadata key `cluster` next to a label `cluster` becomes
`metadata_cluster`). DataFusion folds unquoted identifiers to lower case, so
mixed-case keys such as `traceID` must be double-quoted in SQL.

## Protocol

Everything the client needs is in [`scanpb/scan.proto`](scanpb/scan.proto).

1. `GetSchema` with a path descriptor `["logs"]` or `["streams"]` returns the
   table schema.
2. `GetFlightInfo` with a command descriptor holding a `ScanRequest`
   (projection plus a conjunction of `Predicate`s) returns one endpoint per
   data object section. Each endpoint's ticket is a `Ticket` message.
3. `DoGet` with that ticket streams record batches in the projected schema.

Predicates are a pruning hint. The server pushes down what it can and may
return non-matching rows, so clients must re-apply every filter. The server
never drops a matching row.

### Locations and scaling out

Every ticket is self-contained, so any server can serve any endpoint, and the
server that plans a scan does not have to be the one that serves it. When a
`Locator` is configured, `GetFlightInfo` puts the servers that should serve
each endpoint on it as Flight `Location`s (`grpc+tcp://host:port`, or
`grpc+tls://` when the gRPC server has TLS), most preferred first. Locations
are a routing hint: a client that ignores them, or whose preferred location is
down, can send the ticket to any server, including the one it planned on.

Loki's `dataobj-flight` target gets its locator from a dskit ring when
`-dataobj-flight.ring.enabled` is set (`dataobj_flight.ring` holds the usual
ring options). Every instance joins the ring as both planner and scanner, so
a client can plan on any of them. The object path of a ticket is hashed onto
the ring, so all sections of one object land on the same replica set while the
ring is stable and that replica's object cache stays warm. The ring's
replication factor is the number of locations offered per endpoint; the
client falls through them in order and then to the planner. The ring is
served at `/dataobj-flight/ring`, and
`loki_dataobj_flight_located_endpoints_total` counts endpoints that got
locations.

`tools/dataobj-sql` honors locations: it opens one lazy channel per distinct
location, shared by every scan, and falls back to the planning connection
when a located DoGet fails. `--ignore-locations` sends everything to `--addr`
instead, for debugging or when `--addr` is a proxy that balances per request.
The local `tools/dataobj-flight` binary has no ring; `-locations` stamps a
fixed list on every endpoint so the client path can be exercised with two
processes over the same directory.

Note that a plain Kubernetes ClusterIP service in front of the pool does not
spread load by itself: tonic keeps one HTTP/2 connection per channel, so every
call from a client would pin to one pod. The ring and locations are what fan
DoGet out.

## Bounding fan-out and warming up

A planned scan has one endpoint per section (or per group of archive
objects), and DataFusion executes every partition of a plan at once. Left
unbounded, a 24 h scan opens thousands of DoGet streams and memory on the
servers grows with the plan rather than with their size; in the dev cell that
OOM-killed every 4 GiB replica. Two bounds, both on by default:

- `dataobj-sql --max-partitions N` (default 16) deals the endpoints round-robin
  into at most N DataFusion partitions; each partition reads its endpoints one
  after another, so N is the number of DoGet streams a query keeps open across
  the pool.
- `-dataobj-flight.max-concurrent-scans N` (default 16) is what one server
  serves at once; further DoGets wait for a slot. `loki_dataobj_flight_scans_in_flight`
  and `loki_dataobj_flight_scan_queue_duration_seconds` show whether the servers
  or the client are the bottleneck.

Memory also grows with the streams tables the server builds to join labels
into log rows (one per object, a map per stream). A scan now loads only the
label columns it projects or filters on and, when the metastore already
resolved the ticket's stream IDs, only those streams; full tables are cached
up to `-dataobj-flight.streams-cache-bytes` (256 MiB) and opened objects up to
`-dataobj-flight.max-cached-objects` (256). Before this, a 24 h scan of a
tenant with hundreds of thousands of streams per object held gigabytes of
label maps and OOM-killed 8 GiB replicas with the scan bound in place.

`-dataobj-flight.disk-cache.dir` (with `-dataobj-flight.disk-cache.max-size-bytes`,
default 10 GiB) puts a read-through disk cache under every object store the
target reads: data object pages, metastore index objects and archive objects.
Entries are keyed by object and byte range, so a repeated query is served from
local disk and its CPU is the decode cost with no object store traffic; the
first run (cold) and the second (warm) bracket the cost of a query in place.
Point it at an emptyDir in a cell; entries survive a container restart.

## Pushdown

| SQL shape                                  | Server behaviour                                                     |
|--------------------------------------------|----------------------------------------------------------------------|
| projection                                 | Only the requested columns are read from the section.                |
| `timestamp <op> literal`                   | `logs` page/row predicate on the timestamp column.                   |
| `<label> = 'x'` / `<label> IN (...)`       | Resolved against the streams section to a `stream_id IN (...)` predicate; a label that matches no stream short-circuits to an empty result. |
| `<metadata> = 'x'` / `IN (...)`            | `logs` predicate on the metadata column; a key absent from the section short-circuits to an empty result. |
| `stream_id`, `rows`, `uncompressed_size`, `min_timestamp`, `max_timestamp` comparisons on `streams` | Streams section predicates. |
| anything else (`LIKE`, functions, `OR`, ...) | Not pushed down; DataFusion evaluates it.                           |

## Running it

```bash
# 1. Generate data objects (from pkg/logql/bench)
make generate SIZE=268435456

# 2. Serve them
make flight-server            # go run ../../../tools/dataobj-flight -dir data/storage/dataobj

# 3. Query with DataFusion (needs cargo and protoc)
make sql QUERY="SELECT service_name, count(*) FROM logs GROUP BY 1 ORDER BY 2 DESC"
```

Or drive the Rust CLI directly:

```bash
cd tools/dataobj-sql
cargo run --release -- "SELECT level, count(*) FROM logs WHERE service_name = 'nginx' GROUP BY 1"
cargo run --release -- "EXPLAIN SELECT * FROM logs WHERE timestamp > '2024-01-01T06:00:00Z' LIMIT 10"
```

## Comparing with LogQL on the v2 engine

`pkg/logql/bench` has a `dataobj-flight-sql` store that answers LogQL by
translating it to SQL and running it through `dataobj-sql` against an in-process
Flight server. It plugs into `TestStorageEquality`, so the same templated
queries run against the chunk baseline, the v2 engine, and this path, with
results compared and timings reported side by side:

```bash
# from pkg/logql/bench, after `make generate`
make compare                      # fast suite, writes build/storage-equality-report.md
make compare REPORT=/tmp/r.md     # elsewhere
```

The translator covers stream selectors (`=`, `!=`, `=~`, `!~`), line filters,
string label filters, and `sum [by (...)]` of `count_over_time`, `rate`, and
`bytes_over_time`. Range queries are translated only when the range is a
multiple of the step. Everything else returns `engine.ErrNotSupported` and
shows up as a skip. See `flight_sql_translate.go` for the exact rules and
`tools/dataobj-sql --serve` for the JSON protocol the store speaks.

## Not done yet

- No pruning of whole sections by time range before handing out endpoints.
- Schema is discovered once per process at startup; instances in a ring that
  started at different times can disagree on label columns until restarted
  (pin them with `-dataobj-flight.extra-labels` in the meantime).
- No bound on concurrent data object scans per server; the client's
  `target_partitions` is the only limit.
- Label columns are plain `Utf8`; dictionary encoding would cut the wire size.
- `limit` is carried in the request but ignored by the server.
- Only a local filesystem bucket is wired up in `tools/dataobj-flight`.
