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
- Label columns are plain `Utf8`; dictionary encoding would cut the wire size.
- `limit` is carried in the request but ignored by the server.
- Only a local filesystem bucket is wired up in `tools/dataobj-flight`.
