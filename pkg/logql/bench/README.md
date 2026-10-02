# LogQL Benchmark Suite

This directory contains a comprehensive benchmark suite for LogQL, Loki's query language. The suite is designed to generate realistic log data and benchmark various LogQL queries against different storage implementations.

## Overview

The LogQL benchmark suite provides tools to:

1. Generate realistic log data with configurable cardinality, patterns, and time distributions
2. Store the generated data in different storage formats (currently supports "chunk" and "dataobj" formats)
3. Run benchmarks against a variety of LogQL queries, including log filtering and metric queries
4. Compare performance between different storage implementations

## Getting Started

### Prerequisites

- Go 1.21 or later
- At least 2GB of free disk space (for default dataset size)

### Generating Test Data

Before running benchmarks, you need to generate test data:

```bash
# Generate default dataset (2GB)
make generate

# Generate a custom-sized dataset (e.g., 500MB)
make generate SIZE=524288000

# Generate for a specific tenant
make generate TENANT=my-tenant
```

The data generation process:

1. Creates synthetic log data with realistic patterns
2. Stores the data in multiple storage formats for comparison
3. Saves a configuration file that describes the generated dataset

The generated dataset is fully reproducible as it uses a fixed random seed. This ensures that benchmark results are comparable across different runs and environments, making it ideal for performance regression testing.

### Running Benchmarks

Once data is generated, you can run benchmarks:

```bash
# Run all benchmarks
make bench

# List available benchmark queries
make list

# Run benchmarks with interactive UI
make run

# Run with debug output, you need to tail the logs to see the output `tail -f debug.log`
make run-debug

# Stream sample logs from the dataset
make stream
```

## Architecture

### Data Generation

The benchmark suite generates synthetic log data with:

- Configurable label cardinality (clusters, namespaces, services, pods, containers)
- Realistic log patterns for different applications (nginx, postgres, java, etc.)
- Time-based patterns including dense intervals with higher log volume
- Structured metadata and trace context

### Storage Implementations

The suite supports multiple storage implementations:

1. **DataObj Store**: Stores logs as serialized protocol buffer objects
2. **Chunk Store**: Stores logs in compressed chunks similar to Loki's chunk format

### Query Types

The benchmark includes various query types:

- Log filtering queries (e.g., `{app="nginx"} |= "error"`)
- Metric queries (e.g., `rate({app="nginx"}[5m])`)
- Aggregations (e.g., `sum by (status_code) (rate({app="nginx"} | json | status_code != "" [5m]))`)

## Extending the Suite

### Adding New Application Types

The benchmark suite supports multiple application types, each generating different log formats. Currently, it includes applications like web servers, databases, caches, Nginx, Kubernetes, Prometheus, Grafana, Loki, and more.

To add a new application type:

1. Open `faker.go` and add any new data variables needed for your application:

   ```go
   myAppComponents := []string{
       "component1",
       "component2",
       // ...
   }
   ```

2. Add helper methods to the `Faker` struct if needed:

   ```go
   // MyAppComponent returns a random component for my application
   func (f *Faker) MyAppComponent() string {
       return myAppComponents[f.rnd.Intn(len(myAppComponents))]
   }
   ```

3. Add a new entry to the `defaultApplications` slice:

   ```go
   {
       Name: "my-application",
       LogGenerator: func(level string, ts time.Time, f *Faker) string {
           // Generate log line in your desired format (JSON, logfmt, etc.)
           return fmt.Sprintf(
               `level=%s ts=%s component=%s msg="My application log"`,
               level, ts.Format(time.RFC3339), f.MyAppComponent(),
           )
       },
       OTELResource: map[string]string{
           "service_name":    "my-application",
           "service_version": "1.0.0",
           // Add any OpenTelemetry resource attributes
       },
   }
   ```

4. The `LogGenerator` function receives:
   - `level`: The log level (info, warn, error, etc.)
   - `ts`: The timestamp for the log entry
   - `f`: The faker instance for generating random data

5. The `OTELResource` map defines OpenTelemetry resource attributes that will be attached to logs as structured metadata.

After adding your application type, it will be automatically included in the generated dataset with the appropriate distribution based on the generator configuration.

### Adding New Storage Implementations

To add a new storage implementation:

1. Implement the `Store` interface in `store.go`
2. Add the new store to the `cmd/generate/main.go` file
3. Update the benchmark test to include the new store type

### Adding New Query Types

To add new query types:

1. Modify the `GenerateTestCases` method in `generator_query.go`
2. Add new query patterns that test different aspects of LogQL

## SQL over the data objects (DataFusion over Arrow Flight)

The generated data objects can be queried with SQL. `tools/dataobj-flight`
serves them over Arrow Flight (see `pkg/dataobj/arrowflight`), and
`tools/dataobj-sql` is a DataFusion client with a `logs` and a `streams` table.
It needs a Rust toolchain and `protoc`; the first build takes a few minutes.

```bash
make generate SIZE=268435456                      # 256 MB is plenty for a laptop
make flight-server                                # Arrow Flight on 127.0.0.1:8815
make sql QUERY="SELECT service_name, count(*) FROM logs GROUP BY 1 ORDER BY 2 DESC"
make compare                                      # chunk vs v2 engine vs Flight/DataFusion, report in build/
```

### From Grafana with the core Postgres datasource

`dataobj-sql --pg-addr` speaks the Postgres wire protocol (via
`datafusion-postgres`), so Grafana's core Postgres datasource, including
Explore, the visual query builder, autocomplete, and the `$__timeFilter` /
`$__timeGroup` macros, works without any plugin.

```bash
make generate SIZE=268435456
make sql-server          # Flight server + dataobj-sql on 0.0.0.0:5432, foreground
make grafana-sql         # Grafana on :3000 with the 'dataobj-sql' Postgres datasource and a dashboard
```

Then open http://localhost:3000/d/dataobj-sql-bench, or Explore with the
`dataobj-sql` datasource and a query such as:

```sql
SELECT $__timeGroupAlias("timestamp", '5m'), service_name AS metric, count(*) AS lines
FROM logs
WHERE $__timeFilter("timestamp") AND level = 'error'
GROUP BY 1, 2 ORDER BY 1
```

For log lines, keep "Format: Table" (the Grafana 13 Postgres editor offers
only Table and Time series) and put the timestamp first; Explore shows the
table and a Logs panel renders the same frame as log lines:

```sql
SELECT "timestamp", message, service_name
FROM logs
WHERE $__timeFilter("timestamp") AND service_name = 'nginx'
ORDER BY 1 DESC LIMIT 100
```

Set the time range to the generated data (it starts at 2024-01-01 UTC by
default). Any Postgres client works too:

```bash
psql -h 127.0.0.1 -p 5432 -U loki -d datafusion -c 'SELECT count(*) FROM logs'
```

### Archive rules and the `archive_logs` table

An archive rule is a stream selector: matching streams go to the customer's
archive bucket as gzipped OTLP JSON instead of to Loki. The generator models
this, and the Flight server serves such objects as a second table:

```bash
make generate SIZE=268435456 ARCHIVE_RULES='{service_name="nginx"};{service_name="web-server", env="dev"}'
make sql-server          # also serves archive_logs from data/archive/test-tenant
make grafana-sql         # adds http://localhost:3000/d/dataobj-archive
```

`archive_logs` has the envelope columns (`timestamp`, `message`,
`observed_timestamp`, `trace_id`, `span_id`, `severity_text`, ...), one column
per stream label, and two map columns, `resource_attributes` and
`log_attributes`, for everything that is structured metadata in Loki
(`log_attributes['level']`). Every query needs a time range: the archive has no
index, so the timestamp predicate is what selects the five-minute partitions to
read. Live-to-archive correlation is a plain join:

```sql
SELECT a."timestamp", a.service_name, a.message
FROM archive_logs a
JOIN (SELECT DISTINCT trace_id FROM logs
      WHERE $__timeFilter("timestamp") AND level = 'error' AND trace_id IS NOT NULL) l
  ON a.trace_id = l.trace_id
WHERE $__timeFilter(a."timestamp")
ORDER BY 1 DESC LIMIT 100
```

To serve a copy of a real archive-and-replay bucket, point `ARCHIVE_DIR` at the
directory holding `<tenant>/YYYY/MM/DD/HH/mm/*.json.gz` and `ARCHIVE_TENANT` at
the tenant, for example `make sql-server ARCHIVE_DIR=archive-dev ARCHIVE_TENANT=12345`.
Converted push-request objects keep their Loki labels; native OTLP objects go
through Loki's default resource-attribute promotion list (`ARCHIVE_LABELS`
overrides it).

Useful knobs: `FLIGHT_FLAGS=-v make sql-server` logs every scan the Flight
server plans, including the pushed-down predicates;
`RUST_LOG=datafusion_postgres=debug,dataobj_sql=debug` on `dataobj-sql` logs every
statement a client sends. `make sql-server-stop`
kills both processes, `make grafana-stop` the container.

## Remote Correctness Tests

Compare query results between two live Loki endpoints. Requires metadata
from `make discover` and the `remote_correctness` build tag.

    go test -tags=remote_correctness -v ./pkg/logql/bench \
        -addr-1=http://loki-baseline:3100 \
        -addr-2=http://loki-test:3100 \
        -org-id=my-tenant \
        -username=admin -password=secret \
        -metadata-dir=pkg/logql/bench/testdata

Use `-run` to filter queries (same as TestStorageEquality).

## Troubleshooting

- If you see "Data directory is empty" errors, run `make generate` first
- For memory issues, try generating a smaller dataset with `make generate SIZE=524288000`
- For detailed logs, run benchmarks with `make run-debug`

## Performance Profiles

The benchmark suite can generate CPU and memory profiles:

- CPU profile: `cpu.prof`
- Memory profile: `mem.prof`

These can be analyzed with:

```bash
go tool pprof -http=:8080 cpu.prof
go tool pprof -http=:8080 mem.prof
```

## Metadata Discovery

The `discover` tool runs against a real Loki instance to generate `dataset_metadata.json`, which maps available streams, formats, keywords, and other characteristics for query template resolution.

### Prerequisites

Either get credentials for the Loki instance you want to test against or setup a port forward.

Build the discover binary:

```bash
make -C pkg/logql/bench discover/cmd/discover
```

### Running the Discovery Tool

#### Run against a cloud instance

This is the primary workflow (available via `make discover`). It reads TSDB indexes directly from S3 for structural discovery (zero API calls) and uses the Grafana Cloud gateway with HTTP basic auth for content probes (keyword detection, field classification).

```bash
pkg/logql/bench/discover/cmd/discover \
  --address "$LOKI_ADDR" \
  --username "$LOKI_USERNAME" \
  --password "$LOKI_PASSWORD" \
  --tenant "$LOKI_USERNAME" \
  --storage-type s3 \
  --s3.buckets "$S3_BUCKET" \
  --s3.region "$S3_REGION" \
  --s3.endpoint "$S3_ENDPOINT" \
  --s3.access-key-id "$AWS_ACCESS_KEY_ID" \
  --s3.secret-access-key "$AWS_SECRET_ACCESS_KEY" \
  --table-prefix "$TABLE_PREFIX" \
  --selector 'namespace=~"namespace-1|namespace-2|namespace-3"' \
  --max-streams 500 \
  --concurrency 5 \
  --output pkg/logql/bench/testdata \
  --queries-dir pkg/logql/bench/queries
```

#### Via port-forward (alternative)

If you have `kubectl` access with port-forward permissions, you can bypass the auth gateway and hit the query-frontend directly. Use `--tenant` to set the `X-Scope-OrgID` header.

```bash
# In a separate terminal:
kubectl port-forward --context ops-eu-south-0 --namespace loki-ops-002 svc/query-frontend 3100:3100

# Then run discover without --username/--password:
pkg/logql/bench/discover/cmd/discover \
  --address http://localhost:3100 \
  --tenant "$LOKI_USERNAME" \
  --storage-type s3 \
  --s3.buckets "$S3_BUCKET" \
  --s3.region "$S3_REGION" \
  --s3.endpoint "$S3_ENDPOINT" \
  --s3.access-key-id "$AWS_ACCESS_KEY_ID" \
  --s3.secret-access-key "$AWS_SECRET_ACCESS_KEY" \
  --table-prefix "$TABLE_PREFIX" \
  --selector 'namespace=~"namespace-1|namespace-2|namespace-3"' \
  --max-streams 500 \
  --concurrency 5 \
  --output pkg/logql/bench/testdata \
  --queries-dir pkg/logql/bench/queries
```

**Flag reference:**

| Flag                     | Env var                  | Description                                                                                   |
| ------------------------ | ------------------------ | --------------------------------------------------------------------------------------------- |
| `--address`              | `LOKI_ADDR`              | Loki base URL                                                                                 |
| `--tenant`               | `LOKI_ORG_ID`            | X-Scope-OrgID (internal/ops bypass only)                                                      |
| `--username`             | `LOKI_USERNAME`          | HTTP basic auth username (Grafana Cloud)                                                      |
| `--password`             | `LOKI_PASSWORD`          | HTTP basic auth password (Grafana Cloud)                                                      |
| `--bearer-token`         | `LOKI_BEARER_TOKEN`      | Bearer token for Authorization header                                                         |
| `--bearer-token-file`    | `LOKI_BEARER_TOKEN_FILE` | File containing bearer token                                                                  |
| `--output`               | —                        | **Directory** where `dataset_metadata.json` will be written                                   |
| `--queries-dir`          | —                        | Directory containing LogQL query YAML files (default: skip validation)                        |
| `--max-streams`          | —                        | Maximum streams to include (default: 250)                                                     |
| `--from` / `--to`        | —                        | Query time range in RFC3339. Defaults to last 24 hours.                                       |
| `--suites`               | —                        | Comma-separated validation suites: `fast,regression,exhaustive` (default: all)                |
| `--concurrency`          | —                        | Parallel API call limit (default: 5)                                                          |
| `--storage-type`         | —                        | Object storage backend for TSDB index access: `s3`, `gcs`, `azure`, `filesystem`              |
| `--s3.buckets`           | —                        | Comma-separated S3 bucket names                                                               |
| `--s3.region`            | —                        | AWS region for S3 access                                                                      |
| `--s3.endpoint`          | —                        | S3 endpoint URL                                                                               |
| `--s3.access-key-id`     | `AWS_ACCESS_KEY_ID`      | AWS access key ID for S3                                                                      |
| `--s3.secret-access-key` | `AWS_SECRET_ACCESS_KEY`  | AWS secret access key for S3                                                                  |
| `--table-prefix`         | —                        | TSDB index table name prefix (must match `schema_config` in target Loki deployment)           |
| `--selector`             | —                        | Additional label matchers to scope discovery (e.g. `namespace=~"namespace-1\|namespace-2"`) |

### Interpreting the Output

The tool prints to stderr and produces a Validation Report at the end:

- **Exit code 0**: All query templates resolved against the generated metadata.
- **Exit code 1**: One or more query templates failed to resolve. See "Failed Queries" section in output.

**Validation Report sections:**

1. **Resolution Results** — summary counts per suite
2. **Failed Queries** — each failed query with error message (e.g., "no streams with log format: json")
3. **Unreferenced Bounded Set Members** — bounded set entries no query references (candidates for removal)
