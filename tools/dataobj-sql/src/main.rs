//! dataobj-sql runs DataFusion SQL against Loki data objects served by
//! `tools/dataobj-flight`.
//!
//! ```text
//! dataobj-sql "SELECT app, count(*) FROM logs GROUP BY app"
//! echo "SELECT * FROM streams LIMIT 5" | dataobj-sql
//! dataobj-sql --pg-addr 0.0.0.0:5432   # speak the Postgres wire protocol
//! ```

mod exec;
mod filters;
mod pg;
mod provider;
mod scanpb;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Criteria;
use clap::Parser;
use datafusion::arrow::json::ArrayWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::*;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tonic::transport::Channel;

use crate::provider::{DataobjTable, FlightClient, TenantInterceptor};

#[derive(Parser)]
#[command(about = "Run DataFusion SQL against Loki data objects over Arrow Flight")]
struct Args {
    /// Address of the dataobj-flight server.
    #[arg(long, default_value = "http://127.0.0.1:8815")]
    addr: String,

    /// Tables to register from the server. Defaults to every table the
    /// server lists (ListFlights), so new tables such as archive_logs appear
    /// without configuration.
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,

    /// Serve mode: read one JSON request per line from stdin ({"sql": "..."})
    /// and write one JSON response per line to stdout. Used by the Go bench
    /// harness so that process startup is paid once.
    #[arg(long)]
    serve: bool,

    /// Postgres wire protocol mode: listen on HOST:PORT (for example
    /// 0.0.0.0:5432) and answer SQL from any Postgres client, such as psql or
    /// Grafana's core Postgres datasource. No auth, no TLS.
    #[arg(long, value_name = "HOST:PORT")]
    pg_addr: Option<String>,

    /// Tenant sent as X-Scope-OrgID on every Flight call. Later, the pgwire
    /// database name maps to a tenant so one process can front many Grafana
    /// datasources.
    #[arg(long, default_value = "test-tenant")]
    tenant: String,

    /// Maximum concurrent pgwire connections in --pg-addr mode (0 = no limit).
    #[arg(long, default_value_t = 0)]
    pg_max_connections: usize,

    /// SQL statements to run, separated by ';'. Read from stdin when omitted.
    query: Vec<String>,
}

#[derive(Deserialize)]
struct ServeRequest {
    sql: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let channel = Channel::from_shared(args.addr.clone())
        .with_context(|| format!("invalid address {}", args.addr))?
        .connect()
        .await
        .with_context(|| format!("connecting to {}", args.addr))?;
    // Every Flight call carries the tenant as X-Scope-OrgID, which Loki's
    // gRPC auth middleware requires and the scan service uses to pick the
    // tenant's data objects and archive.
    let client: FlightClient =
        FlightServiceClient::with_interceptor(channel, TenantInterceptor::new(&args.tenant)?)
            .max_decoding_message_size(usize::MAX);

    // information_schema is what Grafana's query builder reads to list
    // tables and columns; pg_catalog is added on top in pgwire mode.
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
    let tables = if args.tables.is_empty() {
        list_tables(client.clone())
            .await
            .context("listing tables")?
    } else {
        args.tables.clone()
    };
    for table in &tables {
        let provider = DataobjTable::try_new(client.clone(), table)
            .await
            .with_context(|| format!("fetching schema of table {table}"))?;
        ctx.register_table(provider.name().to_string(), Arc::new(provider))?;
    }

    if let Some(pg_addr) = &args.pg_addr {
        return pg::serve(ctx, pg_addr, &args.tenant, args.pg_max_connections).await;
    }

    if args.serve {
        return serve(&ctx).await;
    }

    let sql = if args.query.is_empty() {
        let mut buf = String::new();
        tokio::io::stdin().read_to_string(&mut buf).await?;
        buf
    } else {
        args.query.join(" ")
    };

    for statement in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        let start = Instant::now();
        let df = ctx
            .sql(statement)
            .await
            .with_context(|| format!("planning: {statement}"))?;
        let batches = df
            .collect()
            .await
            .with_context(|| format!("executing: {statement}"))?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        println!("{}", pretty_format_batches(&batches)?);
        eprintln!("{rows} row(s) in {:.3?}", start.elapsed());
    }

    Ok(())
}

/// Returns the names of the tables the server serves, via ListFlights.
async fn list_tables(mut client: FlightClient) -> anyhow::Result<Vec<String>> {
    let mut stream = client.list_flights(Criteria::default()).await?.into_inner();
    let mut names = Vec::new();
    while let Some(info) = stream.message().await? {
        if let Some(desc) = info.flight_descriptor {
            if let Some(name) = desc.path.first() {
                names.push(name.clone());
            }
        }
    }
    Ok(names)
}

/// Runs statements from stdin, one JSON object per line, and writes one JSON
/// line per statement: {"rows": [...], "row_count": n, "elapsed_ms": f,
/// "wire_bytes": n} or {"error": "..."}. Rows use Arrow's JSON encoding.
async fn serve(ctx: &SessionContext) -> anyhow::Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<ServeRequest>(&line) {
            Err(e) => error_json(&format!("invalid request: {e}")),
            Ok(req) => match run_statement(ctx, &req.sql).await {
                Ok(json) => json,
                Err(e) => error_json(&format!("{e:#}")),
            },
        };
        stdout.write_all(response.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
    }
    Ok(())
}

fn error_json(message: &str) -> String {
    format!(
        "{{\"error\":{}}}",
        serde_json::to_string(message).unwrap_or_default()
    )
}

async fn run_statement(ctx: &SessionContext, sql: &str) -> anyhow::Result<String> {
    exec::WIRE_BYTES.store(0, Ordering::Relaxed);
    let start = Instant::now();

    let batches: Vec<RecordBatch> = ctx.sql(sql).await?.collect().await?;
    let elapsed = start.elapsed();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    let mut buf = Vec::new();
    {
        let mut writer = ArrayWriter::new(&mut buf);
        let refs: Vec<&RecordBatch> = batches.iter().filter(|b| b.num_rows() > 0).collect();
        writer.write_batches(&refs)?;
        writer.finish()?;
    }
    if buf.is_empty() {
        buf.extend_from_slice(b"[]");
    }

    Ok(format!(
        "{{\"rows\":{},\"row_count\":{},\"elapsed_ms\":{:.3},\"wire_bytes\":{}}}",
        String::from_utf8(buf)?,
        rows,
        elapsed.as_secs_f64() * 1000.0,
        exec::WIRE_BYTES.load(Ordering::Relaxed),
    ))
}
