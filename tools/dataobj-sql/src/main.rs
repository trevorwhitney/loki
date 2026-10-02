//! dataobj-sql runs DataFusion SQL against Loki data objects served by
//! `tools/dataobj-flight`.
//!
//! ```text
//! dataobj-sql "SELECT app, count(*) FROM logs GROUP BY app"
//! echo "SELECT * FROM streams LIMIT 5" | dataobj-sql
//! ```

mod exec;
mod filters;
mod provider;
mod scanpb;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use arrow_flight::flight_service_client::FlightServiceClient;
use clap::Parser;
use datafusion::arrow::json::ArrayWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::*;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tonic::transport::Channel;

use crate::provider::DataobjTable;

#[derive(Parser)]
#[command(about = "Run DataFusion SQL against Loki data objects over Arrow Flight")]
struct Args {
    /// Address of the dataobj-flight server.
    #[arg(long, default_value = "http://127.0.0.1:8815")]
    addr: String,

    /// Tables to register from the server.
    #[arg(long, default_value = "logs,streams", value_delimiter = ',')]
    tables: Vec<String>,

    /// Serve mode: read one JSON request per line from stdin ({"sql": "..."})
    /// and write one JSON response per line to stdout. Used by the Go bench
    /// harness so that process startup is paid once.
    #[arg(long)]
    serve: bool,

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

    let channel = Channel::from_shared(args.addr.clone())
        .with_context(|| format!("invalid address {}", args.addr))?
        .connect()
        .await
        .with_context(|| format!("connecting to {}", args.addr))?;
    let client = FlightServiceClient::new(channel).max_decoding_message_size(usize::MAX);

    let ctx = SessionContext::new();
    for table in &args.tables {
        let provider = DataobjTable::try_new(client.clone(), table)
            .await
            .with_context(|| format!("fetching schema of table {table}"))?;
        ctx.register_table(provider.name().to_string(), Arc::new(provider))?;
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
