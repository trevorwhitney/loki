//! Per-statement metrics of the pgwire gateway, served in Prometheus text
//! format on `--metrics-addr`. The Flight servers already export per-scan
//! metrics; this is the per-query view that lines up with Loki's
//! query-frontend latency: one observation per SQL statement, covering
//! planning, every DoGet the plan fans out into, and DataFusion's own work
//! (sort, join, aggregate).

use std::sync::Arc;

use anyhow::Context;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Clone)]
pub struct Metrics {
    pub registry: Arc<Registry>,
    pub statement_duration: HistogramVec,
    pub statements: IntCounterVec,
    pub in_flight: IntGauge,
    pub result_rows: HistogramVec,
}

impl Metrics {
    pub fn new() -> anyhow::Result<Self> {
        let registry = Registry::new();
        let statement_duration = HistogramVec::new(
            HistogramOpts::new(
                "dataobj_sql_statement_duration_seconds",
                "Wall time of one SQL statement on the gateway, from parse to the last result row collected: planning, every Flight DoGet of the plan, and DataFusion's own work.",
            )
            .buckets(prometheus::exponential_buckets(0.05, 2.0, 15)?),
            &["kind"],
        )?;
        let statements = IntCounterVec::new(
            Opts::new("dataobj_sql_statements_total", "SQL statements handled by the gateway."),
            &["kind", "status"],
        )?;
        let in_flight = IntGauge::new(
            "dataobj_sql_statements_in_flight",
            "SQL statements currently executing on the gateway.",
        )?;
        let result_rows = HistogramVec::new(
            HistogramOpts::new("dataobj_sql_result_rows", "Rows returned per statement.")
                .buckets(prometheus::exponential_buckets(1.0, 4.0, 10)?),
            &["kind"],
        )?;
        registry.register(Box::new(statement_duration.clone()))?;
        registry.register(Box::new(statements.clone()))?;
        registry.register(Box::new(in_flight.clone()))?;
        registry.register(Box::new(result_rows.clone()))?;
        Ok(Self {
            registry: Arc::new(registry),
            statement_duration,
            statements,
            in_flight,
            result_rows,
        })
    }

    /// Classifies a statement for the `kind` label: Grafana's catalog probes
    /// are frequent and cheap and would otherwise hide the data queries.
    pub fn kind(sql: &str) -> &'static str {
        let lower = sql.to_ascii_lowercase();
        if lower.contains("information_schema") || lower.contains("pg_catalog") || lower.contains("current_setting(") {
            "catalog"
        } else {
            "query"
        }
    }

    fn render(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        // Encoding errors only happen on a broken metric family; serve what we have.
        let _ = encoder.encode(&self.registry.gather(), &mut buf);
        buf
    }

    /// Serves `/metrics` (any path, really) on addr until the process exits.
    /// A minimal HTTP/1.1 responder is enough for a scrape and keeps the
    /// dependency list unchanged.
    pub async fn serve(self, addr: &str) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr)
            .await
            .with_context(|| format!("binding metrics listener on {addr}"))?;
        log::info!("serving Prometheus metrics on {addr}");
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    log::warn!("metrics accept failed: {e}");
                    continue;
                }
            };
            let metrics = self.clone();
            tokio::spawn(async move {
                let mut req = [0u8; 4096];
                // Read the request head; we answer the same way whatever it says.
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    socket.read(&mut req),
                )
                .await;
                let body = metrics.render();
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(&body).await;
                let _ = socket.shutdown().await;
            });
        }
    }
}
