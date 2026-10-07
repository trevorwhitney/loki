//! Postgres wire protocol front end: `datafusion-postgres` serves the same
//! `SessionContext` that holds the Flight-backed tables, so psql and Grafana's
//! core Postgres datasource can query data objects without a plugin.

use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, AsArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ParamValues;
use datafusion::logical_expr::{
    ColumnarValue, LogicalPlan, ScalarUDF, Signature, SimpleScalarUDF, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion_postgres::arrow_pg::datatypes::df::encode_dataframe;
use datafusion_postgres::datafusion_pg_catalog::pg_catalog::context::EmptyContextProvider;
use datafusion_postgres::datafusion_pg_catalog::setup_pg_catalog;
use datafusion_postgres::hooks::cursor::CursorStatementHook;
use datafusion_postgres::hooks::set_show::SetShowHook;
use datafusion_postgres::hooks::transactions::TransactionStatementHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::api::{ClientInfo, METADATA_DATABASE, METADATA_USER};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::types::format::FormatOptions;
use datafusion_postgres::{serve_with_hooks, QueryHook, ServerOptions};
use log::{debug, info};

use crate::metrics::Metrics;

/// Catalog name DataFusion uses by default; `pg_catalog` is registered under
/// it and `current_database()` reports it.
const CATALOG: &str = "datafusion";

/// Serves `ctx` over pgwire on `addr` (HOST:PORT) until the process exits.
pub async fn serve(
    ctx: SessionContext,
    addr: &str,
    tenant: &str,
    max_connections: usize,
    metrics: Option<Metrics>,
) -> anyhow::Result<()> {
    let (host, port) = addr
        .rsplit_once(':')
        .with_context(|| format!("--pg-addr {addr}: expected HOST:PORT"))?;
    let port: u16 = port
        .parse()
        .with_context(|| format!("--pg-addr {addr}: invalid port"))?;

    let ctx = Arc::new(ctx);
    setup_pg_catalog(&ctx, CATALOG, EmptyContextProvider)
        .map_err(|e| anyhow::anyhow!("registering pg_catalog: {e}"))?;
    ctx.register_udf(current_setting_udf());
    ctx.register_udf(pg_data_type_udf());

    // serve_with_hooks replaces the default hook list, so the built-in ones
    // (cursors, SET/SHOW, BEGIN/COMMIT) must be listed again explicitly.
    let mut hooks: Vec<Arc<dyn QueryHook>> = vec![
        Arc::new(TenantHook {
            tenant: tenant.to_string(),
        }),
        Arc::new(GrafanaCatalogHook),
        Arc::new(CursorStatementHook),
        Arc::new(SetShowHook),
        Arc::new(TransactionStatementHook),
    ];
    if let Some(metrics) = metrics {
        // Last: it takes over plain queries that nothing above handled, so
        // that their full execution can be timed.
        hooks.push(Arc::new(TimedQueryHook { metrics }));
    }

    let opts = ServerOptions::new()
        .with_host(host.to_string())
        .with_port(port)
        .with_max_connections(max_connections);

    info!("serving Postgres wire protocol on {host}:{port} for tenant {tenant} (no auth, no TLS)");
    serve_with_hooks(ctx, &opts, hooks)
        .await
        .with_context(|| format!("pgwire server on {addr}"))
}

/// Placeholder for per-database tenant routing. It never answers a query; it
/// records which pgwire database and user a statement came from so that, once
/// the Flight server is multi-tenant, mapping `database -> tenant` is a local
/// change here rather than a new hook.
struct TenantHook {
    tenant: String,
}

impl TenantHook {
    fn observe<C: ClientInfo + ?Sized>(&self, client: &C, phase: &str) {
        let metadata = client.metadata();
        debug!(
            "{phase}: database={} user={} -> tenant={}",
            metadata
                .get(METADATA_DATABASE)
                .map(String::as_str)
                .unwrap_or("-"),
            metadata
                .get(METADATA_USER)
                .map(String::as_str)
                .unwrap_or("-"),
            self.tenant
        );
    }
}

#[async_trait]
impl QueryHook for TenantHook {
    async fn handle_simple_query(
        &self,
        _statement: &Statement,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        self.observe(client, "simple query");
        None
    }

    async fn handle_extended_parse_query(
        &self,
        _sql: &Statement,
        _session_context: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        self.observe(client, "extended parse");
        None
    }

    async fn handle_extended_query(
        &self,
        _statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }
}

/// Executes plain queries itself so that their whole lifetime can be
/// measured. `datafusion-postgres` encodes a lazy stream and the scan work
/// happens while pgwire drains it, out of reach of a hook, so this hook
/// collects the result first (results here are aggregates or a LIMITed
/// page; the heavy rows stay on the Flight servers) and hands pgwire an
/// in-memory DataFrame. Only simple-protocol `SELECT`/`WITH` statements are
/// taken; everything else falls through to the default path untimed.
struct TimedQueryHook {
    metrics: Metrics,
}

#[async_trait]
impl QueryHook for TimedQueryHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if !matches!(statement, Statement::Query(_)) {
            return None;
        }
        let sql = statement.to_string();
        let kind = Metrics::kind(&sql);
        let format_options = Arc::new(FormatOptions::from_client_metadata(client.metadata()));
        self.metrics.in_flight.inc();
        let started = std::time::Instant::now();
        let result = async {
            let df = session_context
                .sql(&sql)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            let schema = df.schema().inner().clone();
            let batches = df
                .collect()
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let df = session_context
                .read_batches(batches)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            let _ = schema; // the in-memory frame carries the executed schema
            encode_dataframe(df, &Format::UnifiedText, Some(format_options))
                .await
                .map(|r| (Response::Query(r), rows))
        }
        .await;
        let elapsed = started.elapsed().as_secs_f64();
        self.metrics.in_flight.dec();
        self.metrics
            .statement_duration
            .with_label_values(&[kind])
            .observe(elapsed);
        match result {
            Ok((response, rows)) => {
                self.metrics
                    .statements
                    .with_label_values(&[kind, "ok"])
                    .inc();
                self.metrics
                    .result_rows
                    .with_label_values(&[kind])
                    .observe(rows as f64);
                debug!("{kind} statement: {rows} rows in {elapsed:.3}s");
                Some(Ok(response))
            }
            Err(e) => {
                self.metrics
                    .statements
                    .with_label_values(&[kind, "error"])
                    .inc();
                Some(Err(e))
            }
        }
    }

    async fn handle_extended_parse_query(
        &self,
        _sql: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        None
    }

    async fn handle_extended_query(
        &self,
        _statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }
}

/// `current_setting(name [, missing_ok])`, which `datafusion-postgres` 0.18
/// does not provide. Grafana's Postgres datasource runs
/// `SELECT current_setting('server_version_num')::int/100` from its
/// configuration page to detect the server version. The values match the
/// `server_version` that pgwire advertises at startup (16.6). Unknown
/// settings return NULL instead of an error so that probes do not fail.
fn current_setting_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let names = arrays[0].as_string::<i32>();
        let mut builder = StringBuilder::new();
        for name in names.iter() {
            let value = match name.map(str::to_ascii_lowercase).as_deref() {
                Some("server_version_num") => Some("160006"),
                Some("server_version") => Some("16.6"),
                Some("timezone") => Some("UTC"),
                Some("search_path") => Some("public"),
                Some("max_identifier_length") => Some("63"),
                Some("standard_conforming_strings") => Some("on"),
                Some("is_superuser") => Some("off"),
                _ => None,
            };
            builder.append_option(value);
        }
        let array: ArrayRef = Arc::new(builder.finish());
        Ok(ColumnarValue::Array(array))
    };
    ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        "current_setting",
        Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Boolean]),
            ],
            Volatility::Stable,
        ),
        DataType::Utf8,
        Arc::new(func),
    ))
}

/// Fixes up the two catalog statements Grafana's query builder sends.
///
/// Grafana resolves the current `search_path` with a subquery over
/// `generate_series`, `string_to_array` and `user`. `datafusion-pg-catalog`
/// cannot plan that subquery and replaces it at the token level with `IN
/// ('')`, which matches no schema, so tables come back as `public.logs` and
/// the column lookup for a bare table name finds nothing. The hook rewrites
/// that placeholder to the real default schema and maps DataFusion type names
/// to the Postgres names the builder understands (`text`, `bigint`,
/// `timestamp with time zone`, ...).
struct GrafanaCatalogHook;

impl GrafanaCatalogHook {
    fn rewrite(statement: &Statement) -> Option<String> {
        let sql = statement.to_string();
        let is_tables = sql.contains("information_schema.tables");
        let is_columns = sql.contains("information_schema.columns");
        if !(is_tables || is_columns) || !sql.contains("quote_ident(") {
            return None;
        }
        let mut rewritten = sql.replace("IN ('')", "IN ('public')");
        if is_columns {
            rewritten = rewritten.replace(
                "data_type AS \"type\"",
                "pg_data_type(data_type) AS \"type\"",
            );
        }
        if rewritten == sql {
            return None;
        }
        debug!("rewrote Grafana catalog statement: {rewritten}");
        Some(rewritten)
    }
}

#[async_trait]
impl QueryHook for GrafanaCatalogHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let sql = Self::rewrite(statement)?;
        let format_options = Arc::new(FormatOptions::from_client_metadata(client.metadata()));
        let result = async {
            let df = session_context
                .sql(&sql)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            encode_dataframe(df, &Format::UnifiedText, Some(format_options))
                .await
                .map(Response::Query)
        }
        .await;
        Some(result)
    }

    async fn handle_extended_parse_query(
        &self,
        sql: &Statement,
        session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        let sql = Self::rewrite(sql)?;
        Some(
            session_context
                .state()
                .create_logical_plan(&sql)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e))),
        )
    }

    async fn handle_extended_query(
        &self,
        _statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }
}

/// Maps the Arrow type names that DataFusion's `information_schema.columns`
/// reports (`Utf8`, `Int64`, `Timestamp(ns, "UTC")`) to Postgres type names,
/// which is what Grafana's query builder switches on to pick filter widgets.
fn pg_data_type_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let names = arrays[0].as_string::<i32>();
        let mut builder = StringBuilder::new();
        for name in names.iter() {
            builder.append_option(name.map(pg_type_name));
        }
        let array: ArrayRef = Arc::new(builder.finish());
        Ok(ColumnarValue::Array(array))
    };
    ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        "pg_data_type",
        Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        DataType::Utf8,
        Arc::new(func),
    ))
}

fn pg_type_name(arrow: &str) -> String {
    let name = match arrow {
        "Utf8" | "LargeUtf8" | "Utf8View" => "text",
        "Int64" | "UInt64" => "bigint",
        "Int32" | "UInt32" => "integer",
        "Int16" | "UInt16" | "Int8" | "UInt8" => "smallint",
        "Float64" => "double precision",
        "Float32" | "Float16" => "real",
        "Boolean" => "boolean",
        "Date32" | "Date64" => "date",
        "Binary" | "LargeBinary" | "BinaryView" => "bytea",
        t if t.starts_with("Map(") => "jsonb",
        t if t.starts_with("List(") || t.starts_with("LargeList(") => "text[]",
        t if t.starts_with("Timestamp(") && t.contains(", \"") => "timestamp with time zone",
        t if t.starts_with("Timestamp(") => "timestamp without time zone",
        t if t.starts_with("Time32") || t.starts_with("Time64") => "time without time zone",
        t if t.starts_with("Decimal") => "numeric",
        t if t.starts_with("Interval") => "interval",
        other => return other.to_string(),
    };
    name.to_string()
}
