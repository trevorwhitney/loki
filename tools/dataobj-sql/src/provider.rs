//! DataFusion `TableProvider` backed by the Loki data object Flight server.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::FlightDescriptor;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use prost::Message;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::{Request, Status};

use crate::exec::{DataobjFlightExec, PlannedEndpoint};
use crate::filters::to_predicate;
use crate::scanpb::ScanRequest;

/// Flight client that stamps the tenant on every request.
pub type FlightClient = FlightServiceClient<InterceptedService<Channel, TenantInterceptor>>;

/// Adds `x-scope-orgid: <tenant>` to every outgoing call, the header Loki
/// uses for tenant identification on gRPC.
#[derive(Clone)]
pub struct TenantInterceptor {
    tenant: MetadataValue<Ascii>,
}

impl TenantInterceptor {
    pub fn new(tenant: &str) -> anyhow::Result<Self> {
        Ok(Self {
            tenant: tenant
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid tenant {tenant:?}: {e}"))?,
        })
    }
}

impl Interceptor for TenantInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("x-scope-orgid", self.tenant.clone());
        Ok(request)
    }
}

/// Flight clients for the planning server and for every server a planned
/// endpoint has named as a location.
///
/// GetFlightInfo goes to the planning server, the one connection the process
/// is configured with. When the server runs in ring mode each endpoint it
/// returns carries the locations of the ring members that should serve it;
/// DoGet for that endpoint goes to the first of them that answers, and to the
/// planning connection when none does. Channels are opened lazily, once per
/// location, and shared by every scan.
#[derive(Clone)]
pub struct ClientPool {
    planner: FlightClient,
    tenant: TenantInterceptor,
    honor_locations: bool,
    located: Arc<Mutex<HashMap<String, FlightClient>>>,
}

impl ClientPool {
    /// Wraps the planning connection. With `honor_locations` false every
    /// call goes to it, whatever the server says.
    pub fn new(planner: FlightClient, tenant: TenantInterceptor, honor_locations: bool) -> Self {
        Self {
            planner,
            tenant,
            honor_locations,
            located: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// The planning connection.
    pub fn planner(&self) -> FlightClient {
        self.planner.clone()
    }

    /// Clients to try for an endpoint, most preferred first. The planning
    /// connection is always last so an endpoint whose locations are all
    /// unreachable, or unparseable, still gets served. The label names the
    /// location for logging, `None` for the planning connection.
    pub fn candidates(&self, locations: &[String]) -> Vec<(Option<String>, FlightClient)> {
        let mut out = Vec::with_capacity(locations.len() + 1);
        if self.honor_locations {
            for uri in locations {
                if let Some(client) = self.client_for(uri) {
                    out.push((Some(uri.clone()), client));
                }
            }
        }
        out.push((None, self.planner.clone()));
        out
    }

    fn client_for(&self, uri: &str) -> Option<FlightClient> {
        let endpoint = flight_location_to_http(uri)?;
        let mut located = self.located.lock().expect("client pool poisoned");
        if let Some(client) = located.get(&endpoint) {
            return Some(client.clone());
        }
        let channel = match Channel::from_shared(endpoint.clone()) {
            Ok(builder) => builder.connect_lazy(),
            Err(e) => {
                log::warn!("ignoring Flight location {uri}: {e}");
                return None;
            }
        };
        let client = FlightServiceClient::with_interceptor(channel, self.tenant.clone())
            .max_decoding_message_size(usize::MAX);
        located.insert(endpoint, client.clone());
        Some(client)
    }
}

/// Maps a Flight location URI (`grpc+tcp://host:port`, `grpc+tls://host:port`,
/// `grpc://host:port`) to the `http(s)://` form tonic connects to. Other
/// schemes are not supported and yield `None`.
fn flight_location_to_http(uri: &str) -> Option<String> {
    if let Some(rest) = uri.strip_prefix("grpc+tls://") {
        return Some(format!("https://{rest}"));
    }
    for scheme in ["grpc+tcp://", "grpc://"] {
        if let Some(rest) = uri.strip_prefix(scheme) {
            return Some(format!("http://{rest}"));
        }
    }
    if uri.starts_with("http://") || uri.starts_with("https://") {
        return Some(uri.to_string());
    }
    log::warn!("ignoring Flight location with unsupported scheme: {uri}");
    None
}

/// A table served by `tools/dataobj-flight` or Loki's dataobj-flight target.
pub struct DataobjTable {
    name: String,
    schema: SchemaRef,
    pool: ClientPool,
    max_partitions: usize,
}

impl fmt::Debug for DataobjTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataobjTable")
            .field("name", &self.name)
            .field("schema", &self.schema)
            .finish()
    }
}

impl DataobjTable {
    /// Fetches the table schema from the planning server with GetSchema.
    /// `max_partitions` bounds the concurrent DoGet streams of one scan
    /// (see [`DataobjFlightExec`]); 0 means one per endpoint.
    pub async fn try_new(pool: ClientPool, name: &str, max_partitions: usize) -> anyhow::Result<Self> {
        let descriptor = FlightDescriptor::new_path(vec![name.to_string()]);
        let result = pool.planner().get_schema(descriptor).await?.into_inner();
        let schema: Schema = (&result).try_into()?;
        Ok(Self {
            name: name.to_string(),
            schema: Arc::new(schema),
            pool,
            max_partitions,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl TableProvider for DataobjTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if to_predicate(f).is_some() {
                    // The server prunes but does not guarantee exact results.
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema: SchemaRef = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => self.schema.clone(),
        };

        // DataFusion asks for zero columns when it only needs row counts; the
        // server needs at least one column to drive the scan, so ask for the
        // table's first column and let the exec node drop it.
        let mut columns: Vec<String> = projected_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        if columns.is_empty() {
            if let Some(first) = self.schema.fields().first() {
                columns.push(first.name().clone());
            }
        }

        let request = ScanRequest {
            table: self.name.clone(),
            columns,
            predicates: filters.iter().filter_map(to_predicate).collect(),
            limit: limit.map(|l| l as i64).unwrap_or(0),
        };

        let descriptor = FlightDescriptor::new_cmd(request.encode_to_vec());
        let info = self
            .pool
            .planner()
            .get_flight_info(descriptor)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .into_inner();

        let endpoints: Vec<PlannedEndpoint> = info
            .endpoint
            .into_iter()
            .filter_map(|e| {
                let ticket = e.ticket?;
                let locations = e.location.into_iter().map(|l| l.uri).collect();
                Some(PlannedEndpoint { ticket, locations })
            })
            .collect();
        let located = endpoints.iter().filter(|e| !e.locations.is_empty()).count();
        log::debug!(
            "planned {} endpoints for {} ({} with locations)",
            endpoints.len(),
            self.name,
            located
        );
        Ok(Arc::new(DataobjFlightExec::new(
            self.pool.clone(),
            projected_schema,
            endpoints,
            self.max_partitions,
        )))
    }
}
