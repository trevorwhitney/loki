//! DataFusion `TableProvider` backed by the Loki data object Flight server.

use std::fmt;
use std::sync::Arc;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightDescriptor, Ticket};
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

use crate::exec::DataobjFlightExec;
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

/// A table served by `tools/dataobj-flight` or Loki's dataobj-flight target.
pub struct DataobjTable {
    name: String,
    schema: SchemaRef,
    client: FlightClient,
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
    /// Fetches the table schema from the server with GetSchema.
    pub async fn try_new(mut client: FlightClient, name: &str) -> anyhow::Result<Self> {
        let descriptor = FlightDescriptor::new_path(vec![name.to_string()]);
        let result = client.get_schema(descriptor).await?.into_inner();
        let schema: Schema = (&result).try_into()?;
        Ok(Self {
            name: name.to_string(),
            schema: Arc::new(schema),
            client,
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
            .client
            .clone()
            .get_flight_info(descriptor)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .into_inner();

        let tickets: Vec<Ticket> = info.endpoint.into_iter().filter_map(|e| e.ticket).collect();
        Ok(Arc::new(DataobjFlightExec::new(
            self.client.clone(),
            projected_schema,
            tickets,
        )))
    }
}
