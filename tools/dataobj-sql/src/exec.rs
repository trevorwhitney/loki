//! Physical plan node that reads one Flight endpoint per partition.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::Ticket;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

use crate::provider::ClientPool;

type BatchStream = BoxStream<'static, Result<RecordBatch>>;

/// One endpoint of a planned scan: the ticket and the servers that should
/// serve it, most preferred first. No locations means "ask the planner".
#[derive(Clone, Debug)]
pub struct PlannedEndpoint {
    pub ticket: Ticket,
    pub locations: Vec<String>,
}

/// Total bytes of Flight data received since the counter was last reset. Used
/// by the CLI to report wire size per statement; statements run sequentially.
pub static WIRE_BYTES: AtomicU64 = AtomicU64::new(0);

/// Reads a scan planned by the server. Endpoints are dealt round-robin into
/// at most `max_partitions` DataFusion partitions; each partition fetches its
/// endpoints one after another with DoGet from the endpoint's first reachable
/// location (or the planner). DataFusion executes every partition at once, so
/// the partition count is the number of concurrent scans the servers see.
pub struct DataobjFlightExec {
    pool: ClientPool,
    schema: SchemaRef,
    groups: Vec<Vec<PlannedEndpoint>>,
    endpoints: usize,
    properties: Arc<PlanProperties>,
}

impl DataobjFlightExec {
    /// `max_partitions` of 0 keeps one partition per endpoint.
    pub fn new(
        pool: ClientPool,
        schema: SchemaRef,
        endpoints: Vec<PlannedEndpoint>,
        max_partitions: usize,
    ) -> Self {
        let total = endpoints.len();
        let partitions = if max_partitions == 0 {
            total
        } else {
            total.min(max_partitions)
        }
        .max(1);
        let mut groups: Vec<Vec<PlannedEndpoint>> = vec![Vec::new(); partitions];
        for (i, endpoint) in endpoints.into_iter().enumerate() {
            groups[i % partitions].push(endpoint);
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            pool,
            schema,
            groups,
            endpoints: total,
            properties,
        }
    }

    fn located(&self) -> usize {
        self.groups
            .iter()
            .flatten()
            .filter(|e| !e.locations.is_empty())
            .count()
    }
}

impl fmt::Debug for DataobjFlightExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DataobjFlightExec(partitions={}, endpoints={})",
            self.groups.len(),
            self.endpoints
        )
    }
}

impl DisplayAs for DataobjFlightExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let columns: Vec<&str> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        write!(
            f,
            "DataobjFlightExec: partitions={}, endpoints={}, located={}, projection=[{}]",
            self.groups.len(),
            self.endpoints,
            self.located(),
            columns.join(", ")
        )
    }
}

impl ExecutionPlan for DataobjFlightExec {
    fn name(&self) -> &str {
        "DataobjFlightExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let group = self.groups.get(partition).cloned().unwrap_or_default();
        let schema = self.schema.clone();
        let pool = self.pool.clone();

        // flat_map drives one endpoint stream at a time, so a partition is
        // one DoGet in flight.
        let stream = futures::stream::iter(group)
            .flat_map(move |endpoint| endpoint_stream(pool.clone(), schema.clone(), endpoint));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// The record batches of one endpoint: DoGet at the first location that
/// answers, then the planner. A failure here is the DoGet call itself
/// (connection refused, unknown ticket); errors mid-stream are not retried.
fn endpoint_stream(pool: ClientPool, schema: SchemaRef, endpoint: PlannedEndpoint) -> BatchStream {
    futures::stream::once(async move {
        let mut candidates = pool.candidates(&endpoint.locations).into_iter();
        let response = loop {
            let Some((location, mut client)) = candidates.next() else {
                return Err(DataFusionError::Internal(
                    "no Flight client to serve endpoint".to_string(),
                ));
            };
            match client.do_get(endpoint.ticket.clone()).await {
                Ok(response) => break response.into_inner(),
                Err(status) => {
                    let at = location.as_deref().unwrap_or("planner");
                    if candidates.len() == 0 {
                        return Err(external(status));
                    }
                    log::warn!("DoGet at {at} failed, trying next location: {status}");
                }
            }
        };
        let counted = response.map_ok(|data| {
            let n = data.data_header.len() + data.data_body.len() + data.app_metadata.len();
            WIRE_BYTES.fetch_add(n as u64, Ordering::Relaxed);
            data
        });
        let batches =
            FlightRecordBatchStream::new_from_flight_data(counted.map_err(FlightError::from));

        let stream: BatchStream = batches
            .map(move |result| {
                result
                    .map_err(external)
                    .and_then(|batch| conform(batch, &schema))
            })
            .boxed();
        Ok::<BatchStream, DataFusionError>(stream)
    })
    .try_flatten()
    .boxed()
}

fn external<E: std::error::Error + Send + Sync + 'static>(err: E) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

/// Rebuilds a received batch against the plan's schema. The server returns
/// exactly the requested columns, but the IPC round trip can drop or add
/// schema-level details, and a zero-column projection (`SELECT count(*)`) is
/// answered with a single driver column that is dropped here.
fn conform(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        return RecordBatch::try_new_with_options(schema.clone(), vec![], &options)
            .map_err(Into::into);
    }
    if batch.num_columns() != schema.fields().len() {
        return Err(DataFusionError::Internal(format!(
            "server returned {} columns, plan expects {}",
            batch.num_columns(),
            schema.fields().len()
        )));
    }
    RecordBatch::try_new(schema.clone(), batch.columns().to_vec()).map_err(Into::into)
}
