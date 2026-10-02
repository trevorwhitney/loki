//! Physical plan node that reads one Flight endpoint per partition.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
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
use tonic::transport::Channel;

type BatchStream = BoxStream<'static, Result<RecordBatch>>;

/// Total bytes of Flight data received since the counter was last reset. Used
/// by the CLI to report wire size per statement; statements run sequentially.
pub static WIRE_BYTES: AtomicU64 = AtomicU64::new(0);

/// Reads a scan planned by the server: each Flight endpoint becomes one
/// DataFusion partition, fetched with DoGet when the partition executes.
pub struct DataobjFlightExec {
    client: FlightServiceClient<Channel>,
    schema: SchemaRef,
    tickets: Vec<Ticket>,
    properties: Arc<PlanProperties>,
}

impl DataobjFlightExec {
    pub fn new(
        client: FlightServiceClient<Channel>,
        schema: SchemaRef,
        tickets: Vec<Ticket>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(tickets.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            client,
            schema,
            tickets,
            properties,
        }
    }
}

impl fmt::Debug for DataobjFlightExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DataobjFlightExec(partitions={})", self.tickets.len())
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
            "DataobjFlightExec: partitions={}, projection=[{}]",
            self.tickets.len(),
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
        let ticket = self.tickets.get(partition).cloned();
        let schema = self.schema.clone();
        let mut client = self.client.clone();

        let stream = futures::stream::once(async move {
            let Some(ticket) = ticket else {
                return Ok::<BatchStream, DataFusionError>(futures::stream::empty().boxed());
            };

            let response = client.do_get(ticket).await.map_err(external)?.into_inner();
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
            Ok(stream)
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
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
