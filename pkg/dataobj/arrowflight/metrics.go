package arrowflight

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics instruments the scan service so the cost of a query can be derived
// from a scrape: what was planned, what was read from object storage, how long
// decoding took, and what crossed the wire. All collectors are labelled by
// table where that is meaningful. Pass a nil registerer to get working but
// unregistered collectors.
type Metrics struct {
	planDuration     *prometheus.HistogramVec
	plannedTickets   *prometheus.HistogramVec
	locatedEndpoints *prometheus.CounterVec
	scanDuration     *prometheus.HistogramVec
	scanErrors       *prometheus.CounterVec
	rowsServed       *prometheus.CounterVec
	batchesServed    *prometheus.CounterVec
	bytesServed      *prometheus.CounterVec
	scansInFlight    prometheus.Gauge
	scanQueueTime    prometheus.Histogram

	archiveListDuration     prometheus.Histogram
	archiveObjectsListed    prometheus.Counter
	archiveObjectsFetched   prometheus.Counter
	archiveCompressedBytes  prometheus.Counter
	archiveUncompressedByte prometheus.Counter
	archiveFetchDuration    prometheus.Histogram
	archiveDecodeDuration   prometheus.Histogram
	archiveRecordsDecoded   prometheus.Counter
	archiveRecordsMatched   prometheus.Counter
}

// NewMetrics creates the collectors and registers them with reg when it is
// not nil.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	const ns, sub = "loki", "dataobj_flight"
	return &Metrics{
		planDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "plan_duration_seconds",
			Help:    "Time to plan a scan request into endpoints (metastore lookup or archive listing).",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
		}, []string{"table"}),
		plannedTickets: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "planned_tickets",
			Help:    "Endpoints produced per planned scan (sections for data objects, object bundles for the archive).",
			Buckets: prometheus.ExponentialBuckets(1, 2, 14),
		}, []string{"table"}),
		locatedEndpoints: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "located_endpoints_total",
			Help: "Planned endpoints that were given Flight locations by the locator (ring placement).",
		}, []string{"table"}),
		scanDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "scan_duration_seconds",
			Help:    "Time to serve one endpoint (DoGet), from open to last batch.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 14),
		}, []string{"table"}),
		scanErrors: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "scan_errors_total",
			Help: "Endpoints that failed while being served.",
		}, []string{"table"}),
		rowsServed: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "rows_served_total",
			Help: "Rows sent to clients after server-side predicates.",
		}, []string{"table"}),
		batchesServed: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "batches_served_total",
			Help: "Record batches sent to clients.",
		}, []string{"table"}),
		bytesServed: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "bytes_served_total",
			Help: "Arrow buffer bytes of the record batches sent to clients (before IPC framing and compression).",
		}, []string{"table"}),

		scansInFlight: f.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "scans_in_flight",
			Help: "DoGet scans currently running on this server.",
		}),
		scanQueueTime: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "scan_queue_duration_seconds",
			Help:    "Time a DoGet waited for a scan slot when max_concurrent_scans is set.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 10),
		}),
		archiveListDuration: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "archive_list_duration_seconds",
			Help:    "Time spent listing archive partitions for one plan.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
		}),
		archiveObjectsListed: f.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "archive_objects_listed_total",
			Help: "Archive objects selected by time range during planning.",
		}),
		archiveObjectsFetched: f.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "archive_objects_fetched_total",
			Help: "Archive objects fetched from the bucket.",
		}),
		archiveCompressedBytes: f.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "archive_compressed_bytes_total",
			Help: "Compressed bytes fetched from the archive bucket.",
		}),
		archiveUncompressedByte: f.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "archive_uncompressed_bytes_total",
			Help: "Bytes of OTLP JSON after gunzip.",
		}),
		archiveFetchDuration: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "archive_fetch_duration_seconds",
			Help:    "Time to fetch one archive object from the bucket (GET and read).",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}),
		archiveDecodeDuration: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "archive_decode_duration_seconds",
			Help:    "Time to gunzip, decode and normalise one archive object (CPU bound).",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}),
		archiveRecordsDecoded: f.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "archive_records_decoded_total",
			Help: "Log records decoded from archive objects.",
		}),
		archiveRecordsMatched: f.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "archive_records_matched_total",
			Help: "Archive log records that passed the pushed-down predicates.",
		}),
	}
}

// recordBytes estimates the memory footprint of a record batch: the sum of
// its Arrow buffers, including nested children (maps, lists).
func recordBytes(rec arrow.RecordBatch) int64 {
	var total int64
	for _, col := range rec.Columns() {
		total += dataBytes(col.Data())
	}
	return total
}

func dataBytes(d arrow.ArrayData) int64 {
	if isNilData(d) {
		return 0
	}
	var total int64
	for _, b := range d.Buffers() {
		if b != nil {
			total += int64(b.Len())
		}
	}
	for _, child := range d.Children() {
		total += dataBytes(child)
	}
	// Dictionary() returns a typed nil for non-dictionary arrays.
	if d.DataType().ID() == arrow.DICTIONARY {
		total += dataBytes(d.Dictionary())
	}
	return total
}

// isNilData reports whether d is nil or an interface holding a nil *array.Data.
func isNilData(d arrow.ArrayData) bool {
	if d == nil {
		return true
	}
	data, ok := d.(*array.Data)
	return ok && data == nil
}
