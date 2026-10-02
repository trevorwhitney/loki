package arrowflight

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/thanos-io/objstore"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight/scanpb"
	"github.com/grafana/loki/v3/pkg/loghttp/push/otlplabels"
)

// TableArchiveLogs is the table served by an [ArchiveSource].
const TableArchiveLogs = "archive_logs"

// ArchiveConvertedPushScopeAttribute is the scope attribute the archive
// ingester sets on objects it produced from Loki push requests. For those
// objects every resource attribute is a stream label; native OTLP objects go
// through the tenant's attribute promotion list instead.
const ArchiveConvertedPushScopeAttribute = "__al_archive_ingester_converted_push_request__"

// Archive column names beyond the stream labels and promoted metadata keys.
const (
	ColumnObservedTimestamp  = "observed_timestamp"
	ColumnResourceAttributes = "resource_attributes"
	ColumnLogAttributes      = "log_attributes"
)

// archiveBucketDuration is the time bucket of one archive partition:
// <prefix>/YYYY/MM/DD/HH/mm/<uuidv7>.json.gz with mm in five-minute steps.
const archiveBucketDuration = 5 * time.Minute

// DefaultArchiveIndexLabels is Loki's default list of OTLP resource attributes
// that become stream labels (distributor.otlp.default_resource_attributes_as_index_labels).
var DefaultArchiveIndexLabels = []string{
	"service.name", "service.namespace", "service.instance.id",
	"deployment.environment", "deployment.environment.name",
	"cloud.region", "cloud.availability_zone",
	"k8s.cluster.name", "k8s.namespace.name", "k8s.pod.name", "k8s.container.name",
	"container.name", "k8s.replicaset.name", "k8s.deployment.name", "k8s.statefulset.name",
	"k8s.daemonset.name", "k8s.cronjob.name", "k8s.job.name",
}

// DefaultArchiveDiscoverServiceName is Loki's default validation.discover-service-name list.
var DefaultArchiveDiscoverServiceName = []string{
	"service", "app", "application", "app_name", "name", "app_kubernetes_io_name",
	"container", "container_name", "k8s_container_name", "component", "workload", "job", "k8s_job_name",
}

// DefaultArchiveMetadataColumns are the structured metadata keys Loki derives
// from log record fields; they are exposed as flat columns.
var DefaultArchiveMetadataColumns = []string{
	"trace_id", "span_id", "severity_text", "severity_number", "flags",
}

// ArchiveConfig configures an [ArchiveSource].
type ArchiveConfig struct {
	// Bucket holds the archive objects; Prefix is the tenant's directory in
	// it (for example "12345" or "archive/test-tenant").
	Bucket objstore.BucketReader
	Prefix string

	// IndexLabels are the resource attributes promoted to stream labels for
	// native OTLP objects. Defaults to DefaultArchiveIndexLabels.
	IndexLabels []string
	// DiscoverServiceName mirrors Loki's service name discovery. Defaults to
	// DefaultArchiveDiscoverServiceName.
	DiscoverServiceName []string

	// LabelColumns are the label columns of the table. When empty they are
	// discovered from the first SampleObjects objects under Prefix, which is
	// required for converted push requests whose labels are tenant specific.
	LabelColumns []string
	// MetadataColumns are structured metadata keys exposed as flat columns in
	// addition to the log_attributes map. Defaults to
	// DefaultArchiveMetadataColumns.
	MetadataColumns []string
	// SampleObjects bounds label discovery. Defaults to 16.
	SampleObjects int

	// ObjectsPerTicket bounds how many objects one endpoint serves. Defaults
	// to 32. Archive objects are small (kilobytes), so one per endpoint would
	// mean thousands of partitions per query.
	ObjectsPerTicket int
	// Concurrency bounds how many objects are decoded at once across all
	// in-flight scans. Defaults to GOMAXPROCS.
	Concurrency int
	// BatchRows bounds the rows per record batch. Defaults to 4096.
	BatchRows int

	Logger log.Logger
}

func (c *ArchiveConfig) applyDefaults() {
	if len(c.IndexLabels) == 0 {
		c.IndexLabels = DefaultArchiveIndexLabels
	}
	if c.DiscoverServiceName == nil {
		c.DiscoverServiceName = DefaultArchiveDiscoverServiceName
	}
	if c.MetadataColumns == nil {
		c.MetadataColumns = DefaultArchiveMetadataColumns
	}
	if c.SampleObjects <= 0 {
		c.SampleObjects = 16
	}
	if c.ObjectsPerTicket <= 0 {
		c.ObjectsPerTicket = 32
	}
	if c.Concurrency <= 0 {
		c.Concurrency = runtime.GOMAXPROCS(0)
	}
	if c.BatchRows <= 0 {
		c.BatchRows = 4096
	}
	if c.Logger == nil {
		c.Logger = log.NewNopLogger()
	}
}

// ArchiveSource serves the archive_logs table from gzipped OTLP JSON objects
// laid out the way the archive-and-replay ingester writes them. It is purely
// request driven: the schema is static, objects are listed per request from
// the time range in the scan predicates, and only the listed objects are read.
type ArchiveSource struct {
	cfg    ArchiveConfig
	schema *TableSchema

	native    otlplabels.OTLPConfig
	converted otlplabels.OTLPConfig

	sem chan struct{}
}

// NewArchiveSource prepares the archive_logs table. It reads a few objects
// to discover the label columns unless cfg.LabelColumns is set.
func NewArchiveSource(ctx context.Context, cfg ArchiveConfig) (*ArchiveSource, error) {
	cfg.applyDefaults()
	if cfg.Bucket == nil {
		return nil, errors.New("archive: bucket is required")
	}
	cfg.Prefix = strings.Trim(cfg.Prefix, "/")

	a := &ArchiveSource{
		cfg: cfg,
		sem: make(chan struct{}, cfg.Concurrency),
	}

	// Native objects: the configured attributes become labels, everything
	// else is structured metadata. Converted push requests: every resource
	// attribute was a Loki label, so promote them all; the marker itself is
	// dropped from the output.
	a.native = otlplabels.DefaultOTLPConfig(otlplabels.GlobalOTLPConfig{DefaultOTLPResourceAttributesAsIndexLabels: cfg.IndexLabels})
	a.native.ScopeAttributes = []otlplabels.AttributesConfig{{Action: otlplabels.Drop, Attributes: []string{ArchiveConvertedPushScopeAttribute}}}
	a.converted = otlplabels.OTLPConfig{
		ResourceAttributes: otlplabels.ResourceAttributesConfig{
			IgnoreDefaults:   true,
			AttributesConfig: []otlplabels.AttributesConfig{{Action: otlplabels.IndexLabel, Regex: relabel.MustNewRegexp(".*")}},
		},
		ScopeAttributes: []otlplabels.AttributesConfig{{Action: otlplabels.Drop, Attributes: []string{ArchiveConvertedPushScopeAttribute}}},
	}

	labelColumns := cfg.LabelColumns
	if len(labelColumns) == 0 {
		discovered, err := a.discoverLabels(ctx)
		if err != nil {
			return nil, fmt.Errorf("archive: discovering labels under %q: %w", cfg.Prefix, err)
		}
		labelColumns = discovered
	}

	fixed := []arrow.Field{
		{Name: ColumnTimestamp, Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true},
		{Name: ColumnMessage, Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: ColumnObservedTimestamp, Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true},
		{Name: ColumnResourceAttributes, Type: archiveMapType, Nullable: true},
		{Name: ColumnLogAttributes, Type: archiveMapType, Nullable: true},
	}
	a.schema = buildTableSchema(TableArchiveLogs, fixed, labelColumns, cfg.MetadataColumns)

	level.Info(cfg.Logger).Log("msg", "archive table ready", "prefix", cfg.Prefix, "labels", len(labelColumns), "metadata_columns", len(cfg.MetadataColumns))
	return a, nil
}

var archiveMapType = arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)

// Tables implements [Source].
func (a *ArchiveSource) Tables() []string { return []string{TableArchiveLogs} }

// Table implements [Source].
func (a *ArchiveSource) Table(name string) (*TableSchema, bool) {
	if name != TableArchiveLogs {
		return nil, false
	}
	return a.schema, true
}

// discoverLabels unions the stream label names of the first few objects.
func (a *ArchiveSource) discoverLabels(ctx context.Context) ([]string, error) {
	var sampled []string
	errStop := errors.New("enough samples")
	err := a.cfg.Bucket.Iter(ctx, a.cfg.Prefix+"/", func(name string) error {
		if !strings.HasSuffix(name, ".json.gz") {
			return nil
		}
		sampled = append(sampled, name)
		if len(sampled) >= a.cfg.SampleObjects {
			return errStop
		}
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil && !errors.Is(err, errStop) && !os.IsNotExist(err) {
		return nil, err
	}

	names := map[string]struct{}{}
	for _, l := range a.cfg.IndexLabels {
		names[normalizeLabelName(l)] = struct{}{}
	}
	for _, name := range sampled {
		rows, err := a.readObject(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", name, err)
		}
		for _, r := range rows {
			for k := range r.labels {
				names[k] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(names))
	for n := range names {
		out = append(out, n)
	}
	sort.Strings(out)
	level.Debug(a.cfg.Logger).Log("msg", "archive labels discovered", "objects", len(sampled), "labels", strings.Join(out, ","))
	return out, nil
}

// Plan implements [Source]. The timestamp predicates select five-minute
// partitions; the objects in them are listed and bundled into tickets.
func (a *ArchiveSource) Plan(ctx context.Context, req *scanpb.ScanRequest) ([]*scanpb.Ticket, error) {
	if req.GetTable() != TableArchiveLogs {
		return nil, fmt.Errorf("unknown table %q", req.GetTable())
	}
	lower, upper, err := archiveTimeBounds(req.GetPredicates())
	if err != nil {
		return nil, err
	}

	objects, err := a.listObjects(ctx, lower, upper)
	if err != nil {
		return nil, err
	}

	var tickets []*scanpb.Ticket
	for start := 0; start < len(objects); start += a.cfg.ObjectsPerTicket {
		end := min(start+a.cfg.ObjectsPerTicket, len(objects))
		tickets = append(tickets, &scanpb.Ticket{
			Request:     req,
			ObjectPath:  objects[start],
			ObjectPaths: slices.Clone(objects[start:end]),
		})
	}
	level.Debug(a.cfg.Logger).Log("msg", "archive scan planned", "from", lower.UTC().Format(time.RFC3339), "to", upper.UTC().Format(time.RFC3339), "objects", len(objects), "tickets", len(tickets))
	return tickets, nil
}

// archiveTimeBounds derives the scanned time range from the timestamp
// predicates. A lower bound is mandatory: without one every object under the
// prefix would have to be read. A missing upper bound means "until now".
func archiveTimeBounds(preds []*scanpb.Predicate) (lower, upper time.Time, err error) {
	for _, p := range preds {
		if p.GetColumn() != ColumnTimestamp || len(p.GetValues()) != 1 {
			continue
		}
		ns, ok := literalInt64(p.GetValues()[0])
		if !ok {
			continue
		}
		t := time.Unix(0, ns).UTC()
		switch p.GetOp() {
		case scanpb.Op_OP_GT, scanpb.Op_OP_GTE:
			if lower.IsZero() || t.After(lower) {
				lower = t
			}
		case scanpb.Op_OP_LT, scanpb.Op_OP_LTE:
			if upper.IsZero() || t.Before(upper) {
				upper = t
			}
		case scanpb.Op_OP_EQ:
			lower, upper = t, t
		}
	}
	if lower.IsZero() {
		return lower, upper, fmt.Errorf("%s requires a lower timestamp bound (for example WHERE %s BETWEEN ... AND ...): the archive has no index and would otherwise be read in full", TableArchiveLogs, ColumnTimestamp)
	}
	if upper.IsZero() {
		upper = time.Now().UTC()
	}
	if upper.Before(lower) {
		return lower, upper, fmt.Errorf("empty time range for %s: %s is after %s", TableArchiveLogs, lower, upper)
	}
	return lower, upper, nil
}

// listObjects lists every object whose five-minute partition intersects
// [lower, upper]. Listing is per hour prefix so a day costs about two dozen
// LIST calls; a partition's objects are ordered by their uuidv7 names.
func (a *ArchiveSource) listObjects(ctx context.Context, lower, upper time.Time) ([]string, error) {
	firstBucket := lower.Truncate(archiveBucketDuration)
	var objects []string
	for hour := lower.Truncate(time.Hour); !hour.After(upper); hour = hour.Add(time.Hour) {
		dir := path.Join(a.cfg.Prefix, hour.Format("2006/01/02/15")) + "/"
		err := a.cfg.Bucket.Iter(ctx, dir, func(name string) error {
			if !strings.HasSuffix(name, ".json.gz") {
				return nil
			}
			bucket, ok := archivePartitionTime(a.cfg.Prefix, name)
			if !ok || bucket.Before(firstBucket) || bucket.After(upper) {
				return nil
			}
			objects = append(objects, name)
			return nil
		}, objstore.WithRecursiveIter())
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("listing %s: %w", dir, err)
		}
	}
	sort.Strings(objects)
	return objects, nil
}

// archivePartitionTime parses the YYYY/MM/DD/HH/mm partition of an object key.
func archivePartitionTime(prefix, name string) (time.Time, bool) {
	rel := strings.TrimPrefix(strings.TrimPrefix(name, prefix), "/")
	dir := path.Dir(rel)
	t, err := time.Parse("2006/01/02/15/04", dir)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// Scan implements [Source].
func (a *ArchiveSource) Scan(_ context.Context, tkt *scanpb.Ticket) (Scanner, error) {
	req := tkt.GetRequest()
	if req == nil || req.GetTable() != TableArchiveLogs {
		return nil, fmt.Errorf("ticket is not an %s scan", TableArchiveLogs)
	}
	schema, err := a.schema.Project(req.GetColumns())
	if err != nil {
		return nil, err
	}
	paths := tkt.GetObjectPaths()
	if len(paths) == 0 && tkt.GetObjectPath() != "" {
		paths = []string{tkt.GetObjectPath()}
	}
	return newArchiveScanner(a, schema, paths, req), nil
}
