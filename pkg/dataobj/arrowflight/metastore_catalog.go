package arrowflight

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/loki/v3/pkg/dataobj"
	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight/scanpb"
	"github.com/grafana/loki/v3/pkg/dataobj/metastore"
	"github.com/grafana/loki/v3/pkg/dataobj/sections"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/logs"
)

// MetastoreCatalogConfig configures a [MetastoreCatalog].
type MetastoreCatalogConfig struct {
	// Bucket holds the data objects (already prefixed with the data object
	// root, "dataobj/" in a cell). Metastore indexes the same bucket.
	Bucket    objstore.BucketReader
	Metastore metastore.Metastore

	// Tenant is used when a request carries no tenant (local tooling) and
	// for schema discovery at startup. Requests that carry a tenant in their
	// context (X-Scope-OrgID through Loki's gRPC auth middleware) use it.
	Tenant string

	// SchemaWindow is how far back schema discovery looks for label names and
	// metadata keys. Defaults to 24 hours.
	SchemaWindow time.Duration
	// SampleSections bounds how many recent sections are opened to discover
	// structured metadata keys, which the metastore does not index. Defaults
	// to 8.
	SampleSections int
	// ExtraLabels and ExtraMetadata are always part of the schema, for keys
	// that discovery may miss.
	ExtraLabels   []string
	ExtraMetadata []string

	// PrefetchBytes is passed to dataobj.FromBucket when opening objects.
	PrefetchBytes int64
	// MaxCachedObjects and StreamsCacheBytes bound the object cache; 0 means
	// the package defaults.
	MaxCachedObjects  int
	StreamsCacheBytes int64

	Logger log.Logger
}

func (c *MetastoreCatalogConfig) applyDefaults() {
	if c.SchemaWindow <= 0 {
		c.SchemaWindow = 24 * time.Hour
	}
	if c.SampleSections <= 0 {
		c.SampleSections = 8
	}
	if c.Logger == nil {
		c.Logger = log.NewNopLogger()
	}
}

// MetastoreCatalog serves the logs and streams tables of a cell: it plans
// scans with the metastore (time range and label predicates resolve to data
// object sections and stream IDs) and opens only the objects a scan needs.
// This is the per-request, index-driven counterpart of the walking [Catalog].
type MetastoreCatalog struct {
	*objectCache
	cfg    MetastoreCatalogConfig
	tables map[string]*TableSchema
}

// NewMetastoreCatalog discovers the schema for cfg.Tenant over the schema
// window and returns a catalog ready to serve.
func NewMetastoreCatalog(ctx context.Context, cfg MetastoreCatalogConfig) (*MetastoreCatalog, error) {
	cfg.applyDefaults()
	if cfg.Bucket == nil || cfg.Metastore == nil {
		return nil, errors.New("metastore catalog: bucket and metastore are required")
	}
	c := &MetastoreCatalog{
		objectCache: newObjectCache(cfg.Bucket, cfg.Logger),
		cfg:         cfg,
		tables:      make(map[string]*TableSchema),
	}
	c.prefetchBytes = cfg.PrefetchBytes
	if cfg.MaxCachedObjects > 0 {
		c.maxObjects = cfg.MaxCachedObjects
	}
	if cfg.StreamsCacheBytes > 0 {
		c.maxStreamsBytes = cfg.StreamsCacheBytes
	}

	labelNames, metadataKeys, err := c.discoverSchema(ctx)
	if err != nil {
		return nil, err
	}
	c.tables[TableLogs] = NewLogsSchema(labelNames, metadataKeys)
	c.tables[TableStreams] = NewStreamsSchema(labelNames)
	level.Info(cfg.Logger).Log("msg", "metastore catalog ready", "tenant", cfg.Tenant, "schema_window", cfg.SchemaWindow, "labels", len(labelNames), "metadata_keys", len(metadataKeys))
	return c, nil
}

// discoverSchema asks the metastore for the tenant's label names over the
// window and samples the newest sections for structured metadata keys.
func (c *MetastoreCatalog) discoverSchema(ctx context.Context) ([]string, []string, error) {
	labelSet := map[string]struct{}{}
	metaSet := map[string]struct{}{}
	for _, l := range c.cfg.ExtraLabels {
		labelSet[l] = struct{}{}
	}
	for _, m := range c.cfg.ExtraMetadata {
		metaSet[m] = struct{}{}
	}

	if c.cfg.Tenant == "" {
		level.Warn(c.cfg.Logger).Log("msg", "no tenant configured; the logs schema holds only fixed and extra columns until discovery is possible")
	} else {
		tctx := user.InjectOrgID(ctx, c.cfg.Tenant)
		end := time.Now().UTC()
		start := end.Add(-c.cfg.SchemaWindow)

		names, err := c.cfg.Metastore.Labels(tctx, start, end)
		if err != nil {
			return nil, nil, fmt.Errorf("discovering labels: %w", err)
		}
		for _, n := range names {
			labelSet[n] = struct{}{}
		}

		resp, err := c.cfg.Metastore.Sections(tctx, metastore.SectionsRequest{
			Start:    start,
			End:      end,
			Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, matchAllLabel, matchAllValue)},
		})
		if err != nil {
			return nil, nil, fmt.Errorf("discovering sections: %w", err)
		}
		descs := slices.Clone(resp.Sections)
		sort.Slice(descs, func(i, j int) bool { return descs[i].End.After(descs[j].End) })
		for i, d := range descs {
			if i >= c.cfg.SampleSections {
				break
			}
			_, sec, err := c.logsSection(tctx, d.ObjectPath, int(d.SectionIdx), c.cfg.Tenant)
			if err != nil {
				return nil, nil, fmt.Errorf("sampling %s#%d: %w", d.ObjectPath, d.SectionIdx, err)
			}
			ls, err := logs.Open(tctx, sec)
			if err != nil {
				return nil, nil, fmt.Errorf("opening logs section %s#%d: %w", d.ObjectPath, d.SectionIdx, err)
			}
			for _, col := range ls.Columns() {
				if col.Type == logs.ColumnTypeMetadata {
					metaSet[col.Name] = struct{}{}
				}
			}
		}
		level.Debug(c.cfg.Logger).Log("msg", "schema discovered", "tenant", c.cfg.Tenant, "sections_in_window", len(descs), "sampled", min(len(descs), c.cfg.SampleSections))
	}

	return slices.Sorted(maps.Keys(labelSet)), slices.Sorted(maps.Keys(metaSet)), nil
}

// Tables implements [Source].
func (c *MetastoreCatalog) Tables() []string { return []string{TableLogs, TableStreams} }

// Table implements [Source].
func (c *MetastoreCatalog) Table(name string) (*TableSchema, bool) {
	ts, ok := c.tables[name]
	return ts, ok
}

// tenantContext returns ctx carrying the request's tenant, falling back to
// the configured tenant when the request has none.
func (c *MetastoreCatalog) tenantContext(ctx context.Context) (context.Context, string, error) {
	if id, err := tenant.TenantID(ctx); err == nil && id != "" {
		return ctx, id, nil
	}
	if c.cfg.Tenant == "" {
		return nil, "", errors.New("request carries no tenant and no default tenant is configured")
	}
	return user.InjectOrgID(ctx, c.cfg.Tenant), c.cfg.Tenant, nil
}

// Plan implements [Source]: the time range and label predicates of the scan
// request become a metastore sections request; every section becomes one
// ticket carrying the stream IDs the metastore resolved.
func (c *MetastoreCatalog) Plan(ctx context.Context, req *scanpb.ScanRequest) ([]*scanpb.Ticket, error) {
	ts, ok := c.tables[req.GetTable()]
	if !ok {
		return nil, fmt.Errorf("unknown table %q", req.GetTable())
	}
	ctx, tenantID, err := c.tenantContext(ctx)
	if err != nil {
		return nil, err
	}

	timeColumn := ColumnTimestamp
	if req.GetTable() == TableStreams {
		timeColumn = ColumnMinTimestamp
	}
	lower, upper, err := scanTimeBounds(req.GetTable(), timePredicatesAs(req.GetPredicates(), timeColumn))
	if err != nil {
		return nil, err
	}

	matchers, err := labelMatchers(ts, req.GetPredicates())
	if err != nil {
		return nil, err
	}
	// The metastore requires at least one stream matcher (LogQL always has a
	// selector; SQL often has only a time range). A not-equal matcher on a
	// label no stream carries selects every stream. In that case the stream
	// IDs it resolves are "all of them" and are not worth shipping in tickets.
	selective := len(matchers) > 0
	if !selective {
		matchers = []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, matchAllLabel, matchAllValue)}
	}

	resp, err := c.cfg.Metastore.Sections(ctx, metastore.SectionsRequest{Start: lower, End: upper, Matchers: matchers})
	if err != nil {
		return nil, fmt.Errorf("resolving sections: %w", err)
	}

	var tickets []*scanpb.Ticket
	switch req.GetTable() {
	case TableLogs:
		for _, d := range resp.Sections {
			tkt := &scanpb.Ticket{Request: req, ObjectPath: d.ObjectPath, SectionIndex: int32(d.SectionIdx)}
			if selective {
				tkt.StreamIds = slices.Clone(d.StreamIDs)
			}
			tickets = append(tickets, tkt)
		}
	case TableStreams:
		seen := map[string]struct{}{}
		for _, d := range resp.Sections {
			if _, ok := seen[d.ObjectPath]; ok {
				continue
			}
			seen[d.ObjectPath] = struct{}{}
			// The descriptor names a logs section; the tenant's streams
			// section of the same object is resolved at scan time.
			tickets = append(tickets, &scanpb.Ticket{Request: req, ObjectPath: d.ObjectPath, SectionIndex: -1})
		}
	}
	level.Debug(c.cfg.Logger).Log("msg", "metastore scan planned", "tenant", tenantID, "table", req.GetTable(), "from", lower.Format(time.RFC3339), "to", upper.Format(time.RFC3339), "matchers", matchersString(matchers), "sections", len(resp.Sections), "tickets", len(tickets))
	return tickets, nil
}

// Scan implements [Source].
func (c *MetastoreCatalog) Scan(ctx context.Context, tkt *scanpb.Ticket) (Scanner, error) {
	req := tkt.GetRequest()
	if req == nil {
		return nil, errors.New("ticket has no scan request")
	}
	ts, ok := c.tables[req.GetTable()]
	if !ok {
		return nil, fmt.Errorf("unknown table %q", req.GetTable())
	}
	ctx, tenantID, err := c.tenantContext(ctx)
	if err != nil {
		return nil, err
	}
	schema, err := ts.Project(req.GetColumns())
	if err != nil {
		return nil, err
	}

	switch req.GetTable() {
	case TableLogs:
		info, sec, err := c.logsSection(ctx, tkt.GetObjectPath(), int(tkt.GetSectionIndex()), tenantID)
		if err != nil {
			return nil, err
		}
		if ids := tkt.GetStreamIds(); len(ids) > 0 {
			// The metastore already resolved the label predicates to stream
			// IDs; push them as a membership predicate on the stream ID column.
			lits := make([]*scanpb.Literal, 0, len(ids))
			for _, id := range ids {
				lits = append(lits, &scanpb.Literal{Value: &scanpb.Literal_Int64Value{Int64Value: id}})
			}
			scoped := proto.Clone(req).(*scanpb.ScanRequest)
			scoped.Predicates = append(scoped.Predicates, &scanpb.Predicate{Column: ColumnStreamID, Op: scanpb.Op_OP_IN, Values: lits})
			req = scoped
		}
		return c.newLogsScanner(ctx, ts, schema, info, sec, req)

	case TableStreams:
		info, err := c.object(ctx, tkt.GetObjectPath())
		if err != nil {
			return nil, err
		}
		set, err := sections.ForTenant(info.Object.Sections(), tenantID)
		if err != nil {
			return nil, fmt.Errorf("data object %s: %w", tkt.GetObjectPath(), err)
		}
		if set.Streams == nil {
			return nil, fmt.Errorf("data object %s has no streams section for tenant %q", tkt.GetObjectPath(), tenantID)
		}
		return newStreamsScanner(ctx, ts, schema, set.Streams, req)
	}
	return nil, fmt.Errorf("unknown table %q", req.GetTable())
}

// logsSection resolves a metastore section index, which counts logs sections
// across the whole object (the convention the engine uses through
// sections.ForTenant), to the tenant's logs section.
func (c *MetastoreCatalog) logsSection(ctx context.Context, path string, logsIndex int, tenantID string) (*objectInfo, *dataobj.Section, error) {
	info, err := c.object(ctx, path)
	if err != nil {
		return nil, nil, err
	}
	set, err := sections.ForTenant(info.Object.Sections(), tenantID)
	if err != nil {
		return nil, nil, fmt.Errorf("data object %s: %w", path, err)
	}
	sec, ok := set.Logs[logsIndex]
	if !ok {
		return nil, nil, fmt.Errorf("data object %s has no logs section %d for tenant %q", path, logsIndex, tenantID)
	}
	return info, sec, nil
}

// matchAllLabel is a label name no stream carries; `!= matchAllValue` on it
// is the metastore's way of saying "every stream in the time range".
const (
	matchAllLabel = "__dataobj_flight_match_all__"
	matchAllValue = "none"
)

// timePredicatesAs rewrites predicates on column to the logs timestamp
// column name so scanTimeBounds can read them.
func timePredicatesAs(preds []*scanpb.Predicate, column string) []*scanpb.Predicate {
	if column == ColumnTimestamp {
		return preds
	}
	out := make([]*scanpb.Predicate, 0, len(preds))
	for _, p := range preds {
		if p.GetColumn() == column {
			cp := proto.Clone(p).(*scanpb.Predicate)
			cp.Column = ColumnTimestamp
			out = append(out, cp)
			continue
		}
		out = append(out, p)
	}
	return out
}

// labelMatchers turns pushed label equality and membership predicates into
// Prometheus matchers for the metastore. Other operators are left to the
// scanner.
func labelMatchers(ts *TableSchema, preds []*scanpb.Predicate) ([]*labels.Matcher, error) {
	var matchers []*labels.Matcher
	for _, p := range preds {
		b, ok := ts.binding(p.GetColumn())
		if !ok || b.Kind != columnKindLabel {
			continue
		}
		var values []string
		for _, l := range p.GetValues() {
			if s, ok := literalString(l); ok {
				values = append(values, s)
			}
		}
		if len(values) == 0 {
			continue
		}
		switch p.GetOp() {
		case scanpb.Op_OP_EQ:
			m, err := labels.NewMatcher(labels.MatchEqual, b.Source, values[0])
			if err != nil {
				return nil, err
			}
			matchers = append(matchers, m)
		case scanpb.Op_OP_IN:
			quoted := make([]string, len(values))
			for i, v := range values {
				quoted[i] = regexp.QuoteMeta(v)
			}
			m, err := labels.NewMatcher(labels.MatchRegexp, b.Source, strings.Join(quoted, "|"))
			if err != nil {
				return nil, err
			}
			matchers = append(matchers, m)
		}
	}
	return matchers, nil
}

func matchersString(ms []*labels.Matcher) string {
	parts := make([]string, len(ms))
	for i, m := range ms {
		parts[i] = m.String()
	}
	return "{" + strings.Join(parts, ", ") + "}"
}
