package arrowflight

import (
	"context"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight/scanpb"
)

// Source serves one or more tables over the scan protocol. The data object
// [Catalog] and the [ArchiveSource] are the two implementations; a [Server]
// routes each call to the source that owns the requested table.
type Source interface {
	// Tables returns the names of the served tables in a stable order.
	Tables() []string
	// Table returns the schema of the named table.
	Table(name string) (*TableSchema, bool)
	// Plan splits a scan request into independently scannable tickets, one
	// per Flight endpoint. Each ticket carries the request.
	Plan(ctx context.Context, req *scanpb.ScanRequest) ([]*scanpb.Ticket, error)
	// Scan opens a Scanner for a ticket produced by Plan.
	Scan(ctx context.Context, tkt *scanpb.Ticket) (Scanner, error)
}

// Plan implements [Source]: one ticket per data object section.
func (c *Catalog) Plan(_ context.Context, req *scanpb.ScanRequest) ([]*scanpb.Ticket, error) {
	parts, err := c.Partitions(req.GetTable())
	if err != nil {
		return nil, err
	}
	tickets := make([]*scanpb.Ticket, 0, len(parts))
	for _, p := range parts {
		tickets = append(tickets, &scanpb.Ticket{
			Request:      req,
			ObjectPath:   p.ObjectPath,
			SectionIndex: int32(p.SectionIndex),
		})
	}
	return tickets, nil
}
